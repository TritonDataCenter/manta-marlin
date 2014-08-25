/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * lib/agent/lackey.js: per-zone lackey (formerly known as the in-zone agent),
 * responsible for running and monitoring compute zone jobs and servicing HTTP
 * requests from the user.
 */

/*
 * This service runs inside a Marlin compute zone and manages task execution
 * within the zone.  We fetch work, report status, and heartbeat by making HTTP
 * requests to the global zone agent, which listens on a Unix Domain Socket in
 * /var/run.  Being HTTP, we only have a "pull" interface.  The protocol works
 * like this:
 *
 *   o When we're ready to start executing a task, we make a long poll request
 *     asking for work.  When work (i.e. a task) is available, the GZ agent
 *     returns a response describing the currently pending task.
 *
 *     For map tasks, the task description includes everything needed to
 *     complete the task, including the single input key.  For reduce tasks, the
 *     task description includes information about the first N keys and whether
 *     there may be more keys later.
 *
 *   o For reduce tasks only: we need to stream input keys to the user program,
 *     but without ever having to process the entire list of keys at once.  When
 *     the user program has consumed the first N keys, we indicate that to the
 *     GZ agent, which then removes these keys from the queue.  We then request
 *     task information again and get a new response with the next N input keys
 *     and whether there may be any more keys.  This is a long poll because it
 *     may be some time before the next set of keys are available.
 *
 *   o All requests to Manta, initiated either from ourselves or from the user
 *     over the HTTP server we run on localhost, are proxied through the global
 *     zone agent, which automatically injects authentication headers so that
 *     neither we nor the user code needs to worry about private keys.  The
 *     agent keeps track of objects created during task execution so that output
 *     can be propagated to subsequent job phases or reported back to the user
 *     as job output.
 *
 *   o When we finish processing the task, we report either success or failure.
 *     The GZ agent records the result and moves on to the next task.
 */

var mod_assert = require('assert');
var mod_child = require('child_process');
var mod_contract = require('illumos_contract');
var mod_events = require('events');
var mod_fs = require('fs');
var mod_http = require('http');
var mod_jsprim = require('jsprim');
var mod_os = require('os');
var mod_path = require('path');
var mod_util = require('util');

var mod_bunyan = require('bunyan');
var CatStreams = require('catstreams');
var FuzzStream = require('fuzzstream');
var HttpStream = require('httpstream');
var mod_mantacbin = require('manta-compute-bin');
var mod_panic = require('panic');
var mod_restify = require('restify');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_manta = require('manta');
var mod_mautil = require('../util');

var EventEmitter = mod_events.EventEmitter;
var Executor = require('./executor');
var VError = mod_verror.VError;

/* jsl:import ../../../common/lib/errors.js */
require('../errors');

/*
 * Configuration
 */
var mazServerName = 'MarlinLackey';		/* restify server name */
var mazPort = 80;				/* restify server port */
var mazRetryTime = 1 * 1000;			/* between GZ agent requests */
var mazInitStarted = '/var/tmp/.marlin_init_started';	/* disk state for */
var mazInitDone = '/var/tmp/.marlin_init_done';		/* "init" script  */
var mazInitHasRun = false;
var mazInitUndefined = false;
var mazTaskStarted = '/var/tmp/.marlin_task_started';
var mazOutputDir = '/var/tmp/marlin_task';	/* user command output dir */
var mazCoreDir = '/var/tmp/core';		/* core file directory */
var mazCorePattern = 'mrcore.%f.%p';		/* core file pattern */
var mazTaskStdoutFile = mod_path.join(mazOutputDir, 'stdout');
var mazTaskStderrFile = mod_path.join(mazOutputDir, 'stderr');
var mazTaskLivenessInterval = 15 * 1000;
var mazHttpPerRequestBuffer = 1024 * 1024;
var mazHttpConcurrency = 16;
var mazHttpSockets = mazHttpConcurrency;
var mazContractTemplate = {
	'type': 'process',
	'critical': {
	    'pr_empty': true,
	    'pr_hwerr': true,
	    'pr_core': true
	},
	'fatal': {
	    'pr_hwerr': true,
	    'pr_core': true
	},
	'param': {
		/*
		 * For now, we cause child processes to be killed if we
		 * ourselves die.  If we add a mechanism for persisting state,
		 * we could remove this flag and re-adopt our contracts upon
		 * restart.
		 */
		'noorphan': true
	}
};
var mazReduceRetryPolicy = {
    'retries': 4,
    'minTimeout': 1000,
    'maxTimeout': 8000
};
var mazFuzzInput = false;	/* only true in development! */
var mazUdsPath;			/* path to Unix Domain Socket to parent agent */
var mazTaskPath;		/* task environment PATH */

/*
 * Helper objects
 */
var mazLog;			/* logger */
var mazClient;			/* HTTP client for parent agent */
var mazMantaClient;		/* Manta client */
var mazServer;			/* HTTP server for in-zone API */

/*
 * Lackey state
 */
var mazLivenessPending;		/* liveness request pending */
var mazLivenessTimeout;		/* liveness timeout, if any */
var mazRetryTimeout;		/* pending retry timeout, if any */
var mazClientRequests;		/* pending client requests, by request id */
var mazServerRequests;		/* pending server requests, by request id */

/*
 * Task execution state
 */
var mazPipeline;		/* current task pipeline */
var mazCurrentTask;		/* current task */
var mazErrorOpen;		/* error opening input file */
var mazErrorFetch;		/* error fetching task input */
var mazErrorSave;		/* error during task processing */
var mazExecutor;		/* task executor */
var mazExecutorResult;		/* task execution result */
var mazUserEmitted;		/* whether the user has emitted output */

function usage(errmsg)
{
	if (errmsg)
		console.error(errmsg);

	console.error('usage: node lackey.js socket_path');
	process.exit(2);
}

function main()
{
	mazLog = new mod_bunyan({
	    'name': mazServerName,
	    'level': 'debug'
	});

	if (process.argv.length < 3)
		usage();

	mod_panic.enablePanicOnCrash({
	    'skipDump': true,
	    'abortOnPanic': true
	});

	/*
	 * Set up connection to our parent, the global zone agent.
	 */
	mazUdsPath = process.argv[2];
	mazLog.info('using client url "%s"', mazUdsPath);

	mazClient = mod_restify.createJsonClient({
	    'socketPath': mazUdsPath,
	    'log': mazLog,
	    'retry': false
	});
	mazClient.agent = false;
	mazClientRequests = {};

	mazMantaClient = mod_manta.createClient({
	    'socketPath': mazUdsPath,
	    'log': mazLog,
	    'retry': false,
	    'sign': mod_mautil.mantaSignNull
	});

	/*
	 * Set up the process contract template.
	 */
	mod_contract.set_template(mazContractTemplate);

	/*
	 * Construct default path.
	 */
	var base = mod_path.normalize(mod_path.join(__dirname, '../..'));
	mazTaskPath = [
	    mod_path.join(base, 'ubin'),
	    '/opt/local/bin',
	    '/opt/local/sbin',
	    process.env['PATH']
	].join(':');

	/*
	 * Set up restify server for the in-zone Task Control API.
	 */
	mazServer = mod_restify.createServer({
	    'name': mazServerName,
	    'noWriteContinue': true,
	    'log': mazLog.child({
		'component': 'LackeyHttpServer',
		'serializers': mod_restify.bunyan.serializers
	    }),
	    'version': ['1.0.0']
	});

	mod_http.globalAgent.maxSockets = mazHttpSockets;

	mazServerRequests = {};
	mazServer.pre(mod_restify.pre.sanitizePath());
	mazServer.use(mod_restify.requestLogger());
	mazServer.use(function trackRequest(request, response, next) {
		mazServerRequests[request['id']] = request;
		next();
	});
	mazServer.use(mod_restify.queryParser());

	mazServer.on('uncaughtException', mod_mautil.maRestifyPanic);
	mazServer.on('after', mod_restify.auditLogger({ 'log': mazLog }));
	mazServer.on('after', function (request, response) {
		delete (mazServerRequests[request['id']]);
	});
	mazServer.on('error', function (err) {
		mazLog.fatal(err, 'failed to start server: %s', err.mesage);
		process.exit(1);
	});

	/*
	 * All incoming requests are just proxied directly to our parent agent.
	 */
	/* BEGIN JSSTYLED */
	mazServer.get(/.*/, mazApiProxy);
	mazServer.put(/.*/, mazApiProxy);
	mazServer.post(/.*/, mazApiProxy);
	mazServer.del(/.*/, mazApiProxy);
	mazServer.head(/.*/, mazApiProxy);
	/* END JSSTYLED */

	/*
	 * Figure out whether we've executed the zone initialization script yet.
	 * There are three possibilities:
	 *
	 *   - the "done" file exists, in which we don't need to do anything
	 *
	 *   - the "running" file exists, in which case we crashed while the
	 *     script was running, and the zone is in an undefined state
	 *
	 *   - neither exists, in which case we just need to make sure to run
	 *     the "init" script when we pick up our first task
	 */
	if (mod_fs.existsSync(mazInitStarted))
		mazInitUndefined = true;
	else if (mod_fs.existsSync(mazInitDone))
		mazInitHasRun = true;

	/*
	 * Create our output directory, start up the in-zone HTTP server, then
	 * ask our parent for work to do.
	 */
	mod_vasync.pipeline({
	    'funcs': [
	        function cleanupCores(_, callback) {
			mazCleanupCores(callback);
		},
	        function mkOutputDir(_, callback) {
			mod_fs.mkdir(mazOutputDir, function (err) {
				if (err && err['code'] == 'EEXIST')
					err = null;
				callback(err);
			});
		},
		function mkCoreDir(_, callback) {
			mod_fs.mkdir(mazCoreDir, function (err) {
				if (err && err['code'] == 'EEXIST')
					err = null;
				callback(err);
			});
		},
		function runCoreadm(_, callback) {
			mod_child.execFile('coreadm', [ '-p',
			    mod_path.join(mazCoreDir, mazCorePattern),
			    process.pid ],
			    function (err, stdout, stderr) {
				if (err)
					err = new VError(err,
					    'coreadm failed: stderr = "%s"',
					    stderr);
				callback(err);
			    });
		},
		function startListening(_, callback) {
			mazServer.listen(mazPort, function () {
				mazLog.info('server listening on port %d',
				    mazPort);
				callback();
			});
		}
	    ]
	}, function (err) {
		if (err) {
			mazLog.fatal(err,
			    'initialization failed (dumping core)');
			process.abort();
		}

		mazTaskFetch();
	});
}

/*
 * Remove any core files created by ourselves or a user task.  We do this on
 * startup because we don't handle crashing properly as it is, and we don't want
 * to erroneously upload the wrong core file (or a core file from ourselves!) as
 * part of a task.  Global zone core files will still be created for ourselves.
 */
function mazCleanupCores(callback)
{
	mod_fs.readdir(mazCoreDir, function (err, entries) {
		if (err) {
			if (err['code'] == 'ENOENT')
				err = null;
			callback(err);
			return;
		}

		entries.forEach(function (entry) {
			var path = mod_path.join(mazCoreDir, entry);
			mazLog.info('removing %s', path);
			mod_fs.unlinkSync(path);
		});

		callback();
	});
}

/*
 * Asks our parent (the global zone agent) for work to do.  This is a long poll,
 * and it may well be a long time before there's anything for us to do.  If the
 * request fails for any reason, we simply try again later, using a timeout to
 * avoid running away in the event of a persistent failure of our parent.
 */
function mazTaskFetch()
{
	mazRetryTimeout = undefined;

	mazParentRequest('GET /my/jobs/task/task?wait=true', 200,
	    function (rqcallback) {
		mazClient.get('/my/jobs/task/task?wait=true', rqcallback);
	    }, function (err, task) {
		/*
		 * The only error that could be returned here is a 40x
		 * indicating a bad request, which would indicate a bug.
		 */
		mod_assert.ok(!err);
		mod_assert.ok(mazPipeline === undefined);
		mod_assert.ok(mazCurrentTask === undefined);
		mod_assert.ok(mazExecutor === undefined);
		mod_assert.ok(mazExecutorResult === undefined);
		mod_assert.ok(mazErrorOpen === undefined);
		mod_assert.ok(mazErrorFetch === undefined);
		mod_assert.ok(mazErrorSave === undefined);

		mazCurrentTask = task;
		mazUserEmitted = false;

		mazLivenessTimeout = setInterval(
		    mazTaskLivenessTick, mazTaskLivenessInterval);
		mazPipeline = mod_vasync.pipeline({
		    'arg': task,
		    'funcs': mazTaskStages
		}, function (suberr) {
			clearInterval(mazLivenessTimeout);
			mod_assert.ok(mazPipeline !== undefined);
			mazPipeline = undefined;
			mazTaskReport(suberr);
		});
	    });
}

/*
 * The only one of these pipeline stages that can fail is the "cleanup" stage
 * (which should never fail in practice).  The others always emit success so
 * that subsequent stages will always run.
 */
var mazTaskStages = [
	mazTaskRunCleanup,
	mazTaskRunInit,
	mazTaskRunNote,
	mazTaskRunSpawn,
	mazTaskRunSaveStdout,
	mazTaskRunSaveStderr,
	mazTaskRunSaveCores
];

function mazTaskRunCleanup(task, callback)
{
	/*
	 * We buffer each task's stdout and stderr so that we can upload it to
	 * Manta when the task completes.  Since these files can be very large,
	 * we want to remove them between tasks.
	 */
	mod_vasync.forEachParallel({
	    'func': mod_mautil.maWrapIgnoreError(mod_fs.unlink, [ 'ENOENT' ]),
	    'inputs': [ mazTaskStdoutFile, mazTaskStderrFile ]
	}, callback);
}

function mazTaskRunInit(task, callback)
{
	mazLog.info('picking up task', task);

	if (mazInitUndefined) {
		mazLog.error('lackey crashed during zone init');
		callback(new Error('lackey crashed during zone init'));
		return;
	}

	if (!task['taskPhase'].hasOwnProperty('init'))
		mazInitHasRun = true;

	if (mazInitHasRun) {
		mazLog.info('init script non-existent or already run');
		callback();
		return;
	}

	mazLog.info('running init script');
	mod_fs.writeFileSync(mazInitStarted, '');
	mazExecutor = new MarlinExecutor(task, true, mazLog);
	mazExecutor.start();
	mazExecutor.on('done', function (result) {
		/*
		 * If we succeeded, just rename our sentinel file so that if we
		 * crash, we know we don't have to try to run the task again.
		 * If we failed, we'll report the error (which will cause our
		 * parent to reset this zone, since we'll be in an undefined
		 * state).
		 */
		if (result['code'] === 0) {
			mazLog.info('init script ran okay');
			mazInitHasRun = true;
			mod_fs.renameSync(mazInitStarted, mazInitDone);
		} else {
			mazLog.error('init script failed', result);
			mazExecutorResult = result;
		}

		callback();
	});
}

function mazTaskRunNote(task, callback)
{
	/*
	 * Check whether we've already tried to execute this task.  If so, fail
	 * now, since we're in an undefined state.  If not, save a file to
	 * indicate that we now have started it.
	 */
	mod_vasync.pipeline({
	    'funcs': [
		function noteCheck(_, subcb) {
			mod_fs.readFile(mazTaskStarted, function (err, buf) {
				if (err && err['code'] == 'ENOENT') {
					subcb();
					return;
				}

				if (buf.toString('utf8') == task['taskId']) {
					subcb(new VError('lackey crashed'));
					return;
				}

				subcb(err);
			});
		},
		function noteSave(_, subcb) {
			/*
			 * This doesn't need to be synchronous (fsync'd) because
			 * if the system crashes, we won't be asked to process
			 * the same task again.  We also don't need to worry
			 * about writing a half-corrupt file: in that case, it
			 * won't match the same taskId when we come up, which is
			 * correct because we didn't actually start executing
			 * that task yet.
			 */
			mod_fs.writeFile(mazTaskStarted, task['taskId'], subcb);
		}
	    ]
	}, callback);
}

function mazTaskRunSpawn(task, callback)
{
	if (mazExecutorResult !== undefined) {
		mazLog.info('skipping execution because init failed');
		callback();
		return;
	}

	mod_assert.ok(mazInitHasRun);
	mazExecutor = new MarlinExecutor(task, false, mazLog);
	mazExecutor.start();

	if (task['taskPhase']['type'] == 'reduce')
		mazExecutor.on('drain', mazDoneBatch);

	mazExecutor.on('done', function (result) {
		mazExecutorResult = result;
		callback();
	});
}

/*
 * Invoked when we've finished processing a batch of reduce keys to report that
 * we've processed these keys and to fetch the next batch.
 */
function mazDoneBatch(batch)
{
	mazLog.info('finished batch', batch);

	if (batch['taskInputKeys'].length === 0 ||
	    mazExecutorResult !== undefined)
		/* Nothing more to do. */
		return;

	var arg = {
	    'key': batch['taskInputKeys'][0],
	    'nkeys': batch['taskInputKeys'].length
	};

	mazParentRequest('POST /my/jobs/task/commit', 204,
	    function (rqcallback) {
		mazClient.post('/my/jobs/task/commit', arg, rqcallback);
	    }, function (err) {
		if (err) {
			mazLog.fatal(err, 'fatal error updating reduce state');
			process.exit(1);
		}

		if (batch['taskInputDone'] || mazCurrentTask === undefined)
			return;

		mazParentRequest('GET /my/jobs/task/task?wait=true', 200,
		    function (subrequestcallback) {
			mazClient.get('/my/jobs/task/task?wait=true',
			    subrequestcallback);
		    }, function (suberr, task) {
			if (suberr) {
				mazLog.fatal(suberr, 'fatal error on batching');
				process.exit(1);
			}

			mazLog.info('next batch', task);
			mod_assert.equal(mazCurrentTask['taskId'],
			    task['taskId']);
			mazCurrentTask = task;

			var nextbatch = {
			    'taskInputKeys': task['taskInputKeys'],
			    'taskInputDone': task['taskInputDone']
			};

			mazExecutor.batch(nextbatch);
		});
	    });
}

function mazTaskRunSaveStdout(task, callback)
{
	if (!mazInitHasRun) {
		mazLog.info('skipping stdout capture for init task');
		callback();
		return;
	}

	if (mazUserEmitted) {
		mazLog.info('skipping stdout capture because the user task ' +
		    'already emitted output');
		callback();
		return;
	}

	mazFileSave('stdout', mazTaskStdoutFile,
	    mazCurrentTask['taskOutputKey'], callback);
}

function mazTaskRunSaveStderr(task, callback)
{
	if (mazExecutorResult !== undefined &&
	    mazExecutorResult['code'] === 0) {
		mazLog.info('skipping stderr capture because result is ok');
		callback();
		return;
	}

	mazFileSave('stderr', mazTaskStderrFile,
	    mazCurrentTask['taskErrorKey'], callback);
}

function mazTaskRunSaveCores(task, callback)
{
	if (mazExecutorResult === undefined) {
		mazLog.info('skipping core capture because no result found');
		callback();
		return;
	}

	if (!mazExecutorResult['core']) {
		mazLog.info('skipping core capture because no core dumped');
		callback();
		return;
	}

	mod_fs.readdir(mazCoreDir, function (err, entries) {
		if (err) {
			mazLog.warn(err, 'failed to list core directory');
			callback();
			return;
		}

		if (entries.length === 0) {
			mazLog.warn('expected core file, but found none');
			callback();
			return;
		}

		mazLog.info('found core files', entries);

		var dot1 = entries[0].indexOf('.');
		var dot2 = entries[0].lastIndexOf('.');

		if (dot1 == -1 || dot2 == -1 || dot1 == dot2) {
			mazLog.warn('core file name was bogus: ', entries[0]);
			callback();
			return;
		}

		var base = entries[0].substr(dot1 + 1, dot2 - dot1 - 1);
		var file = mod_path.join(mazCoreDir, entries[0]);
		var objname = mazCurrentTask['taskCoreBase'] + 'core.' +
		    base + '.' + mod_uuid.v4();
		mazFileSave('core', file, objname, function () {
			mod_fs.unlink(file, function (err2) {
				mod_assert.ok(!err2);
				callback();
			});
		});
	});
}

function mazFileSave(name, filename, key, callback)
{
	mazLog.info('capturing %s from %s', name, filename);

	mod_mantacbin.mantaFileSave({
	    'client': mazMantaClient,
	    'filename': filename,
	    'key': key,
	    'log': mazLog,
	    'iostream': name
	}, function (err) {
		if (err) {
			err = new VError(err, 'error saving %s', name);
			mazLog.error(err);

			if (!mazErrorSave)
				mazErrorSave = err;
		}

		callback();
	});
}

function mazTaskReport(err)
{
	var arg, error, uri;

	arg = {};
	if (mazCurrentTask['taskPhase']['type'] != 'reduce') {
		mod_assert.equal(mazCurrentTask['taskInputKeys'].length, 1);
		arg['key'] = mazCurrentTask['taskInputKeys'][0];
	}

	error = mazDecodeError(err);
	if (error !== null) {
		uri = '/my/jobs/task/fail';
		arg['error'] = error;
		mazLog.info('reporting task failure', arg);
	} else {
		uri = '/my/jobs/task/commit';
		mazLog.info('reporting task success', arg);
	}

	mazParentRequest('POST ' + uri, 204, function (rqcallback) {
		mazClient.post(uri, arg, rqcallback);
	}, function (e) {
		/*
		 * The only error we can see here is a 409 indicating that this
		 * task has already been failed or committed.  That means that
		 * the user explicitly updated the task state already, and their
		 * explicit update takes precedence.  (If they committed and we
		 * thought they failed, we trust their indication that they
		 * successfully processed the input before failing.  If they
		 * failed but we thought they exited with success, we trust that
		 * they really did fail.)
		 */
		if (e)
			mazLog.error(e, 'ignored error reporting task status');
		else
			mazLog.info('reported task status ok');
		mazCurrentTask = undefined;
		mazExecutor = undefined;
		mazExecutorResult = undefined;
		mazErrorOpen = undefined;
		mazErrorFetch = undefined;
		mazErrorSave = undefined;
		mazTaskFetch();
	});
}

/*
 * Determine if any error occurred during the execution of this task, and report
 * the first or most important one.  Multiple errors may occur, in which case we
 * report the first one of the following that we saw:
 *
 *     o internal failure to process the execution pipeline itself
 *     o internal failure to open the input file
 *     o internal failure to fork/exec the user process
 *     o internal failure to fetch reduce task input
 *     o user process dumped core
 *     o user process killed by signal
 *     o user process exited with non-zero status
 *     o failed to save stdout
 */
function mazDecodeError(pipelineError)
{
	var err = pipelineError || mazErrorOpen || mazExecutorResult['error'];
	var code = mazInitHasRun ? EM_USERTASK : EM_TASKINIT;

	if (err) {
		return ({
		    'code': EM_INTERNAL,
		    'message': 'internal error',
		    'messageInternal': err.message
		});
	}

	if (mazErrorFetch) {
		return ({
		    'code': EM_SERVICEUNAVAILABLE,
		    'message': mazErrorFetch.message
		});
	}

	if (mazExecutorResult['core']) {
		return ({
		    'code': code,
		    'message': 'user command or child process dumped core'
		});
	}

	if (mazExecutorResult['signal'] !== null) {
		return ({
		    'code': code,
		    'message': 'user command terminated by ' +
		        mazExecutorResult['signal']
		});
	}

	if (mazExecutorResult['code'] !== 0) {
		return ({
		    'code': code,
		    'message': 'user command exited with code ' +
		        mazExecutorResult['code']
		});
	}

	if (mazErrorSave) {
		return ({
		    'code': EM_INTERNAL,
		    'message': 'internal error',
		    'messageInternal': mazErrorSave.message
		});
	}

	return (null);
}

function mazTaskLivenessTick()
{
	if (mazLivenessPending)
		return;

	mazLog.debug('attempting to report liveness');
	mazLivenessPending = new Date();
	mazParentRequest('POST /my/jobs/task/live', undefined,
	    function (rqcallback) {
		mazLivenessPending = undefined;
		mazClient.post('/my/jobs/task/live', rqcallback);
	    }, function (err) {
		if (err)
			mazLog.warn(err, 'failed to report liveness');
	    });
}

/*
 * Make a request to our parent agent and retry until either it succeeds or
 * fails with a 40x error.  This is used for cases where we cannot proceed
 * without the parent's response (e.g., fetching the next task, or committing
 * the result of a task).
 */
function mazParentRequest(opname, expected_code, makerequest, callback,
    attempt)
{
	var uuid = mod_uuid.v4();

	if (!attempt)
		attempt = 1;

	mazClientRequests[uuid] = {
	    'time': new Date(),
	    'opname': opname,
	    'attempt': attempt
	};

	mazLog.debug('parent request start', opname, attempt);
	makerequest(function (err, request, response, result) {
		delete (mazClientRequests[uuid]);

		if (!err) {
			if (response.statusCode == expected_code ||
			    (expected_code === undefined &&
			    response.statusCode >= 200 &&
			    response.statusCode < 400)) {
				mazLog.debug('parent request okay',
				    opname, attempt);
				callback(null, result);
				return;
			}

			mazLog.debug('parent request %s: expected %s ' +
			    'status, got %s (body: %s)', opname,
			    expected_code || 'ok', response.statusCode,
			    response.body || '<unknown>');
		}

		if (response && response.statusCode >= 400 &&
		    response.statusCode != 410 &&
		    response.statusCode < 500) {
			/*
			 * We're pretty hosed if we got a 40x error.  It means
			 * we did something wrong, but we obviously don't know
			 * what.
			 */
			mazLog.error('parent request %s returned %s: %s',
			    opname, response.statusCode, response.body);
			callback(err);
			return;
		}

		mazLog.warn(err || '', opname + ' failed; will retry in ' +
		    mazRetryTime);

		mazRetryTimeout = setTimeout(function () {
			mazParentRequest(opname, expected_code, makerequest,
			    callback, attempt + 1);
		}, mazRetryTime);
	});
}

/*
 * All incoming requests are just proxied to our parent agent.
 */
function mazApiProxy(request, response, next)
{
	var uuid = request.id();

	if (request.method.toLowerCase() == 'put' &&
	    !mod_jsprim.startsWith(request.url, '/my/jobs/task/') &&
	    request.headers['x-manta-stream'] == 'stdout')
		mazUserEmitted = true;

	var op = mod_mautil.maHttpProxy({
	    'request': request,
	    'response': response,
	    'server': {
	        'socketPath': mazUdsPath
	    }
	}, function (err) {
		delete (mazClientRequests[uuid]);
		next();
	});

	mazClientRequests[uuid] = { 'time': new Date(), 'op': op };
}


/*
 * The Marlin Executor adds additional semantics on top of the basic shell
 * executor: specifically, it's an object with the same interface as Executor,
 * but it constructs the lower-level Executor parameters based on Marlin-level
 * parameters:
 *
 *     jobId		current job id
 *
 *     rIdx		current reducer index (reduce only)
 *     (reduce only)
 *
 *     taskPhase	see job phase Moray object specification
 *
 *     taskInputKeys	list of input objects to process
 *
 *     taskInputFile	filename for first (and only) input object
 *     (locally-mapped only)
 *
 *     taskInputDone	if true, then there will be no further keys to process
 *     (reduce only)
 *
 *     taskInputRemote	remote URL from which to fetch file
 *     (remote only)
 *
 *     taskOutputBase	suggested basename for output objects
 *
 *     login		suggested login name
 *
 * Consumers use the following methods:
 *
 *     start()		begin execution
 *
 *     batch(...)	insert a new batch of keys according to the given
 *     			parameters (taskInputKeys, taskInputDone)
 *     			(only allowed for remote reduce tasks)
 *
 * and get the following events:
 *
 *     'done'		See Executor's "done" event
 *
 *     'drain'		Emitted when all known objects have been fed to the
 *     			current task.  (This doesn't mean that the user's
 *     			process has read or processed these objects.)  The
 *     			consumer should fetch the next set of objects and invoke
 *     			batch(), which will either add new objects or end the
 *     			input stream.
 */
function MarlinExecutor(args, isInit, log)
{
	var object;

	this.mex_args = args;
	this.mex_log = log;
	this.mex_isinit = isInit;
	this.mex_reduce = !isInit && args['taskPhase']['type'] == 'reduce';
	this.mex_env = {
	    'HOME': '/root',
	    'PATH': mazTaskPath,
	    'MANTA_OUTPUT_BASE': args['taskOutputBase'],
	    'MANTA_NO_AUTH': 'true',
	    'MANTA_URL': 'http://localhost:80/',
	    'MANTA_JOB_ID': args['jobId'],
	    'MANTA_USER': args['login']
	};
	this.mex_infd = undefined;
	this.mex_instream = undefined;
	this.mex_exec = undefined;
	this.mex_batch = undefined;
	this.mex_barrier = undefined;
	this.mex_cache = undefined;
	this.mex_done = false;

	if (this.mex_reduce) {
		this.mex_env['MANTA_REDUCER'] = args['rIdx'];
	} else if (!isInit) {
		mod_assert.equal(args['taskInputKeys'].length, 1);
		object = args['taskInputKeys'][0];
		this.mex_env['mc_input_key'] = object;
		this.mex_env['MANTA_INPUT_OBJECT'] = object;
	}

	if (!isInit && args['taskInputFile'] !== undefined) {
		this.mex_env['mc_input_file'] = args['taskInputFile'];
		this.mex_env['MANTA_INPUT_FILE'] = args['taskInputFile'];
	}

	EventEmitter.call(this);
}

mod_util.inherits(MarlinExecutor, EventEmitter);

MarlinExecutor.prototype.start = function ()
{
	var executor = this;
	var barrier, outstream, errstream, filename, instream;
	var log;

	barrier = this.mex_barrier = mod_vasync.barrier();

	/* Set up stdout and stderr. */
	barrier.start('open stdout');
	outstream = mod_fs.createWriteStream(
	    this.mex_isinit ? '/dev/null' : mazTaskStdoutFile);
	outstream.on('open', function () { barrier.done('open stdout'); });

	barrier.start('open stderr');
	errstream = mod_fs.createWriteStream(mazTaskStderrFile);
	errstream.on('open', function () { barrier.done('open stderr'); });

	/* Set up stdin. */
	if (!this.mex_isinit && this.mex_args['taskInputFile'] !== undefined)
		filename = this.mex_args['taskInputFile'];
	else if (this.mex_isinit || !this.mex_args['taskInputRemote'])
		filename = '/dev/null';

	log = this.mex_log;

	if (filename !== undefined) {
		barrier.start('open stdin');
		mod_fs.open(filename, 'r', function (err, fd) {
			if (err) {
				mazErrorOpen = new VError(
				    err, 'failed to open input');
				mazLog.error(mazErrorOpen);
			} else {
				executor.mex_infd = fd;
			}

			barrier.done('open stdin');
		});
	} else if (!this.mex_isinit) {
		mod_assert.ok(this.mex_args['taskInputRemote'] !== undefined);

		/*
		 * Set up an HttpCatStream using the given remote URL.
		 */
		this.mex_cache = new ObjectCache(function (url) {
			return (mod_restify.createClient({
			    'socketPath': url,
			    'log': log.child({ 'client': url }),
			    'version': '*'
			}));
		}, log.child({ 'component': 'ObjectCache' }));

		instream = this.mex_instream = new CatStreams({
		    'log': log.child({ 'component': 'CatStream' }),
		    'perRequestBuffer': mazHttpPerRequestBuffer,
		    'maxConcurrency': mazHttpConcurrency
		});

		instream.on('error', function (err) {
			/*
			 * This likely indicates a 500 or other persistent error
			 * while fetching data for the reducer.  The easiest
			 * thing to do is send EOF to the child process and
			 * allow it to exit normally but ignore its exit status.
			 * We record the error here, and it will take precedence
			 * over any error that the user task would cause us to
			 * emit.
			 */
			mazErrorFetch = new VError(err,
			    'error fetching inputs');
			log.error(mazErrorFetch);

			if (executor.mex_exec !== undefined &&
			    !executor.mex_done) {
				log.info('killing user contract');
				executor.mex_exec.abort();
			}
		});

		instream.on('data', function check(chunk) {
			mod_assert.ok(executor.mex_exec !== undefined &&
			    executor.mex_exec.ex_child !== undefined,
			    'received data before we were ready');
			instream.removeListener('data', check);
		});

		instream.on('end', function () {
			log.info('CatStreams ended');
			mod_assert.ok(executor.mex_exec !== undefined &&
			    executor.mex_exec.ex_child !== undefined,
			    'received end before we were ready');
		});

		/*
		 * Pause the stream until the child process is ready for data.
		 * With the Node 0.10 readable-stream interface, our pause()
		 * will not prevent 'end' from being emitted on a zero-byte
		 * stream, so we must avoid ending the stream until the child is
		 * ready for it.
		 */
		instream.pause();

		/*
		 * Reduce tasks are complex because we need to update our parent
		 * as we finish feeding each batch of files to the user code.
		 * For reduce tasks, committing key X means only that X has been
		 * fed to the user's code, not that it's finished processing it.
		 * At the very end we'll do a commit of zero keys to indicate
		 * that the user's code has actually completed.
		 *
		 * onBatchDone commits the current batch of keys.  That will
		 * check for taskInputDone and end the input stream as
		 * necessary.  If there are zero keys to process in this task,
		 * we'll handle that below by ending the input stream.
		 */
		instream.on('request-drain', function () {
			executor.emit('drain', executor.mex_batch);
		});
	}

	barrier.on('drain', function () {
		if (mazErrorFetch || mazErrorOpen) {
			mazLog.warn(
			    'skipping execution because of previous error');
			mod_assert.ok(executor.mex_infd === undefined);
			errstream.destroy();
			outstream.destroy();
			executor.emit('done');
			return;
		}

		executor.mex_exec = new Executor({
		    'exec': executor.mex_isinit ?
			executor.mex_args['taskPhase']['init'] :
		        executor.mex_args['taskPhase']['exec'],
		    'env': executor.mex_env,
		    'log': executor.mex_log,
		    'stdout': outstream,
		    'stderr': errstream,
		    'stdin': executor.mex_infd !== undefined ?
		        executor.mex_infd : executor.mex_instream
		});

		executor.mex_exec.start();

		if (executor.mex_infd !== undefined) {
			mod_fs.close(executor.mex_infd);
			executor.mex_infd = -1;
		} else if (!executor.mex_isinit) {
			/*
			 * It's kind of annoying that we have to do this here,
			 * rather than earlier when we set up the stream, but
			 * the behavior of Node 0.10-style streams is that they
			 * can emit 'end' while paused (or without having read
			 * past the end), and we could miss that event in the
			 * case of a 0-byte stream if we didn't wait til now.
			 */
			mod_assert.ok(executor.mex_args['taskInputRemote'] !==
			    undefined);
			executor.batch({
			    'taskInputKeys': executor.mex_args['taskInputKeys'],
			    'taskInputDone': executor.mex_args['taskInputDone']
			});
		}

		outstream.destroy();
		errstream.destroy();

		executor.mex_exec.on('done', function (result) {
			executor.emit('done', result);
			executor.mex_done = true;

			if (executor.mex_instream !== undefined &&
			    executor.mex_instream.readable)
				log.info('task exited before reading ' +
				    'all of its input');
		});
	});
};

MarlinExecutor.prototype.batch = function (state)
{
	mod_assert.ok(this.mex_instream !== undefined);

	var remote = this.mex_args['taskInputRemote'];
	var stream = this.mex_instream;
	var cache = this.mex_cache;
	var log = this.mex_log;

	state['taskInputKeys'].forEach(function (iobject) {
		stream.cat(function (options) {
			var path, s, f;
			path = mod_mantacbin.mantaObjectNameEncode(iobject);
			s = new HttpStream({
			    'client': cache.get(remote),
			    'path': path,
			    'log': log.child({ 'httpstream': path }),
			    'highWaterMark': options['highWaterMark'],
			    'retryPolicy': mazReduceRetryPolicy
			});

			if (mazFuzzInput) {
				f = new FuzzStream();
				s.pipe(f);
				s = f;
			}

			return (s);
		});
	});

	if (state['taskInputDone']) {
		this.mex_log.info('ending CatStream');
		stream.cat(null);
	}

	this.mex_batch = state;
};

/*
 * Generic object cache: this object provides get(key), which instantiates a new
 * object using "constructor" and caches it under the specified key.  Subsequent
 * requests for the same key return the same object unless it's been removed
 * with "remove".  This is used to implement an HTTP client cache indexed by
 * host.
 */
function ObjectCache(constructor, log)
{
	this.oc_cons = constructor;
	this.oc_cache = {};
	this.os_log = log;
}

ObjectCache.prototype.get = function (key)
{
	if (!this.oc_cache.hasOwnProperty(key)) {
		this.os_log.trace('creating new object for key "%s"', key);
		this.oc_cache[key] = this.oc_cons(key);
	}

	return (this.oc_cache[key]);
};

ObjectCache.prototype.remove = function (key, obj)
{
	if (arguments.length < 2 || this.oc_cache[key] == obj) {
		this.os_log.trace('removing object for key "%s"', key);
		delete (this.oc_cache[key]);
	}
};

main();
