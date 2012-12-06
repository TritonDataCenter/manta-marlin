/*
 * lib/agent/zone-agent.js: per-zone agent, responsible for running and
 * monitoring compute zone jobs and servicing HTTP requests from the user.
 */

/*
 * This agent runs inside a Marlin compute zone and manages task execution
 * within the zone.  We fetch work, report status, and heartbeat by making HTTP
 * requests to the global zone agent, which listens on a Unix Domain Socket in
 * /tmp.  Being HTTP, we only have a "pull" interface.  The protocol works like
 * this:
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
 *     over the HTTP server we run on localhost:8080, are proxied through the
 *     global zone agent, which automatically injects authentication headers so
 *     that neither we nor the user code needs to worry about private keys.  The
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
var mod_extsprintf = require('extsprintf');
var mod_fs = require('fs');
var mod_http = require('http');
var mod_jsprim = require('jsprim');
var mod_os = require('os');
var mod_path = require('path');

var mod_bunyan = require('bunyan');
var mod_kang = require('kang');
var mod_panic = require('panic');
var mod_restify = require('restify');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_httpcat = require('../http-cat-stream.js');
var mod_manta = require('manta');
var mod_mautil = require('../util');

var sprintf = mod_extsprintf.sprintf;
var VError = mod_verror.VError;

/*
 * Configuration
 */
var mazHostname = mod_os.hostname();
var mazServerName = 'marlin_zone-agent';	/* restify server name */
var mazPort = 8080;				/* restify server port */
var mazRetryTime = 1 * 1000;			/* between GZ agent requests */
var mazOutputDir = '/var/tmp/marlin_task';	/* user command output dir */
var mazTaskStdoutFile = mod_path.join(mazOutputDir, 'stdout');
var mazTaskStderrFile = mod_path.join(mazOutputDir, 'stderr');
var mazTaskLivenessInterval = 5 * 1000;
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
		 * ourselves die.  When we add a simple mechanism for persisting
		 * state, we can remove this flag and re-adopt our contracts
		 * upon restart.
		 */
		'noorphan': true
	}
};

/*
 * Immutable server state
 */
var mazLog;		/* logger */
var mazUdsPath;		/* path to Unix Domain Socket to parent agent */
var mazClient;		/* HTTP client for parent agent */
var mazServer;		/* HTTP server for in-zone API */
var mazMantaClient;	/* Manta client */

/*
 * Dynamic task state.  We keep this in pseudo-globals because we only ever work
 * on one task at a time, and doing it this way makes it easier to debug via
 * kang and postmortem tools.
 */
var mazTimeout;			/* pending timeout, if any */
var mazLivenessTimeout;		/* liveness timeout, if any */
var mazLivenessPending;		/* liveness request pending */
var mazCurrentTask;		/* current task */
var mazExecStatus;		/* last exec status */
var mazTaskResult;		/* last task result */
var mazErrorSave;		/* error during task processing */
var mazContract;		/* last contract */
var mazInputFd;			/* input file descriptor */
var mazInputStream;		/* current input stream */
var mazUserEmitted = false;	/* whether the user has emitted output */
var mazServerRequests = {};	/* pending server requests, by request id */
var mazClientRequests = {};	/* pending client requests, by request id */
var mazPipeline;		/* current task pipeline */
var mazCounters = {		/* status and error counters */
    'invocations': 0,		/* attempted user "exec" invocations */
    'keys_started': 0,		/* number of keys we've started processing */
    'keys_ok': 0,		/* number of keys successfully processed */
    'keys_fail': 0,		/* number of keys failed */
    'agentrq_sent': 0,		/* number of ctrl requests sent to parent */
    'agentrq_ok': 0,		/* number of ctrl requests succeeded */
    'agentrq_fail': 0,		/* number of ctrl requests failed */
    'agentrq_proxy_sent': 0,	/* number of proxy requests sent */
    'agentrq_proxy_ok': 0,	/* number of proxy requests succeeded */
    'agentrq_proxy_fail': 0	/* number of proxy requests failed */
};

function usage(errmsg)
{
	if (errmsg)
		console.error(errmsg);

	console.error('usage: node zone-agent.js socket_path');
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
	 * Set up restify server for the in-zone Task Control API.
	 */
	mazServer = mod_restify.createServer({
	    'name': mazServerName,
	    'log': mazLog
	});

	mazServer.use(function (request, response, next) {
		mazServerRequests[request['id']] = request;
		next();
	});
	mazServer.use(mod_restify.acceptParser(mazServer.acceptable));
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

	mazServer.get('/kang/.*',
	    mod_restify.bodyParser({ 'mapParams': false }),
	    mod_kang.knRestifyHandler({
		'uri_base': '/kang',
		'service_name': 'marlin',
		'component': 'zone-agent',
		'ident': mazHostname,
		'version': '0',
		'list_types': mazKangListTypes,
		'list_objects': mazKangListObjects,
		'get': mazKangGetObject,
		'stats': mazKangStat
	    }));

	/*
	 * All incoming requests are just proxied directly to our parent agent.
	 */
	mazServer.get('/.*', mazApiProxy);
	mazServer.put('/.*', mazApiProxy);
	mazServer.post('/.*', mazApiProxy);
	mazServer.del('/.*', mazApiProxy);
	mazServer.head('/.*', mazApiProxy);

	mazServer.on('expectContinue', function (request, response, route) {
		/*
		 * When we receive a request with "Expect: 100-continue", we run
		 * the route as normal, since this will necessarily be a
		 * proxying request.  We don't call writeContinue() until we
		 * have the upstream's "100 Continue".  That's done by
		 * maHttpProxy.
		 */
		route.run(request, response);
	});

	/*
	 * Create our output directory, start up the in-zone HTTP server, then
	 * ask our parent for work to do.
	 */
	mod_fs.mkdir(mazOutputDir, function (err) {
		if (err && err['code'] != 'EEXIST') {
			mazLog.fatal(err, 'failed to create "%s"',
			    mazOutputDir);
			throw (err);
		}

		mazServer.listen(mazPort, function () {
			mazLog.info('server listening on port %d', mazPort);
			mazTaskFetch();
		});
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
	mazTimeout = undefined;

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
		mod_assert.ok(mazExecStatus === undefined);
		mod_assert.ok(mazTaskResult === undefined);
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
	mazTaskRunSpawn,
	mazTaskRunClose,
	mazTaskRunSaveStdout,
	mazTaskRunSaveStderr
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

function mazTaskRunSpawn(task, callback)
{
	var reduce, keys, key, exec, env, child, done;
	var cache, infile, instream, outstream, errstream;

	mazLog.info('picking up task', task);
	reduce = task['taskPhase']['type'] == 'reduce';
	keys = task['taskInputKeys'];
	exec = task['taskPhase']['exec'];

	outstream = mod_fs.createWriteStream(mazTaskStdoutFile);
	errstream = mod_fs.createWriteStream(mazTaskStderrFile);

	env = {
	    'PATH': '/opt/local/gnu/bin:/opt/local/bin:' +
		process.env['PATH'] + ':' +
		mod_path.normalize(mod_path.join(__dirname, '../../cmd')),
	    'MANTA_OUTPUT_BASE': mazCurrentTask['taskOutputBase']
	};

	if (!reduce) {
		mod_assert.equal(keys.length, 1);
		key = keys[0];
		env['mc_input_key'] = key;
	}

	mod_assert.ok(mazInputStream === undefined);
	mod_assert.ok(mazInputFd === undefined);

	if (mazCurrentTask.hasOwnProperty('taskInputFile')) {
		infile = mazCurrentTask['taskInputFile'];
 		env['mc_input_file'] = infile;

		mod_fs.open(infile, 'r', function (err, fd) {
			if (err) {
				mazLog.fatal(err, 'failed to open input file');
				throw (err);
			}

			mazInputFd = fd;
			onOpen();
		});
	} else if (mazCurrentTask['taskInputRemote']) {
		cache = new mod_httpcat.ObjectCache(function (url) {
			return (mod_restify.createClient({
			    'socketPath': url,
			    'log': mazLog.child({
				'component': 'client-' + url
			    })
			}));
		}, mazLog.child({ 'component': 'ObjectCache' }));

		instream = mazInputStream = new mod_httpcat.HttpCatStream({
		    'clients': cache,
		    'log': mazLog.child({ 'component': 'HttpCatStream' })
		});

		instream.on('error', function (err) {
			mazLog.error(err, 'fatal error on http input stream');
			instream.destroy();
			/* XXX kill child process? */
		});

		keys.forEach(function (ikey) {
			instream.write({
			    'url': mazCurrentTask['taskInputRemote'],
			    'uri': ikey
			});
		});

		instream.pause();

		/*
		 * For map tasks, we simply end the input stream, which will
		 * cause the user code to exit when it finishes reading it,
		 * after which we'll commit the results and move on.  For map
		 * tasks, committing key X means that key X has been fully
		 * processed.
		 *
		 * Reduce tasks are a little more complex because we need to
		 * update our parent as we finish feeding each batch of files to
		 * the user code.  So in this case, committing key X means only
		 * that X has been fed to the user's code, not that it's
		 * finished processing it.  At the very end we'll do a commit of
		 * zero keys to indicate that the user's code has actually
		 * completed.
		 *
		 * mazDoneBatch commits the current batch of keys.  That will
		 * check for taskInputDone and end the input stream as
		 * necessary.
		 */
		if (!reduce)
			instream.end();
		else
			instream.on('drain', mazDoneBatch);
	}

	mazLog.info('launching "%s" for input keys %j', exec, keys);
	mazCounters['invocations']++;
	mazCounters['keys_started']++;

	var ndone = 0;
	var nexpected = infile ? 3 : 2;
	var onOpen = function () {
		if (++ndone < nexpected)
			return;

		/*
		 * The input stream may either come directly from a file or from
		 * our own stream of remote Manta objects.  In the file case, we
		 * use spawn's redirection mechanism to avoid having to pipe it
		 * through ourselves, and we must be sure to close our copy of
		 * the file after forking to avoid leaking the fd.
		 */
		child = mod_child.spawn('bash', [ '-c', exec ], {
		    'stdio': [
			infile ? mazInputFd : null,
			outstream,
			errstream ],
		    'env': env
		});

		mazContract = mod_contract.latest();

		if (infile) {
			mod_fs.close(mazInputFd);
			mazInputFd = undefined;
		} else if (instream) {
			instream.pipe(child.stdin);
			instream.resume();

			child.stdin.on('error', function (err) {
				if (err['code'] == 'EPIPE')
					return;

				mazLog.warn(err, 'error writing to child');
			});
		}

		outstream.destroy();
		errstream.destroy();

		child.on('exit', function (code, sig) {
			if (code === 0 || code) {
				mazExecStatus = 'user command exited with ' +
				    'status ' + code;
				mazTaskResult = (code === 0 ? 'ok' : 'fail');
			} else {
				mazExecStatus = 'user command killed by ' +
				    'signal ' + sig;
				mazTaskResult = 'fail';
			}

			mazLog.info(mazExecStatus);
		});

		done = function (ev) {
			mazLog.info('critical contract event', ev);

			if (mazContract !== undefined) {
				/*
				 * Abandon the contract, dispose it (because
				 * it maintains its own reference), and remove
				 * listeners so that this function doesn't get
				 * invoked for a different event on task "i"
				 * (which we took for dead) after we've moved on
				 * to "i+1".
				 */
				mazContract.abandon();
				mazContract.dispose();
				mazContract.removeAllListeners();
				mazContract = undefined;
				if (instream) {
					instream.removeListener('drain',
					    mazDoneBatch);
					instream.destroy();
				}
				mazInputStream = undefined;
				mazInputFd = undefined;
				callback();
			}
		};

		mazContract.on('pr_empty', done);
		mazContract.on('pr_hwerr', done);
		mazContract.on('pr_core', done);
	};

	outstream.on('open', onOpen);
	errstream.on('open', onOpen);
}

function mazTaskRunClose(task, callback)
{
	if (mazInputStream === undefined) {
		mazLog.info('no stdin stream');
		callback();
		return;
	}

	if (!mazInputStream.readable) {
		mazLog.info('stdin stream already closed');
		mazInputStream = undefined;
		callback();
		return;
	}

	/*
	 * We must close the input stream before moving on because otherwise the
	 * global zone agent won't be able to remove the hyprlofs mapping for it
	 * when we advance to the next key.
	 */
	mazLog.info('closing stdin stream');
	mazInputStream.on('close', function () {
		mazInputStream = undefined;
		callback();
	});

	mazInputStream.destroy();
}

function mazTaskRunSaveStdout(task, callback)
{
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
	if (mazTaskResult == 'ok') {
		mazLog.info('skipping stderr capture because result is ok');
		callback();
		return;
	}

	mazFileSave('stderr', mazTaskStderrFile,
	    mazCurrentTask['taskErrorKey'], callback);
}


function mazFileSave(name, filename, key, callback)
{
	if (!key) {
		/*
		 * XXX The only case this is possible today is for a reduce job
		 * with zero keys to process.  This should be fixed in the agent
		 * by providing an output key in this case based on the job's
		 * URI, but at the moment we don't have that information.
		 */
		mazLog.warn('error picking manta key for %s: ' +
		    'taskOutputKey/taskErrorKey not set', name);
		callback();
		return;
	}

	mazLog.info('capturing %s from %s', name, filename);

	mod_mautil.mantaFileSave({
	    'client': mazMantaClient,
	    'filename': filename,
	    'key': key,
	    'log': mazLog,
	    'stderr': name == 'stderr'
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
	var arg, uri;

	mazLog.info('reporting task status');

	arg = {};
	if (mazCurrentTask['taskPhase']['type'] != 'reduce') {
		mod_assert.equal(mazCurrentTask['taskInputKeys'].length, 1);
		arg['key'] = mazCurrentTask['taskInputKeys'][0];
	}

	/*
	 * If there was no error with pipeline execution itself, check if there
	 * was another error encountered while processing the task.
	 */
	if (!err)
		err = mazErrorSave;

	/*
	 * In order to report the first error, we first check whether we failed
	 * to execute the user task, and then check if we hit another error
	 * (like failure to save output).
	 */
	if (mazTaskResult == 'fail') {
		mazCounters['keys_fail']++;
		uri = '/my/jobs/task/fail';
		arg['error'] = {
		    'message': mazExecStatus
		};
	} else if (err) {
		mazCounters['keys_fail']++;
		mazLog.error(err, 'task failed to execute');
		uri = '/my/jobs/task/fail';
		arg['error'] = {
		    'message': 'internal error',
		    'messageInternal': err.message
		};
	} else {
		mazCounters['keys_ok']++;
		uri = '/my/jobs/task/commit';
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
		mazExecStatus = undefined;
		mazTaskResult = undefined;
		mazErrorSave = undefined;
		mazTaskFetch();
	});
}

function mazTaskLivenessTick()
{
	if (mazLivenessPending)
		return;

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

function mazDoneBatch()
{
	var arg = {
	    'key': mazCurrentTask['taskInputKeys'][0],
	    'nkeys': mazCurrentTask['taskInputKeys'].length
	};

	mazLog.info('finished batch', arg);

	mazParentRequest('POST /my/jobs/task/commit', 204,
	    function (rqcallback) {
		mazClient.post('/my/jobs/task/commit', arg, rqcallback);
	    }, function (err) {
		if (err) {
			mazLog.fatal(err, 'fatal error updating reduce state');
			process.exit(1);
		}

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
			task['taskInputKeys'].forEach(function (ikey) {
				mazInputStream.write({
				    'url': task['taskInputRemote'],
				    'uri': ikey
				});
			});

			/*
			 * With each lap, we process either a batch of keys OR
			 * the end-of-input marker.  If we were to process both,
			 * the user program could exit successfully before
			 * mazDoneBatch is invoked, causing us to move on before
			 * we're ready.  This is still a bit fragile.
			 */
			if (task['taskInputKeys'].length === 0 &&
			    task['taskInputDone'])
				mazInputStream.end();
		});
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

	mazCounters['agentrq_sent']++;
	mazClientRequests[uuid] = {
	    'time': new Date(),
	    'opname': opname,
	    'attempt': attempt
	};

	makerequest(function (err, request, response, result) {
		delete (mazClientRequests[uuid]);

		if (!err) {
			if (response.statusCode == expected_code ||
			    (expected_code === undefined &&
			    response.statusCode >= 200 &&
			    response.statusCode < 400)) {
				mazCounters['agentrq_ok']++;
				callback(null, result);
				return;
			}

			err = new VError('expected %s status, got %s ' +
			    '(body %s)', expected_code || 'ok',
			    response.statusCode, response.body);
		}

		mazCounters['agentrq_fail']++;

		if (response && response.statusCode >= 400 &&
		    response.statusCode < 500) {
			/*
			 * We're pretty hosed if we got a 40x error.  It means
			 * we did something wrong, but we obviously don't know
			 * what.
			 */
			mazLog.error('%s returned %s: %s', opname,
			    response.statusCode, response.body);
			callback(err);
			return;
		}

		mazLog.warn(err, '%s failed; will retry in %s', opname,
		    mazRetryTime);

		mazTimeout = setTimeout(function () {
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
	var uuid = request.id;

	if (request.url.substr(0, '/kang/'.length) == '/kang/') {
		next();
		return;
	}

	if (request.method.toLowerCase() == 'put' &&
	    !mod_jsprim.startsWith(request.url, '/my/jobs/task/'))
		mazUserEmitted = true;

	/*
	 * XXX The failure semantics here are pretty tricky.  Recall that we can
	 * receive two kinds of requests here: GET/POST for control requests
	 * like GET /task, POST /commit, POST /fail, and so on; and PUT for new
	 * Manta objects.
	 *
	 * In both cases, as much as possible we want to isolate the user from
	 * transient failures of both Marlin and Manta.  Control requests are
	 * always small, so we can buffer their contents and keep sending them
	 * until we succeed, regardless of whether they fail with ECONNREFUSED,
	 * 50x, or whatever.  (We would stop if we ever saw 40x, since that
	 * indicates a problem with the request itself.)  We won't respond to
	 * the user until we succeed, so they're effectively throttled.
	 *
	 * PUTs are a little more complicated.  They're arbitrarily large, so we
	 * can't buffer them in memory.  But we also don't have to fail them
	 * just because the GZ agent is currently down.  Instead, we can pause
	 * the request as soon as it comes in but start proxying it to the GZ
	 * agent immediately (by connecting and sending the headers).  Once we
	 * got the 100-continue, we unpause the incoming request, and any
	 * subsequent failure gets passed along to the user.  But transient
	 * failure to connect to the GZ results in the same behavior as for
	 * control requests: the request blocks and the user is throttled until
	 * the GZ agent comes back.
	 *
	 * None of this is implemented yet.  Right now, failures are passed
	 * right on back to the user.
	 */
	mazCounters['agentrq_proxy_sent']++;

	var op = mod_mautil.maHttpProxy({
	    'request': request,
	    'response': response,
	    'server': {
	        'socketPath': mazUdsPath
	    }
	}, function (err) {
		delete (mazClientRequests[uuid]);

		if (err)
			mazCounters['agentrq_proxy_fail']++;
		else
			mazCounters['agentrq_proxy_ok']++;

		next();
	});

	mazClientRequests[uuid] = { 'time': new Date(), 'op': op };
}


/*
 * Kang (introspection) entry points
 */
function mazKangListTypes()
{
	return (['zoneagent', 'zoneagent_requests']);
}

function mazKangListObjects(type)
{
	if (type == 'zoneagent')
		return ([ mazHostname ]);

	return (Object.keys(mazServerRequests));
}

function mazKangGetObject(type, id)
{
	if (type == 'zoneagent')
		return ({
		    'task': mazCurrentTask,
		    'timeout': mazTimeout ? 'present' : 'none',
		    'udsPath': mazUdsPath,
		    'execStatus': mazExecStatus,
		    'pipeline': mazPipeline
		});

	var request = mazServerRequests[id];

	return ({
	    id: request.id,
	    method: request.method,
	    url: request.url,
	    time: request.time
	});
}

function mazKangStat()
{
	return (mazCounters);
}

main();
