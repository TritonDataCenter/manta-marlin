/*
 * lib/agent/zone-agent.js: per-zone agent, responsible for running and
 * monitoring compute zone jobs and servicing HTTP requests from the user.
 */

var mod_assert = require('assert');
var mod_child = require('child_process');
var mod_contract = require('illumos_contract');
var mod_fs = require('fs');
var mod_http = require('http');
var mod_os = require('os');
var mod_path = require('path');

var mod_bunyan = require('bunyan');
var mod_kang = require('kang');
var mod_panic = require('panic');
var mod_restify = require('restify');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_mautil = require('../util');

/*
 * Configuration
 */
var mazHostname = mod_os.hostname();
var mazServerName = 'marlin_zone-agent';	/* restify server name */
var mazPort = 8080;				/* restify server port */
var mazRetryTime = 10 * 1000;			/* between GZ agent requests */
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
var mazContract;		/* last contract */
var mazInputStream;		/* current input stream */
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
	    'level': 'info'
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

	mazParentRequest('GET /task?wait=true', 200, function (rqcallback) {
		mazClient.get('/task?wait=true', rqcallback);
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

		mazCurrentTask = task;

		mazPipeline = mod_vasync.pipeline({
		    'arg': task,
		    'funcs': mazTaskStages
		}, function (suberr) {
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
	mazTaskRunTimerInit,
	mazTaskRunCleanup,
	mazTaskRunSpawn,
	mazTaskRunClose,
	mazTaskRunSaveOutput,
	mazTaskRunTimerFini
];

function mazTaskRunTimerInit(task, callback)
{
	mazLivenessTimeout = setInterval(
	    mazTaskLivenessTick, mazTaskLivenessInterval);
	callback();
}

function mazTaskRunTimerFini(task, callback)
{
	clearInterval(mazLivenessTimeout);
	callback();
}

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
	var infile, instream, outstream, errstream;

	mazLog.info('picking up task', task);
	reduce = task['taskPhase']['type'] == 'reduce';
	keys = task['taskInputKeys'];
	exec = task['taskPhase']['exec'];

	outstream = mod_fs.createWriteStream(mazTaskStdoutFile);
	errstream = mod_fs.createWriteStream(mazTaskStderrFile);

	env = {
	    'PATH': process.env['PATH'] + ':' + mod_path.normalize(
	        mod_path.join(__dirname, '../../cmd'))
	};

	if (!reduce) {
		mod_assert.equal(keys.length, 1);
		key = keys[0];
		env['mc_input_key'] = key;
	}

	mod_assert.ok(mazInputStream === undefined);

	if (mazCurrentTask.hasOwnProperty('taskInputFile')) {
		infile = mazCurrentTask['taskInputFile'];
		instream = mazInputStream = mod_fs.createReadStream(infile);
 		env['mc_input_file'] = infile;
	}

	mazLog.info('launching "%s" for input keys %j', exec, keys);
	mazCounters['invocations']++;
	mazCounters['keys_started']++;

	/*
	 * Although the child's stdin, stdout, and stderr are actually files,
	 * the only way to accomplish this in Node (until we get a replacement
	 * for the old customFds feature) is to pipe input/output through
	 * ourselves.
	 */
	child = mod_child.spawn('bash', [ '-c', exec ], { 'env': env });
	mazContract = mod_contract.latest();

	if (instream)
		instream.pipe(child.stdin);
	child.stdout.pipe(outstream);
	child.stderr.pipe(errstream);

	child.stdin.on('error', function (err) {
		if (err['code'] == 'EPIPE')
			return;

		mazLog.warn(err, 'error writing to child stdin');
	});

	child.on('exit', function (code, sig) {
		if (code === 0 || code) {
			mazExecStatus = 'user command exited with status ' +
			    code;
			mazTaskResult = (code === 0 ? 'ok' : 'fail');
		} else {
			mazExecStatus = 'user command killed by signal ' + sig;
			mazTaskResult = 'fail';
		}

		mazLog.info(mazExecStatus);
	});

	done = function (ev) {
		mazLog.info('critical contract event', ev);

		if (mazContract !== undefined) {
			mazContract.abandon();
			mazContract.removeAllListeners();
			mazContract = undefined;
			callback();
		}
	};

	mazContract.on('pr_empty', done);
	mazContract.on('pr_hwerr', done);
	mazContract.on('pr_core', done);
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

function mazTaskRunSaveOutput(task, callback)
{
	var filename = mazTaskStdoutFile;
	var opened = false;

	mazLog.info('capturing stdout');

	/*
	 * It's technically racy to stat first and then open, but there's
	 * nothing else running at this point that can mess with this file.
	 * Errors during this process are not fatal to the task.
	 */
	mod_fs.stat(filename, function (err, stat) {
		if (err) {
			mazLog.warn(err, 'stat(%s)', filename);
			callback();
			return;
		}

		if (stat['size'] === 0) {
			mazLog.info('stdout is empty');
			callback();
			return;
		}

		var instream = mod_fs.createReadStream(filename);

		instream.on('error', function (suberr) {
			if (!opened) {
				mazLog.warn(suberr, 'opening %s', filename);
				callback();
			} else {
				mazLog.warn(suberr, 'reading %s', filename);
			}
		});

		instream.on('open', function () {
			opened = true;

			var name = mazCurrentTask['taskInputKeys'][0];

			/*
			 * XXX This is not a good general approach to naming.
			 * This should use the owner's account, but we don't
			 * have that here.
			 */
			if (mazCurrentTask['taskPhase']['type'] == 'reduce')
				name = mod_path.join(mod_path.dirname(name),
				    mazCurrentTask['taskGroupId']);

			var uri = mod_path.join('/object', name + '.out');

			var request = mod_http.request({
			    'socketPath': mazUdsPath,
			    'method': 'PUT',
			    'path': uri,
			    'headers': {
				'content-length': stat['size']
			    }
			});

			instream.pipe(request);

			request.on('response', function (response) {
				if (response.statusCode != 201)
					mazLog.warn('stdout capture: server ' +
					    'returned %s', response.statusCode);

				callback();
			});

			request.on('error', function (suberr) {
				mazLog.warn(suberr, 'stdout capture: ' +
				    'request failed');
				callback();
			});
		});
	});
}

function mazTaskReport(err)
{
	var arg, uri;

	mazLog.info('reporting task status');

	arg = {};
	if (mazCurrentTask['taskPhase']['type'] != 'reduce') {
		mod_assert.equal(mazCurrentTask['taskInputKeys'].length, 1);
		arg['key'] =  mazCurrentTask['taskInputKeys'][0];
	}

	if (err) {
		mazCounters['keys_fail']++;
		mazLog.error(err, 'task failed to execute');
		uri = '/fail';
		arg['error'] = {
		    'message': 'internal error'
		};
	} else if (mazTaskResult == 'fail') {
		mazCounters['keys_fail']++;
		uri = '/fail';
		arg['error'] = {
		    'message': mazExecStatus
		};
	} else {
		mazCounters['keys_ok']++;
		uri = '/commit';
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
			mazLog.warn(e, 'ignored error reporting task status');
		mazCurrentTask = undefined;
		mazExecStatus = undefined;
		mazTaskResult = undefined;
		mazTaskFetch();
	});
}

function mazTaskLivenessTick()
{
	if (mazLivenessPending)
		return;

	mazLivenessPending = new Date();
	mazParentRequest('POST /live', undefined, function (rqcallback) {
		mazLivenessPending = undefined;
		mazClient.post('/live', rqcallback);
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

			err = new mod_verror.VError('expected %s ' +
			    'status, got %s (body %s)', expected_code || 'ok',
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
