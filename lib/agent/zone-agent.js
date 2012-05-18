/*
 * lib/agent/zone-agent.js: per-zone agent, responsible for running and
 * monitoring compute zone jobs and servicing HTTP requests from the user.
 */

var mod_assert = require('assert');
var mod_child = require('child_process');
var mod_fs = require('fs');
var mod_http = require('http');
var mod_os = require('os');
var mod_path = require('path');

var mod_bunyan = require('bunyan');
var mod_kang = require('kang');
var mod_panic = require('panic');
var mod_restify = require('restify');
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
var mazTimeout;		/* pending timeout, if any */
var mazPendingRequest;	/* pending parent request */
var mazCurrentTask;	/* current task */
var mazCurrentKey;	/* current key */
var mazExecStatus;	/* last exec status */
var mazInputStream;	/* current input stream */
var mazRequests = {};	/* pending requests, by request id */
var mazPipeline;	/* current task pipeline */
var mazCounters = {	/* status and error counters */
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
	 * Set up restify server for the in-zone Task Control API.
	 */
	mazServer = mod_restify.createServer({
	    'name': mazServerName,
	    'log': mazLog
	});

	mazServer.use(function (request, response, next) {
		mazRequests[request['id']] = request;
		next();
	});
	mazServer.use(mod_restify.acceptParser(mazServer.acceptable));
	mazServer.use(mod_restify.queryParser());

	mazServer.on('uncaughtException', mod_mautil.maRestifyPanic);
	mazServer.on('after', mod_restify.auditLogger({ 'log': mazLog }));
	mazServer.on('after', function (request, response) {
		delete (mazRequests[request['id']]);
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

	mazServer.listen(mazPort, function () {
		mazLog.info('server listening on port %d', mazPort);
	});

	/*
	 * Create our output directory, then ask our parent for work to do.
	 */
	mod_fs.mkdir(mazOutputDir, function (err) {
		if (err && err['code'] != 'EEXIST') {
			mazLog.fatal(err, 'failed to create "%s"',
			    mazOutputDir);
			throw (err);
		}

		mazTaskFetch();
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
	}, function (task) {
		mod_assert.ok(mazPipeline === undefined);
		mod_assert.ok(mazCurrentTask === undefined);
		mod_assert.ok(mazCurrentKey === undefined);
		mod_assert.ok(mazExecStatus === undefined);

		mazCurrentTask = task;
		mazCurrentKey = task['taskInputKey'];

		mazPipeline = mod_vasync.pipeline({
		    'arg': task,
		    'funcs': mazTaskStages
		}, function (err) {
			mod_assert.ok(mazPipeline !== undefined);
			mazPipeline = undefined;
			mazTaskReport(err);
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
	mazTaskRunSaveOutput
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
	var key, exec, env, child;
	var infile, instream, outstream, errstream;

	mazLog.info('picking up task', task);
	key = mazCurrentKey;
	exec = task['taskPhase']['exec'];

	outstream = mod_fs.createWriteStream(mazTaskStdoutFile);
	errstream = mod_fs.createWriteStream(mazTaskStderrFile);

	env = {
	    'mc_input_key': key,
	    'PATH': process.env['PATH'] + ':' + mod_path.normalize(
	        mod_path.join(__dirname, '../../cmd'))
	};

	mod_assert.ok(mazInputStream === undefined);

	if (mazCurrentTask.hasOwnProperty('taskInputFile')) {
		infile = mazCurrentTask['taskInputFile'];
		instream = mazInputStream = mod_fs.createReadStream(infile);
 		env['mc_input_file'] = infile;
	}

	mazLog.info('launching "%s" for input key "%s"', exec, key);
	mazCounters['invocations']++;
	mazCounters['keys_started']++;

	/*
	 * Although the child's stdin, stdout, and stderr are actually files,
	 * the only way to accomplish this in Node (until we get a replacement
	 * for the old customFds feature) is to pipe input/output through
	 * ourselves.
	 */
	child = mod_child.spawn('bash', [ '-c', exec ], { 'env': env });

	if (instream)
		instream.pipe(child.stdin);
	child.stdout.pipe(outstream);
	child.stderr.pipe(errstream);

	child.stdin.on('error', function (err) {
		if (err['code'] == 'EPIPE')
			return;

		mazLog.warn(err, 'error writing to child stdin');
	});

	child.on('exit', function (code) {
		mazLog.info('command completed with status %s', code);
		mazExecStatus = code;
		callback();
	});
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

			/* XXX need more thought about naming this key. */
			var uri = '/object/' + mazCurrentKey + '.out';

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
	arg = { 'key': mazCurrentKey };

	if (err) {
		mazCounters['keys_fail']++;
		mazLog.error(err, 'task failed to execute');
		uri = '/fail';
		arg['error'] = {
		    'message': 'internal error'
		};
	} else if (mazExecStatus !== 0) {
		mazCounters['keys_fail']++;
		mazLog.error('user exec failed with status %s', mazExecStatus);
		uri = '/fail';
		arg['error'] = {
		    'message': 'task exited with status ' + mazExecStatus
		};
	} else {
		mazCounters['keys_ok']++;
		uri = '/commit';
	}

	mazParentRequest('POST ' + uri, 204, function (rqcallback) {
		mazClient.post(uri, arg, rqcallback);
	}, function () {
		mazCurrentKey = undefined;
		mazCurrentTask = undefined;
		mazExecStatus = undefined;
		mazTaskFetch();
	});
}

/*
 * Most of the time, if we're making a request to our parent, we cannot make
 * forward progress until we get a response.  This is obviously true when we
 * initially ask our parent what work we should be doing, but also when
 * committing or failing a task.  Therefore, if we fail to make a request to our
 * parent, we retry indefinitely (with a timeout so we don't run away with the
 * CPU).  Either our parent will eventually answer and we can make progress
 * again, or this zone will be torn down by an administrator.
 */
function mazParentRequest(opname, expected_code, makerequest, callback)
{
	mazCounters['agentrq_sent']++;
	mod_assert.ok(mazPendingRequest === undefined);
	mazPendingRequest = opname;
	makerequest(function (err, request, response, result) {
		mod_assert.ok(mazPendingRequest == opname);
		mazPendingRequest = undefined;
		/*
		 * XXX Don't retry 400s. Specifically, we shouldn't retry
		 * "commit" or "fail", since the task itself may have already
		 * reported one or the other and we should defer to that.
		 */
		if (!err && response.statusCode != expected_code)
			err = new mod_verror.VError(null, 'expected status ' +
			    '%s, got %s (body %s)', expected_code,
			    response.statusCode, response.body);

		if (err) {
			mazCounters['agentrq_fail']++;
			mazLog.error(err, '%s failed', opname);
			mazTimeout = setTimeout(function () {
				mazParentRequest(opname, expected_code,
				    makerequest, callback);
			}, mazRetryTime);
			return;
		}

		mazCounters['agentrq_ok']++;
		callback(result);
	});
}

/*
 * All incoming requests are just proxied to our parent agent.
 */
function mazApiProxy(request, response, next)
{
	var opname;

	if (request.url.substr(0, '/kang/'.length) == '/kang/') {
		next();
		return;
	}

	/*
	 * XXX These requests should be auto-retried like the above control
	 * requests are, at least for non-Manta requests.
	 * XXX they should also run asynchronously rather than use
	 * mazPendingRequest.
	 */
	mazCounters['agentrq_proxy_sent']++;
	mod_assert.ok(mazPendingRequest === undefined);

	mazPendingRequest = mod_mautil.maHttpProxy({
	    'request': request,
	    'response': response,
	    'server': {
	        'socketPath': mazUdsPath
	    }
	}, function (err) {
		mod_assert.equal(mazPendingRequest['summary'], opname);
		mazPendingRequest = undefined;

		if (err)
			mazCounters['agentrq_proxy_fail']++;
		else
			mazCounters['agentrq_proxy_ok']++;

		next();
	});

	opname = mazPendingRequest['summary'];
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

	return (Object.keys(mazRequests));
}

function mazKangGetObject(type, id)
{
	if (type == 'zoneagent')
		return ({
		    'task': mazCurrentTask,
		    'pendingRequest': mazPendingRequest,
		    'currentKey': mazCurrentKey,
		    'timeout': mazTimeout ? 'present' : 'none',
		    'udsPath': mazUdsPath,
		    'execStatus': mazExecStatus,
		    'pipeline': mazPipeline
		});

	var request = mazRequests[id];

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
