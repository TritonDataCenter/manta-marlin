/*
 * lib/agent/zone-agent.js: per-zone agent, responsible for running and
 * monitoring compute zone jobs and servicing HTTP requests from the user.
 */

var mod_assert = require('assert');
var mod_child = require('child_process');
var mod_fs = require('fs');
var mod_os = require('os');
var mod_path = require('path');

var mod_bunyan = require('bunyan');
var mod_kang = require('kang');
var mod_panic = require('panic');
var mod_restify = require('restify');
var mod_verror = require('verror');

var mod_mautil = require('../util');

/*
 * Configuration
 */
var mazHostname = mod_os.hostname();
var mazServerName = 'marlin_zone-agent';	/* restify server name */
var mazPort = 8080;				/* restify server port */
var mazOutputDir = '/var/tmp/marlin_task';	/* user command output dir */
var mazRetryTime = 10 * 1000;			/* between GZ agent requests */

/*
 * Immutable server state
 */
var mazLog;		/* logger */
var mazUdsPath;		/* path to Unix Domain Socket to parent agent */
var mazClient;		/* HTTP client for parent agent */
var mazServer;		/* HTTP server for in-zone API */

/*
 * Dynamic task state
 */
var mazTimeout;		/* pending timeout, if any */
var mazPendingRequest;	/* pending parent request */
var mazCurrentTask;	/* current task */
var mazCurrentKey;	/* current key */
var mazDoneKeys;	/* set of processed keys */
var mazCount = 0;	/* used to uniquify output filenames */
var mazRequests = {};	/* pending requests, by request id */
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
	    'level': 'trace'
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
	    'log': mazLog
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

		mazTaskInit();
	});
}

/*
 * Asks our parent (the global zone agent) for work to do.  This is a long poll,
 * and it may well be a long time before there's anything for us to do.  If the
 * request fails for any reason, we simply try again later, using a timeout to
 * avoid running away in the event of a persistent failure of our parent.
 */
function mazTaskInit()
{
	mazTimeout = undefined;

	mazParentRequest('GET /task?wait=true', 200, function (rqcallback) {
		mazClient.get('/task?wait=true', rqcallback);
	}, function (task) {
		mazLog.info('picking up task', task);
		mod_assert.ok(mazCurrentTask === undefined);
		mazCurrentTask = task;
		mazTaskRun();
	});
}

/*
 * Iterates on the current task by invoking the user's "exec" string on the next
 * input key.  Recall that we only process one task at a time, so we store most
 * of our state in global variables to aid debugging.
 */
function mazTaskRun()
{
	var task = mazCurrentTask;
	var key = mazCurrentTask['taskInputKey'];
	var filename = mazCurrentTask['taskInputFile'];
	var unq = ++mazCount;

	mod_assert.ok(mazCurrentKey === undefined);
	mazCurrentKey = key;

	var phase = task['taskPhase'];
	var outfile = mod_path.join(mazOutputDir,
	    'key' + unq + '.out');
	var errfile = mod_path.join(mazOutputDir,
	    'key' + unq + '.err');

	var outstream = mod_fs.createWriteStream(outfile);
	var errstream = mod_fs.createWriteStream(errfile);

	mazLog.info('launching "%s" for input key "%s"', phase['exec'], key);
	mazCounters['invocations']++;
	mazCounters['keys_started']++;

	var child = mod_child.spawn('bash', [ '-c', phase['exec'] ], {
	    'env': {
		'mc_input_key': key,
		'mc_input_file': filename,
		'PATH': process.env['PATH'] + ':' + mod_path.normalize(
		    mod_path.join(__dirname, '../../cmd'))
	    }
	});

	child.stdout.pipe(outstream);
	child.stderr.pipe(errstream);

	child.on('exit', function (code) {
		mazLog.info('command completed with status %s', code);

		/*
		 * Notify our parent that we've processed this key.  For
		 * failures, we will eventually support a retry policy, but for
		 * now we just report the failed key to our parent.  Either way,
		 * if there are more input keys left, we invoke mazTaskRun again
		 * to process them.  If not, we've got nothing left to do --
		 * ever.  Any further keys will be processed under a different
		 * task by a different zone, so we just wait for our parent to
		 * tear down this zone.
		 */
		var arg = { 'key': key };
		var uri;

		if (code === 0) {
			uri = '/commit';
			mazCounters['keys_ok']++;
		} else {
			uri = '/fail';
			arg['error'] = {
			    'message': 'task exec exited with status ' + code
			};

			mazCounters['keys_fail']++;
			mazLog.error('key "%s" failed (exit code %s)',
			    key, code);
		}

		mazParentRequest('POST ' + uri, 204, function (rqcallback) {
			mazClient.post(uri, arg, rqcallback);
		}, function () {
			mazCurrentKey = undefined;
			mazCurrentTask = undefined;
			mazTaskInit();
		});
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
		    'Task (original)': mazCurrentTask,
		    'mazPendingRequest': mazPendingRequest,
		    'mazCurrentKey': mazCurrentKey,
		    'mazDoneKeys': mazDoneKeys,
		    'mazCount': mazCount,
		    'mazTimeout': mazTimeout ? 'present' : 'none',
		    'mazUdsPath': mazUdsPath
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
