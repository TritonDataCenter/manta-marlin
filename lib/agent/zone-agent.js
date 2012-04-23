/*
 * lib/agent/zone-agent.js: per-zone agent, responsible for running and
 * monitoring compute zone jobs and servicing HTTP requests from the user.
 */

var mod_assert = require('assert');
var mod_child = require('child_process');
var mod_fs = require('fs');
var mod_os = require('os');
var mod_path = require('path');
var mod_util = require('util');

var mod_bunyan = require('bunyan');
var mod_kang = require('kang');
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
var mazInputKeys;	/* list of unprocessed input keys */
var mazDoneKeys;	/* set of processed keys */
var mazCount = 0;	/* used to uniquify output filenames */
var mazCounters = {	/* status and error counters */
    'invocations': 0,		/* attempted user "exec" invocations */
    'keys_started': 0,		/* keys we've started processing */
    'keys_ok': 0,		/* keys we've successfully processed */
    'keys_fail': 0,		/* keys we failed to process */
    'agentrq_sent': 0,		/* requests sent to parent agent */
    'agentrq_ok': 0,		/* successful requests to parent agent */
    'agentrq_fail': 0		/* failed requests to parent agent */
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
	mazLog = new mod_bunyan({ 'name': mazServerName });

	if (process.argv.length < 3)
		usage();

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

	mazServer.use(mod_restify.acceptParser(mazServer.acceptable));
	mazServer.use(mod_restify.queryParser());
	mazServer.use(mod_restify.bodyParser({ mapParams: false }));

	mazServer.on('uncaughtException', mod_mautil.maRestifyPanic);
	mazServer.on('after', mod_restify.auditLogger({ 'log': mazLog }));
	mazServer.on('error', function (err) {
		mazLog.fatal(err, 'failed to start server: %s', err.mesage);
		process.exit(1);
	});

	mazServer.get('/kang/.*', mod_kang.knRestifyHandler({
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

	/* XXX add other routes */

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
		mazInputKeys = task['taskInputKeys'].slice(0);
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
	var key = mazInputKeys.shift();
	var unq = ++mazCount;

	mod_assert.ok(mazCurrentKey === undefined);
	mazCurrentKey = key;

	var phase = task['job']['jobPhases'][task['taskPhasenum']];
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
		'mc_input_key': key
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
		var uri;
		if (code === 0) {
			uri = '/commit';
			mazCounters['keys_ok']++;
			mazLog.error('key "%s" failed (exit code %s)',
			    key, code);
		} else {
			uri = '/fail';
			mazCounters['keys_fail']++;
		}

		mazParentRequest('POST ' + uri, 204, function (rqcallback) {
			mazClient.post(uri, { 'key': key }, rqcallback);
		}, function () {
			mazCurrentKey = undefined;

			if (mazInputKeys.length > 0)
				mazTaskRun();
			else
				mazLog.info('all keys processed');
		});
	});
}

/*
 * Most of the time, if we're making a request to our parent, we cannot make
 * forward progress until we get a response.  This is obviously true when we
 * initially ask our parent what work we should be doing, but also when
 * committing progress reports or failing the whole task.  Therefore, if we fail
 * to make a request to our parent, we retry indefinitely (with a timeout so we
 * don't run away with the CPU).  Either our parent will eventually answer and
 * we can make progress again, or this zone will be torn down by an
 * administrator.
 */
function mazParentRequest(opname, expected_code, makerequest, callback)
{
	mazCounters['agentrq_sent']++;
	mazPendingRequest = opname;
	makerequest(function (err, request, response, result) {
		mazPendingRequest = undefined;
		if (!err && response.statusCode != expected_code)
			err = new mod_verror.VError(null, 'expected status ' +
			    '%s, got %s (body %s)', expected_code,
			    response.statusCode, response.body);

		if (err) {
			mazCounters['agentrq_fail']++;
			mazLog.error(err, '%s failed', opname);
			mazTimeout = setTimeout(function () {
				mazParentRequest(opname, expected_code,
				    callback, makerequest);
			}, mazRetryTime);
			return;
		}

		mazCounters['agentrq_ok']++;
		callback(result);
	});
}

/*
 * Kang (introspection) entry points
 */
function mazKangListTypes()
{
	return (['zoneagent']);
}

function mazKangListObjects(type)
{
	return ([ mazHostname ]);
}

function mazKangGetObject(type, state)
{
	return ({
	    'Task (original)': mazCurrentTask,
	    'mazPendingRequest': mazPendingRequest,
	    'mazCurrentKey': mazCurrentKey,
	    'mazInputKeys': mazInputKeys,
	    'mazDoneKeys': mazDoneKeys,
	    'mazCount': mazCount,
	    'mazTimeout': mazTimeout ? 'present' : 'none',
	    'mazUdsPath': mazUdsPath
	});
}

function mazKangStat()
{
	return (mazCounters);
}

main();
