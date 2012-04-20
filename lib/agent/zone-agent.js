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
var mazCurrentTask;	/* current task */
var mazCurrentKey;	/* current key */
var mazInputKeys;	/* list of unprocessed input keys */
var mazDoneKeys;	/* set of processed keys */
var mazCount = 0;	/* used to uniquify output filenames */

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
	    'ident': mod_os.hostname(),
	    'version': '0',
	    'list_types': mazKangListTypes,
	    'list_objects': mazKangListObjects,
	    'get': mazKangGetObject
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

	mazClient.get('/task?wait=true',
	    function (err, request, response, task) {
		if (!err && response.statusCode != 200)
			err = new mod_verror.VError(null, 'received ' +
			    'unexpected status: %s (body: %s)',
			    response.statusCode, response.body);

		if (err) {
			mazLog.error(err, 'initial params request failed');
			mazTimeout = setTimeout(mazTaskInit, mazRetryTime);
			return;
		}

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

	var child = mod_child.spawn('bash', [ '-c', phase['exec'] ], {
	    'env': {
		'mc_input_key': key
	    }
	});

	child.stdout.pipe(outstream);
	child.stderr.pipe(errstream);

	child.on('exit', function (code) {
		mazLog.info('command completed with status %s', code);

		if (code !== 0) {
			/*
			 * The task failed.  XXX Report failure to the GZ agent.
			 */
			mazLog.error('task failed (exit code %s)', code);
			mazCurrentKey = undefined;
			return;
		}

		/*
		 * Notify our parent that we've successfully processed this key.
		 * If there are more input keys left, we invoke mazTaskRun again
		 * to process them.  Otherwise, we've got nothing left to do --
		 * ever.  Any further keys will be processed under a different
		 * task by a different zone, so we just wait for our parent to
		 * tear down this zone.
		 */
		mazTaskKeyDone(key, function (err) {
			mazCurrentKey = undefined;

			if (mazInputKeys.length > 0)
				mazTaskRun();
			else
				mazLog.info('all keys processed');
		});
	});
}

/*
 * Notify our parent that the given key has been processed.  We will keep trying
 * as necessary, and "callback" will not be invoked until we successfully do so.
 */
function mazTaskKeyDone(key, callback)
{
	mod_assert.equal(mazCurrentKey, key);

	mazClient.post('/commit', { 'key': key },
	    function (err, request, response, rv) {
		if (!err && response.statusCode != 204)
			err = new mod_verror.VError(null, 'received ' +
			    'unexpected status: %s (body: %s)',
			    response.statusCode, response.body);

		/*
		 * This call is not allowed to fail, and the only reasonable way
		 * it could is if the parent agent has transiently failed (i.e.
		 * crashed, and is currently being restarted).  We cannot
		 * reasonably proceed until this key is committed, so we just
		 * keep trying.
		 */
		if (err) {
			mazLog.warn(err, 'failed to commit key "%s"; ' +
			    'will retry later', key);
			mazTimeout = setTimeout(function () {
				mazTaskKeyDone(key, callback);
			}, mazRetryTime);
			return;
		}

		callback();
	    });
}

/*
 * Kang (introspection) entry points
 */
function mazKangListTypes()
{
	return (['global_state']);
}

function mazKangListObjects(type)
{
	return ([ 0 ]);
}

function mazKangGetObject(type, state)
{
	return ({
	    'Task (original)': mazCurrentTask,
	    'mazCurrentKey': mazCurrentKey,
	    'mazInputKeys': mazInputKeys,
	    'mazDoneKeys': mazDoneKeys,
	    'mazCount': mazCount,
	    'mazTimeout': mazTimeout ? 'present' : 'none',
	    'mazUdsPath': mazUdsPath
	});
}

main();
