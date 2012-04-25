/*
 * lib/agent/agent.js: compute node Marlin agent
 *
 * The agent runs in the global zone of participating compute and storage nodes
 * and manages tasks run on that node.  It's responsible for setting up compute
 * zones for user jobs, executing the jobs, monitoring the user code, tearing
 * down the zones, and emitting progress updates to the appropriate job manager.
 */

var mod_assert = require('assert');
var mod_fs = require('fs');
var mod_os = require('os');
var mod_path = require('path');
var mod_util = require('util');

var mod_bunyan = require('bunyan');
var mod_getopt = require('posix-getopt');
var mod_jsprim = require('jsprim');
var mod_mkdirp = require('mkdirp');
var mod_kang = require('kang');
var mod_restify = require('restify');
var mod_vasync = require('vasync');

var mod_job = require('../job');
var mod_mautil = require('../util');

var mod_agent_zone = require('./zone');

/* Global agent state. */
var maTasks = {};		/* all tasks, by taskid */
var maTasksQueued = [];		/* waiting tasks */
var maRecentTasks = [];		/* recently completed tasks */
var nRecentTasks = 10;		/* number of recent tasks to keep */
var maZones = {};		/* all zones, by zonename */
var maZonesReady = [];		/* ready zones */
var maLog;			/* global logger */
var maServer;			/* global restify server */
var maRequests = {};		/* pending requests, by id */
var maCounters = {
    'tasks_submitted': 0,	/* tasks received */
    'tasks_dispatched': 0,	/* tasks dispatched to a zone */
    'tasks_ok': 0,		/* tasks completed successfully */
    'tasks_fail': 0,		/* tasks that failed fatally */
    'keys_committed': 0,	/* individual keys committed */
    'zones_added': 0,		/* zones added to Marlin */
    'zones_readied': 0,		/* zones transitioned to "ready" */
    /* XXX track disabled zones too. */
    'file_writes_started': 0,	/* incoming file write requests */
    'file_writes_ok': 0,	/* completed file write requests */
    'file_writes_fail': 0	/* failed file write requests */
};

/* Configuration options */
var maPort	 = 8080;
var maServerName = 'marlin_agent';
var maLogStreams = [ { 'stream': process.stdout } ];
var maFakeMantaRoot = '/var/tmp/fake-manta/';

function usage(errmsg)
{
	if (errmsg)
		console.error(errmsg);

	console.error('usage: node agent.js [-o logfile] [-p port]');
	process.exit(2);
}

function main()
{
	var parser = new mod_getopt.BasicParser('o:p:', process.argv);
	var option;

	while ((option = parser.getopt()) !== undefined) {
		if (option.error)
			usage();

		if (option.option == 'o') {
			maLogStreams = [ { 'path': option.optarg } ];
			console.log('logging to %s', option.optarg);
			continue;
		}

		if (option.option == 'p') {
			maPort = parseInt(option.optarg, 10);
			if (isNaN(maPort) || maPort < 0 || maPort > 65535)
				usage('invalid port');
			continue;
		}
	}

	maLog = new mod_bunyan({
	    'name': maServerName,
	    'streams': maLogStreams,
	    'serializers': {
		'task': function (task) { return (task.logKey()); }
	    }
	});

	maServer = mod_restify.createServer({
	    'name': maServerName,
	    'log': maLog
	});

	maServer.use(function (request, response, next) {
		maRequests[request['id']] = request;
		next();
	});
	maServer.use(mod_restify.acceptParser(maServer.acceptable));
	maServer.use(mod_restify.queryParser());
	maServer.use(mod_restify.bodyParser({ 'mapParams': false }));

	maServer.on('uncaughtException', mod_mautil.maRestifyPanic);
	maServer.on('after', mod_restify.auditLogger({
	    'body': true,
	    'log': maLog
	}));
	maServer.on('after', function (request, response) {
		delete (maRequests[request['id']]);
	});
	maServer.on('error', function (err) {
		maLog.fatal(err, 'failed to start server: %s', err.message);
		process.exit(1);
	});

	maServer.post('/tasks', maHttpTasksCreate);
	maServer.get('/tasks', maHttpTasksList);
	maServer.get('/tasks/:taskid', maHttpTasksState);
	maServer.post('/zones', maHttpZonesAdd);
	maServer.get('/zones', maHttpZonesList);

	maServer.get('/kang/.*', mod_kang.knRestifyHandler({
	    'uri_base': '/kang',
	    'service_name': 'marlin',
	    'component': 'agent',
	    'ident': mod_os.hostname(),
	    'version': '0',
	    'list_types': maKangListTypes,
	    'list_objects': maKangListObjects,
	    'get': maKangGetObject,
	    'stats': maKangStats
	}));

	maServer.listen(maPort, function () {
		maLog.info('server listening on port %d', maPort);
	});

	mod_agent_zone.maZoneApiCallback(maTaskApiSetup);
}

/*
 * maTask: agent representation of task state.  This object is initially
 * constructed from one of two paths:
 *
 *    (1) a new task is dispatched to this node from a Job Manager
 *
 *    (2) upon agent startup, when we reload the state of all current jobs
 *
 * Both paths pass the following arguments:
 *
 *    job		Job definition (see Jobs API doc)
 *
 *    taskId		unique task identifier
 *
 *    taskInputKeys	input keys
 *
 *    taskPhasenum	phase number for this task
 *
 *    taskHost		hostname on which this task is started
 *
 *    taskOutputKeys	output keys
 *
 *    taskPartialKeys	emitted but not yet committed objects
 *
 *    taskState		"queued", "loading", "running", or "done"
 *
 * The reload path may also pass these arguments:
 * XXX but this is not yet implemented.
 *
 *    taskMachine	compute zone we've assigned to this job (if any)
 *
 *    taskStartTime	time when the task started running (if started)
 *
 *    taskEndTime	time when the task completed (if completed)
 *
 *    taskResult	"success" or "failure" (if completed)
 */
function maTask(args)
{
	/*
	 * The following job and task fields should always be present.
	 */
	this.t_job_id = args['job']['jobId'];
	this.t_job_name = args['job']['jobName'];
	this.t_job_phases = mod_jsprim.deepCopy(args['job']['phases']);

	this.t_id = args['taskId'];
	this.t_input = args['taskInputKeys'].slice(0);
	this.t_donekeys = [];
	this.t_failkeys = [];
	this.t_phasenum = args['taskPhasenum'];
	this.t_host = args['taskHost'];
	this.t_output = args['taskOutputKeys'];
	this.t_partial = args['taskPartialKeys'];
	this.t_state = args['taskState'];

	/* Dynamic fields. */
	this.t_phase = this.t_job_phases[this.t_phasenum];
	this.t_log = maLog.child({ 'task': this });
	this.t_load_assets = undefined;
}

/* task states */
maTask.TASK_S_INIT	= 'init';
maTask.TASK_S_QUEUED	= 'queued';
maTask.TASK_S_LOADING	= 'loading';
maTask.TASK_S_RUNNING	= 'running';
maTask.TASK_S_DONE	= 'done';

maTask.prototype.httpState = function ()
{
	var rv = {
	    'job': {
		'jobId': this.t_job_id,
		'jobName': this.t_job_name,
		'jobPhases': this.t_job_phases
	    },
	    'taskId': this.t_id,
	    'taskHost': this.t_host,
	    'taskPhasenum': this.t_phasenum,
	    'taskState': this.t_state,
	    'taskInputKeys': this.t_input,
	    'taskDoneKeys': this.t_donekeys,
	    'taskFailedKeys': this.t_failkeys,
	    'taskOutputKeys': this.t_output,
	    'taskPartialKeys': this.t_partial
	};

	if (this.t_machine)
		rv['taskMachine'] = this.t_machine;

	if (this.t_start)
		rv['taskStart'] = this.t_start;

	if (this.t_done)
		rv['taskDone'] = this.t_done;

	if (this.t_result)
		rv['taskResult'] = this.t_result;

	return (rv);
};

maTask.prototype.logKey = function ()
{
	return ({
	    'jobId': this.t_job_id,
	    'jobName': this.t_job_name,
	    'taskId': this.t_taskId,
	    'taskState': this.t_state
	});
};

/*
 * Dispatches queued tasks to available zones until we run out of work to do it
 * or zones to do it in.
 */
function maTasksPoke()
{
	var zone, task;

	while (maZonesReady.length > 0 && maTasksQueued.length > 0) {
		zone = maZonesReady.shift();
		mod_assert.equal(zone.z_state,
		    mod_agent_zone.maZone.ZONE_S_READY);
		mod_assert.ok(zone.z_task === undefined);

		task = maTasksQueued.shift();
		mod_assert.equal(task.t_state, maTask.TASK_S_QUEUED);
		mod_assert.ok(task.t_machine === undefined);

		zone.z_task = task;
		zone.z_state = mod_agent_zone.maZone.ZONE_S_BUSY;
		task.t_machine = zone.z_zonename;
		task.t_state = maTask.TASK_S_LOADING;
		task.t_start = new Date();

		task.t_log.info('loading assets', task.t_phase);
		maTaskLoadAssets(task, function (err) {
			if (err) {
				task.t_log.error(err, 'failed to load assets');
				maTaskMarkDone(task, 'failure');
				return;
			}

			task.t_log.info('dispatching task');
			maTaskDispatch(task);
		});
	}
}

/*
 * Loads the task's assets into its assigned zone.  "next" is invoked only if
 * there are no errors.
 */
function maTaskLoadAssets(task, callback)
{
	mod_assert.equal(task.t_state, maTask.TASK_S_LOADING);

	if (!task.t_phase.hasOwnProperty('assets')) {
		callback();
		return;
	}

	/*
	 * XXX A very large number of assets here could cause us to use lots of
	 * file descriptors, a potential source of DoS.  forEachParallel could
	 * have a maxConcurrency property that queues work.
	 */
	task.t_load_assets = mod_vasync.forEachParallel({
	    'inputs': task.t_phase['assets'],
	    'func': maTaskLoadAsset.bind(null, task)
	}, function (err) {
		task.t_load_assets = undefined;
		callback(err);
	});
}

/*
 * Loads one asset for the given task into its compute zone.
 */
function maTaskLoadAsset(task, asset, callback)
{
	var zone = maZones[task.t_machine];
	var srcpath = mod_path.join(maFakeMantaRoot, asset);
	var dstpath = mod_path.join(zone.z_root, 'assets', asset);

	mod_mkdirp(mod_path.dirname(dstpath), function (err) {
		if (err) {
			callback(err);
			return;
		}

		var input = mod_fs.createReadStream(srcpath);
		var output = mod_fs.createWriteStream(dstpath,
		    { 'mode': 0777 });
		mod_util.pump(input, output, callback);
	});
}

/*
 * Actually dispatches the given task to its assigned zone.
 */
function maTaskDispatch(task)
{
	var zone, waiters;

	zone = maZones[task.t_machine];
	mod_assert.equal(task.t_state, maTask.TASK_S_LOADING);
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_BUSY);

	task.t_state = maTask.TASK_S_RUNNING;
	maCounters['tasks_dispatched']++;

	waiters = zone.z_waiters;
	zone.z_waiters = [];

	waiters.forEach(function (w) {
		maTaskApiTask(zone, w['request'], w['response'], w['next']);
	});
}

/*
 * Check whether the given task has finished and update it appropriately if so.
 */
function maTaskCheckDone(task)
{
	var result;

	if (task.t_donekeys.length + task.t_failkeys.length !=
	    task.t_input.length)
		return;

	if (task.t_failkeys.length > 0) {
		result = 'failure';
		maCounters['tasks_fail']++;
	} else {
		result = 'success';
		maCounters['tasks_ok']++;
	}

	maTaskMarkDone(task, result);
}

/*
 * Mark the given task finished, remove it from the pending tasks table, and
 * make the zone in which it was running ready for reuse.
 */
function maTaskMarkDone(task, result)
{
	var zone;

	task.t_result = result;
	task.t_state = maTask.TASK_S_DONE;
	task.t_done = new Date();
	task.t_log.info('task completed with result "%s"', result);

	mod_assert.ok(maTasks.hasOwnProperty(task.t_id));
	delete (maTasks[task.t_id]);
	maRecentTasks.push(task);
	if (maRecentTasks.length > nRecentTasks)
		maRecentTasks.shift();

	zone = maZones[task.t_machine];
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_BUSY);

	zone.z_state = mod_agent_zone.maZone.ZONE_S_UNINIT;
	zone.z_task = undefined;
	mod_agent_zone.maZoneMakeReady(zone, maZoneReady);
}

/*
 * Invoked as a callback when the given zone transitions to the "ready" state.
 */
function maZoneReady(zone)
{
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_READY);
	maZonesReady.push(zone);
	maCounters['zones_readied']++;
	maTasksPoke();
}

/*
 * HTTP entry points
 */
var maTaskInputSchema = {
    'type': 'object',
    'properties': {
	'job': mod_job.mlJobSchema,
	'taskId': { 'type': 'string', 'required': true },
	'taskInputKeys': mod_job.mlJobSchema['properties']['inputKeys'],
	'taskPhasenum': {
	    'type': 'integer',
	    'required': true
	}
    }
};

/* POST /tasks */
function maHttpTasksCreate(request, response, next)
{
	var args, error, taskargs, task;

	args = request.body || {};
	error = mod_jsprim.validateJsonObject(maTaskInputSchema, args);

	if (error) {
		error = new mod_restify.InvalidArgumentError(error.message);
	} else if (args['taskPhasenum'] >= args['job']['phases'].length) {
		error = new mod_restify.InvalidArgumentError(
		    'taskPhasenum: must be a valid job phase');
	} else if (maTasks.hasOwnProperty(args['taskId'])) {
		error = new mod_restify.InvalidArgumentError(
		    'taskId: task already exists');
	}

	if (error) {
		next(error);
		return;
	}

	taskargs = Object.create(args);
	taskargs['taskHost'] = mod_os.hostname();
	taskargs['taskOutputKeys'] = [];
	taskargs['taskPartialKeys'] = [];
	taskargs['taskState'] = maTask.TASK_S_INIT;

	task = new maTask(taskargs);
	maTasks[task.t_id] = task;
	maCounters['tasks_submitted']++;

	task.t_state = maTask.TASK_S_QUEUED;
	task.t_log.info('enqueued new task');
	maTasksQueued.push(task);

	response.send(task.httpState());
	next();

	maTasksPoke();
}

/* GET /tasks */
function maHttpTasksList(request, response, next)
{
	var id, task, rv;

	rv = [];

	for (id in maTasks) {
		task = maTasks[id];

		rv.push({
		    'jobId': task.t_job_id,
		    'taskId': task.t_id
		});
	}

	maRecentTasks.forEach(function (rtask) {
		rv.push({
		    'jobId': rtask.t_job_id,
		    'taskId': rtask.t_id
		});
	});

	response.send(rv);
	next();
}

/* GET /tasks/:taskid/ */
function maHttpTasksState(request, response, next)
{
	var taskid, task, i;

	taskid = request.params['taskid'];
	if (maTasks.hasOwnProperty(taskid)) {
		task = maTasks[taskid];
	} else {
		for (i = 0; i < maRecentTasks.length; i++) {
			if (maRecentTasks[i].t_id == taskid) {
				task = maRecentTasks[i];
				break;
			}
		}
	}

	if (task === undefined) {
		next(new mod_restify.NotFoundError('no such task'));
		return;
	}

	response.send(task.httpState());
	next();
}

/* POST /zones */
function maHttpZonesAdd(request, response, next)
{
	var zonename, zone;

	if (!request.query.hasOwnProperty('zonename') ||
	    request.query['zonename'].length === 0) {
		response.send(new mod_restify.InvalidArgumentError(
		    'missing argument: "zonename"'));
		next();
		return;
	}

	zonename = request.query['zonename'];

	if (maZones.hasOwnProperty(zonename)) {
		response.send(new mod_restify.InvalidArgumentError(
		    'attempted to add duplicate zone ' + zonename));
		next();
		return;
	}

	request.log.info('creating zone "%s"', zonename);
	zone = mod_agent_zone.maZoneAdd(zonename, maLog);
	maZones[zone.z_zonename] = zone;
	maCounters['zones_added']++;
	mod_agent_zone.maZoneMakeReady(zone, maZoneReady);

	response.send(204);
	next();
}

/* GET /zones */
function maHttpZonesList(request, response, next)
{
	var rv = [];

	for (var zonename in maZones)
		rv.push(maZones[zonename].httpState());

	response.send(rv);
	next();
}

/*
 * Task Control API entry points
 */

function maTaskApiSetup(zone, s)
{
	s.use(mod_restify.acceptParser(s.acceptable));
	s.use(mod_restify.queryParser());

	s.on('uncaughtException', mod_mautil.maRestifyPanic);
	s.on('after', mod_restify.auditLogger({ 'log': zone.z_log }));

	/*
	 * We prepend the body parser to everything except PUT /object/...,
	 * since that streams potentially large amounts of arbitrary data.
	 */
	s.get('/task',
	    mod_restify.bodyParser({ 'mapParams': false }),
	    maTaskApiTask.bind(null, zone));
	s.post('/commit',
	    mod_restify.bodyParser({ 'mapParams': false }),
	    maTaskApiCommit.bind(null, zone));
	s.post('/fail',
	    mod_restify.bodyParser({ 'mapParams': false }),
	    maTaskApiFail.bind(null, zone));

	/*
	 * We proxy the entire Manta API under /object.
	 */
	s.get('/object/.*', maTaskApiManta.bind(null, zone));
	s.put('/object/.*', maTaskApiManta.bind(null, zone));
	s.post('/object/.*', maTaskApiManta.bind(null, zone));
	s.del('/object/.*', maTaskApiManta.bind(null, zone));
	s.head('/object/.*', maTaskApiManta.bind(null, zone));
}

/*
 * GET /task: fetch the task currently assigned to this zone.  By default, this
 * entry point always returns immediately, either with 200 plus the task or 204
 * if no task is assigned right now.  The in-zone agent invokes this with
 * wait=true, which means to block until a task is available.
 */
function maTaskApiTask(zone, request, response, next)
{
	if (zone.z_task && zone.z_task.t_state == maTask.TASK_S_RUNNING) {
		response.send(zone.z_task.httpState());
		next();
		return;
	}

	if (request.query['wait'] != 'true') {
		response.send(204);
		next();
		return;
	}

	zone.z_waiters.push({
	    'time': Date.now(),
	    'request': request,
	    'response': response,
	    'next': next
	});
}

/*
 * POST /commit: indicate that the given key has been successfully processed.
 */
function maTaskApiCommit(zone, request, response, next)
{
	/* XXX validate task state */
	var body = request.body || {};
	var task = zone.z_task;

	if (!body.hasOwnProperty('key')) {
		response.send(new mod_restify.InvalidArgumentError(
		    '"key" is required'));
		next();
		return;
	}

	/* XXX update partialKeys */

	/*
	 * XXX This should handle duplicate commits better, as well as the case
	 * where a key gets committed after previously having been failed or
	 * vice versa.
	 */
	task.t_donekeys.push(body['key']);
	maCounters['keys_committed']++;
	maTaskCheckDone(task);
	response.send(204);
	next();
}

/*
 * POST /fail: indicate that the given task failed to process a particular key.
 */
function maTaskApiFail(zone, request, response, next)
{
	/* XXX validate task state */
	var body = request.body || {};
	var task = zone.z_task;

	if (!body.hasOwnProperty('key')) {
		/*
		 * We consider it an error to not specify a key, since this
		 * request comes from our in-zone agent, who should know better.
		 * We could support this to mean "something horrible has gone
		 * wrong which has nothing to do with the current key".
		 */
		response.send(new mod_restify.InvalidArgumentError(
		    '"key" is required'));
		next();
		return;
	}

	/* XXX update partialKeys */

	/*
	 * XXX This should handle duplicate fails better, as well as the case
	 * where a key gets committed after previously having been failed or
	 * vice versa.
	 */
	task.t_failkeys.push(body['key']);
	maCounters['keys_failed']++;
	maTaskCheckDone(task);
	response.send(204);
	next();
}

/*
 * /object/:key: We proxy the entire Manta API here.  The only thing that we do
 * specially here is note objects that get created so we can "commit" them only
 * if the task successfully completes.
 */
function maTaskApiManta(zone, request, response, next)
{
	/* XXX validate task state */
	/* XXX audit logger shouldn't include body. */
	var task = zone.z_task;
	var path = request.url.substr('/object/'.length);

	if (request.method != 'PUT') {
		/* XXX */
		next(new mod_restify.ConflictError('not yet implemented'));
		return;
	}

	/*
	 * XXX The way this should eventually work is that we make the request
	 * to Manta, and when we get the 100-continue to start sending data,
	 * *then* we commit the object to the task's "partialKeys" set.  (Until
	 * then, it might be totally invalid -- even an invalid key name, which
	 * we wouldn't want to add to partialKeys.)
	 *
	 * Until we have a Manta backend to test against, we just store
	 * everything in /var/tmp.  We mimic the above behavior by doing the
	 * validation here, then updating partialKeys, then writing the file
	 * out.
	 */
	if (path.length === 0) {
		next(new mod_restify.InvalidArgumentError('cannot write "/"'));
		return;
	}

	if (path[path.length - 1] == '/') {
		next(new mod_restify.InvalidArgumentError(
		    'object name cannot end with "/"'));
		return;
	}

	/*
	 * Check for attempts to write outside the allowed hierarchy.  We assume
	 * that the allowed hierarchy only contains files and regular
	 * directories, and so can only be escaped via "..", so it's sufficient
	 * to normalize the path and validate that it's under the allowed
	 * hierarchy.
	 */
	var fspath = mod_path.normalize(maFakeMantaRoot + path);

	if (fspath.substr(0, maFakeMantaRoot.length) != maFakeMantaRoot) {
		next(new mod_restify.InvalidArgumentError(
		    'invalid object path'));
		return;
	}

	maCounters['file_writes_started']++;
	request.maError = false;

	/* XXX handle duplicate writes to this key */
	task.t_partial.push(path);

	var onerror = function (err) {
		if (request.maError)
			return;

		maCounters['file_writes_fail']++;
		request.maError = true;
		request.log.error(err);
		next(err);
	};

	var outstream = mod_fs.createWriteStream(fspath, { 'mode': 0777 });
	outstream.on('error', onerror);
	request.on('error', onerror);

	request.pipe(outstream);
	request.on('end', function () {
		if (request.maError)
			return;

		maCounters['file_writes_ok']++;
		response.send(201);
		next();
	});
}

/*
 * Kang (introspection) entry points
 */
function maKangListTypes()
{
	return ([ 'zones', 'pending_requests', 'pending_tasks',
	    'recent_tasks' ]);
}

function maKangListObjects(type)
{
	if (type == 'pending_tasks')
		return (Object.keys(maTasks));

	if (type == 'recent_tasks')
		/* JSSTYLED */
		return (maRecentTasks.map(function (_, i) { return (i); }));

	if (type == 'pending_requests')
		return (Object.keys(maRequests));

	return (Object.keys(maZones));
}

function maKangGetObject(type, id)
{
	if (type == 'pending_tasks') {
		var task = maTasks[id];
		var rv = task.httpState();
		if (task.t_load_assets)
			rv['loadAssets'] = task.t_load_assets;
		return (rv);
	}

	if (type == 'recent_tasks')
		return (maRecentTasks[id].httpState());

	if (type == 'zones')
		return (maZones[id].httpState());

	var request = maRequests[id];

	return ({
	    id: request.id,
	    method: request.method,
	    url: request.url,
	    time: request.time
	});
}

function maKangStats()
{
	return (maCounters);
}

main();
