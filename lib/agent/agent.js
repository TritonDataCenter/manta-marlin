/*
 * lib/agent/agent.js: compute node Marlin agent
 *
 * The agent runs in the global zone of participating compute and storage nodes
 * and manages tasks run on that node.  It's responsible for setting up compute
 * zones for user jobs, executing the jobs, monitoring the user code, tearing
 * down the zones, and emitting progress updates to the appropriate job manager.
 */

var mod_assert = require('assert');
var mod_os = require('os');
var mod_path = require('path');

var mod_bunyan = require('bunyan');
var mod_jsprim = require('jsprim');
var mod_kang = require('kang');
var mod_getopt = require('posix-getopt');
var mod_restify = require('restify');

var mod_job = require('../job');
var mod_mautil = require('../util');

var mod_agent_zone = require('./zone');

/* Global agent state. */
var maTasks = {};		/* all tasks, by taskid */
var maTasksQueued = [];		/* waiting tasks */
var maZoneManager;		/* global zone manager */
var maLog;			/* global logger */
var maServer;			/* global restify server */
var maCounters = {
    'tasks_submitted': 0,	/* tasks received */
    'tasks_dispatched': 0,	/* tasks dispatched to a zone */
    'tasks_ok': 0,		/* tasks completed successfully */
    'tasks_fail': 0,		/* tasks that failed fatally */
    'keys_committed': 0		/* individual keys committed */
    /* XXX zone counters */
};

/* Configuration options */
var maPort	 = 8080;
var maServerName = 'marlin_agent';
var maLogStreams = [ { 'stream': process.stdout } ];

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

	maServer.use(mod_restify.acceptParser(maServer.acceptable));
	maServer.use(mod_restify.queryParser());
	maServer.use(mod_restify.bodyParser({ 'mapParams': false }));

	maServer.on('uncaughtException', mod_mautil.maRestifyPanic);
	maServer.on('after', mod_restify.auditLogger({ 'log': maLog }));
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

	maZoneManager = mod_agent_zone.maZoneInit(maLog);
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
	this.t_phasenum = args['taskPhasenum'];
	this.t_host = args['taskHost'];
	this.t_output = args['taskOutputKeys'];
	this.t_partial = args['taskPartialKeys'];
	this.t_state = args['taskState'];

	/* Dynamic fields. */
	this.t_phase = this.t_job_phases[this.t_phasenum];
	this.t_log = maLog.child({ 'task': this });
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

maTask.prototype.commitInputKey = function (key, callback)
{
	/*
	 * XXX this should be more intelligent about duplicate keys.  Also, this
	 * may be better off outside this class.  See caller.
	 */
	this.t_donekeys.push(key);
	maCounters['keys_committed']++;

	if (this.t_donekeys.length == this.t_input.length) {
		this.t_state = maTask.TASK_S_DONE;
		this.t_done = new Date();
		this.t_result = 'success';
		maCounters['tasks_ok']++;
	}

	callback();
};

maTask.prototype.fail = function (callback)
{
	this.t_state = maTask.TASK_S_DONE;
	this.t_done = new Date();
	this.t_result = 'failure';
	maCounters['tasks_fail']++;
	callback();
};

/*
 * Instantiates a new task for the given input arguments.
 */
function maTaskCreate(args)
{
	var taskargs;

	taskargs = Object.create(args);
	taskargs['taskHost'] = mod_os.hostname();
	taskargs['taskOutputKeys'] = [];
	taskargs['taskPartialKeys'] = [];
	taskargs['taskState'] = maTask.TASK_S_INIT;

	return (new maTask(taskargs));
}

/*
 * If a suitable compute zone is available to process the given task, dispatches
 * the task to that zone.  Otherwise enqueues the task for dispatch when such a
 * zone becomes available.
 */
function maTaskInsert(task)
{
	mod_assert.ok(task.t_state == maTask.TASK_S_INIT);
	maCounters['tasks_submitted']++;

	var zone = maZoneManager.avail();

	if (!zone) {
		task.t_state = maTask.TASK_S_QUEUED;
		task.t_log.info('enqueued task (no suitable zones available)');
		maTasksQueued.push(task);
		return;
	}

	task.t_machine = zone.z_zonename; /* XXX private */
	task.t_state = maTask.TASK_S_LOADING;
	task.t_start = new Date();

	maZoneManager.zoneRunTask(zone, task);
	maCounters['tasks_dispatched']++;
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
	var args, error, task;

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

	task = maTaskCreate(args);
	maTasks[task.t_id] = task;

	maTaskInsert(task);
	response.send(task.httpState());
	next();
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

	response.send(rv);
	next();
}

/* GET /tasks/:taskid/ */
function maHttpTasksState(request, response, next)
{
	if (!maTasks.hasOwnProperty(request.params['taskid'])) {
		next(new mod_restify.NotFoundError('no such task'));
		return;
	}

	var task = maTasks[request.params['taskid']];
	response.send(task.httpState());
	next();
}

/* POST /zones */
function maHttpZonesAdd(request, response, next)
{
	if (!request.query.hasOwnProperty('zonename') ||
	    request.query['zonename'].length === 0) {
		response.send(new mod_restify.InvalidArgumentError(
		    'missing argument: "zonename"'));
		next();
		return;
	}

	maZoneManager.addZone(request.query['zonename']);
	response.send(204);
	next();
}

/* GET /zones */
function maHttpZonesList(request, response, next)
{
	/* XXX private */
	var rv = [];

	for (var zonename in maZoneManager.mz_zones)
		rv.push(maZoneManager.mz_zones[zonename].httpState());

	response.send(rv);
	next();
}

/*
 * Kang (introspection) entry points
 */
function maKangListTypes()
{
	return ([ 'tasks', 'zones' ]);
}

function maKangListObjects(type)
{
	if (type == 'tasks')
		return (Object.keys(maTasks));

	return (Object.keys(maZoneManager.mz_zones));
}

function maKangGetObject(type, id)
{
	if (type == 'tasks')
		return (maTasks[id].httpState());

	return (maZoneManager.mz_zones[id].httpState());
}

function maKangStats()
{
	return (maCounters);
}

main();
