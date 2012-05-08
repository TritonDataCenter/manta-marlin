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
var mod_http = require('http');
var mod_os = require('os');
var mod_path = require('path');
var mod_url = require('url');
var mod_util = require('util');

var mod_bunyan = require('bunyan');
var mod_getopt = require('posix-getopt');
var mod_jsprim = require('jsprim');
var mod_kang = require('kang');
var mod_mkdirp = require('mkdirp');
var mod_panic = require('panic');
var mod_restify = require('restify');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_job = require('../job');
var mod_mautil = require('../util');
var mod_agent_zone = require('./zone');

/* Global agent state. */
var maTaskGroups = {};		/* all task groups, by taskid */
var maTaskGroupsQueued = [];	/* waiting task groups */
var maRecentTaskGroups = [];	/* recently completed task groups */
var maNRecentTaskGroups = 10;	/* number of recent task groups to keep */
var maZones = {};		/* all zones, by zonename */
var maZonesReady = [];		/* ready zones */
var maLog;			/* global logger */
var maServer;			/* global restify server */
var maRequests = {};		/* pending requests, by id */
var maCounters = {
    'taskgroups_submitted': 0,	/* task groups received */
    'taskgroups_dispatched': 0,	/* task groups dispatched to a zone */
    'taskgroups_ok': 0,		/* task groups completed successfully */
    'taskgroups_fail': 0,	/* task groups that failed fatally */
    'keys_failed': 0,		/* individual keys that failed */
    'keys_committed': 0,	/* individual keys committed */
    'zones_added': 0,		/* zones added to Marlin */
    'zones_readied': 0,		/* zones transitioned to "ready" */
    'zones_disabled': 0,	/* zones permanently out of commission */
    'mantarq_proxy_sent': 0,	/* requests forwarded to Manta */
    'mantarq_proxy_return': 0	/* responses received from Manta */
};

/* Configuration options */
var maPort	 = 8080;
var maServerName = 'marlin_agent';
var maLogStreams = [ { 'stream': process.stdout, 'level': 'info' } ];
var maMantaHost  = 'localhost';
var maMantaPort  = 8081;

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
		'taskgroup': function (taskgroup) {
		    return (taskgroup.logKey());
		}
	    }
	});

	mod_panic.enablePanicOnCrash({
	    'skipDump': true,
	    'abortOnPanic': true
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

	maServer.post('/taskgroups', maHttpTaskGroupsCreate);
	maServer.get('/taskgroups', maHttpTaskGroupsList);
	maServer.get('/taskgroups/:taskgroupid', maHttpTaskGroupsState);
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
 * maTaskGroup: agent representation of task group state.  This object is
 * constructed when a new task group is dispatched to this node.  Arguments:
 *
 *    job			Job definition (see Jobs API doc)
 *
 *    taskGroupId		unique task group identifier
 *
 *    taskGroupInputKeys	input keys
 *
 *    taskGroupPhasenum		phase number for this task group
 *
 *    taskGroupHost		hostname on which this task group is started
 *
 *    taskGroupOutputKeys	output keys
 *
 *    taskGroupPartialKeys	emitted but not yet committed objects
 *
 *    taskGroupState		"queued", "loading", "running", or "done"
 */
function maTaskGroup(args)
{
	this.t_job_id = args['job']['jobId'];
	this.t_job_name = args['job']['jobName'];
	this.t_job_phases = mod_jsprim.deepCopy(args['job']['phases']);

	this.t_id = args['taskGroupId'];
	this.t_input = args['taskGroupInputKeys'].slice(0);
	this.t_donekeys = [];
	this.t_failkeys = [];
	this.t_discardedkeys = [];
	this.t_phasenum = args['taskGroupPhasenum'];
	this.t_host = args['taskGroupHost'];
	this.t_outputkeys = args['taskGroupOutputKeys'];
	this.t_partial = args['taskGroupPartialKeys'];
	this.t_state = args['taskGroupState'];

	/*
	 * The "seen" fields are like their list counterparts above, but are
	 * used as objects to deduplicate entries in the list.  We keep the
	 * list, too, in order to preserve the order of entries added.
	 */
	this.t_seendone = {};
	this.t_seenfail = {};
	this.t_seenpartial = {};
	this.t_seenoutput = {};
	this.t_seendiscarded = {};

	/* Dynamic fields. */
	this.t_phase = this.t_job_phases[this.t_phasenum];
	this.t_log = maLog.child({ 'taskgroup': this });
	this.t_load_assets = undefined;
}

/* task states */
maTaskGroup.TASKGROUP_S_INIT	= 'init';
maTaskGroup.TASKGROUP_S_QUEUED	= 'queued';
maTaskGroup.TASKGROUP_S_LOADING	= 'loading';
maTaskGroup.TASKGROUP_S_RUNNING	= 'running';
maTaskGroup.TASKGROUP_S_DONE	= 'done';

maTaskGroup.prototype.httpState = function ()
{
	var rv = {
	    'job': {
		'jobId': this.t_job_id,
		'jobName': this.t_job_name,
		'jobPhases': this.t_job_phases
	    },
	    'taskGroupId': this.t_id,
	    'taskGroupHost': this.t_host,
	    'taskGroupPhasenum': this.t_phasenum,
	    'taskGroupState': this.t_state,
	    'taskGroupInputKeys': this.t_input,
	    'taskGroupDoneKeys': this.t_donekeys,
	    'taskGroupFailedKeys': this.t_failkeys,
	    'taskGroupOutputKeys': this.t_outputkeys,
	    'taskGroupPartialKeys': this.t_partial,
	    'taskGroupDiscardedKeys': this.t_discardedkeys
	};

	if (this.t_machine)
		rv['taskGroupMachine'] = this.t_machine;

	if (this.t_start)
		rv['taskGroupStart'] = this.t_start;

	if (this.t_done)
		rv['taskGroupDone'] = this.t_done;

	if (this.t_result)
		rv['taskGroupResult'] = this.t_result;

	return (rv);
};

maTaskGroup.prototype.logKey = function ()
{
	return ({
	    'jobId': this.t_job_id,
	    'jobName': this.t_job_name,
	    'taskGroupId': this.t_id,
	    'taskGroupState': this.t_state
	});
};

/*
 * Dispatches queued task groups to available zones until we run out of work to
 * do it or zones to do it in.
 */
function maTaskGroupsPoke()
{
	var zone, taskgroup;

	while (maZonesReady.length > 0 && maTaskGroupsQueued.length > 0) {
		zone = maZonesReady.shift();
		mod_assert.equal(zone.z_state,
		    mod_agent_zone.maZone.ZONE_S_READY);
		mod_assert.ok(zone.z_taskgroup === undefined);

		taskgroup = maTaskGroupsQueued.shift();
		mod_assert.equal(taskgroup.t_state,
		    maTaskGroup.TASKGROUP_S_QUEUED);
		mod_assert.ok(taskgroup.t_machine === undefined);

		zone.z_taskgroup = taskgroup;
		zone.z_state = mod_agent_zone.maZone.ZONE_S_BUSY;
		taskgroup.t_machine = zone.z_zonename;
		taskgroup.t_state = maTaskGroup.TASKGROUP_S_LOADING;
		taskgroup.t_start = new Date();

		taskgroup.t_log.info('loading assets', taskgroup.t_phase);
		maTaskGroupLoadAssets(taskgroup, function (err) {
			if (err) {
				taskgroup.t_log.error(err,
				    'failed to load assets');
				maTaskGroupMarkDone(taskgroup, 'failure');
				return;
			}

			taskgroup.t_log.info('dispatching taskgroup');
			maTaskGroupDispatch(taskgroup);
		});
	}
}

/*
 * Loads the task's assets into its assigned zone.  "next" is invoked only if
 * there are no errors.
 */
function maTaskGroupLoadAssets(taskgroup, callback)
{
	mod_assert.equal(taskgroup.t_state, maTaskGroup.TASKGROUP_S_LOADING);

	if (!taskgroup.t_phase.hasOwnProperty('assets')) {
		callback();
		return;
	}

	/*
	 * XXX A very large number of assets here could cause us to use lots of
	 * file descriptors, a potential source of DoS.  forEachParallel could
	 * have a maxConcurrency property that queues work.
	 */
	taskgroup.t_load_assets = mod_vasync.forEachParallel({
	    'inputs': taskgroup.t_phase['assets'],
	    'func': maTaskGroupLoadAsset.bind(null, taskgroup)
	}, function (err) {
		taskgroup.t_load_assets = undefined;
		callback(err);
	});
}

/*
 * Loads one asset for the given task group into its compute zone.
 */
function maTaskGroupLoadAsset(taskgroup, asset, callback)
{
	var zone = maZones[taskgroup.t_machine];
	var dstpath = mod_path.join(zone.z_root, 'assets', asset);

	mod_mkdirp(mod_path.dirname(dstpath), function (err) {
		if (err) {
			callback(err);
			return;
		}

		var output = mod_fs.createWriteStream(dstpath,
		    { 'mode': 0777 });

		output.on('error', callback);

		output.on('open', function () {
			var request = mod_http.get({
			    'host': maMantaHost,
			    'port': maMantaPort,
			    'path': asset
			});

			request.on('error', function (suberr) {
				output.end();
				callback(suberr);
			});

			request.on('response', function (response) {
				if (response.statusCode != 200) {
					output.end();
					callback(new mod_verror.VError(null,
					    'error retrieving asset ' +
					    '(status code %s)',
					    response.statusCode));
					return;
				}

				response.pipe(output);
				response.on('end', callback);
			});
		});
	});
}

/*
 * Actually dispatches the given task to its assigned zone.
 */
function maTaskGroupDispatch(taskgroup)
{
	var zone, waiters;

	zone = maZones[taskgroup.t_machine];
	mod_assert.equal(taskgroup.t_state, maTaskGroup.TASKGROUP_S_LOADING);
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_BUSY);

	taskgroup.t_state = maTaskGroup.TASKGROUP_S_RUNNING;
	maCounters['taskgroups_dispatched']++;

	waiters = zone.z_waiters;
	zone.z_waiters = [];

	waiters.forEach(function (w) {
		maTaskApiTask(w['request'], w['response'], w['next']);
	});
}

/*
 * Check whether the given task group has finished and update it appropriately.
 */
function maTaskGroupCheckDone(taskgroup)
{
	var result;

	if (taskgroup.t_donekeys.length + taskgroup.t_failkeys.length !=
	    taskgroup.t_input.length)
		return;

	if (taskgroup.t_failkeys.length > 0) {
		result = 'failure';
		maCounters['taskgroups_fail']++;
	} else {
		result = 'success';
		maCounters['taskgroups_ok']++;
	}

	maTaskGroupMarkDone(taskgroup, result);
}

/*
 * Mark the given task group finished, remove it from the pending tasks table,
 * and make the zone in which it was running ready for reuse.
 */
function maTaskGroupMarkDone(taskgroup, result)
{
	var zone;

	taskgroup.t_result = result;
	taskgroup.t_state = maTaskGroup.TASKGROUP_S_DONE;
	taskgroup.t_done = new Date();
	taskgroup.t_log.info('taskgroup completed with result "%s"', result);

	mod_assert.ok(maTaskGroups.hasOwnProperty(taskgroup.t_id));
	delete (maTaskGroups[taskgroup.t_id]);
	maRecentTaskGroups.unshift(taskgroup);
	if (maRecentTaskGroups.length > maNRecentTaskGroups)
		maRecentTaskGroups.pop();

	zone = maZones[taskgroup.t_machine];
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_BUSY);

	zone.z_state = mod_agent_zone.maZone.ZONE_S_UNINIT;
	zone.z_taskgroup = undefined;
	mod_agent_zone.maZoneMakeReady(zone, maZoneReady);
}

/*
 * Invoked as a callback when the given zone transitions to the "ready" state
 * (or fails to do so).
 */
function maZoneReady(zone, err)
{
	if (err) {
		mod_assert.equal(zone.z_state,
		    mod_agent_zone.maZone.ZONE_S_DISABLED);
		maCounters['zones_disabled']++;
		zone.z_log.error('zone removed from service because it ' +
		    'could not be made ready');
		return;
	}

	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_READY);
	maCounters['zones_readied']++;
	maZonesReady.push(zone);
	maTaskGroupsPoke();
}

/*
 * HTTP entry points
 */
var maTaskGroupInputSchema = {
    'type': 'object',
    'properties': {
	'job': mod_job.mlJobSchema,
	'taskGroupId': { 'type': 'string', 'required': true },
	'taskGroupInputKeys': mod_job.mlJobSchema['properties']['inputKeys'],
	'taskGroupPhasenum': {
	    'type': 'integer',
	    'required': true
	}
    }
};

/* POST /taskgroups */
function maHttpTaskGroupsCreate(request, response, next)
{
	var args, error, taskargs, taskgroup;

	args = request.body || {};
	error = mod_jsprim.validateJsonObject(maTaskGroupInputSchema, args);

	if (error) {
		error = new mod_restify.InvalidArgumentError(error.message);
	} else if (args['taskGroupPhasenum'] >= args['job']['phases'].length) {
		error = new mod_restify.InvalidArgumentError(
		    'taskGroupPhasenum: must be a valid job phase');
	} else if (maTaskGroups.hasOwnProperty(args['taskGroupId'])) {
		error = new mod_restify.InvalidArgumentError(
		    'taskGroupId: taskgroup already exists');
	}

	if (error) {
		next(error);
		return;
	}

	taskargs = Object.create(args);
	taskargs['taskGroupHost'] = mod_os.hostname();
	taskargs['taskGroupOutputKeys'] = [];
	taskargs['taskGroupPartialKeys'] = [];
	taskargs['taskGroupState'] = maTaskGroup.TASKGROUP_S_INIT;

	taskgroup = new maTaskGroup(taskargs);
	maTaskGroups[taskgroup.t_id] = taskgroup;
	maCounters['taskgroups_submitted']++;

	taskgroup.t_state = maTaskGroup.TASKGROUP_S_QUEUED;
	taskgroup.t_log.info('enqueued new taskgroup');
	maTaskGroupsQueued.push(taskgroup);

	response.send(taskgroup.httpState());
	next();

	maTaskGroupsPoke();
}

/* GET /taskgroups */
function maHttpTaskGroupsList(request, response, next)
{
	var id, taskgroup, rv;

	rv = [];

	for (id in maTaskGroups) {
		taskgroup = maTaskGroups[id];

		rv.push({
		    'jobId': taskgroup.t_job_id,
		    'taskGroupId': taskgroup.t_id
		});
	}

	maRecentTaskGroups.forEach(function (rtask) {
		rv.push({
		    'jobId': rtask.t_job_id,
		    'taskGroupId': rtask.t_id
		});
	});

	response.send(rv);
	next();
}

/* GET /tasksgroups/:taskgroupid/ */
function maHttpTaskGroupsState(request, response, next)
{
	var taskid, taskgroup, i;

	taskid = request.params['taskgroupid'];
	if (maTaskGroups.hasOwnProperty(taskid)) {
		taskgroup = maTaskGroups[taskid];
	} else {
		for (i = 0; i < maRecentTaskGroups.length; i++) {
			if (maRecentTaskGroups[i].t_id == taskid) {
				taskgroup = maRecentTaskGroups[i];
				break;
			}
		}
	}

	if (taskgroup === undefined) {
		next(new mod_restify.NotFoundError('no such taskgroup'));
		return;
	}

	response.send(taskgroup.httpState());
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
	var init = function maTaskApiInit(request, response, next) {
		request.maZone = zone;
		next();
	};

	s.get('/task', mod_restify.bodyParser({ 'mapParams': false }),
	    init, maTaskApiTask);
	s.post('/commit', mod_restify.bodyParser({ 'mapParams': false }),
	    init, maTaskApiValidate, maTaskApiCommit);
	s.post('/fail',
	    mod_restify.bodyParser({ 'mapParams': false }),
	    init, maTaskApiValidate, maTaskApiFail);

	/*
	 * We proxy the entire Manta API under /object.
	 */
	var methods = [ 'get', 'put', 'post', 'del', 'head' ];
	methods.forEach(function (method) {
		s[method]('/object/.*', init,
		    maTaskApiValidate, maTaskApiManta);
	});
}

/*
 * Checks whether the task group associated with the given zone can accept Task
 * Control API requests right now.  If not, fail the given request.
 */
function maTaskApiValidate(request, response, next)
{
	var zone = request.maZone;

	if (zone.z_taskgroup &&
	    zone.z_taskgroup.t_state == maTaskGroup.TASKGROUP_S_RUNNING) {
		next();
	} else {
		next(new mod_restify.ConflictError('invalid zone state'));
	}
}

/*
 * GET /task: fetch the task currently assigned to this zone.  By default, this
 * entry point always returns immediately, either with 200 plus the task or 204
 * if no task is assigned right now.  The in-zone agent invokes this with
 * wait=true, which means to block until a task is available.
 */
function maTaskApiTask(request, response, next)
{
	var zone = request.maZone;

	if (zone.z_taskgroup &&
	    zone.z_taskgroup.t_state == maTaskGroup.TASKGROUP_S_RUNNING) {
		response.send(zone.z_taskgroup.httpState());
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
 * Invoked when we finish processing an input key to merge the emitted keys for
 * this task into either outputKeys or discardedKeys, depending on whether the
 * this input key was successfully processed.
 */
function maTaskGroupMergePartialKeys(taskgroup, success)
{
	var seen, list;

	if (success) {
		list = taskgroup.t_outputkeys;
		seen = taskgroup.t_seenoutput;
	} else {
		list = taskgroup.t_discardedkeys;
		seen = taskgroup.t_seendiscarded;
	}

	taskgroup.t_partial.forEach(function (key) {
		if (!seen.hasOwnProperty(key)) {
			seen[key] = true;
			list.push(key);
		}
	});

	taskgroup.t_partial = [];
	taskgroup.t_seenpartial = {};
}

/*
 * POST /commit: indicate that the given key has been successfully processed.
 */
function maTaskApiCommit(request, response, next)
{
	var body = request.body || {};
	var zone = request.maZone;
	var taskgroup = zone.z_taskgroup;
	var key;

	mod_assert.equal(taskgroup.t_state, maTaskGroup.TASKGROUP_S_RUNNING);

	if (!body.hasOwnProperty('key')) {
		next(new mod_restify.InvalidArgumentError('"key" is required'));
		return;
	}

	key = body['key'];

	if (taskgroup.t_seenfail.hasOwnProperty(key)) {
		next(new mod_restify.ConflictError(
		    'key "' + key + '" has already failed'));
		return;
	}

	if (!taskgroup.t_seendone.hasOwnProperty(key)) {
		maTaskGroupMergePartialKeys(taskgroup, true);
		taskgroup.t_seendone[key] = true;
		taskgroup.t_donekeys.push(body['key']);
		maCounters['keys_committed']++;
		maTaskGroupCheckDone(taskgroup);
	}

	response.send(204);
	next();
}

/*
 * POST /fail: indicate that the given task failed to process a particular key.
 */
function maTaskApiFail(request, response, next)
{
	var body = request.body || {};
	var zone = request.maZone;
	var taskgroup = zone.z_taskgroup;
	var key;

	mod_assert.equal(taskgroup.t_state, maTaskGroup.TASKGROUP_S_RUNNING);

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

	key = body['key'];

	if (taskgroup.t_seendone.hasOwnProperty(key)) {
		next(new mod_restify.ConflictError(
		    'key "' + key + '" has already been committed'));
		return;
	}

	if (!taskgroup.t_seenfail.hasOwnProperty(key)) {
		maTaskGroupMergePartialKeys(taskgroup, false);
		taskgroup.t_seenfail[key] = true;
		taskgroup.t_failkeys.push(body['key']);
		maCounters['keys_failed']++;
		maTaskGroupCheckDone(taskgroup);
	}

	response.send(204);
	next();
}

/*
 * /object/:key: We proxy the entire Manta API here.  The only thing that we do
 * specially here is note objects that get created so we can "commit" them only
 * if the task successfully completes.
 */
function maTaskApiManta(request, response, next)
{
	var zone, taskgroup, proxyargs, path;

	zone = request.maZone;
	taskgroup = zone.z_taskgroup;
	mod_assert.equal(taskgroup.t_state, maTaskGroup.TASKGROUP_S_RUNNING);

	proxyargs = {
	    'request': request,
	    'response': response,
	    'server': {
	        'host': maMantaHost,
		'port': maMantaPort
	    }
	};

	if (request.method != 'PUT') {
		maMantaForward(proxyargs, next);
		return;
	}

	/*
	 * Only upon receiving "continue" from the server do we add this object
	 * to the task's partialKeys.  Until that point, the name itself might
	 * be invalid.  If we fail before the request completes, whether or not
	 * we've seen "continue", the object will not be committed to Manta, and
	 * the user may see a "partial" key that doesn't actually exist.  Either
	 * one of two things should happen: either the client will fail, in
	 * which case the non-existent key will become discarded anyway, or the
	 * client will retry and succeed, in which case the key will exist.
	 */
	path = mod_url.parse(request.url).pathname;
	proxyargs['continue'] = function () {
		if (!taskgroup.t_seenpartial.hasOwnProperty(path)) {
			taskgroup.t_seenpartial[path] = true;
			taskgroup.t_partial.push(path);

			/*
			 * When we add persistence here, we should pause() the
			 * request and resume() it when we've committed this
			 * change to disk so that we cannot crash after having
			 * committed the object to Manta but before we've
			 * committed it here.
			 */
		}
	};

	maMantaForward(proxyargs, next);
}

function maMantaForward(proxyargs, callback)
{
	maCounters['mantarq_proxy_sent']++;
	mod_mautil.maHttpProxy(proxyargs, function () {
		maCounters['mantarq_proxy_return']++;
		callback();
	});
}

/*
 * Kang (introspection) entry points
 */
function maKangListTypes()
{
	return ([ 'zones', 'pending_requests', 'pending_task_groups',
	    'recent_task_groups' ]);
}

function maKangListObjects(type)
{
	if (type == 'pending_task_groups')
		return (Object.keys(maTaskGroups));

	if (type == 'recent_task_groups')
		return (maRecentTaskGroups.map(
		    /* JSSTYLED */
		    function (_, i) { return (i); }));

	if (type == 'pending_requests')
		return (Object.keys(maRequests));

	return (Object.keys(maZones));
}

function maKangGetObject(type, id)
{
	if (type == 'pending_task_groups') {
		var taskgroup = maTaskGroups[id];
		var rv = taskgroup.httpState();
		if (taskgroup.t_load_assets)
			rv['loadAssets'] = taskgroup.t_load_assets;
		return (rv);
	}

	if (type == 'recent_task_groups')
		return (maRecentTaskGroups[id].httpState());

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
