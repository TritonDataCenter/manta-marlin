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
var mod_uuid = require('node-uuid');

var mod_bunyan = require('bunyan');
var mod_getopt = require('posix-getopt');
var mod_jsprim = require('jsprim');
var mod_kang = require('kang');
var mod_mkdirp = require('mkdirp');
var mod_panic = require('panic');
var mod_restify = require('restify');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_config = require('../config');
var mod_mautil = require('../util');
var mod_moray = require('./moray');
var mod_schema = require('../schema');
var mod_agent_zone = require('./zone');

/* Global agent state. */
var maTaskGroups = {};		/* all task groups, by taskid */
var maTaskGroupsQueued = [];	/* waiting task groups */
var maRecentTaskGroups = [];	/* recently completed task groups */
var maNRecentTaskGroups = 30;	/* number of recent task groups to keep */
var maZones = {};		/* all zones, by zonename */
var maZonesReady = [];		/* ready zones */
var maLog;			/* global logger */
var maServer;			/* global restify server */
var maMoray;			/* Moray interface */
var maTimeout;			/* tick timeout */
var maTickStart;		/* last time a tick started */
var maTickDone;			/* last time a tick completed */
var maSaveThrottle;		/* throttle for saving requests */
var maRequests = {};		/* pending requests, by id */
var maEtags = {};		/* last known etag for each task group */
var maCounters = {
    'taskgroups_submitted': 0,	/* task groups received */
    'taskgroups_dispatched': 0,	/* task groups dispatched to a zone */
    'taskgroups_done': 0,	/* task groups completed */
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
var maHost       = mod_os.hostname();
var maServerName = 'marlin_agent';
var maLogStreams = [ { 'stream': process.stdout, 'level': 'info' } ];
var maZoneAutoReset = true;	/* false for debugging only */
var maTaskGroupInterval = 5 * 1000;
var maTickInterval = 1 * 1000;
var maTaskGroupSaveInterval = 5 * 1000;

/*
 * We have two different Manta configurations: maManta{Host,Port} describes how
 * to connect to Manta proper.  We connect to this service over HTTP to retrieve
 * assets, and we expect it to contain all Manta objects.  maMantaLocalRoot
 * is the directory where Manta objects _that are stored on this machine_ are
 * kept.  For now, for testing, these point to the same set of objects, because
 * we're only running a single Manta instance that holds all files locally, but
 * in general the local objects will be a subset of those available from the
 * Manta service.
 */
var maMantaHost  = 'localhost';
var maMantaPort  = 8081;
var maMantaLocalRoot = '/var/tmp/mock-manta';

function usage(errmsg)
{
	if (errmsg)
		console.error(errmsg);

	console.error('usage: node agent.js [-o logfile] [-p port]');
	process.exit(2);
}

function main()
{
	if (!process.env['MORAY_URL'])
		usage('MORAY_URL must be set in the environment.');

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

	maMoray = new mod_moray.RemoteMoray({
	    'url': process.env['MORAY_URL'],
	    'log': maLog,
	    'taskGroupInterval': maTaskGroupInterval,
	    'taskGroupsBucket': mod_config.mcBktTaskGroups
	});

	maSaveThrottle = new mod_mautil.Throttler(maTaskGroupSaveInterval);

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

	maServer.post('/zones', maHttpZonesAdd);
	maServer.get('/zones', maHttpZonesList);

	maServer.get('/kang/.*', mod_kang.knRestifyHandler({
	    'uri_base': '/kang',
	    'service_name': 'marlin',
	    'component': 'agent',
	    'ident': maHost,
	    'version': '0',
	    'list_types': maKangListTypes,
	    'list_objects': maKangListObjects,
	    'get': maKangGetObject,
	    'stats': maKangStats
	}));

	maServer.listen(maPort, function () {
		maLog.info('server listening on port %d', maPort);
		setInterval(maTick, maTickInterval);
	});

	mod_agent_zone.maZoneApiCallback(maTaskApiSetup);
}

/*
 * Invoked once per second to potentially kick off a poll for more records from
 * Moray and a save for our existing records.  Each of these actions is
 * throttled so that it won't happen either while another one is ongoing or
 * if it's been too recent since the last one.
 */
function maTick()
{
	maTickStart = new Date();
	maMoray.watchTaskGroups(maHost, maOnTaskGroups);
	maSaveTaskGroups();
	maTickDone = new Date();
}

/*
 * Invoked by the Moray interface when new task group records are discovered.
 * We're careful to ignore anything that's either invalid or doesn't represent
 * an actual new task group for us to work on, then we either queue up or start
 * executing the task group.
 */
function maOnTaskGroups(groups)
{
	groups.forEach(function (groupent) {
		var tgid, group, error, taskgroup;

		group = groupent['value'];

		if (!group.hasOwnProperty('taskGroupId')) {
			/* Completely bogus. */
			return;
		}

		tgid = group['taskGroupId'];
		if (maEtags.hasOwnProperty(tgid)) {
			if (maEtags[tgid] == groupent['etag'])
				/*
				 * XXX Redundant notification.  This shouldn't
				 * happen once we have proper polling in Moray.
				 */
				return;

			maEtags[tgid] = groupent['etag'];
			taskgroup = maTaskGroups[tgid];

			if (!taskgroup || group['state'] != 'cancelled') {
				maLog.warn('ignoring update for task group %s',
				    tgid);
				return;
			}

			maTaskGroupCancel(taskgroup);
			return;
		}

		maEtags[tgid] = groupent['etag'];
		error = mod_jsprim.validateJsonObject(
		    mod_schema.sMorayTaskGroup, group);
		if (error) {
			maLog.error(error,
			    'task group %s record is invalid', tgid);
			return;
		}

		if (group['state'] == 'done' || group['state'] == 'cancelled') {
			maLog.warn('ignoring task group %s in state %s',
			    tgid, group['state']);
			return;
		}

		if (group['state'] != 'dispatched') {
			/* XXX */
			maLog.warn('ignoring task group %s (state %s ' +
			    'not yet supported)', tgid, group['state']);
			return;
		}

		/*
		 * We can't have seen this task group before because any group
		 * we've seen before would either be present in maEtags (and
		 * ignored above) or have state "done" (also ignored above).
		 */
		mod_assert.ok(!maTaskGroups.hasOwnProperty(tgid));
		taskgroup = maTaskGroups[tgid] = new maTaskGroup(group);
		maCounters['taskgroups_submitted']++;

		taskgroup.t_state = maTaskGroup.TASKGROUP_S_QUEUED;
		taskgroup.t_log.info('enqueued new taskgroup');
		maTaskGroupsQueued.push(taskgroup);
	});

	maTaskGroupsPoke();
}

/*
 * Invoked by the tick handler to save any task groups that have been modified
 * since the last save, as long as there isn't another save ongoing and it has
 * been long enough since the last save.
 */
function maSaveTaskGroups()
{
	if (maSaveThrottle.tooRecent())
		return;

	maLog.trace('starting task group save');
	maSaveThrottle.start();

	/*
	 * Identify the task groups that need saving and for each one, create
	 * the new record and mark the generation that we're about to save as
	 * save-in-progress.
	 */
	var savegroups = {};

	mod_jsprim.forEachKey(maTaskGroups, function (tgid, group) {
		mod_assert.ok(group.t_save_pending === undefined);

		if (group.t_saved >= group.t_dirty)
			return;

		group.t_save_pending = group.t_dirty;
		savegroups[tgid] = group.morayState();
	});

	if (mod_jsprim.isEmpty(savegroups)) {
		maSaveThrottle.done();
		return;
	}

	/*
	 * Finally, issue the save request.  Upon completion, update the state
	 * of each of the groups we tried to save to indicate that the save
	 * completed successfully (if it did) and clear the save-pending flag.
	 */
	maMoray.saveTaskGroups(savegroups, function (err) {
		maSaveThrottle.done();

		if (err) {
			maLog.warn(err, 'failed to save task groups');
		} else {
			maLog.info('saved task groups',
			    Object.keys(savegroups));
		}

		mod_jsprim.forEachKey(savegroups, function (tgid, _) {
			var group = maTaskGroups[tgid];
			mod_assert.ok(group.t_save_pending !== undefined);

			if (err) {
				group.t_save_pending = undefined;
				return;
			}

			group.t_saved = group.t_save_pending;
			group.t_save_pending = undefined;

			/*
			 * If this group is done and this save was the last one,
			 * we can remove the group entirely.
			 */
			if (group.t_state != maTaskGroup.TASKGROUP_S_DONE ||
			    group.t_saved != group.t_dirty)
				return;

			maLog.info('removing done, saved task group %s', tgid);
			maTaskGroupRemove(group);
		});
	});
}

function maTaskGroupRemove(taskgroup)
{
	var tgid = taskgroup.t_id;

	mod_assert.equal(maTaskGroups[tgid], taskgroup);
	delete (maTaskGroups[tgid]);
	delete (maEtags[tgid]);

	maRecentTaskGroups.unshift(taskgroup);
	if (maRecentTaskGroups.length > maNRecentTaskGroups)
		maRecentTaskGroups.pop();
}

/*
 * maTaskGroup: agent representation of task group state.  This object is
 * constructed with the following properties when a new task group is dispatched
 * to this node:
 *
 *    jobId			unique job identifier
 *
 *    taskGroupId		unique task group identifier
 *
 *    inputKeys			input keys
 *
 *    phase			job phase for this task group
 *
 *    phaseNum			job phase number for this task group
 */
function maTaskGroup(args)
{
	/* Immutable job and task group state */
	this.t_job_id = args['jobId'];
	this.t_job_phases = mod_jsprim.deepCopy(args['phase']);
	this.t_phasenum = args['phaseNum'];
	this.t_phase = args['phase'];

	this.t_id = args['taskGroupId'];
	this.t_input = args['inputKeys'].slice(0);
	this.t_host = mod_os.hostname();
	this.t_log = maLog.child({ 'taskgroup': this });
	this.t_map_keys = this.t_phase['type'] != 'generic';

	/* Dynamic state */
	this.t_load_assets = undefined;
	this.t_state = maTaskGroup.TASKGROUP_S_INIT;
	this.t_machine = undefined;	/* assigned zonename */
	this.t_start = undefined;	/* time we start loading assets */
	this.t_done = undefined;	/* time when the task group completed */
	this.t_failmsg = undefined;	/* fail message, if we fail to start */
	this.t_pending = false;		/* pending "commit" or "fail" */
	this.t_cancelled = undefined;

	this.t_rqqueue = mod_vasync.queuev({
	    'concurrency': 1,
	    'worker': function queuefunc(task, callback) { task(callback); }
	});

	/*
	 * The task group is made up of individual tasks, each of which
	 * represents the execution of the user's code on one of the input keys.
	 * That execution may emit any number of output keys, which are recorded
	 * in t_current_outputs (an array, to preserve order) and t_current_seen
	 * (an object, to efficiently ignore duplicate keys).
	 */
	this.t_current_start = undefined;	/* start time for this key */
	this.t_current_input = undefined;	/* input key */
	this.t_current_outputs = undefined;	/* output keys (list) */
	this.t_current_seen = undefined;	/* output keys (object) */

	/*
	 * As each task completes, we track the result ("success" or "fail") as
	 * well as the actual keys emitted, which may end up becoming output
	 * keys (if result is "success") or discarded keys (if result is
	 * "failed").  We don't bother constructing a result nor a list of
	 * output or discarded keys for the whole task group because the job
	 * worker only cares about per-task results.  (In some cases, the same
	 * task may be run as part of multiple task groups on multiple hosts, in
	 * which case the job worker will pick and choose individual task
	 * results from multiple task groups.  So the overall task group status
	 * is not meaningful.)
	 */
	this.t_results = [];

	/*
	 * "dirty" indicates the current in-memory generation number.
	 * "saved" indicates the last generation number that has been
	 * successfully written back to Moray.
	 */
	this.t_dirty = 0;
	this.t_saved = 0;
	this.t_save_pending = undefined;
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
	    'id': this.t_id,
	    'phase': this.t_phase,
	    'state': this.t_state,
	    'inputKeys': this.t_input,
	    'results': this.t_results,
	    'cancelled': this.t_cancelled
	};

	if (this.t_machine)
		rv['machine'] = this.t_machine;
	if (this.t_start)
		rv['start'] = this.t_start;
	if (this.t_done)
		rv['done'] = this.t_done;
	if (this.t_failmsg)
		rv['failmsg'] = this.t_failmsg;
	if (this.t_current_input)
		rv['currentInput'] = this.t_current_input;
	if (this.t_current_outputs)
		rv['currentOutputs'] = this.t_current_outputs;

	return (rv);
};

maTaskGroup.prototype.taskState = function ()
{
	var rv;

	mod_assert.ok(this.t_current_input !== undefined);

	rv = {
	    'taskPhase': this.t_phase,
	    'taskInputKey': this.t_current_input,
	    'taskOutputKeys': this.t_current_output
	};

	if (this.t_map_keys)
		rv['taskInputFile'] = mod_path.join(
		    maZones[this.t_machine].z_manta_root, this.t_current_input);

	return (rv);
};

maTaskGroup.prototype.logKey = function ()
{
	return ({
	    'jobId': this.t_job_id,
	    'taskGroupId': this.t_id,
	    'taskGroupState': this.t_state
	});
};

maTaskGroup.prototype.morayState = function ()
{
	var state =
	    (this.t_state == maTaskGroup.TASKGROUP_S_INIT ||
	    this.t_state == maTaskGroup.TASKGROUP_S_QUEUED) ? 'dispatched' :
	    this.t_state == maTaskGroup.TASKGROUP_S_DONE ? 'done' : 'running';

	return ({
	    'jobId': this.t_job_id,
	    'taskGroupId': this.t_id,
	    'host': maHost,
	    'inputKeys': this.t_input,
	    'phase': this.t_phase,
	    'phaseNum': this.t_phasenum,
	    'state': state,
	    'results': this.t_results
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
		taskgroup.t_dirty++;

		taskgroup.t_pipeline = mod_vasync.pipeline({
		    'arg': taskgroup,
		    'funcs': maTaskGroupStagesDispatch
		}, function (err) {
			taskgroup.t_pipeline = undefined;

			if (err) {
				err = new mod_verror.VError(err,
				    'failed to dispatch taskgroup');
				taskgroup.t_log.error(err);
				taskgroup.t_failmsg = err.message;
				maTaskGroupMarkDone(taskgroup);
			}
		});
	}
}

var maTaskGroupStagesDispatch = [
	maTaskGroupLoadAssets,
	maTaskGroupDispatch
];

/*
 * Loads the task's assets into its assigned zone.  "next" is invoked only if
 * there are no errors.
 */
function maTaskGroupLoadAssets(taskgroup, callback)
{
	taskgroup.t_log.info('loading assets', taskgroup.t_phase);

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
function maTaskGroupDispatch(taskgroup, callback)
{
	var zone;

	taskgroup.t_log.info('dispatching taskgroup');

	zone = maZones[taskgroup.t_machine];
	mod_assert.equal(taskgroup.t_state, maTaskGroup.TASKGROUP_S_LOADING);
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_BUSY);
	taskgroup.t_state = maTaskGroup.TASKGROUP_S_RUNNING;
	taskgroup.t_dirty++;
	maCounters['taskgroups_dispatched']++;

	maTaskGroupAdvance(taskgroup, function () {
		callback();

		var waiters = zone.z_waiters;
		zone.z_waiters = [];
		waiters.forEach(function (w) { w.callback(w); });
	});
}

/*
 * Invoked after processing each input key (successfully or not) to advance the
 * group to the next key.  If this is a storage-map task, this operation also
 * updates the zone's hyprlofs mappings.  Although this function operates
 * asynchronously and those asynchronous operations can fail, the "advance"
 * operation itself never fails.  If a hyprlofs operation fails, we determine
 * the scope of the failure (the task or the entire task group) and update the
 * taskgroup state accordingly.  The caller will never get an error in its
 * callback.
 *
 * All calls must be funneled through the task group's work queue to ensure that
 * there is only ever one "advance" operation ongoing for a given task group.
 */
function maTaskGroupAdvance(taskgroup, callback)
{
	var i, key, now, zone;

	mod_assert.ok(!taskgroup.t_pending);

	if (taskgroup.t_cancelled || taskgroup.t_failmsg ||
	    taskgroup.t_results.length >= taskgroup.t_input.length) {
		taskgroup.t_current_start = undefined;
		taskgroup.t_current_input = undefined;
		taskgroup.t_current_outputs = undefined;
		taskgroup.t_current_seen = undefined;
		maTaskGroupMarkDone(taskgroup);
		callback();
		return;
	}

	i = taskgroup.t_results.length;
	key = taskgroup.t_input[i];
	now = new Date();

	if (!taskgroup.t_map_keys) {
		taskgroup.t_current_start = now;
		taskgroup.t_current_input = key;
		taskgroup.t_current_outputs = [];
		taskgroup.t_current_seen = {};
		callback();
		return;
	}

	zone = maZones[taskgroup.t_machine];
	taskgroup.t_pending = true;
	zone.z_hyprlofs.removeAll(function (err) {
		mod_assert.ok(taskgroup.t_pending);

		if (err) {
			/*
			 * The only way this should be possible is if the
			 * underlying hyprlofs mount gets unmounted.  This
			 * should be impossible because (a) the user shouldn't
			 * have permission to do that, even as root inside the
			 * zone, and (b) we still hold the file descriptor open,
			 * blocking the unmount.  So given that something very
			 * bad must have happened, we just fail the whole task
			 * group by setting failmsg and advancing again.
			 */
			taskgroup.t_pending = false;
			taskgroup.t_log.error(err, 'aborting taskgroup ' +
			    'because failed to remove hyprlofs mappings');
			taskgroup.t_failmsg = 'internal error';
			maTaskGroupAdvance(taskgroup, callback);
			return;
		}

		/*
		 * XXX kludge until we figure out what paths on manta really
		 * look like.
		 */
		var keypath = key[0] == '/' ? key.substr(1) : key;
		zone.z_hyprlofs.addMappings([
		    [ mod_path.join(maMantaLocalRoot, key), keypath ]
		], function (suberr) {
			mod_assert.ok(taskgroup.t_pending);
			taskgroup.t_pending = false;

			if (!suberr) {
				taskgroup.t_current_start = new Date();
				taskgroup.t_current_input = key;
				taskgroup.t_current_outputs = [];
				taskgroup.t_current_seen = {};
				callback();
				return;
			}

			/*
			 * The only reasonable way this could fail is if the
			 * user specified an input key that doesn't exist, in
			 * which case we simply fail this task and advance
			 * again.  If there's actually some other persistent
			 * problem, we'll either catch this when advancing again
			 * or the user will see all of their tasks as failed.
			 */
			suberr = new mod_verror.VError(suberr,
			    'failed to load "%s"', key);
			taskgroup.t_log.error(suberr);
			taskgroup.t_dirty++;
			taskgroup.t_results.push({
			    'input': key,
			    'outputs': [],
			    'result': 'fail',
			    'startTime': now,
			    'doneTime': new Date(),
			    'error': {
				'code': 'EJ_NOENT',
				'message': 'failed to load "' + key + '"'
			    }
			});
			maCounters['keys_failed']++;
			maTaskGroupAdvance(taskgroup, callback);
		});
	});
}

/*
 * Cancel the given task group.
 */
function maTaskGroupCancel(taskgroup)
{
	if (taskgroup.t_state == maTaskGroup.TASKGROUP_S_QUEUED) {
		/* Just remove it from the list of queued tasks. */
		var i;
		for (i = 0; i < maTaskGroupsQueued.length; i++) {
			if (maTaskGroupsQueued[i] === taskgroup)
				break;
		}

		mod_assert.ok(i < maTaskGroupsQueued.length);
		maTaskGroupsQueued.splice(i, 1);
		taskgroup.t_cancelled = new Date();
		taskgroup.t_log.info('removed from queue (task cancelled)');
		maTaskGroupRemove(taskgroup);
		return;
	}

	if (taskgroup.t_state != maTaskGroup.TASKGROUP_S_LOADING &&
	    taskgroup.t_state != maTaskGroup.TASKGROUP_S_RUNNING) {
		taskgroup.t_log.info('task cancelled (no-op in state %s)',
		    taskgroup.t_state);
		return;
	}

	taskgroup.t_cancelled = new Date();
	taskgroup.t_log.info('task cancelled');
}

/*
 * Check whether the given task group is finished.  If so, update it, remove it
 * from the pending tasks table, and make the zone in which it was running ready
 * for reuse.
 */
function maTaskGroupMarkDone(taskgroup)
{
	var zone;

	mod_assert.ok(taskgroup.t_cancelled || taskgroup.t_failmsg ||
	    taskgroup.t_results.length == taskgroup.t_input.length);

	maCounters['taskgroups_done']++;
	taskgroup.t_state = maTaskGroup.TASKGROUP_S_DONE;
	taskgroup.t_done = new Date();
	taskgroup.t_log.info('taskgroup completed');
	taskgroup.t_dirty++;

	zone = maZones[taskgroup.t_machine];
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_BUSY);
	zone.z_taskgroup = undefined;

	if (maZoneAutoReset) {
		zone.z_state = mod_agent_zone.maZone.ZONE_S_UNINIT;
		mod_agent_zone.maZoneMakeReady(zone, maZoneReady);
	} else {
		zone.z_state = mod_agent_zone.maZone.ZONE_S_DISABLED;
	}
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
		/*
		 * If this request identifies a zone that is currently disabled,
		 * we take this as a request to try to make it ready again.
		 */
		if (maZones[zonename].z_state ==
		    mod_agent_zone.maZone.ZONE_S_DISABLED) {
			mod_agent_zone.maZoneMakeReady(maZones[zonename],
			    maZoneReady);
			response.send(204);
			next();
		} else {
			next(new mod_restify.InvalidArgumentError(
			    'attempted to add duplicate zone ' + zonename));
		}

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
	    init, maTaskApiCommit);
	s.post('/fail',
	    mod_restify.bodyParser({ 'mapParams': false }),
	    init, maTaskApiFail);

	/*
	 * We proxy the entire Manta API under /object.
	 */
	var methods = [ 'get', 'put', 'post', 'del', 'head' ];
	methods.forEach(function (method) {
		s[method]('/object/.*', init, maTaskApiManta);
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
	    zone.z_taskgroup.t_state == maTaskGroup.TASKGROUP_S_RUNNING)
		return (true);

	next(new mod_restify.ConflictError('invalid zone state'));
	return (false);
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
		response.send(zone.z_taskgroup.taskState());
		next();
		return;
	}

	if (request.query['wait'] != 'true') {
		response.send(204);
		next();
		return;
	}

	zone.z_waiters.push({
	    'callback': function (w) {
		maTaskApiTask(w['request'], w['response'], w['next']);
	    },
	    'time': Date.now(),
	    'request': request,
	    'response': response,
	    'next': next
	});
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

	if (!body.hasOwnProperty('key')) {
		next(new mod_restify.InvalidArgumentError('"key" is required'));
		return;
	}

	key = body['key'];

	/*
	 * While it doesn't make any sense for a user to submit concurrent
	 * "commit" or "fail" requests, such requests should have ACID
	 * semantics.  Since they all operate on a single piece of state (the
	 * current key), we simply process them serially by sending them through
	 * a work queue.
	 */
	taskgroup.t_rqqueue.push(function (callback) {
		if (!maTaskApiValidate(request, response, next)) {
			callback();
			return;
		}

		mod_assert.equal(taskgroup.t_state,
		    maTaskGroup.TASKGROUP_S_RUNNING);

		/*
		 * XXX It's still an open question whether the "key" parameter
		 * should even be part of the API.  On the one hand, it doesn't
		 * make much sense, since there's only one value that's valid
		 * here -- the current key.  On the other hand, requiring this
		 * can be a useful check that we haven't gotten out of sync with
		 * the zone agent, which can otherwise be very difficult to
		 * debug.
		 */
		if (key !== taskgroup.t_current_input) {
			next(new mod_restify.ConflictError(
			    'key "' + key + '" is not the current key ("' +
			    taskgroup.t_current_input + '")'));
			callback();
			return;
		}

		taskgroup.t_dirty++;
		taskgroup.t_results.push({
		    'input': taskgroup.t_current_input,
		    'machine': taskgroup.t_machine,
		    'result': 'ok',
		    'outputs': taskgroup.t_current_outputs,
		    'startTime': taskgroup.t_current_start,
		    'doneTime': new Date()
		});

		maCounters['keys_committed']++;
		maTaskGroupAdvance(taskgroup, callback);
	}, function () {
		response.send(204);
		next();
	});
}

/*
 * POST /fail: indicate that the given task failed to process a particular key.
 */
function maTaskApiFail(request, response, next)
{
	var body = request.body || {};
	var zone = request.maZone;
	var taskgroup = zone.z_taskgroup;
	var key, error;

	if (!body.hasOwnProperty('key')) {
		/*
		 * We consider it an error to not specify a key, since this
		 * request comes from our in-zone agent, who should know better.
		 * We could support this to mean "something horrible has gone
		 * wrong which has nothing to do with the current key".
		 */
		next(new mod_restify.InvalidArgumentError('"key" is required'));
		return;
	}

	key = body['key'];

	/*
	 * Make sure the error is valid: it must have string fields for "code"
	 * and "message".
	 */
	if (!body['error'])
		error = {};
	else
		error = body['error'];

	if (!error['code'] || typeof (error['code']) != 'string')
		error['code'] = 'EJ_USER';

	if (!error['message'] || typeof (error['message']) != 'string')
		error['message'] = 'no message given';

	/*
	 * See comments in maTaskApiCommit.
	 */
	taskgroup.t_rqqueue.push(function (callback) {
		if (!maTaskApiValidate(request, response, next)) {
			callback();
			return;
		}

		mod_assert.equal(taskgroup.t_state,
		    maTaskGroup.TASKGROUP_S_RUNNING);

		if (key != taskgroup.t_current_input) {
			next(new mod_restify.ConflictError(
			    'key "' + key + '" is not the current key ("' +
			    taskgroup.t_current_input + '")'));
			return;
		}

		taskgroup.t_dirty++;
		taskgroup.t_results.push({
		    'input': taskgroup.t_current_input,
		    'machine': taskgroup.t_machine,
		    'result': 'fail',
		    'outputs': [],
		    'discarded': taskgroup.t_current_outputs,
		    'startTime': taskgroup.t_current_start,
		    'doneTime': new Date(),
		    'error': error
		});

		maCounters['keys_failed']++;
		maTaskGroupAdvance(taskgroup, callback);
	}, function () {
		response.send(204);
		next();
	});
}

/*
 * /object/:key: We proxy the entire Manta API here.  The only thing that we do
 * specially here is note objects that get created so we can "commit" them only
 * if the task successfully completes.
 */
function maTaskApiManta(request, response, next)
{
	var zone, key, taskgroup, proxyargs;

	if (!maTaskApiValidate(request, response, next))
		return;

	zone = request.maZone;
	taskgroup = zone.z_taskgroup;
	key = mod_url.parse(request.url).pathname.substr('/object'.length);
	mod_assert.equal(taskgroup.t_state, maTaskGroup.TASKGROUP_S_RUNNING);

	proxyargs = {
	    'request': request,
	    'response': response,
	    'server': {
	        'host': maMantaHost,
		'port': maMantaPort,
		'path': key
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
	proxyargs['continue'] = function () {
		if (!taskgroup.t_current_seen.hasOwnProperty(key)) {
			taskgroup.t_current_seen[key] = true;
			taskgroup.t_current_outputs.push(key);

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
	    'recent_task_groups', 'global' ]);
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

	if (type == 'global')
		return ([ 0 ]);

	return (Object.keys(maZones));
}

function maKangGetObject(type, id)
{
	if (type == 'pending_task_groups') {
		var taskgroup = maTaskGroups[id];
		var rv = taskgroup.httpState();
		rv['rqqueue'] = taskgroup.t_rqqueue;
		if (taskgroup.t_load_assets)
			rv['loadAssets'] = taskgroup.t_load_assets;
		if (taskgroup.t_current_seen)
			rv['currentSeen'] = taskgroup.t_current_seen;
		return (rv);
	}

	if (type == 'recent_task_groups')
		return (maRecentTaskGroups[id].httpState());

	if (type == 'zones')
		return (maZones[id].httpState());

	if (type == 'global') {
		return ({
		    'tickStart': maTickStart,
		    'tickDone': maTickDone,
		    'saveThrottle': maSaveThrottle
		});
	}

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
