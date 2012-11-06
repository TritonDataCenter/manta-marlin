/*
 * lib/agent/agent.js: compute node Marlin agent
 *
 * The agent runs in the global zone of participating compute and storage nodes
 * and manages tasks run on that node.  It's responsible for setting up compute
 * zones for user jobs, executing the jobs, monitoring the user code, tearing
 * down the zones, and emitting progress updates to the appropriate job worker.
 */

/*
 * Rewrite TODO:
 *    - code review agent, zone-agent
 *    - need to figure out how best to deal with cancelled jobs: currently we
 *      have to keep them in memory indefinitely, or else we'll keep picking up
 *      uncancelled tasks in those jobs and not realize that they're cancelled.
 *      But it would be nice if when those jobs were eventually cleared out we
 *      also removed their tasks and also removed the job from our cache.
 *
 * Would be nice:
 * - use a queue instead of forEachParallel to avoid DOS resulting from very
 *   large numbers of assets.
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
var mod_extsprintf = require('extsprintf');
var mod_getopt = require('posix-getopt');
var mod_jsprim = require('jsprim');
var mod_kang = require('kang');
var mod_mkdirp = require('mkdirp');
var mod_panic = require('panic');
var mod_restify = require('restify');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var sprintf = mod_extsprintf.sprintf;
var VError = mod_verror.VError;

var mod_moray = require('moray');

var mod_adnscache = require('../adnscache');
var mod_mamoray = require('../moray');
var mod_mautil = require('../util');
var mod_schema = require('../schema');
var mod_agent_zone = require('./zone');

/* Global agent state. */
var maJobs = {};		/* all jobs, by id */
var maTasks = {};		/* all tasks, by id */
var maTasksReduce = {};		/* all reduce tasks, by id */
var maTaskGroups = {};		/* all task groups, by id */
var maTaskGroupsQueued = [];	/* waiting task groups */
var maZones = {};		/* all zones, by zonename */
var maZonesReady = [];		/* ready zones */
var maJobSaveThrottle;		/* job save throttle */
var maLog;			/* global logger */
var maServer;			/* global restify server */
var maMoray;			/* Moray interface */
var maMorayQueue;		/* Moray queue */
var maMorayPending;		/* Currently connecting */
var maMorayTaskPoll;		/* Poll state for task bucket */
var maMorayTaskThrottle;
var maTimeout;			/* tick timeout */
var maTickStart;		/* last time a tick started */
var maTickDone;			/* last time a tick completed */
var maDnsCache;			/* local DNS cache */
var maRequests = {};		/* pending requests, by id */
var maCounters = {
    'taskgroups_dispatched': 0,	/* task groups dispatched to a zone */
    'taskgroups_done': 0,	/* task groups completed */
    'tasks_failed': 0,		/* individual tasks that failed */
    'tasks_committed': 0,	/* individual tasks committed */
    'zones_added': 0,		/* zones added to Marlin */
    'zones_readied': 0,		/* zones transitioned to "ready" */
    'zones_disabled': 0,	/* zones permanently out of commission */
    'mantarq_proxy_sent': 0,	/* requests forwarded to Manta */
    'mantarq_proxy_return': 0	/* responses received from Manta */
};

/* Configuration options */
var maRestifyServerName = 'marlin_agent';
var maLogStreams = [ {
    'stream': process.stdout,
    'level': process.env['LOG_LEVEL'] || 'debug'
} ];
var maZoneAutoReset = true;	/* false for debugging only */
var maZoneSaveLogs = true;	/* save zone agent logs before reset */
var maZoneLogRoot = '/var/smartdc/marlin/log/zones';
var maZoneLivenessInterval = 15000;
var maMorayMaxRecords = 1000;
var maRingbufferSize = 1000;
var maMorayOptions = {
    'limit': maMorayMaxRecords,
    'noCache': true,
    'sort': {
	'attribute': '_txn_snap',
	'order': 'ASC'
    }
};
var maConf, maBucketNames;
var maServerName, maPort, maMantaHost, maMantaPort, maMorayHost, maMorayPort;
var maTimeTick, maTimeJobSave, maTimePoll;

var maConfSchema = {
    'type': 'object',
    'properties': {
	'instanceUuid': mod_schema.sStringRequiredNonEmpty,
	'port': mod_schema.sTcpPortRequired,

	'manta': {
	    'type': 'object',
	    'required': true,
	    'properties': {
		'url': mod_schema.sStringRequiredNonEmpty
	    }
	},

	'moray': {
	    'type': 'object',
	    'required': true,
	    'properties': {
		'url': mod_schema.sStringRequiredNonEmpty,
		'reconnect': {
		    'required': true,
		    'type': 'object',
		    'properties': {
			'maxTimeout': mod_schema.sIntervalRequired,
			'retries': mod_schema.sIntervalRequired
		    }
		}
	    }
	},

	'dns': {
	    'type': 'object',
	    'required': true,
	    'properties': {
		'nameservers': {
		    'type': 'array',
		    'items': mod_schema.sStringRequiredNonEmpty,
		    'minItems': 1
		},
		'triggerInterval': mod_schema.sIntervalRequired,
		'graceInterval':   mod_schema.sIntervalRequired
	    }
	},

	'buckets': {
	    'type': 'object',
	    'required': true,
	    'properties': {
		'job': mod_schema.sStringRequiredNonEmpty,
		'task': mod_schema.sStringRequiredNonEmpty,
		'taskinput': mod_schema.sStringRequiredNonEmpty,
		'taskoutput': mod_schema.sStringRequiredNonEmpty
	    }
	},

	'tunables': {
	    'type': 'object',
	    'required': true,
	    'properties': {
		'maxPendingPuts': mod_schema.sIntervalRequired,
		'timeJobSave': mod_schema.sIntervalRequired,
		'timePoll': mod_schema.sIntervalRequired,
		'timeTick': mod_schema.sIntervalRequired
	    }
	},

	'zoneDefaults': {
	    'type': 'object',
	    'required': true
	},

	'zonesFile': mod_schema.sString
    }
};

function usage(errmsg)
{
	if (errmsg)
		console.error(errmsg);

	console.error('usage: node agent.js [-o logfile] conffile');
	process.exit(2);
}

function main()
{
	var parser = new mod_getopt.BasicParser('o:', process.argv);
	var option;

	while ((option = parser.getopt()) !== undefined) {
		if (option.error)
			usage();

		if (option.option == 'o') {
			maLogStreams = [ { 'path': option.optarg } ];
			console.log('logging to %s', option.optarg);
			continue;
		}
	}

	if (parser.optind() >= process.argv.length)
		usage();

	maLog = new mod_bunyan({
	    'name': maRestifyServerName,
	    'streams': maLogStreams
	});

	if (!process.env['NO_ABORT_ON_CRASH']) {
		mod_panic.enablePanicOnCrash({
		    'skipDump': true,
		    'abortOnPanic': true
		});
	}

	maInitConf(process.argv[parser.optind()]);
	maInitDirs();
	maInitDns();
	maInitMoray();
	maInitHttp(function () {
		maLog.info('server listening on port %d', maPort);
		setInterval(maTick, maTimeTick);
	});

	mod_agent_zone.maZoneApiCallback(maTaskApiSetup);

	if (maConf['zonesFile'])
		maInitZones(maConf['zonesFile']);
}

/*
 * Read our global configuration.
 */
function maInitConf(filename)
{
	var conf, url;

	conf = maConf = mod_mautil.readConf(maLog, maConfSchema, filename);
	maLog.info('configuration', conf);

	maServerName = conf['instanceUuid'];
	maPort = conf['port'];
	maTimeJobSave = conf['tunables']['timeJobSave'];
	maTimePoll = conf['tunables']['timePoll'];
	maTimeTick = conf['tunables']['timeTick'];
	maJobSaveThrottle = new mod_mautil.Throttler(
	    conf['tunables']['timeJobSave']);

	url = mod_url.parse(conf['manta']['url']);
	maMantaHost = url['hostname'];
	if (url['port'])
		maMantaPort = parseInt(url['port'], 10);
	else
		maMantaPort = 80;

	url = mod_url.parse(conf['moray']['url']);
	maMorayHost = url['hostname'];
	maMorayPort = parseInt(url['port'], 10);

	maBucketNames = {};
	mod_jsprim.forEachKey(maConf['buckets'], function (n, b) {
		maBucketNames[b] = n;
	});
}

/*
 * Create directories used to store log files.
 */
function maInitDirs()
{
	maLog.info('creating "%s"', maZoneLogRoot);
	mod_mkdirp.sync(maZoneLogRoot);
}

/*
 * Initialize our DNS helper interfaces.
 */
function maInitDns()
{
	/*
	 * Availability of Manta services relies on locating them via DNS as
	 * they're used.  Since this agent runs in the global zone of SDC
	 * compute nodes where DNS is not available, we do our own DNS lookups.
	 */
	maDnsCache = new mod_adnscache.AsyncDnsCache({
	    'log': maLog.child({ 'component': 'dns-cache' }),
	    'nameServers': maConf['dns']['nameservers'].slice(0),
	    'triggerInterval': maConf['dns']['triggerInterval'],
	    'graceInterval': maConf['dns']['graceInterval'],
	    'onResolve': maTaskGroupsPoke
	});

	maDnsCache.add(maMantaHost);
	maDnsCache.add(maMorayHost);
}

/*
 * Initialize our Moray helper interfaces.
 */
function maInitMoray()
{
	maMorayTaskPoll = new mod_mamoray.PollState(maTimePoll,
	    maRingbufferSize);
	maMorayTaskThrottle = new mod_mautil.Throttler(maTimePoll);

	maMorayQueue = new mod_mamoray.MorayWriteQueue({
	    'log': maLog.child({ 'component': 'moray-queue' }),
	    'client': function () { return (maMoray); },
	    'buckets': maConf['buckets'],
	    'maxpending': maConf['tunables']['maxPendingPuts']
	});
}

/*
 * Initialize our restify server, which is how we're told which zones are
 * available.  We also support kang via this server.
 */
function maInitHttp(callback)
{
	maServer = mod_restify.createServer({
	    'name': maRestifyServerName,
	    'log': maLog.child({ 'component': 'http-server' })
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
	    'log': maLog.child({ 'component': 'audit-log' })
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
	    'ident': maRestifyServerName,
	    'version': '0',
	    'list_types': maKangListTypes,
	    'list_objects': maKangListObjects,
	    'get': maKangGetObject,
	    'stats': maKangStats
	}));

	maServer.listen(maPort, function () {
		maLog.info('server listening on port %d', maPort);
		setInterval(maTick, maTimeTick);
	});
}

/*
 * Reads names of compute zones from the given file.  This is half-baked,
 * intended only to ease deployment until we have real persistent state for this
 * agent.
 */
function maInitZones(filename)
{
	maLog.info('reading zones from "%s"', filename);
	mod_fs.readFile(filename, function (err, contents) {
		if (err) {
			if (err['code'] == 'ENOENT')
				return;

			maLog.fatal(err, 'failed to read zones file');
			throw (err);
		}

		var lines = contents.toString('utf-8').split('\n');
		lines.forEach(function (line) {
			line = line.trim();

			if (line.length === 0 || line[0] == '#')
				return;

			maZoneAdd(line);
		});
	});
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
	maDnsCache.update();

	if (!maMoray) {
		maLog.warn('tick skipped (no moray connection)');
		maMorayConnect();
		maTickDone = new Date();
		return;
	}

	if (!maJobSaveThrottle.tooRecent()) {
		maJobSaveThrottle.start();
		maJobSaveThrottle.done();
		maMarkJobs(maTickStart);
	}

	maPoll(maTickStart.getTime());
	maCheckZonesLiveness(maMoray);
	maMorayQueue.flush();
	maTickDone = new Date();
}

function maPoll(now)
{
	var req;

	/*
	 * Fetch updates to job records.
	 */
	mod_jsprim.forEachKey(maJobs, function (jobid, job) {
		var filter = sprintf('(&(jobId=%s)%s)', jobid,
		    job.j_poll.filter());

		req = mod_mamoray.poll({
		    'client': maMoray,
		    'options': maMorayOptions,
		    'now': now,
		    'log': maLog,
		    'throttle': job.j_update_throttle,
		    'bucket': maConf['buckets']['job'],
		    'filter': filter,
		    'onrecord': function (record) {
			    if (job.j_poll.record(record))
				    maMorayOnRecord(record);
		    }
		});

		job.j_poll.request(filter, req);
	});

	/*
	 * Fetch new and updated task records.
	 */
	var incremental_filter = maMorayTaskPoll.filter();
	var cancelled_filter = incremental_filter.length > 0 ? '' :
	    '(!(state=cancelled))';
	var task_filter = sprintf(
	    '(&(server=%s)(!(state=done))(!(state=aborted))%s%s)',
	    maServerName, incremental_filter, cancelled_filter);

	req = mod_mamoray.poll({
	    'client': maMoray,
	    'options': maMorayOptions,
	    'now': now,
	    'log': maLog,
	    'throttle': maMorayTaskThrottle,
	    'bucket': maConf['buckets']['task'],
	    'filter': task_filter,
	    'onrecord': function (record) {
		    if (maMorayTaskPoll.record(record))
			    maMorayOnRecord(record);
	    }
	});

	maMorayTaskPoll.request(task_filter, req);

	/*
	 * Fetch new taskinput records for pending reduce tasks.
	 */
	mod_jsprim.forEachKey(maTasksReduce, function (taskid) {
		var task = maTasks[taskid];

		if (maTaskReadAllRecords(task)) {
			maLog.debug('removing "%s" from maTasksReduce', taskid);
			delete (maTasksReduce[taskid]);
			return;
		}

		var filter = sprintf('(&(taskId=%s)%s)', task.t_id,
		    task.t_poll.filter());

		mod_mamoray.poll({
		    'client': maMoray,
		    'options': maMorayOptions,
		    'now': now,
		    'log': maLog,
		    'throttle': task.t_poll.p_throttle,
		    'bucket': maConf['buckets']['taskinput'],
		    'filter': filter,
		    'onrecord': function (record) {
			if (task.t_poll.record(record))
				maMorayOnRecord(record);
		    },
		    'ondone': function (_, start_time, count) {
			task.t_last_poll = start_time;
			task.t_last_count = count;

			/*
			 * Wake up waiters blocked on this query.  Waiters can
			 * only be satisfied by this query if either the task
			 * has input available or has read all input records
			 * (i.e. is done), AND the group is currently running.
			 */
			if ((task.t_xinput.length > 0 ||
			    maTaskReadAllRecords(task)) &&
			    task.t_group && task.t_group.g_state ==
			    maTaskGroup.TASKGROUP_S_RUNNING)
				maZoneWakeup(maZones[task.t_group.g_machine]);
		    }
		});
	});
}

/*
 * Ensure that we have a valid Moray client.
 */
function maMorayConnect()
{
	var client, ip;

	if (maMoray || maMorayPending)
		return;

	ip = maDnsCache.lookupv4(maMorayHost);
	if (!ip) {
		maLog.warn('no IP available for "%s"', maMorayHost);
		return;
	}

	maMorayPending = true;
	client = mod_moray.createClient({
	    'host': ip,
	    'port': maMorayPort,
	    'log': maLog.child({ 'component': 'moray-client' }),
	    'reconnect': true,
	    'retry': maConf['moray']['reconnect']
	});

	client.on('error', function (err) {
		maMorayPending = false;
		maLog.error(err, 'moray client error');
	});

	client.on('close', function () {
		maLog.error('moray client closed');
	});

	client.on('connect', function () {
		maLog.info('moray client connected');
		mod_assert.ok(!maMoray || maMoray == client);
		maMoray = client;
		maMorayPending = false;
	});
}

/*
 * We keep track of job records because the job definition specifies how to run
 * individual tasks and so that we can cancel pending tasks when a job is
 * cancelled.
 */
function maJob(jobid, save_interval, poll_interval)
{
	this.j_id = jobid;
	this.j_record = undefined;		/* last received job record */
	this.j_groups = {};			/* pending task groups */
	this.j_cancelled = undefined;		/* time job cancelled */
	this.j_save_interval = save_interval;	/* save frequency */
	this.j_dirtied = undefined;		/* time last taskid dirtied */
	this.j_saving = undefined;		/* taskid being saved */
	this.j_update_throttle = new mod_mautil.Throttler(poll_interval);
	this.j_poll = new mod_mamoray.PollState(poll_interval,
	    maRingbufferSize);
}

function maJobCreate(jobid)
{
	maJobs[jobid] = new maJob(jobid, maTimeJobSave, maTimePoll);
}

function maJobUpdated(job, record)
{
	var first = job.j_record === undefined;

	if (job.j_record && job.j_record['_etag'] == record['_etag'])
		return;

	job.j_record = record;

	if (record['value']['timeCancelled']) {
		maLog.info('job "%s": cancelled', job.j_id);
		job.j_cancelled = record['value']['timeCancelled'];

		mod_jsprim.forEachKey(job.j_groups, function (groupid) {
			maTaskGroupCancel(maTaskGroups[groupid]);
		});

		return;
	}

	if (!first)
		return;

	mod_jsprim.forEachKey(job.j_groups, function (groupid) {
		var group = maTaskGroups[groupid];
		maTaskGroupSetJob(group, job);
	});
}

function maTask(record)
{
	this.t_id = record['value']['taskId'];	/* task identifier */
	this.t_record = record;			/* current Moray state */
	this.t_xinput = [];			/* external input records */
	this.t_group = undefined;		/* assigned task group */
	this.t_input_done = undefined;		/* time input completed */
	this.t_cancelled = false;		/* task is cancelled */

	/* used for reduce tasks only */
	this.t_poll = new mod_mamoray.PollState(maTimePoll, maRingbufferSize);
	this.t_last_poll = undefined;
	this.t_last_count = undefined;
}

function maTaskReadAllRecords(task)
{
	if (!task.t_input_done || !task.t_last_poll)
		return (false);

	if (task.t_last_poll <= task.t_input_done)
		return (false);

	/*
	 * If we received the limit number of records in our last search, then
	 * we don't know that we've received them all.
	 */
	if (task.t_last_count === maMorayMaxRecords)
		return (false);

	return (true);
}

function maTaskCreate(taskid, record)
{
	var value, jobid, pi, groupid;
	var job, task, group;

	value = record['value'];
	jobid = value['jobId'];
	pi = value['phaseNum'];
	groupid = jobid + '/' + pi;

	task = new maTask(record);
	maTasks[taskid] = task;

	if (record['value']['timeInputDone'])
		task.t_input_done = Date.parse(
		    record['value']['timeInputDone']);

	if (!record['value']['key'])
		maTasksReduce[taskid] = true;

	if (maTaskGroups.hasOwnProperty(groupid)) {
		maTaskGroupAppendTask(maTaskGroups[groupid], task);
		return;
	}

	if (!maJobs.hasOwnProperty(jobid))
		maJobs[jobid] = new maJob(jobid, maTimeJobSave);

	job = maJobs[jobid];
	job.j_groups[groupid] = true;

	group = new maTaskGroup(groupid, job, pi);
	maTaskGroups[groupid] = group;
	group.g_log.debug('created group for task "%s"', task.t_id);
	maTaskGroupAppendTask(group, task);

	if (record['value']['key'])
		group.g_login =
		    mod_mautil.pathExtractFirst(record['value']['key']);

	if (job.j_record)
		maTaskGroupSetJob(group, job);
}

function maTaskUpdated(task, record)
{
	/*
	 * The only external updates we care about are cancellation and
	 * completion of the input stream (for reduce tasks only).
	 */
	var group = task.t_group;

	if (task.t_record['_etag'] == record['_etag'])
		return;

	if (group.g_state == maTaskGroup.TASKGROUP_S_DONE) {
		task.t_record['_etag'] = record['_etag'];

		if (maMorayQueue.pending('task', task.t_id))
			maTaskDirty(task);

		return;
	}

	if (record['value']['state'] == 'cancelled') {
		group.g_log.info('task "%s": cancelled externally', task.t_id);
		task.t_cancelled = true;

		if (group.g_task == task) {
			/*
			 * Unlucky.  We're currently executing the very task
			 * that was just cancelled.  There's no clean way to
			 * abort it except to reset the zone.
			 * XXX would be great to have a function which collects
			 * the pending tasks, removes them from this group,
			 * removes the group, and adds the tasks to a new group.
			 * This essentially would reset the zone and continue
			 * running tasks for the same job and phase.  This
			 * should also be used when the zone agent times out.
			 * For now, we punt by just aborting all the tasks.
			 */
			group.g_log.info('cancelling all tasks in group');
			group.g_failmsg = 'pending task cancelled';
			maTaskGroupAbortQueued(group, 'EJ_INTERNAL',
			    'internal error');
			maTaskGroupAbort(group);
			return;
		}

		for (var i = 0; i < group.g_tasks.length; i++) {
			if (group.g_tasks[i] == task)
				break;
		}

		if (i < group.g_tasks.length) {
			group.g_tasks.splice(i, 1);
			group.g_log.info('task "%s": removed from queue',
			    task.t_id);
		}

		return;
	}

	if (record['value']['timeInputDone']) {
		maLog.debug('task "%s": input done', task.t_id);
		task.t_input_done = Date.parse(
		    record['value']['timeInputDone']);

		/*
		 * Wake up waiters for zones waiting for task input to be
		 * complete, if we've also read all of the corresponding
		 * taskinput records.
		 */
		if (group.g_state == maTaskGroup.TASKGROUP_S_RUNNING &&
		    task.t_xinput.length === 0 && maTaskReadAllRecords(task))
			maZoneWakeup(maZones[group.g_machine]);
	}

	task.t_record['_etag'] = record['_etag'];

	if (maMorayQueue.pending('task', task.t_id))
		maTaskDirty(task);
}

/*
 * Remove a task from global state.  It's assumed that it's not referenced by
 * any other structures by this point.
 */
function maTaskRemove(task)
{
	maLog.debug('task "%s": removing', task.t_id);
	delete (maTasks[task.t_id]);
	delete (maTasksReduce[task.t_id]);
}

function maTaskAppendKey(task, keyinfo)
{
	maLog.debug('task "%s": appending key', task.t_id, keyinfo);
	task.t_xinput.push(keyinfo);

	var group = task.t_group;

	if (group.g_login === undefined)
		group.g_login = mod_mautil.pathExtractFirst(keyinfo['key']);

	if (group.g_phase && group.g_state == maTaskGroup.TASKGROUP_S_INIT) {
		group.g_state = maTaskGroup.TASKGROUP_S_QUEUED;
		maTaskGroupsQueued.push(group);
		maTaskGroupsPoke();
		return;
	}

	if (group.g_state == maTaskGroup.TASKGROUP_S_RUNNING &&
	    task.t_xinput.length == 1)
		maZoneWakeup(maZones[group.g_machine]);
}

function maTaskDirty(task)
{
	maMorayQueue.dirty('task', task.t_id, task.t_record['value'],
	    { 'etag': task.t_record['_etag'] });

	/*
	 * If the task has been completed or aborted, we can remove it
	 * immediately.  The MorayQueue still has a reference and will keep
	 * attempting to save the record until it succeeds.
	 */
	if (task.t_record['state'] == 'done' ||
	    task.t_record['state'] == 'aborted')
		maTaskRemove(task);

	/*
	 * Keep track of the last time each job is dirtied.  See maMarkJobs.
	 */
	var job = maJobs[task.t_record['value']['jobId']];
	if (!job)
		return;

	job.j_dirtied = Date.now();
	job.j_saving = task.t_id;
}

function maTaskError(task, when, code, message)
{
	maCounters['tasks_failed']++;

	task.t_record['value']['state'] = 'aborted';
	task.t_record['value']['result'] = 'fail';
	task.t_record['value']['timeDone'] = mod_jsprim.iso8601(Date.now());
	task.t_record['value']['error'] = {
	    'code': code,
	    'message': message
	};

	maTaskDirty(task);
}

function maTaskDone(task, machine, nout, when)
{
	maCounters['tasks_committed']++;

	task.t_record['value']['nOutputs'] = nout;
	task.t_record['value']['state'] = 'done';
	task.t_record['value']['result'] = 'ok';
	task.t_record['value']['timeDone'] = mod_jsprim.iso8601(when);
	task.t_record['value']['machine'] = machine;

	maTaskDirty(task);
}

function maTaskUpdate(task, machine, nout, when)
{
	if (task.t_record['value']['state'] == 'dispatched')
		task.t_record['value']['state'] = 'running';

	task.t_record['value']['nOutputs'] = nout;
	task.t_record['value']['machine'] = machine;

	maTaskDirty(task);
}

/*
 * Invoked by the Moray interface when new records are discovered.
 */
function maMorayOnRecord(record)
{
	var name, error, jobid, taskid;

	name = maBucketNames[record['bucket']];
	mod_assert.ok(mod_schema.sBktJsonSchemas.hasOwnProperty(name),
	    'poller returned record for an unknown bucket ' + record['bucket']);

	error = mod_jsprim.validateJsonObject(
	    mod_schema.sBktJsonSchemas[name], record['value']);
	if (error) {
		maLog.warn(error, 'ignoring record (invalid)', record);
		return;
	}

	jobid = record['value']['jobId'];
	taskid = record['value']['taskId'];
	maLog.debug('record: "%s" "%s" etag %s', name, record['key'],
	    record['_etag']);

	if (name == 'job') {
		if (!maJobs.hasOwnProperty(jobid)) {
			maLog.warn('ignoring record (unknown job)', record);
			return;
		}

		maJobUpdated(maJobs[jobid], record);
		return;
	}

	if (name == 'taskinput') {
		if (!maTasks.hasOwnProperty(taskid)) {
			maLog.warn('ignoring record (unknown task)', record);
			return;
		}

		maTaskAppendKey(maTasks[taskid], record['value']);
		return;
	}

	mod_assert.equal(name, 'task',
	    'poller returned a moray object we didn\'t ask for: ' + name);

	if (maTasks.hasOwnProperty(taskid))
		maTaskUpdated(maTasks[taskid], record);
	else if (record['value']['state'] != 'cancelled' &&
	    (!maJobs.hasOwnProperty(record['value']['jobId']) ||
	    !maJobs[record['value']['jobId']].j_cancelled))
		maTaskCreate(taskid, record);
}

function maMarkJobs(now)
{
	/*
	 * Our goal is to make sure that at least one task for each job is saved
	 * periodically.  We keep track of the last time a task was dirtied for
	 * this job, including which task it was.  If we find a job whose last
	 * dirty time was too long ago, if the write is not still pending, we
	 * dirty another task.
	 */
	mod_jsprim.forEachKey(maJobs, function (jobid, job) {
		if (job.j_dirtied &&
		    now - job.j_dirtied < job.j_save_interval) {
			maLog.debug('job "%s": skipping save (too recent)',
			    job.j_id);
			return;
		}

		if (job.j_saving &&
		    maMorayQueue.pending('task', job.j_saving)) {
			maLog.debug('job "%s": skipping save (already pending)',
			    job.j_id, job.j_saving);
			return;
		}

		for (var groupid in job.j_groups) {
			var group = maTaskGroups[groupid];
			var task = group.g_task || group.g_tasks[0];
			if (task) {
				maLog.debug('job "%s": saving via task',
				    job.j_id, task.t_id);
				maTaskDirty(task);
				break;
			}
		}
	});
}

function maCheckZonesLiveness()
{
	var now = Date.now();

	mod_jsprim.forEachKey(maZones, function (zonename, zone) {
		var group = zone.z_taskgroup;

		if (!group || group.g_state != maTaskGroup.TASKGROUP_S_RUNNING)
			return;

		if (now - zone.z_last_contact <= maZoneLivenessInterval)
			return;

		zone.z_log.error('zone agent has timed out');
		zone.z_failed = Date.now();
		group.g_log.warn('aborting (internal timeout)');
		group.g_failmsg = 'internal timeout';

		/*
		 * For now, we abort all of the individual tasks in this group.
		 * We could retry them instead.
		 */
		maTaskGroupAbortQueued(group, 'EJ_INTERNAL', 'internal error');
		maTaskGroupAbort(group);
	});
}

function maTaskGroupAbortQueued(group, code, message)
{
	var now = Date.now();
	var tasks = group.g_tasks;
	group.g_tasks = [];
	tasks.forEach(function (task) {
		maTaskError(task, now, code, message);
	});
}

/*
 * Aborts a task group.  The caller must have already dealt with all queued
 * tasks and set g_failmsg.
 */
function maTaskGroupAbort(group)
{
	mod_assert.ok(group.g_failmsg !== undefined);
	mod_assert.equal(group.g_tasks.length, 0);

	group.g_rqqueue.push(function (callback) {
		if (group.g_state != maTaskGroup.TASKGROUP_S_RUNNING &&
		    group.g_state != maTaskGroup.TASKGROUP_S_LOADING)
			return;

		if (group.g_task && !group.g_task.t_record['value']['result']) {
			maTaskError(group.g_task, Date.now(), 'EJ_INTERNAL',
			    'internal abort');
			group.g_task.t_record['value']['state'] = 'aborted';
		}

		maTaskGroupAdvance(group, callback);
	}, function () {});
}

/*
 * Marlin state is externally described in terms of individual tasks, but for
 * efficiency we group together tasks for the same job and phase that we will
 * run serially in a single compute zone.  This object maintains a queue of
 * input keys to be processed as well as program state associated with the
 * execution of these tasks in a zone.
 *
 * For map phases, the task group consists of many separate tasks, each with one
 * input key.  For reduce phases, the task group is just a single task, but with
 * many input keys.
 */
function maTaskGroup(groupid, job, pi)
{
	/* Immutable job and task group state */
	this.g_groupid = groupid;	/* group identifier */
	this.g_jobid = job.j_id;	/* job identifier */
	this.g_phasei = pi;		/* phase number */

	/* filled in asynchronously when we retrieve the job record */
	this.g_phase = undefined;	/* phase specification */
	this.g_map_keys = undefined;	/* whether to hyprlofs map keys */
	this.g_multikey = undefined;	/* whether this is a reduce phase */

	/* filled in asynchronously */
	this.g_login = undefined;	/* user's login name */

	/* Dynamic state */
	this.g_state = maTaskGroup.TASKGROUP_S_INIT;
	this.g_log = maLog.child({ 'component': 'group-' + groupid });
	this.g_machine = undefined;	/* assigned zonename */
	this.g_load_assets = undefined;	/* assets vasync cookie */
	this.g_pipeline = undefined;	/* dispatch vasync cookie */
	this.g_failmsg = undefined;	/* fail message, if we fail to start */
	this.g_pending = false;		/* pending "commit" or "fail" */

	this.g_rqqueue = mod_vasync.queuev({
	    'concurrency': 1,
	    'worker': function queuefunc(task, callback) { task(callback); }
	});

	/*
	 * Execution state: we keep track of the currently executing task
	 * itself and the list of future tasks.  Recall that tasks are the
	 * single indivisible unit of work, so all input and output keys are
	 * associated with exactly one task.  Map tasks always have exactly one
	 * input key, which must be available when we start the task.  Reduce
	 * tasks may have any number of input and output keys, but they are
	 * streamed in so that only one is required in order to actually start
	 * executing the task.  Both map and reduce tasks may have any number of
	 * output keys, which must therefore be streamed out.
	 *
	 * Also recall that the in-zone agent has only a "pull" interface based
	 * around tasks.  We can't send messages to it directly, and it doesn't
	 * know anything about task groups.  The protocol works like this:
	 *
	 *    o When the in zone agent (ZA) is ready to start executing a task,
	 *      it makes a long poll HTTP request asking for work.  See
	 *      maTaskApiTask below.  When work (i.e. a task group) is
	 *      available, we return a response describing the currently pending
	 *      task.  See httpState() below.
	 *
	 *      For map tasks, the task description includes everything needed
	 *      to complete the task, including the single input key.  For
	 *      reduce tasks, the task description includes information about
	 *      the first N keys and whether there may be more keys later.
	 *
	 *    o For reduce tasks only: the ZA needs to stream input keys to the
	 *      user's program, but without ever having to process the entire
	 *      list of keys at once.  When it finishes processing the first N
	 *      keys, it indicates that to the agent, which then removes these
	 *      keys from memory.  The ZA then requests task information again
	 *      and gets a new response with the next N input keys and whether
	 *      there may be any more keys.  This is a long poll because it may
	 *      be some time before the next set of keys are available.
	 *
	 *    o When an output key is emitted from any task, if it's one of the
	 *      first few keys, it's appended to the task record, which is then
	 *      marked dirty.  Otherwise, a new task output record is emitted.
	 *
	 *    o When the ZA finishes processing the task, it reports either
	 *      success or failure.  The task's state, result, and error fields
	 *      are updated, the task is marked dirty, and the agent moves on to
	 *      the next task in the same way.  When all tasks are exhausted,
	 *      the group is removed.  When the last group for a job is removed,
	 *      the job is removed.
	 */
	this.g_start = undefined;		/* start time of current task */
	this.g_task = undefined;		/* currently executing task */
	this.g_noutput = 0;			/* current nr of output keys */
	this.g_tasks = [];			/* queued tasks */
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
		'jobid': this.g_jobid,
		'phasei': this.g_phasei,
		'state': this.g_state,
		'machine': this.g_machine,
		'start': this.g_start,
		'failmsg': this.g_failmsg,
		'pending': this.g_pending,
		'load_assets': this.g_load_assets
	};

	var task = this.g_task || this.g_tasks[0];

	if (task) {
		rv['task'] = task.t_id;
		rv['task_multikey'] = this.g_multikey;
		rv['task_ninput'] = task.t_xinput.length;
		rv['task_input_done'] = task.t_input_done || false;
	}

	return (rv);
};

maTaskGroup.prototype.taskState = function ()
{
	var rv, outbase, base, host;

	mod_assert.ok(this.g_phase !== undefined);
	mod_assert.ok(this.g_task !== undefined);

	rv = {
	    'taskId': this.g_task.t_id,
	    'taskPhase': this.g_phase
	};

	if (this.g_login) {
		outbase = mod_path.join('/', this.g_login, 'jobs', this.g_jobid,
		    'stor');

		if (this.g_multikey)
			base = 'reduce';
		else if (this.g_task.t_record['value']['p0key'])
			base = this.g_task.t_record['value']['p0key'];
		else {
			base = this.g_task.t_record['value']['key'];

			if (mod_jsprim.startsWith(base, outbase))
				base = base.substr(outbase.length);
		}

		rv['taskErrorKey'] = mod_path.join(outbase,
		    base + '.' + this.g_phasei + '.err.' + this.g_task.t_id);
		rv['taskOutputKey'] = mod_path.join(outbase,
		    base + '.' + this.g_phasei + '.' + this.g_task.t_id);
		rv['taskOutputBase'] = mod_path.join(outbase,
		    base + '.' + this.g_phasei + '.');
	}

	if (this.g_multikey) {
		rv['taskInputKeys'] = this.g_task.t_xinput.slice(0, 10).map(
		    function (k) { return (k['key']); });

		/*
		 * We only indicate to the zone agent that input is complete if
		 * the task's input has been fully written out to Moray (as
		 * indicated by t_input_done) *and* we've actually read all
		 * taskinput records up to that point.
		 */
		rv['taskInputDone'] = maTaskReadAllRecords(this.g_task);

		/*
		 * XXX Ideally, this should be resolved each time the in-zone
		 * agent tries to use it, rather than once at the beginning of
		 * the reduce task, since we could have a failover at any point.
		 */
		host = maDnsCache.lookupv4(maMantaHost);
		rv['taskInputRemote'] = 'http://' + host + ':' + maMantaPort;
	} else {
		rv['taskInputKeys'] = [ this.g_task.t_record['value']['key'] ];
		rv['taskInputDone'] = true;

		if (this.g_map_keys)
			rv['taskInputFile'] = mod_path.join(
			    maZones[this.g_machine].z_manta_root,
			    rv['taskInputKeys'][0]);
	}

	return (rv);
};

function maTaskGroupSetJob(group, job)
{
	mod_assert.ok(job.j_record !== undefined);
	mod_assert.ok(job.j_groups.hasOwnProperty(group.g_groupid));
	mod_assert.equal(group.g_state, maTaskGroup.TASKGROUP_S_INIT);

	group.g_phase = job.j_record['value']['phases'][group.g_phasei];
	group.g_map_keys = group.g_phase['type'] == 'storage-map';
	group.g_multikey = group.g_phase['type'] == 'reduce';

	if (!group.g_multikey || group.g_tasks[0].t_xinput.length > 0) {
		group.g_state = maTaskGroup.TASKGROUP_S_QUEUED;
		maTaskGroupsQueued.push(group);
		maTaskGroupsPoke();
	}
}

function maTaskGroupAppendTask(group, task)
{
	group.g_log.debug('appending task "%s"', task.t_id);
	task.t_group = group;
	group.g_tasks.push(task);

	if (group.g_state == maTaskGroup.TASKGROUP_S_RUNNING &&
	    group.g_task === undefined)
		maZoneWakeup(maZones[group.g_machine]);
}

/*
 * Dispatches queued task groups to available zones until we run out of work to
 * do it or zones to do it in.
 */
function maTaskGroupsPoke()
{
	var zone, group;

	while (maZonesReady.length > 0 && maTaskGroupsQueued.length > 0) {
		/*
		 * This should rarely be an issue after startup, but we must not
		 * dispatch task groups before we've resolved a Manta hostname
		 * because we need that to fetch assets, to fetch objects for
		 * reducers, and to emit output files.  Once the hostname is
		 * resolved, the DNS cache will poke us again.
		 */
		if (!maDnsCache.lookupv4(maMantaHost)) {
			maLog.warn('delaying taskgroup dispatch because ' +
			    'host "%s" has not been resolved', maMantaHost);
			return;
		}

		zone = maZonesReady.shift();
		mod_assert.equal(zone.z_state,
		    mod_agent_zone.maZone.ZONE_S_READY);
		mod_assert.ok(zone.z_taskgroup === undefined);

		group = maTaskGroupsQueued.shift();
		maLog.debug('assigning group "%s" to zone "%s"',
		    group.g_groupid, zone.z_zonename);
		mod_assert.equal(group.g_state, maTaskGroup.TASKGROUP_S_QUEUED);
		mod_assert.ok(group.g_machine === undefined);

		zone.z_taskgroup = group;
		zone.z_state = mod_agent_zone.maZone.ZONE_S_BUSY;
		group.g_machine = zone.z_zonename;
		group.g_state = maTaskGroup.TASKGROUP_S_LOADING;

		group.g_pipeline = mod_vasync.pipeline({
		    'arg': group,
		    'funcs': maTaskGroupStagesDispatch
		}, function (err) {
			group.g_pipeline = undefined;

			if (err) {
				err = new mod_verror.VError(err,
				    'failed to dispatch task');
				group.g_log.error(err);
				group.g_failmsg = err.message;
				maTaskGroupAbortQueued(group, 'EJ_INIT',
				    err.message);
				maTaskGroupAbort(group);
			}
		});
	}
}

var maTaskGroupStagesDispatch = [
	maTaskGroupSetProperties,
	maTaskGroupLoadAssets,
	maTaskGroupDispatch
];

/*
 * Sets the zone's vmadm(1M) properties (e.g., max_swap and
 * max_physical_memory).
 */
function maTaskGroupSetProperties(group, callback)
{
	var zone = maZones[group.g_machine];
	var options = {};

	if (!group.g_multikey || !group.g_phase.hasOwnProperty('memory')) {
		callback();
		return;
	}

	options['max_physical_memory'] = group.g_phase['memory'];
	options['max_swap'] = 2 * group.g_phase['memory'];

	mod_agent_zone.maZoneSet(zone, options, callback);
}

/*
 * Loads the task's assets into its assigned zone.  "next" is invoked only if
 * there are no errors.
 */
function maTaskGroupLoadAssets(group, callback)
{
	group.g_log.info('loading assets', group.g_phase);

	mod_assert.equal(group.g_state, maTaskGroup.TASKGROUP_S_LOADING);

	if (!group.g_phase.hasOwnProperty('assets')) {
		callback();
		return;
	}

	/*
	 * XXX A very large number of assets here could cause us to use lots of
	 * file descriptors, a potential source of DoS.  forEachParallel could
	 * have a maxConcurrency property that queues work.
	 */
	group.g_load_assets = mod_vasync.forEachParallel({
	    'inputs': group.g_phase['assets'],
	    'func': maTaskGroupLoadAsset.bind(null, group)
	}, function (err) {
		group.g_load_assets = undefined;
		callback(err);
	});
}

/*
 * Loads one asset for the given task group into its compute zone.
 */
function maTaskGroupLoadAsset(group, asset, callback)
{
	var zone = maZones[group.g_machine];
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
			    'host': maDnsCache.lookupv4(maMantaHost),
			    'port': maMantaPort,
			    'path': asset,
			    'headers': {
				'x-marlin': true
			    }
			});

			request.on('error', function (suberr) {
				output.end();
				callback(suberr);
			});

			request.on('response', function (response) {
				if (response.statusCode != 200) {
					output.end();
					callback(new mod_verror.VError(null,
					    'error retrieving asset "%s" ' +
					    '(status code %s)', asset,
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
function maTaskGroupDispatch(group, callback)
{
	var zone;

	zone = maZones[group.g_machine];
	group.g_log.info('dispatching taskgroup to zone "%s"', zone.z_zonename);
	mod_assert.equal(group.g_state, maTaskGroup.TASKGROUP_S_LOADING);
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_BUSY);
	zone.z_last_contact = Date.now();
	group.g_state = maTaskGroup.TASKGROUP_S_RUNNING;
	maCounters['taskgroups_dispatched']++;

	group.g_rqqueue.push(function (subcallback) {
		maTaskGroupAdvance(group, function () {
			subcallback();
			maZoneWakeup(zone);
		});
	}, callback);
}

/*
 * Wakes up any zone agent requests currently polling on work to do.
 */
function maZoneWakeup(zone)
{
	var waiters = zone.z_waiters;
	zone.z_waiters = [];
	waiters.forEach(function (w) { w.callback(w); });
}

/*
 * Invoked after processing each task (successfully or not) to advance the group
 * to the next task.  If this is a storage-map group, this operation also
 * updates the zone's hyprlofs mappings.  Although this function operates
 * asynchronously and those asynchronous operations can fail, the "advance"
 * operation itself never fails.  If a hyprlofs operation fails, we determine
 * the scope of the failure (the task or the entire group) and update the
 * taskgroup state accordingly.  The caller will never get an error in its
 * callback.
 *
 * All calls must be funneled through the task group's work queue to ensure that
 * there is only ever one "advance" operation ongoing for a given task group.
 */
function maTaskGroupAdvance(group, callback)
{
	var now, task, key, zone;

	mod_assert.ok(!group.g_pending,
	    'concurrent calls to maTaskGroupAdvance');

	group.g_task = undefined;
	group.g_start = undefined;
	group.g_noutput = 0;

	if (group.g_tasks.length === 0) {
		maTaskGroupMarkDone(group);
		callback();
		return;
	}

	/* Code that fails the task group must clear all pending tasks, too. */
	mod_assert.ok(group.g_failmsg === undefined);

	now = new Date();
	task = group.g_task = group.g_tasks.shift();
	task.t_record['value']['timeStarted'] = mod_jsprim.iso8601(now);
	task.t_record['value']['firstOutputs'] = [];

	if (!group.g_map_keys) {
		group.g_start = now;
		callback();
		return;
	}

	key = task.t_record['value'];

	if (key['key'][0] != '/') {
		maTaskError(task, now, 'EJ_NOENT', 'failed to load key');
		maTaskGroupAdvance(group, callback);
		return;
	}

	zone = maZones[group.g_machine];
	group.g_pending = true;
	zone.z_hyprlofs.removeAll(function (err) {
		mod_assert.ok(group.g_pending);

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
			group.g_pending = false;
			group.g_failmsg = 'internal error';
			group.g_log.error(err, 'aborting taskgroup because ' +
			    'failed to remove hyprlofs mappings');
			maTaskGroupAbortQueued(group, 'EJ_INTERNAL',
			    'internal error');
			maTaskGroupAdvance(group, callback);
			return;
		}

		var rootkeypath = mod_path.join('/zones', key['zonename'],
		    'root', 'manta', key['account'], key['objectid']);
		var localkeypath = key['key'].substr(1);

		zone.z_hyprlofs.addMappings([
		    [ rootkeypath, localkeypath ]
		], function (suberr) {
			mod_assert.ok(group.g_pending);
			group.g_pending = false;

			if (!suberr) {
				group.g_start = new Date();
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
			    'failed to load %j from %s', key, rootkeypath);
			group.g_log.error(suberr);
			maTaskError(task, new Date(), 'EJ_NOENT',
			    'failed to load key');
			maTaskGroupAdvance(group, callback);
		});
	});
}

/*
 * Cancel a task group in any state.  It's assumed that no external state needs
 * to be updated (generally because the whole job has already been cancelled, so
 * it's not necessary to update all of the individual tasks).
 */
function maTaskGroupCancel(group)
{
	var i, tasks;

	group.g_log.info('cancelled in state "%s"', group.g_state);

	if (group.g_state == maTaskGroup.TASKGROUP_S_QUEUED) {
		for (i = 0; i < maTaskGroupsQueued.length; i++) {
			if (maTaskGroupsQueued[i] === group)
				break;
		}

		mod_assert.ok(i < maTaskGroupsQueued.length);
		maTaskGroupsQueued.splice(i, 1);

		group.g_state = maTaskGroup.TASKGROUP_S_INIT;
	}

	/*
	 * Even if the group is currently running, only one task may be running,
	 * so clear out all of the others right now.  We don't actually need to
	 * update their records in Moray since the job itself was cancelled.
	 * XXX would be better if this didn't know that about it's caller.
	 */
	tasks = group.g_tasks;
	group.g_tasks = [];
	tasks.forEach(maTaskRemove);

	if (group.g_state == maTaskGroup.TASKGROUP_S_INIT) {
		maTaskGroupRemove(group);
		return;
	}

	mod_assert.ok(group.g_state == maTaskGroup.TASKGROUP_S_LOADING ||
	    group.g_state == maTaskGroup.TASKGROUP_S_RUNNING);
	group.g_failmsg = 'job cancelled';
	maTaskGroupAbort(group);
}

/*
 * Removes all references to this task group.  It's assumed at this point that
 * no tasks currently reference the group and the group is not enqueued to run.
 */
function maTaskGroupRemove(group)
{
	mod_assert.ok(group.g_state != maTaskGroup.TASKGROUP_S_QUEUED);
	mod_assert.equal(group.g_tasks.length, 0);
	mod_assert.equal(maTaskGroups[group.g_groupid], group);

	var job = maJobs[group.g_jobid];

	delete (job.j_groups[group.g_groupid]);
	delete (maTaskGroups[group.g_groupid]);

	if (mod_jsprim.isEmpty(job.j_groups) && !job.j_cancelled)
		delete (maJobs[job.j_id]);

	group.g_log.info('removed');
}

/*
 * Called when the task group has no more input keys to process.  Removes this
 * task immediately and resets the zone for use by another group.
 */
function maTaskGroupMarkDone(group)
{
	var zone;

	maCounters['taskgroups_done']++;
	group.g_log.info('group done (state = "%s")', group.g_state);
	group.g_state = maTaskGroup.TASKGROUP_S_DONE;
	maTaskGroupRemove(group);

	zone = maZones[group.g_machine];
	mod_assert.equal(zone.z_taskgroup, group);
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_BUSY);
	zone.z_taskgroup = undefined;

	if (zone.z_failed) {
		maZoneDisable(zone,
		    new VError('zone failed: %s', zone.z_failed));
		return;
	}

	if (!maZoneAutoReset) {
		maZoneDisable(zone, new VError('auto-reset is disabled'));
		return;
	}

	zone.z_state = mod_agent_zone.maZone.ZONE_S_UNINIT;

	if (!maZoneSaveLogs) {
		mod_agent_zone.maZoneMakeReady(zone, maZoneReady);
		return;
	}

	var outname = mod_path.join(maZoneLogRoot,
	    group.g_jobid + '.' + group.g_phasei + '.' + mod_uuid.v4());
	var logstream = mod_agent_zone.maZoneAgentLog(zone);
	var outstream = mod_fs.createWriteStream(outname);
	logstream.pipe(outstream);
	zone.z_log.info('copying zone agent log to "%s"', outname);

	function onLogError(err) {
		zone.z_log.warn(err, 'failed to read log for copy');
		mod_agent_zone.maZoneMakeReady(zone, maZoneReady);
		logstream.removeListener('end', onEnd);
		outstream.removeListener('error', onErr);
		outstream.destroy();
	}
	logstream.on('error', onLogError);

	function onEnd() {
		mod_agent_zone.maZoneMakeReady(zone, maZoneReady);
		outstream.removeListener('error', onErr);
		logstream.removeListener('error', onLogError);
	}
	logstream.on('end', onEnd);

	function onErr(err) {
		zone.z_log.warn(err, 'failed to write log copy');
		mod_agent_zone.maZoneMakeReady(zone, maZoneReady);
		logstream.removeListener('end', onEnd);
		logstream.removeListener('error', onLogError);
		logstream.destroy();
	}
	outstream.on('error', onErr);
}

/*
 * Invoked as a callback when the given zone transitions to the "ready" state
 * (or fails to do so).
 */
function maZoneReady(zone, err, reason)
{
	if (err) {
		mod_assert.equal(zone.z_state,
		    mod_agent_zone.maZone.ZONE_S_DISABLED);
		maZoneDisable(zone,
		    new VError(err, 'zone could not be made ready'));
		return;
	}

	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_READY);

	var options = maConf['zoneDefaults'];
	mod_agent_zone.maZoneSet(zone, options, function (suberr) {
		if (suberr) {
			maZoneDisable(zone, new VError(suberr,
			    'failed to set properties'));
			return;
		}

		maCounters['zones_readied']++;
		maZonesReady.push(zone);
		maTaskGroupsPoke();
	});
}

function maZoneDisable(zone, err)
{
	zone.z_state = mod_agent_zone.maZone.ZONE_S_DISABLED;
	maCounters['zones_disabled']++;
	zone.z_log.error(err, 'zone removed from service');
}

/*
 * HTTP entry points
 */
function maZoneAdd(zonename)
{
	var zone;

	if (maZones.hasOwnProperty(zonename)) {
		/*
		 * If this request identifies a zone that is currently disabled,
		 * we take this as a request to try to make it ready again.
		 */
		if (maZones[zonename].z_state ==
		    mod_agent_zone.maZone.ZONE_S_DISABLED) {
			mod_agent_zone.maZoneMakeReady(maZones[zonename],
			    maZoneReady);
			return (null);
		}

		return (new Error(
		    'attempted to add duplicate zone ' + zonename));
	}

	maLog.info('adding zone "%s"', zonename);
	zone = mod_agent_zone.maZoneAdd(zonename,
	    maLog.child({ 'component': 'zone-' + zonename }));
	maZones[zone.z_zonename] = zone;
	maCounters['zones_added']++;
	mod_agent_zone.maZoneMakeReady(zone, maZoneReady);
	return (null);
}

/* POST /zones */
function maHttpZonesAdd(request, response, next)
{
	var zonename, error;

	if (!request.query.hasOwnProperty('zonename') ||
	    request.query['zonename'].length === 0) {
		next(new mod_restify.InvalidArgumentError(
		    'missing argument: "zonename"'));
		return;
	}

	zonename = request.query['zonename'];
	error = maZoneAdd(zonename);

	if (error) {
		next(new mod_restify.InvalidArgumentError(error.message));
		return;
	}

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
	s.post('/fail', mod_restify.bodyParser({ 'mapParams': false }),
	    init, maTaskApiFail);
	s.post('/live', init, maTaskApiLive);

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
	    zone.z_taskgroup.g_state == maTaskGroup.TASKGROUP_S_RUNNING)
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
	var group = zone.z_taskgroup;

	if (zone.z_state == mod_agent_zone.maZone.ZONE_S_UNINIT) {
		/*
		 * This zone isn't ready.  This may be because of an inherent
		 * race in the zone reset path: we close the server socket and
		 * wake up any waiting requests, but it's possible that Node
		 * accepted a connection before we closed the server socket and
		 * only emitted the event on the subsequent tick (i.e., now),
		 * after the reset code woke up any waiters that it knew about.
		 * We have to catch this here and send the request out.
		 *
		 * It's also possible that this is a new client on the other end
		 * of the zone reset, and it's just managed to come up before we
		 * knew about it.  In that case, we're introducing an additonal
		 * delay here, which is unfortunate but shouldn't affect
		 * correctness (or latency of user jobs, unless all zones on
		 * this machine are saturated).
		 */
		request.log.debug('caught zone agent too early/too late');
		response.send(new mod_restify.ServiceUnavailableError());
		next();
		return;
	}

	if (group && group.g_state == maTaskGroup.TASKGROUP_S_RUNNING &&
	    group.g_task) {
		maZoneHeartbeat(zone);

		if (!group.g_multikey || group.g_task.t_xinput.length > 0 ||
		    maTaskReadAllRecords(group.g_task)) {
			response.send(group.taskState());
			next();
			return;
		}
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
	var group = zone.z_taskgroup;
	var now, nkeys;

	if (!maTaskApiValidate(request, response, next))
		return;

	maZoneHeartbeat(zone);

	/*
	 * There are two arguments for this resource:
	 *
	 *    key	name of the current key
	 *		(map tasks, and non-final call for reduce tasks)
	 *
	 *    nkeys     number of keys processed
	 *		(non-final call for reduce tasks only)
	 *
	 * While it doesn't make any sense for a user to submit concurrent
	 * "commit" or "fail" requests, such requests should have ACID
	 * semantics.  Since they all operate on a single piece of state (the
	 * current key), we simply process them serially by sending them through
	 * a work queue.
	 */
	now = new Date();
	group.g_rqqueue.push(function (callback) {
		if (!maTaskApiValidate(request, response, callback))
			return;

		mod_assert.equal(group.g_state,
		    maTaskGroup.TASKGROUP_S_RUNNING);

		if (!group.g_multikey || group.g_task.t_xinput.length > 0) {
			if (!body.hasOwnProperty('key')) {
				callback(new mod_restify.InvalidArgumentError(
				    '"key" required'));
				return;
			}

			var first;
			if (group.g_multikey)
				first = group.g_task.t_xinput[0]['key'];
			else
				first = group.g_task.t_record['value']['key'];

			if (body['key'] != first) {
				callback(new mod_restify.ConflictError('key "' +
				    body['key'] + '" is not the current key (' +
				    first + ')'));
				return;
			}
		}

		if (group.g_multikey && group.g_task.t_xinput.length > 0) {
			if (!body.hasOwnProperty('nkeys')) {
				callback(new mod_restify.InvalidArgumentError(
				    '"nkeys" required'));
				return;
			}

			nkeys = parseInt(body['nkeys'], 10);
			if (isNaN(nkeys)) {
				callback(new mod_restify.InvalidArgumentError(
				    '"nkeys" must be a number'));
				return;
			}

			group.g_task.t_xinput =
			    group.g_task.t_xinput.slice(nkeys);
			maTaskUpdate(group.g_task, group.g_machine,
			    group.g_noutput, now);
			callback();
			return;
		}

		maTaskDone(group.g_task, group.g_machine, group.g_noutput, now);
		maTaskGroupAdvance(group, callback);
	}, function (err) {
		if (err) {
			next(err);
		} else {
			response.send(204);
			next();
		}
	});
}

/*
 * POST /fail: indicate that the given task has failed
 */
function maTaskApiFail(request, response, next)
{
	var body = request.body || {};
	var zone = request.maZone;
	var group = zone.z_taskgroup;
	var now, error;

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

	if (!maTaskApiValidate(request, response, next))
		return;

	maZoneHeartbeat(zone);

	/*
	 * See comments in maTaskApiCommit.
	 */
	now = new Date();
	group.g_rqqueue.push(function (callback) {
		if (!maTaskApiValidate(request, response, callback))
			return;

		mod_assert.equal(group.g_state,
		    maTaskGroup.TASKGROUP_S_RUNNING);

		group.g_task.t_record['value']['machine'] = group.g_machine;
		maTaskError(group.g_task, now, error['code'], error['message']);
		maTaskGroupAdvance(group, callback);
	}, function (sent) {
		if (!sent)
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
	var zone, key, group, proxyargs, reducer;

	if (!maTaskApiValidate(request, response, next))
		return;

	zone = request.maZone;
	group = zone.z_taskgroup;
	key = mod_url.parse(request.url).pathname.substr('/object'.length);
	mod_assert.equal(group.g_state, maTaskGroup.TASKGROUP_S_RUNNING);

	/*
	 * This is hokey, but that's the story until we have proper auth.
	 */
	if (!group.g_login ||
	    mod_mautil.pathExtractFirst(key) != group.g_login) {
		request.log.warn('permission denied: group login, key:',
		    group.g_login, key);
		response.send(403);
		next();
		return;
	}

	maZoneHeartbeat(zone);

	if (request.method == 'PUT') {
		/*
		 * XXX consider what happens if we crash before saving this.
		 */
		reducer = request.headers['x-marlin-reducer'];

		if (request.headers['x-marlin-stream'] &&
		    request.headers['x-marlin-stream'] == 'stderr') {
			group.g_task.t_record['value']['stderr'] = key;
		} else if (++group.g_noutput <= 5) {
			group.g_task.t_record['value']['firstOutputs'].push({
			    'key': key,
			    'rIdx': reducer,
			    'timeCreated': mod_jsprim.iso8601(Date.now())
			});
		} else {
			var uuid = mod_uuid.v4();
			maMorayQueue.dirty('taskoutput', uuid, {
			    'jobId': group.g_jobid,
			    'taskId': group.g_task.t_id,
			    'key': key,
			    'kIdx': reducer,
			    'timeCreated': mod_jsprim.iso8601(Date.now())
			});
		}

		/*
		 * mcat uses the x-marlin-reference header to indicate that it's
		 * not actually creating a new file, but just marking it for
		 * output for this task.  In that case, we simply skip the proxy
		 * step.
		 */
		if (request.headers['x-marlin-reference']) {
			response.send(204);
			next();
			return;
		}
	}

	proxyargs = {
	    'request': request,
	    'response': response,
	    'server': {
		'headers': {
		    'x-marlin': 'true'
		},
	        'host': maDnsCache.lookupv4(maMantaHost),
		'port': maMantaPort,
		'path': key
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

function maZoneHeartbeat(zone)
{
	zone.z_last_contact = Date.now();
}

function maTaskApiLive(request, response, next)
{
	if (!maTaskApiValidate(request, response, next))
		return;

	maZoneHeartbeat(request.maZone);
	response.send(200);
	next();
}

/*
 * Kang (introspection) entry points
 */
function maKangListTypes()
{
	return ([ 'zones', 'pending_requests', 'pending_task_groups',
	    'global' ]);
}

function maKangListObjects(type)
{
	if (type == 'pending_task_groups')
		return (Object.keys(maTaskGroups));

	if (type == 'pending_requests')
		return (Object.keys(maRequests));

	if (type == 'global')
		return ([ 0 ]);

	return (Object.keys(maZones));
}

function maKangGetObject(type, id)
{
	if (type == 'pending_task_groups') {
		var group = maTaskGroups[id];
		var rv = group.httpState();
		rv['rqqueue'] = group.g_rqqueue;
		if (group.g_load_assets)
			rv['loadAssets'] = group.g_load_assets;
		return (rv);
	}

	if (type == 'zones')
		return (maZones[id].httpState());

	if (type == 'global') {
		return ({
		    'tickStart': maTickStart,
		    'tickDone': maTickDone,
		    'nGroupsQueued': maTaskGroupsQueued.length,
		    'nZonesReady': maZonesReady.length,
		    'nReduceTasks': Object.keys(maTasksReduce).length
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
