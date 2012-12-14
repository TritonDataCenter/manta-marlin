/*
 * lib/agent/agent.js: compute node Marlin agent
 */

/*
 * Marlin agent
 *
 * This agent runs in the global zone of participating compute and storage nodes
 * and manages tasks run on that node.  It's responsible for setting up compute
 * zones for user jobs, executing the jobs, monitoring the user code, tearing
 * down the zones, and emitting progress updates to the appropriate job worker.
 *
 * This agent is tightly-coupled with the in-zone agent that runs in each
 * compute zone and manages tasks execution in that zone.
 *
 *
 * Jobs, tasks, task groups, and task streams
 *
 * Recall that users submit "jobs" to Marlin, and each job is made up of 1 or
 * more phases in which a user command is supposed to be invoked on a single
 * Manta object at a time (for map phases) or many objects at once (for reduce
 * phases).  To execute each phase, Marlin divides the work into individual map
 * and reduce "tasks", with each task representing a single execution of the
 * user's command on one or more Manta objects.
 *
 * To preserve isolation, tasks are executed inside dedicated compute zones that
 * are reset to their initial state in between tenants.  But because this reset
 * is somewhat expensive, tasks are allowed to run sequentially in a single
 * compute zone without resetting the zone, as long as all such tasks belong to
 * the same "task group", which means they're part of the same job and phase.
 *
 * While the tasks executed sequentially in a zone are part of one task group,
 * tasks from a single group may be run in parallel "task streams", each stream
 * executed in its own zone.  Since some tasks in a group may take longer than
 * others to execute, in order to maximize concurrency within each group, a task
 * is not assigned to a particular stream until the corresponding zone is
 * actually ready to execute the task.
 *
 * For the rest of this discussion, it may be helpful to think of task groups
 * simply as jobs.  The intuition is right: tasks in different jobs cannot be
 * executed in the same zone without resetting the zone between them.  It's just
 * that different phases of the same job may as well be separate jobs when it
 * comes to zone resets.
 *
 *
 * Task scheduling
 *
 * The algorithm for scheduling tasks on compute zones is driven by several
 * principles:
 *
 * (1) Let the kernel do as much of the work as possible.  The kernel is good at
 *     scheduling heavy workloads on finite resources and deals well with CPU
 *     saturation.  This principle drives us to configure systems with as many
 *     zones as possible to make sure the kernel has plenty of work with which
 *     to make best use of the CPUs.
 *
 * (2) The system should be responsive to incoming jobs, even when it's already
 *     very busy.  To accomplish this, we define a threshold of "reserve" zones
 *     that are available only to newly arriving task groups.  From an
 *     implementation perspective, when the number of available zones drops
 *     below the reserve threshold, existing task groups cannot grow to use
 *     the remaining zones -- only newly created task groups can use them, and
 *     each new group can only use one zone.  This policy allows newly submitted
 *     jobs, small or large, to get initial results quickly even when large jobs
 *     are occupying most of the system's resources.
 *
 * (3) All task groups should share *all* the non-reserved zones in some fair
 *     way.  If there's only one task group, it should run on all non-reserved
 *     zones.  If there are two task groups of equal size (see below), they
 *     should each get half of the non-reserved zones.
 *
 *     A task group's share of zones is determined by the number of tasks ready
 *     to run in that group divided by the total number of tasks ready to run in
 *     the system, except that the share cannot be less than 1 zone.  (Using
 *     tasks instead of counting each task group once avoids having users break
 *     up large jobs into multiple smaller ones just to get more concurrency.)
 *     As an example, if a system with 512 non-reserved zones has one task group
 *     with 50 tasks ready to run, and another task group with 150 tasks ready
 *     to run, then the first group gets 128 zones and the second group gets
 *     384.
 *
 * (4) We assume that setting up a zone for a particular task group is
 *     relatively cheap, that running a subsequent task from the same group in
 *     the same zone is free, but that resetting a zone for use by a different
 *     group is very expensive.  As a result, when there's a "tie" for the
 *     number of shares, we keep a zone running whichever task group it's
 *     already running.  For example, if a 512-zone box is saturated with 512
 *     task groups, each one scheduled on one zone, and another task group
 *     arrives, that group will be queued until one of the other groups
 *     completes, since they all have the same number of shares (1).
 *
 * Several examples help illustrate desired behavior.
 *
 * Example 1: Maximizing utilization and sharing resources fairly.
 *
 *    On an idle system, a large task group arrives to process thousands of
 *    objects.  This group is scheduled on all of the non-reserve zones.  When
 *    a second large task group arrives, the system gradually switches half of
 *    the zones to processing the new task group.  When a third large group
 *    arrives, each of the three groups is scheduled onto 1/3 of the available
 *    zones.
 *
 * Example 2: Maintaining responsiveness for incoming small jobs.
 *
 *    On an idle system, a large task group arrives to process thousands of
 *    objects.  As described in Example 1, this group will be scheduled onto all
 *    of the system's non-reserve zones.  When a small group arrives to process
 *    a single task, it immediately gets scheduled onto one of the reserve
 *    zones.  Additional tasks for the same group will be scheduled on the same
 *    zone, but if the number of tasks grows large, its growing share causes it
 *    to be scheduled on some of the zones previously used for the first group,
 *    as described in Example 1.
 *
 * Example 3: A saturated system.
 *
 *    A system is saturated with as many task groups as there are available
 *    zones, including reserves.  Each task group is thus scheduled onto exactly
 *    one zone.  As additional task groups arrive, they will be unable to run
 *    until one of the existing task groups completes.
 *
 * Example 4: A pathological job.
 *
 *    On an idle system, a large task group arrives whose execution goes into an
 *    infinite loop.  All non-reserve zones are occupied in this loop.  As new
 *    small groups arrive, they're able to run immediately in a reserve zone
 *    (see example 2).  But if one of these groups becomes large, it's not
 *    immediately able to get its expected half of the zones, since most of the
 *    zones are still occupied in the loop.  Eventually, the system decides that
 *    the first job's desired concurrency has exceeded its actual concurrency by
 *    too much for too long and start killing off these tasks, allowing the
 *    second large job to take some of the zones.
 *
 * With all this in mind, the actual scheduling algorithm works like this:
 *
 * When a new task arrives:
 *
 *     If the task does not belong to any existing task group, and there is a
 *     zone available to run the task (reserve or not), then the task is
 *     scheduled on that zone.  If there are no zones available, then the newly
 *     created task group is enqueued (in FIFO order) on a ready-to-run queue.
 *
 *     If the task belongs to an existing task group and the system has at least
 *     "nreserve + 1" zones available, then the task is scheduled on any
 *     available zone.  Otherwise, the task is enqueued in its task group.  The
 *     fact that "nreserve" zones are off limits to existing task groups
 *     ensures responsiveness for new task groups (principle (2)), and the fact
 *     that the expansion is otherwise unbounded ensures that jobs can expand to
 *     use all of the non-reserve resources (principle (3)).
 *
 * When a zone finishes executing a task:
 *
 *     If this is the only zone executing tasks for the given task group, then
 *     the zone picks up the next task in the same task group.  This results
 *     from principles (3) and (4): all groups are entitled to at least one
 *     zone, and it's too expensive to reset a zone for a different group when
 *     we know we're just going to have to reset it again to finish the group
 *     it's currently executing.
 *
 *     Otherwise, if there are any task groups on the ready-to-run queue, the
 *     zone resets and picks up the task group that's been waiting the longest.
 *     This comes from the fairness part of principle (3): by proportion, the
 *     waiting task group has a delta between desired concurrency (at least 1)
 *     and actual concurrency (0, for a percentage delta of infinity) that
 *     exceeds that of the current zone.
 *
 *     Otherwise, all task groups must already be scheduled on at least one
 *     zone, but the distribution may not match the desired concurrency levels
 *     for each task group.  We first compute whether this group would be above
 *     or below its desired concurrency if the current zone is assigned to a
 *     different group.  If it wouldn't be above it, then the zone picks up the
 *     next task in the same task group.  Otherwise, the zone resets and
 *     switches to the task group with the biggest delta between actual and
 *     desired concurrency.  We compute this by iterating all the task groups.
 */

var mod_assert = require('assert');
var mod_fs = require('fs');
var mod_http = require('http');
var mod_path = require('path');
var mod_url = require('url');
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


/*
 * Global agent state
 */

var maDnsWaiters = [];		/* queue of streams waiting on DNS */
var maJobs = {};		/* all jobs, by id */
var maTasks = {};		/* all tasks, by id */
var maTasksDeathrow = {};	/* tasks awaiting final save */
var maTasksReduce = {};		/* all reduce tasks, by id */
var maTaskGroups = {};		/* all task groups, by id */
var maTaskGroupsQueued = [];	/* ready-to-run task groups */
var maZones = {};		/* all zones, by zonename */
var maZonesReady = [];		/* ready zones */
var maJobHeartbeatThrottle;	/* job heartbeat throttle */
var maLog;			/* global logger */
var maServer;			/* global restify server */
var maMoray;			/* Moray interface */
var maMorayQueue;		/* Moray queue */
var maMorayPending;		/* Currently connecting */
var maMorayTaskPoll;		/* Poll state for task bucket */
var maMorayTaskThrottle;
var maTickStart;		/* last time a tick started */
var maTickDone;			/* last time a tick completed */
var maDnsCache;			/* local DNS cache */
var maRequests = {};		/* pending requests, by id */
var maCounters = {
    'streams_dispatched': 0,	/* streams dispatched to a zone */
    'streams_done': 0,		/* streams completed */
    'tasks_failed': 0,		/* individual tasks that failed */
    'tasks_committed': 0,	/* individual tasks committed */
    'zones_added': 0,		/* zones added to Marlin */
    'zones_readied': 0,		/* zones transitioned to "ready" */
    'zones_disabled': 0,	/* zones permanently out of commission */
    'mantarq_proxy_sent': 0,	/* requests forwarded to Manta */
    'mantarq_proxy_return': 0	/* responses received from Manta */
};
var maNTasks = 0;		/* total nr of tasks (used for scheduling) */
var maNZones = 0;		/* total nr of zones (used for scheduling) */
var maNZonesReserved = 0;	/* total nr of zones reserved */
var maZeroByteFilename = '/var/run/marlin/.zero';

/*
 * Static configuration
 */

var maRestifyServerName = 'MarlinAgent';
var maLogStreams = [ {
    'stream': process.stdout,
    'level': process.env['LOG_LEVEL'] || 'info'
} ];
var maZoneAutoReset = true;	/* false for debugging only */
var maZoneSaveLogs = true;	/* save zone agent logs before reset */
var maCheckNTasks = true;	/* in debug, periodically verify task count */
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


/*
 * Dynamic configuration
 */

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
		'httpMaxSockets': mod_schema.sNonNegativeInteger,
		'maxPendingPuts': mod_schema.sIntervalRequired,
		'timeJobSave': mod_schema.sIntervalRequired,
		'timePoll': mod_schema.sIntervalRequired,
		'timeTick': mod_schema.sIntervalRequired,
		'zoneReserveMin': mod_schema.sIntervalRequired,
		'zoneReservePercent': mod_schema.sPercentRequired
	    }
	},

	'zoneDefaults': {
	    'type': 'object',
	    'required': true
	},

	'zonesFile': mod_schema.sString
    }
};


/*
 * Mainline and initialization
 */

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

function usage(errmsg)
{
	if (errmsg)
		console.error(errmsg);

	console.error('usage: node agent.js [-o logfile] conffile');
	process.exit(2);
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
	maJobHeartbeatThrottle = new mod_mautil.Throttler(
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

	mod_http.globalAgent.maxSockets =
	    conf['tunables']['httpMaxSockets'] || 512;
}

/*
 * Create directories used to store log files.
 */
function maInitDirs()
{
	maLog.info('creating "%s"', maZoneLogRoot);
	mod_mkdirp.sync(maZoneLogRoot);

	maLog.info('creating zero-byte "%s"', maZeroByteFilename);
	mod_mkdirp.sync(mod_path.dirname(maZeroByteFilename));
	mod_fs.writeFileSync(maZeroByteFilename, '');
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
	    'onResolve': maDnsResolved
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
	maServer.post('/zones/:zonename/disable', maHttpZoneDisable);
	maServer.del('/zones/:zonename', maHttpZoneDelete);

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

function maDnsResolved()
{
	if (maDnsWaiters.length === 0)
		return;

	maLog.info('unblocking %d streams waiting on DNS',
	    maDnsWaiters.length);

	var waiters = maDnsWaiters;
	maDnsWaiters = [];
	waiters.forEach(function (callback) { callback(); });
}


/*
 * Periodic work
 */

/*
 * Invoked once per timeTick milliseconds to potentially kick off a poll for
 * more records from Moray and a save for our existing records.  Each of these
 * actions is throttled so that it won't happen either while another one is
 * ongoing or if it's been too recent since the last one.
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

	if (!maJobHeartbeatThrottle.tooRecent()) {
		maJobHeartbeatThrottle.start();
		maJobHeartbeatThrottle.done();
		maJobsHeartbeat(maTickStart);
	}

	maPoll(maTickStart.getTime());
	maZonesCheckLiveness(maMoray);
	maMorayQueue.flush();

	if (maCheckNTasks)
		maCheckTaskCount();

	maDeathrowReap();

	maTickDone = new Date();
}

/*
 * Ensure that we have a valid Moray client.  If there's one ready or being
 * initialized, we do nothing here.  Otherwise, we create a new one and start
 * initializing it.
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
 * Fetch updates to jobs, tasks, and taskinputs.  This is how we discover all
 * new work.
 */
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
			maLog.info('removing "%s" from maTasksReduce', taskid);
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
			/* Wake up waiters blocked on this query. */
			if (task.t_stream !== undefined &&
			    maTaskStreamHasWork(task.t_stream))
				maZoneWakeup(maZones[task.t_stream.s_machine]);
		    }
		});
	});
}

/*
 * Invoked during polling when new or updated records are discovered.
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

	if (maTasksDeathrow.hasOwnProperty(taskid)) {
		if (maMorayQueue.pending('task', taskid)) {
			var task = maTasksDeathrow[taskid];

			if (task.t_record['_etag'] == record['_etag'])
				return;

			task.t_record['value']['timeCommitted'] =
			    record['value']['timeCommitted'];
			task.t_record['_etag'] = record['_etag'];
			maMorayQueue.dirty('task', task.t_id,
			    task.t_record['value'],
			    { 'etag': task.t_record['_etag'] });
		}

		return;
	}

	if (maTasks.hasOwnProperty(taskid)) {
		maTaskUpdated(maTasks[taskid], record);
		return;
	}

	if (record['value']['state'] != 'cancelled' &&
	    (!maJobs.hasOwnProperty(record['value']['jobId']) ||
	    !maJobs[record['value']['jobId']].j_cancelled))
		maTaskCreate(record);
}

/*
 * Go through all jobs and trigger a task save for any job which hasn't had any
 * tasks updated in a while.  This tells the worker that we're still alive.
 */
function maJobsHeartbeat(now)
{
	/*
	 * We keep track of the last time a task was dirtied for this job,
	 * including which task it was.  If we find a job whose last dirty time
	 * was too long ago, if the write is not still pending, we dirty another
	 * task.
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

		var task = maJobPickTask(job);

		if (task) {
			maLog.info('job "%s": saving via task',
			    job.j_id, task.t_id);
			maTaskDirty(task);
		}
	});
}

/*
 * Returns any task from this job, if the job has any tasks.  Recall that our
 * mechanism of heartbeating to the worker is to update any task associated with
 * the job, which is why it doesn't matter which task it is.
 */
function maJobPickTask(job)
{
	var group, stream;
	var groupid, streamid;

	for (groupid in job.j_groups) {
		group = maTaskGroups[groupid];

		for (streamid in group.g_streams) {
			stream = group.g_streams[streamid];
			if (stream.s_task)
				return (stream.s_task);
		}

		mod_assert.ok(group.g_tasks.length > 0);
		return (group.g_tasks[0]);
	}
}

/*
 * Take a pass over all zones to see whether it's been too long since we've
 * heard from their in-zone agent, in which case we time out the running task
 * and disable the zone.
 */
function maZonesCheckLiveness()
{
	var now = Date.now();

	mod_jsprim.forEachKey(maZones, function (zonename, zone) {
		var stream = zone.z_taskstream;

		if (!stream ||
		    stream.s_state != maTaskStream.TASKSTREAM_S_RUNNING)
			return;

		if (now - zone.z_last_contact <= maZoneLivenessInterval)
			return;

		/*
		 * A zone agent timeout is a pretty serious failure.  We disable
		 * the zone.  Eventually we'll need alarms on this to make it
		 * clear that operator intervention is required to diagnose and
		 * clear the failure.  We could potentially even skip this by
		 * collecting core files dumped by the zone, logs from the zone,
		 * a ptree of the zone, and a core of any running zone-agent
		 * process, and then resetting the zone.
		 */
		zone.z_log.error('zone agent has timed out');
		zone.z_failed = Date.now();

		maTaskStreamAbort(stream, {
		    'code': 'EJ_INTERNAL',
		    'message': 'internal error',
		    'messageInternal': 'zone agent timed out'
		});
	});
}

/*
 * In debug mode, we periodically check that the total number of tasks in the
 * system matches what we expect.
 */
function maCheckTaskCount()
{
	var found = {};
	var maybe = {};
	var taskcount = 0;

	mod_jsprim.forEachKey(maTasks, function () { taskcount++; });

	if (taskcount != maNTasks) {
		maLog.fatal('maCheckTaskCount failed: maNTasks = %d, but ' +
		    'found %d tasks in maTasks', maNTasks, taskcount);
		process.abort();
	}

	mod_jsprim.forEachKey(maTaskGroups, function (_, group) {
		group.g_tasks.forEach(function (task, i) {
			if (!maTasks.hasOwnProperty(task.t_id)) {
				maLog.fatal('maCheckTaskCount failed: ' +
				    'found task "%s" on group "%s" idx %d ' +
				    'not in maTasks', task.t_id,
				    group.g_groupid, i);
				process.abort();
			}

			found[task.t_id] = true;
		});

		mod_jsprim.forEachKey(group.g_streams, function (__, stream) {
			/*
			 * XXX These linkages should be refactored so that we
			 * don't need gross checks like this.  maTaskRemove
			 * should be removed from maTaskDirty, and the callers
			 * of maTaskDone and maTaskError should just call
			 * another function that removes the linkages.
			 */
			if (stream.s_error !== undefined)
				return;

			if (stream.s_task === undefined)
				return;

			var task = stream.s_task;

			/*
			 * There are still some cases where tasks that have
			 * bombed out are still linked to their stream and
			 * already removed from global state, but that's not
			 * always the case.
			 */
			if (task.t_record['value']['result'] !== undefined ||
			    maJobs[group.g_jobid].j_cancelled !== undefined) {
				maybe[task.t_id] = true;
				return;
			}

			if (!maTasks.hasOwnProperty(task.t_id)) {
				maLog.fatal('maCheckTaskCount ' +
				    'failed: found task "%s" on ' +
				    'stream "%s" not in maTasks',
				    task.t_id, stream.s_id);
				process.abort();
			}

			found[stream.s_task.t_id] = true;
		});
	});

	mod_jsprim.forEachKey(maTasks, function (taskid) {
		if (!found.hasOwnProperty(taskid) &&
		    !maybe.hasOwnProperty(taskid)) {
			maLog.fatal('maCheckTaskCount failed: found task ' +
			    '"%s" on maTasks, but not in groups', taskid);
			process.abort();
		}

		delete (found[taskid]);
		delete (maybe[taskid]);
	});

	mod_jsprim.forEachKey(found, function (taskid) {
		maLog.fatal('maCheckTaskCount failed: found task "%s" ' +
		    'in groups, but not on maTasks', taskid);
		process.abort();
	});
}

function maDeathrowReap()
{
	mod_jsprim.forEachKey(maTasksDeathrow, function (taskid, _) {
		if (!maMorayQueue.pending('task', taskid))
			delete (maTasksDeathrow[taskid]);
	});
}


/*
 * Classes
 */

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

maJob.prototype.kangState = function ()
{
	return ({
	    'jobid': this.j_id,
	    'record': this.j_record,
	    'saveInterval': this.j_save_interval,
	    'dirtied': this.j_dirtied,
	    'saving': this.j_saving,
	    'throttle': this.j_update_throttle,
	    'poll': this.j_poll
	});
};

/*
 * Recall that all work in Marlin is assigned in terms of individual tasks, each
 * of which specifies the single execution of the user's command on one (for
 * map) or more (for reduce) Manta objects.  maTask objects keep track of the
 * running state of a task.
 */
function maTask(record)
{
	this.t_id = record['value']['taskId'];	/* task identifier */
	this.t_record = record;			/* current Moray state */
	this.t_xinput = [];			/* external input records */
	this.t_group = undefined;		/* assigned task group */
	this.t_stream = undefined;		/* assigned task stream */
	this.t_cancelled = false;		/* task is cancelled */
	this.t_nread = 0;			/* number of inputs read */
	this.t_ninputs = record['value']['nInputs'];	/* nr of inputs */

	/* used for reduce tasks only */
	this.t_poll = new mod_mamoray.PollState(maTimePoll, maRingbufferSize);
}

/*
 * To preserve isolation, tasks are executed in dedicated compute zones that are
 * set back to a clean state in between tenants.  A set of tasks that may be
 * run in the same zone without a reset are called a "task group".  See the
 * big comment at the top of this file.
 */
function maTaskGroup(groupid, job, pi)
{
	/* Immutable job and task group state */
	this.g_groupid = groupid;	/* group identifier */
	this.g_jobid = job.j_id;	/* job identifier */
	this.g_phasei = pi;		/* phase number */

	/* filled in asynchronously when we retrieve the job record */
	this.g_token = undefined;	/* auth token */
	this.g_phase = undefined;	/* phase specification */
	this.g_map_keys = undefined;	/* whether to hyprlofs map keys */
	this.g_multikey = undefined;	/* whether this is a reduce phase */

	/* filled in asynchronously */
	this.g_login = undefined;	/* user's login name */

	/* dynamic state */
	this.g_log = maLog.child({ 'component': 'group-' + groupid });
	this.g_state = maTaskGroup.TASKGROUP_S_INIT;
	this.g_tasks = [];		/* queue of tasks to be run */
	this.g_streams = {};		/* set of concurrent streams */
	this.g_nstreams = 0;		/* number of streams running */
	this.g_nrunning = 0;		/* number of tasks running */
}

/*
 * TaskGroups may be in one of three states:
 *
 *    INIT	The group has been created because we were assigned some tasks
 *    		for it, but these tasks are not yet ready to run because either
 *    		we haven't fetched the group's job record yet or (for reduce) we
 *    		don't have any keys yet.
 *
 *
 *    QUEUED	The group's tasks are ready-to-run, but there are no zones
 *    		available to run the group's tasks yet.
 *
 *    RUNNING	The group's tasks have started running.
 */
maTaskGroup.TASKGROUP_S_INIT = 'init';
maTaskGroup.TASKGROUP_S_QUEUED = 'queued';
maTaskGroup.TASKGROUP_S_RUNNING = 'running';

maTaskGroup.prototype.kangState = function ()
{
	return ({
	    'groupid': this.g_groupid,
	    'jobid': this.g_jobid,
	    'phasei': this.g_phasei,
	    'phase': this.g_phase,
	    'token': this.g_token,
	    'login': this.g_login,
	    'mapKeys': this.g_map_keys,
	    'multiKey': this.g_multikey,
	    'ntasks': this.g_tasks.length,
	    'nrunning': this.g_nrunning,
	    'nstreams': this.g_nstreams,
	    'share': maSchedGroupShare(this)
	});
};

function maTaskStream(stream_id, group, machine)
{
	/* immutable state */
	this.s_id = stream_id;		/* stream identifier */
	this.s_group = group;		/* maTaskGroup object */
	this.s_machine = machine;	/* assigned zonename */

	/* helper objects */
	this.s_log = group.g_log.child({ 'component': 'stream-' + stream_id });

	/* dynamic state */
	this.s_state = maTaskStream.TASKSTREAM_S_LOADING;
	this.s_load_assets = undefined;		/* assets vasync cookie */
	this.s_pipeline = undefined;		/* dispatch vasync cookie */
	this.s_error = undefined;		/* stream error */

	/* used to serialize "commit" and "fail" */
	this.s_pending = false;

	this.s_rqqueue = mod_vasync.queuev({
	    'concurrency': 1,
	    'worker': function queuefunc(task, callback) { task(callback); }
	});

	/*
	 * Execution state: we keep track of the currently executing task and
	 * the input and output keys for this task.  Map tasks always have
	 * exactly one input key, which must be available when we start the
	 * task.  Reduce tasks may have any number of input keys, but they are
	 * streamed in so that only one is required in order to actually start
	 * executing the task.  Both map and reduce tasks may have any number of
	 * output keys, which must be streamed out.
	 *
	 */
	this.s_start = undefined;		/* start time of current task */
	this.s_task = undefined;		/* currently executing task */
	this.s_noutput = 0;			/* current nr of output keys */
}

/*
 * Task stream states:
 *
 *    LOADING		The zone is being set up to run tasks from this stream's
 *    			task group.  This involves updating zone properties
 *    			(like DRAM and swap) and downloading assets.
 *
 *    RUNNING		The zone is running some tasks.
 *
 *    DONE		The zone is longer running any tasks for this stream.
 *
 * Task streams are not created until they're ready to go, so they start
 * immediately in the LOADING state.  They also get removed from global
 * visibility when they enter the DONE state, but some asynchronous callbacks
 * can still have references and need to know if the stream is still valid.
 */
maTaskStream.TASKSTREAM_S_LOADING	= 'loading';
maTaskStream.TASKSTREAM_S_RUNNING	= 'running';
maTaskStream.TASKSTREAM_S_DONE		= 'done';

maTaskStream.prototype.kangState = function ()
{
	return ({
	    'id': this.s_id,
	    'jobid': this.s_group.g_jobid,
	    'phasei': this.s_group.g_phasei,
	    'machine': this.s_machine,
	    'state': this.s_state,
	    'vasyncAssets': this.s_load_assets,
	    'vasyncPipeline': this.s_pipeline,
	    'error': this.s_error,
	    'pending_update': this.s_pending,
	    'taskStart': this.s_start,
	    'taskId': this.s_task ? this.s_task.t_id : undefined,
	    'taskNOutput': this.s_noutput
	});
};

/*
 * Given a stream, return an object representing the current state.  This will
 * be passed to the zone agent and must contain all information necessary to
 * execute the current task and generate output.  For details on how the zone
 * agent uses this information, see the comment in zone-agent.js.
 */
function maTaskStreamState(stream)
{
	var group, task, rv, outbase, base;

	group = stream.s_group;
	mod_assert.ok(group.g_phase !== undefined);

	task = stream.s_task;
	mod_assert.ok(task !== undefined);

	rv = {};

	/*
	 * Start with the basics:
	 *
	 *    taskId			Task identifier
	 *
	 *    taskPhase			Full phase description, taken straight
	 *    				from the job record.  This tells the
	 *    				zone agent exactly what to execute.
	 */
	rv['taskId'] = task.t_id;
	rv['taskPhase'] = group.g_phase;

	/*
	 * Next we construct suggested names for the stderr object, the first
	 * stdout object, and the base name for subsequent stdout objects:
	 *
	 *    taskErrorKey		Suggested name for stderr object.
	 *
	 *    taskOutputKey		Suggested name for first "anonymous"
	 *    				stdout object.  ("Anonymous" in this
	 *    				case refers to the fact that the user
	 *    				didn't specify a name and must not
	 *    				depend on whatever name is assigned, as
	 *    				this could be replaced in the future
	 *    				with a direct pipe.)
	 *
	 *    taskOutputBase		Suggested base name for subsequent
	 *    				"anonymous" stdout objects.
	 */
	outbase = mod_path.join('/', group.g_login, 'jobs',
	    group.g_jobid, 'stor');

	if (group.g_multikey)
		base = 'reduce';
	else if (task.t_record['value']['p0key'])
		base = task.t_record['value']['p0key'];
	else {
		base = task.t_record['value']['key'];

		if (mod_jsprim.startsWith(base, outbase))
			base = base.substr(outbase.length);
	}

	rv['taskErrorKey'] = mod_path.join(outbase,
	    base + '.' + group.g_phasei + '.err.' + task.t_id);
	rv['taskOutputKey'] = mod_path.join(outbase,
	    base + '.' + group.g_phasei + '.' + task.t_id);
	rv['taskOutputBase'] = mod_path.join(outbase,
	    base + '.' + group.g_phasei + '.');

	/*
	 * Finally, specify the input, in the form of:
	 *
	 *    taskInputKeys	List of current input keys.  For map tasks,
	 *    			there is always exactly one key.  For reduce
	 *    			tasks, this includes a subset of keys that will
	 *    			be processed in this phase.  After processing
	 *    			each batch, the zone agent will make additional
	 *    			requests to process the next set of keys.
	 *
	 *    taskInputDone	If true, then there will be no input keys after
	 *    			the ones specified by taskInputKeys.  If false,
	 *    			there may or may not be more keys, and the zone
	 *    			agent will have to check again later to see if
	 *    			more arrived.
	 *
	 *    taskInputRemote	URL of Manta instance from which to fetch keys.
	 *    (remote only)	This is only used for tasks which fetch keys
	 *    			from elsewhere, which is currently only reduce
	 *    			tasks, but map tasks could also operate this way
	 *    			in the future when there's insufficient capacity
	 *    			on storage nodes to run all the tasks locally.
	 *
	 *    taskInputFile	Full path (inside the zone) to the local file
	 *    (local only)	corresponding to the input key.
	 */
	if (group.g_multikey) {
		rv['taskInputKeys'] = task.t_xinput.slice(0, 10).map(
		    function (k) { return (k['key']); });
		rv['taskInputDone'] = maTaskReadAllRecords(task);
		rv['taskInputRemote'] = '/tmp/.marlin.sock';
	} else {
		rv['taskInputKeys'] = [ task.t_record['value']['key'] ];
		rv['taskInputDone'] = true;

		if (group.g_map_keys)
			rv['taskInputFile'] = mod_path.join(
			    maZones[stream.s_machine].z_manta_root,
			    rv['taskInputKeys'][0]);
	}

	return (rv);
}


/*
 * Create a new job record for the given jobid.  At this point, we don't know
 * anything about it except for its id, but we'll fetch the details when we next
 * poll.
 */
function maJobCreate(jobid)
{
	maJobs[jobid] = new maJob(jobid, maTimeJobSave, maTimePoll);
}

/*
 * Update an existing job object based on a potentially updated record.
 */
function maJobUpdated(job, record)
{
	var first = job.j_record === undefined;

	if (job.j_record && job.j_record['_etag'] == record['_etag'])
		return;

	job.j_record = record;

	if (record['value']['timeCancelled'] && !job.j_cancelled) {
		maLog.info('job "%s": cancelled', job.j_id);
		job.j_cancelled = record['value']['timeCancelled'];

		mod_jsprim.forEachKey(job.j_groups, function (groupid) {
			maTaskGroupCancel(maTaskGroups[groupid]);
		});

		/*
		 * TODO We currently have to keep this record in memory
		 * indefinitely or else we might pick up uncancelled tasks in
		 * cancelled jobs and not realize that they're cancelled.  (This
		 * may not be strictly necessary any more as long as those tasks
		 * are not updated again and we make sure to go a full poll
		 * interval that receives at least one record before removing
		 * the job.)
		 */
		return;
	}

	if (!first)
		return;

	mod_jsprim.forEachKey(job.j_groups, function (groupid) {
		var group = maTaskGroups[groupid];
		maTaskGroupSetJob(group, job);
	});
}


/*
 * Create a new task object for the given record.
 */
function maTaskCreate(record)
{
	var value, taskid, jobid, pi, groupid;
	var job, task, group;

	value = record['value'];
	taskid = value['taskId'];
	jobid = value['jobId'];
	pi = value['phaseNum'];
	groupid = jobid + '/' + pi;

	task = new maTask(record);
	maNTasks++;
	maTasks[taskid] = task;
	if (!record['value']['key'])
		maTasksReduce[taskid] = true;

	if (maTaskGroups.hasOwnProperty(groupid)) {
		maTaskGroupAppendTask(maTaskGroups[groupid], task);
		return;
	}

	if (!maJobs.hasOwnProperty(jobid))
		maJobCreate(jobid);

	job = maJobs[jobid];
	job.j_groups[groupid] = true;

	group = new maTaskGroup(groupid, job, pi);
	maTaskGroups[groupid] = group;
	group.g_log.debug('created group for job "%s" phase %d ' +
	    '(from task "%s")', jobid, pi, task.t_id);
	maTaskGroupAppendTask(group, task);

	if (job.j_record)
		maTaskGroupSetJob(group, job);
}

/*
 * Update an existing task object based on a potentially updated record.
 */
function maTaskUpdated(task, record)
{
	/*
	 * The only external updates we care about are cancellation and
	 * completion of the input stream (for reduce tasks only).
	 */
	var group = task.t_group;
	var needdirty = false;

	if (task.t_record['_etag'] == record['_etag'])
		return;

	/*
	 * No matter what else we do below, we must update our etag so that
	 * future writes will not wind up in an EtagConflict loop.  The code
	 * below is responsible for merging the remote changes so that we don't
	 * clobber them (if necessary).
	 */
	task.t_record['_etag'] = record['_etag'];
	needdirty = maMorayQueue.pending('task', task.t_id);

	/*
	 * Copy over fields that the worker may have written while we were
	 * working on the task.
	 */
	task.t_record['value']['nInputs'] = record['value']['nInputs'];
	task.t_record['value']['timeInputDone'] =
	    record['value']['timeInputDone'];

	if (record['value']['state'] == 'cancelled' && !task.t_cancelled) {
		group.g_log.info('task "%s": cancelled externally', task.t_id);

		task.t_cancelled = true;
		task.t_record['value']['state'] = 'cancelled';
		task.t_record['value']['error'] = record['value']['error'];

		if (task.t_stream) {
			maTaskStreamAbort(task.t_stream, {
			    'code': 'EJ_INTERNAL',
			    'message': 'internal abort',
			    'messageInternal': 'task cancelled externally'
			}, true);

			task.t_stream = undefined;
			maTaskRemove(task);
		} else {
			for (var i = 0; i < group.g_tasks.length; i++) {
				if (group.g_tasks[i] == task)
					break;
			}

			if (i < group.g_tasks.length) {
				group.g_tasks.splice(i, 1);
				group.g_log.info(
				    'task "%s": removed from queue', task.t_id);
			}

			maTaskRemove(task);

			if (group.g_nstreams === 0 &&
			    group.g_tasks.length === 0) {
				if (group.g_state ==
				    maTaskGroup.TASKGROUP_S_QUEUED)
					maTaskGroupCancel(group);
				else
					maTaskGroupRemove(group);
			}
		}
	}

	if (record['value']['timeInputDone'] && !task.t_cancelled &&
	    task.t_ninputs === undefined) {
		maLog.info('task "%s": input done', task.t_id);

		task.t_ninputs = record['value']['nInputs'];

		/*
		 * Wake up waiters for zones waiting for task input to be
		 * complete, if we've also read all of the corresponding
		 * taskinput records.
		 */
		if (task.t_stream !== undefined &&
		    task.t_xinput.length === 0 && maTaskReadAllRecords(task))
			maZoneWakeup(maZones[task.t_stream.s_machine]);
	}

	if (needdirty)
		maTaskDirty(task);
}

/*
 * Returns true if all of the given task's input records have been read.
 */
function maTaskReadAllRecords(task)
{
	if (task.t_ninputs === undefined)
		return (false);

	if (task.t_ninputs < task.t_nread)
		maLog.warn('task "%s": ninputs (%d) < nread (%d)',
		    task.t_id, task.t_ninputs, task.t_nread, task);

	return (task.t_ninputs <= task.t_nread);
}

/*
 * Returns true if there is work available for the in-zone agent on this stream.
 * This is true if the stream has any map task or if the stream has a reduce
 * task with at least one input object waiting or has finished reading all
 * input records.
 */
function maTaskStreamHasWork(stream)
{
	mod_assert.ok(stream.s_group);

	if (stream.s_task === undefined)
		return (false);

	var group = stream.s_group;
	var task = stream.s_task;

	if (!group.g_multikey)
		return (true);

	return (task.t_xinput.length > 0 || maTaskReadAllRecords(task));
}

/*
 * Remove a task from global state.  It's assumed that it's not referenced by
 * any other structures by this point.
 */
function maTaskRemove(task)
{
	maLog.debug('task "%s": removing', task.t_id);
	mod_assert.ok(task.t_stream === undefined);
	delete (maTasks[task.t_id]);
	delete (maTasksReduce[task.t_id]);
	maNTasks--;
}

/*
 * Append a new input key to the given reduce task.
 */
function maTaskAppendKey(task, keyinfo)
{
	maLog.debug('task "%s": appending key', task.t_id, keyinfo);
	task.t_xinput.push(keyinfo);
	task.t_nread++;

	var group = task.t_group;

	if (group.g_phase && group.g_state == maTaskGroup.TASKGROUP_S_INIT) {
		group.g_state = maTaskGroup.TASKGROUP_S_QUEUED;
		maTaskGroupsQueued.push(group);
		maSchedIdle();
		return;
	}

	if (task.t_stream !== undefined && task.t_xinput.length == 1)
		maZoneWakeup(maZones[task.t_stream.s_machine]);
}

/*
 * Schedule an update for the given task's record in Moray.
 */
function maTaskDirty(task)
{
	/*
	 * If the task has been completed or aborted, we can remove it
	 * immediately.  The MorayQueue will still has a reference and will keep
	 * attempting to save the record until it succeeds.  We must keep a
	 * reference in maTasksDeathrow so that we can update the expected etag
	 * if other updates come in after this point.
	 */
	if (task.t_record['value']['state'] == 'done' ||
	    task.t_record['value']['state'] == 'aborted') {
		if (maTasks.hasOwnProperty(task.t_id)) {
			maTaskRemove(task);
			maTasksDeathrow[task.t_id] = task;
		} else {
			maLog.warn('dirtied task "%s" after removing it',
			    task.t_id, task.t_record, new Error('stack').stack);
		}
	}

	maMorayQueue.dirty('task', task.t_id, task.t_record['value'],
	    { 'etag': task.t_record['_etag'] });

	/*
	 * Keep track of the last time each job is dirtied.
	 * See maJobsHeartbeat.
	 */
	var job = maJobs[task.t_record['value']['jobId']];
	if (!job)
		return;

	job.j_dirtied = Date.now();
	job.j_saving = task.t_id;
}

/*
 * Mark a task as failed with the given error.  The error object should contain
 * "code" and "message" fields, as well as "messageInternal" if the "code" is
 * EJ_INTERNAL.
 */
function maTaskError(task, when, error)
{
	mod_assert.ok(task.t_record['value']['result'] === undefined);
	task.t_group.g_nrunning--;
	maCounters['tasks_failed']++;

	task.t_record['value']['state'] = 'aborted';
	task.t_record['value']['result'] = 'fail';
	task.t_record['value']['timeDone'] = mod_jsprim.iso8601(when);
	task.t_record['value']['error'] = error;

	maTaskDirty(task);
}

/*
 * Mark a task as successfully completed with the given number of output keys.
 */
function maTaskDone(task, machine, nout, when)
{
	mod_assert.ok(task.t_record['value']['result'] === undefined);
	task.t_group.g_nrunning--;
	maCounters['tasks_committed']++;

	task.t_record['value']['nOutputs'] = nout;
	task.t_record['value']['state'] = 'done';
	task.t_record['value']['result'] = 'ok';
	task.t_record['value']['timeDone'] = mod_jsprim.iso8601(when);
	task.t_record['value']['machine'] = machine;

	maTaskDirty(task);
}

/*
 * Mark the given task as running in machine "machine" and having emitted "nout"
 * inputs so far.
 */
function maTaskUpdate(task, machine, nout)
{
	mod_assert.ok(task.t_record['value']['result'] === undefined);

	if (task.t_record['value']['state'] == 'dispatched')
		task.t_record['value']['state'] = 'running';

	task.t_record['value']['nOutputs'] = nout;
	task.t_record['value']['machine'] = machine;

	maTaskDirty(task);
}

function maTaskStreamAbort(stream, error, skipTaskUpdate)
{
	stream.s_rqqueue.push(function (callback) {
		if (stream.s_state == maTaskStream.TASKSTREAM_S_DONE) {
			stream.s_log.warn('ignoring stream abort because ' +
			    'the stream is already done', error);
			return;
		}

		stream.s_log.error('aborting', error);
		stream.s_error = error;

		if (stream.s_task === undefined) {
			stream.s_log.error('failed to initialize stream; ' +
			    'assuming task group failure');
			maTaskGroupError(stream.s_group, error);
		} else {
			var task = stream.s_task;

			if (!skipTaskUpdate &&
			    task.t_record['value']['result'] === undefined) {
				task.t_stream = undefined;
				maTaskError(task, Date.now(), error);
			} else
				stream.s_group.g_nrunning--;
		}

		maTaskStreamAdvance(stream, callback);
	}, function () {});
}

function maTaskGroupSetJob(group, job)
{
	mod_assert.ok(job.j_record !== undefined);
	mod_assert.ok(job.j_groups.hasOwnProperty(group.g_groupid));
	mod_assert.equal(group.g_state, maTaskGroup.TASKGROUP_S_INIT);

	group.g_token = job.j_record['value']['authToken'];
	group.g_phase = job.j_record['value']['phases'][group.g_phasei];
	group.g_map_keys = group.g_phase['type'] == 'storage-map';
	group.g_multikey = group.g_phase['type'] == 'reduce';
	group.g_login = job.j_record['value']['auth']['login'];

	if (!group.g_multikey || group.g_tasks[0].t_xinput.length > 0) {
		group.g_state = maTaskGroup.TASKGROUP_S_QUEUED;
		maTaskGroupsQueued.push(group);
		maSchedIdle();
	}
}

/*
 * Invoked when we discover a new task for a group.  This just enqueues it in
 * the task group.  Streams will pick up queued tasks as zones become available.
 * We also poke the scheduler, since this may have changed our shares.
 */
function maTaskGroupAppendTask(group, task)
{
	group.g_log.debug('appending task "%s"', task.t_id);
	task.t_group = group;
	group.g_tasks.push(task);

	maSchedGroup(group);
}


/*
 * Task scheduling
 */

/*
 * Update the number of reserve zones based on the current total number of
 * zones.
 */
function maSchedUpdateReserve()
{
	var nreserved = Math.floor(
	    maNZones * maConf['tunables']['zoneReservePercent'] / 100);
	mod_assert.ok(nreserved <= maNZones);
	mod_assert.ok(nreserved >= 0);

	nreserved = Math.max(nreserved, maConf['tunables']['zoneReserveMin']);
	maLog.info('updating reserve to %d zones', nreserved);
	maNZonesReserved = nreserved;
}

/*
 * Given a task group, determine how many zones this group should be allocated.
 * See the comment at the top of this file for much more detail.
 */
function maSchedGroupShare(group)
{
	var ntasks, nzones;

	/*
	 * The number of running tasks (g_nrunning) is almost the same as the
	 * number of streams (g_nstreams), but during initialization the task
	 * that will be run on some stream may still be on g_tasks, in which
	 * case we would double-count it here.
	 */
	ntasks = group.g_nrunning + group.g_tasks.length;
	mod_assert.ok(ntasks <= maNTasks);

	nzones = Math.floor(
	    (maNZones - maNZonesReserved) * (ntasks / maNTasks));

	return (Math.min(ntasks, Math.max(1, nzones)));
}

/*
 * Determines whether the given task group has more than its fair share of
 * zones.  If so, the caller may decide to repurpose one of those zones for some
 * other group.
 */
function maSchedGroupOversched(group)
{
	/*
	 * To keep the share computation quick, maSchedGroupShare estimates the
	 * share of zones by comparing the group's number of tasks to the total
	 * number of tasks, rounding down, but with a minimum share of 1.  But
	 * this computation isn't stable: consider a system with three zones and
	 * two task groups.  Each group gets 1 zone to start, then eventually
	 * the third zone will be assigned to one of them, but at that point
	 * whichever group has the extra zone will appear overscheduled, and
	 * we'll end up bouncing a zone back and forth between the two groups.
	 *
	 * To avoid such thrashing, we only say that a group has too many zones
	 * if it has strictly more than one more than it's fair share.  However,
	 * if the share is actually 1, we say the zone is overscheduled if it
	 * has more than that 1 zone.  If we didn't special-case this, every
	 * group would be guaranteed at least *two* zones, even on a saturated
	 * system.
	 */
	var nzones = maSchedGroupShare(group);
	mod_assert.ok(nzones > 0 && nzones <= maNZones);

	if (nzones == 1)
		return (group.g_nstreams > 1);

	return (group.g_nstreams - 1 > nzones);
}

/*
 * Dispatches queued task groups to available zones until we run out of work to
 * do it or zones in which to do it.
 */
function maSchedIdle()
{
	var zone, group;

	while (maZonesReady.length > 0 && maTaskGroupsQueued.length > 0) {
		group = maTaskGroupsQueued.shift();

		mod_assert.equal(group.g_state, maTaskGroup.TASKGROUP_S_QUEUED);
		group.g_state = maTaskGroup.TASKGROUP_S_RUNNING;

		zone = maZonesReady.shift();
		maSchedDispatch(group, zone);
	}

	while (maZonesReady.length > maNZonesReserved) {
		/*
		 * This is the expensive part.  Generally, we wouldn't expect to
		 * be executing this loop a lot, since it only happens when
		 * zones switch from an overscheduled task group to an
		 * underscheduled one.  Besides that, this operation is at worse
		 * O(nzones).  But if we spend a lot of time here, we could
		 * cache the results.
		 */
		group = maSchedPickGroup();

		if (group === undefined)
			break;

		zone = maZonesReady.shift();
		maSchedDispatch(group, zone);
	}
}

/*
 * Finds the task group having the most "need" of another zone, as computed
 * based on the difference between desired and actual concurrency as a
 * proportion of actual concurrency.
 */
function maSchedPickGroup()
{
	var bestGroup, bestValue;

	mod_jsprim.forEachKey(maTaskGroups, function (_, group) {
		var nzones, value;

		if (group.g_state != maTaskGroup.TASKGROUP_S_RUNNING)
			return;

		if (group.g_tasks.length === 0)
			return;

		mod_assert.ok(group.g_nstreams > 0);

		nzones = maSchedGroupShare(group);
		if (nzones <= group.g_nstreams)
			return;

		value = (nzones - group.g_nstreams) / group.g_nstreams;
		if (bestValue === undefined || value > bestValue) {
			bestValue = value;
			bestGroup = group;
		}
	});

	return (bestGroup);
}

/*
 * Invoked when we think there might be additional work to do for a single
 * group that can be parallelized with a new stream.
 */
function maSchedGroup(group)
{
	if (maZonesReady.length === 0)
		return;

	if (group.g_nstreams > 0 && maZonesReady.length <= maNZonesReserved)
		return;

	if (group.g_tasks.length === 0)
		return;

	if (group.g_state == maTaskGroup.TASKGROUP_S_INIT)
		return;

	var zone = maZonesReady.shift();
	maSchedDispatch(group, zone);
}

/*
 * Invoked when scheduling a task group on a zone.  Creates a new stream and
 * sets it running.
 */
function maSchedDispatch(group, zone)
{
	var streamid, stream;

	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_READY);
	mod_assert.ok(zone.z_taskstream === undefined);
	mod_assert.equal(group.g_state, maTaskGroup.TASKGROUP_S_RUNNING);

	streamid = group.g_groupid + '/' + zone.z_zonename;
	stream = new maTaskStream(streamid, group, zone.z_zonename);
	stream.s_state = maTaskStream.TASKSTREAM_S_LOADING;

	zone.z_state = mod_agent_zone.maZone.ZONE_S_BUSY;
	zone.z_taskstream = stream;

	group.g_streams[streamid] = stream;
	group.g_nstreams++;

	stream.s_log.info('created stream for job "%s" phase %d (group ' +
	    'concurrency now %d)', group.g_jobid, group.g_phasei,
	    group.g_nstreams);

	stream.s_pipeline = mod_vasync.pipeline({
	    'arg': stream,
	    'funcs': maTaskStreamStagesDispatch
	}, function (err) {
		stream.s_pipeline = undefined;

		if (!err)
			return;

		err = new VError(err, 'failed to dispatch task');
		maTaskStreamAbort(stream, {
		    'code': 'EJ_INIT',
		    'message': err.message
		});
	});
}

var maTaskStreamStagesDispatch = [
	maTaskStreamSetProperties,
	maTaskStreamWaitDns,
	maTaskStreamLoadAssets,
	maTaskStreamDispatch
];

/*
 * Sets the zone's vmadm(1M) properties (e.g., max_swap and
 * max_physical_memory).
 */
function maTaskStreamSetProperties(stream, callback)
{
	var group = stream.s_group;
	var zone = maZones[stream.s_machine];
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
 * Waits until we have valid IPs for any DNS names we need in order to process
 * the task.  We need these in order to fetch assets, to fetch remote input
 * files, and to save output.
 */
function maTaskStreamWaitDns(stream, callback)
{
	if (maDnsCache.lookupv4(maMantaHost)) {
		callback();
		return;
	}

	maLog.warn('delaying stream dispatch because host "%s" has not ' +
	    'been resolved', maMantaHost);
	maDnsWaiters.push(function () {
		maTaskStreamWaitDns(stream, callback);
	});
}

/*
 * Loads the task's assets into its assigned zone.  "next" is invoked only if
 * there are no errors.
 */
function maTaskStreamLoadAssets(stream, callback)
{
	var group = stream.s_group;

	mod_assert.equal(stream.s_state, maTaskStream.TASKSTREAM_S_LOADING);

	stream.s_log.info('loading assets for phase', group.g_phase);

	if (!group.g_phase.hasOwnProperty('assets')) {
		callback();
		return;
	}

	/*
	 * XXX A very large number of assets here could cause us to use lots of
	 * file descriptors, a potential source of DoS.  forEachParallel could
	 * have a maxConcurrency property that queues work.
	 */
	stream.s_load_assets = mod_vasync.forEachParallel({
	    'inputs': group.g_phase['assets'],
	    'func': maTaskStreamLoadAsset.bind(null, stream)
	}, function (err) {
		stream.s_load_assets = undefined;
		callback(err);
	});
}

/*
 * Loads one asset for the given task group into its compute zone.
 */
function maTaskStreamLoadAsset(stream, asset, callback)
{
	var group = stream.s_group;
	var zone = maZones[stream.s_machine];
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
				'authorization': sprintf('Token %s',
				    group.g_token)
			    }
			});

			request.on('error', function (suberr) {
				output.end();
				callback(suberr);
			});

			request.on('response', function (response) {
				if (response.statusCode != 200) {
					output.end();
					callback(new VError(
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
function maTaskStreamDispatch(stream, callback)
{
	mod_assert.equal(stream.s_state, maTaskStream.TASKSTREAM_S_LOADING);

	stream.s_log.info('starting stream execution');
	maCounters['streams_dispatched']++;

	var zone = maZones[stream.s_machine];
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_BUSY);
	zone.z_last_contact = Date.now();

	stream.s_rqqueue.push(function (subcallback) {
		mod_assert.equal(stream.s_state,
		    maTaskStream.TASKSTREAM_S_LOADING);
		stream.s_state = maTaskStream.TASKSTREAM_S_RUNNING;

		maTaskStreamAdvance(stream, function () {
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
 * Invoked after processing each task (successfully or not) to advance the
 * stream to the next task.  If this task is part of a storage-map group, this
 * operation also updates the zone's hyprlofs mappings.  Although this function
 * operates asynchronously and those asynchronous operations can fail, the
 * "advance" operation itself never fails.  If a hyprlofs operation fails, we
 * determine the scope of the failure (the task or the entire stream) and update
 * the stream state accordingly.  The caller will never get an error in its
 * callback.
 *
 * All calls must be funneled through the task group's work queue to ensure that
 * there is only ever one "advance" operation ongoing for a given task group.
 */
function maTaskStreamAdvance(stream, callback)
{
	var now, task, group, key, zone;

	mod_assert.ok(!stream.s_pending,
	    'concurrent calls to maTaskStreamAdvance');

	if (stream.s_error !== undefined ||
	    maZones[stream.s_machine].z_quiesced !== undefined ||
	    stream.s_group.g_tasks.length === 0 ||
	    maSchedGroupOversched(stream.s_group)) {
		maTaskStreamCleanup(stream);
		callback();
		return;
	}

	group = stream.s_group;
	group.g_nrunning++;
	task = group.g_tasks.shift();
	mod_assert.ok(task.t_stream === undefined);
	task.t_stream = stream;

	now = new Date();
	stream.s_task = task;
	stream.s_noutput = 0;
	stream.s_start = undefined;

	task.t_record['value']['timeStarted'] = mod_jsprim.iso8601(now);
	task.t_record['value']['firstOutputs'] = [];

	if (!group.g_map_keys) {
		stream.s_start = now;
		callback();
		return;
	}

	key = task.t_record['value'];

	if (key['key'][0] != '/') {
		task.t_stream = undefined;
		maTaskError(task, now, {
		    'code': 'EJ_NOENT',
		    'message': 'failed to load key'
		});
		maTaskStreamAdvance(stream, callback);
		return;
	}

	zone = maZones[stream.s_machine];
	stream.s_pending = true;
	zone.z_hyprlofs.removeAll(function (err) {
		mod_assert.ok(stream.s_pending);

		if (err) {
			/*
			 * The only way this should be possible is if the
			 * underlying hyprlofs mount gets unmounted.  This
			 * should be impossible because (a) the user shouldn't
			 * have permission to do that, even as root inside the
			 * zone, and (b) we still hold the file descriptor open,
			 * blocking the unmount.  So given that something very
			 * bad must have happened, we just fail the whole
			 * stream.
			 */
			stream.s_pending = false;
			maTaskStreamAbort(stream, {
			    'code': 'EJ_INTERNAL',
			    'message': 'internal error',
			    'messageInternal': sprintf(
				'failed to unmap hyprlofs: %s', err.message)
			});
			callback();
			return;
		}

		var rootkeypath = key['objectid'] == '/dev/null' ?
		    maZeroByteFilename :
		    mod_path.join('/zones', key['zonename'],
		    'root', 'manta', key['account'], key['objectid']);
		var localkeypath = key['key'].substr(1);

		zone.z_hyprlofs.addMappings([
		    [ rootkeypath, localkeypath ]
		], function (suberr) {
			mod_assert.ok(stream.s_pending);
			stream.s_pending = false;

			if (!suberr) {
				stream.s_start = new Date();
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
			stream.s_log.error(suberr);
			task.t_stream = undefined;
			maTaskError(task, new Date(), {
			    'code': 'EJ_NOENT',
			    'message': 'failed to load key',
			    'messageInternal': suberr.message
			});
			maTaskStreamAdvance(stream, callback);
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

	/* Clear out any queued tasks. */
	tasks = group.g_tasks;
	group.g_tasks = [];
	tasks.forEach(maTaskRemove);

	if (group.g_state == maTaskGroup.TASKGROUP_S_RUNNING) {
		/*
		 * Recall that once a group starts running, there must always be
		 * at least one stream for it.  We cancel each separate stream
		 * for this group, but we cannot remove ourselves from global
		 * visibility until the last stream actually completes.
		 */
		mod_assert.ok(!mod_jsprim.isEmpty(group.g_streams));
		mod_assert.ok(group.g_nstreams > 0);

		mod_jsprim.forEachKey(group.g_streams, function (_, stream) {
			maTaskStreamAbort(stream, {
			    'code': 'EJ_CANCELLED',
			    'message': 'job cancelled'
			}, true);

			if (stream.s_task) {
				stream.s_task.t_stream = undefined;
				maTaskRemove(stream.s_task);
			}
		});

		return;
	}

	if (group.g_state == maTaskGroup.TASKGROUP_S_QUEUED) {
		mod_assert.ok(mod_jsprim.isEmpty(group.g_streams));
		mod_assert.ok(group.g_nstreams === 0);

		for (i = 0; i < maTaskGroupsQueued.length; i++) {
			if (maTaskGroupsQueued[i] === group)
				break;
		}

		mod_assert.ok(i < maTaskGroupsQueued.length);
		maTaskGroupsQueued.splice(i, 1);

		group.g_state = maTaskGroup.TASKGROUP_S_INIT;
	}

	maTaskGroupRemove(group);
}

/*
 * Mark all tasks in a group as failed, as when assets could not be found.
 * XXX This could be handled better.  At the moment, we're going to go through a
 * lot of churn to figure this out, instead of failing the job completely.
 */
function maTaskGroupError(group, error)
{
	var now = new Date();
	var tasks = group.g_tasks;

	group.g_tasks = [];

	tasks.forEach(function (task) {
		++group.g_nrunning; /* XXX because maTaskError decrements it */
		maTaskError(task, now, error);
	});
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
	mod_assert.ok(mod_jsprim.isEmpty(group.g_streams));
	mod_assert.equal(group.g_nstreams, 0);
	mod_assert.equal(group.g_nrunning, 0);

	var job = maJobs[group.g_jobid];

	delete (job.j_groups[group.g_groupid]);
	delete (maTaskGroups[group.g_groupid]);

	if (mod_jsprim.isEmpty(job.j_groups) && !job.j_cancelled)
		delete (maJobs[job.j_id]);

	group.g_log.info('removed');
}

/*
 * Called when the stream has been terminated, either because the task group has
 * finished, the zone has been repurposed for another task group, or the stream
 * has failed fatally.  Removes this stream immediately and resets the zone for
 * use by another group.
 */
function maTaskStreamCleanup(stream)
{
	var task, group, zone;

	group = stream.s_group;
	mod_assert.equal(group.g_streams[stream.s_id], stream);

	zone = maZones[stream.s_machine];

	if (stream.s_task !== undefined) {
		task = stream.s_task;
		if (!(task.t_cancelled ||
		    zone.z_quiesced ||
		    task.t_record['value']['result'] !== undefined ||
		    maJobs[task.t_group.g_jobid].j_cancelled)) {
			stream.s_log.fatal('stream cancelled with task ' +
			    'for unknown reason');
			process.abort();
		}

		if (maTasks.hasOwnProperty(task.t_id)) {
			stream.s_log.fatal('stream cancelled with task ' +
			    'still in maTasks');
			process.abort();
		}
	}

	maCounters['streams_done']++;
	stream.s_log.info('stream terminated (state = "%s"), error = ',
	    stream.s_state, stream.s_error || 'no error');
	stream.s_state = maTaskStream.TASKSTREAM_S_DONE;

	delete (group.g_streams[stream.s_id]);
	if (--group.g_nstreams === 0) {
		if (group.g_tasks.length === 0) {
			maTaskGroupRemove(group);
		} else {
			/*
			 * This is an unusual situation where we took the last
			 * zone away from a group that has more tasks.  The
			 * stream may have failed fatally.  Whatever the reason,
			 * we re-enqueue this task group.
			 */
			group.g_state = maTaskGroup.TASKGROUP_S_QUEUED;
			maTaskGroupsQueued.push(group);
			process.nextTick(maSchedIdle);
		}
	}

	mod_assert.equal(zone.z_taskstream, stream);
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_BUSY);
	zone.z_taskstream = undefined;

	if (zone.z_quiesced !== undefined) {
		maZoneDisable(zone, new VError('zone disabled by operator'));
		return;
	}

	if (zone.z_failed !== undefined) {
		maZoneDisable(zone, new VError('zone failed'));
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

	if (zone.z_quiesced !== undefined) {
		maZoneDisable(zone, new VError('zone disabled by operator'));
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
		maSchedIdle();
	});
}

function maZoneDisable(zone, err)
{
	zone.z_state = mod_agent_zone.maZone.ZONE_S_DISABLED;
	maNZones--;
	maCounters['zones_disabled']++;
	zone.z_log.error(err, 'zone removed from service');

	maSchedUpdateReserve();
	maSchedIdle();
}

function maZoneAdd(zonename)
{
	var zone;

	if (maZones.hasOwnProperty(zonename)) {
		/*
		 * If this request identifies a zone that is currently disabled,
		 * we take this as a request to try to make it ready again.
		 */
		zone = maZones[zonename];
		if (zone.z_state == mod_agent_zone.maZone.ZONE_S_DISABLED) {
			zone.z_quiesced = undefined;
			maNZones++;
			mod_agent_zone.maZoneMakeReady(zone, maZoneReady);
			maSchedUpdateReserve();
			return (null);
		}

		return (new Error(
		    'attempted to add duplicate zone ' + zonename));
	}

	maLog.info('adding zone "%s"', zonename);
	zone = mod_agent_zone.maZoneAdd(zonename,
	    maLog.child({ 'component': 'zone-' + zonename }));
	maZones[zone.z_zonename] = zone;
	maNZones++;
	maCounters['zones_added']++;
	mod_agent_zone.maZoneMakeReady(zone, maZoneReady);
	maSchedUpdateReserve();
	return (null);
}


/*
 * HTTP entry points
 */

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

/* POST /zones/:zonename/disable */
function maHttpZoneDisable(request, response, next)
{
	var zonename, zone;

	zonename = request.params['zonename'];

	if (!maZones.hasOwnProperty(zonename)) {
		next(new mod_restify.NotFoundError());
		return;
	}

	zone = maZones[zonename];

	if (zone.z_state == mod_agent_zone.maZone.ZONE_S_DISABLED) {
		response.send(204);
		next();
		return;
	}

	if (zone.z_state == mod_agent_zone.maZone.ZONE_S_BUSY ||
	    zone.z_state == mod_agent_zone.maZone.ZONE_S_UNINIT) {
		if (!zone.z_quiesced)
			zone.z_quiesced = Date.now();

		response.send(204);
		next();
		return;
	}

	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_READY);
	mod_assert.ok(zone.z_taskstream === undefined);

	for (var i = 0; i < maZonesReady.length; i++) {
		if (maZonesReady[i] == zone)
			break;
	}

	if (i < maZonesReady.length)
		maZonesReady.splice(i, 1);

	maZoneDisable(zone, new VError('zone disabled by operator'));
	response.send(204);
	next();
}

/*
 * DELETE /zones/:zonename
 */
function maHttpZoneDelete(request, response, next)
{
	var zonename, zone;

	zonename = request.params['zonename'];

	if (!maZones.hasOwnProperty(zonename)) {
		next(new mod_restify.NotFoundError());
		return;
	}

	zone = maZones[zonename];

	if (zone.z_state != mod_agent_zone.maZone.ZONE_S_DISABLED) {
		next(new mod_restify.ConflictError('zone is not disabled'));
		return;
	}

	mod_agent_zone.maZoneRemove(zone, function (err) {
		if (err) {
			next(err);
			return;
		}

		maLog.info('removing zone "%s"', zonename);
		delete (maZones[zonename]);
		response.send(204);
		next();
	});
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
	 * We prepend the body parser to our own control APIs, but not the ones
	 * that we forward to Manta, since those stream potentially large
	 * amounts of arbitrary data.
	 */
	var init = function maTaskApiInit(request, response, next) {
		request.maZone = zone;
		next();
	};

	s.get('/my/jobs/task/task',
	     mod_restify.bodyParser({ 'mapParams': false }),
	    init, maTaskApiTask);
	s.post('/my/jobs/task/commit',
	    mod_restify.bodyParser({ 'mapParams': false }),
	    init, maTaskApiCommit);
	s.post('/my/jobs/task/fail',
	    mod_restify.bodyParser({ 'mapParams': false }),
	    init, maTaskApiFail);
	s.post('/my/jobs/task/live', init, maTaskApiLive);

	/*
	 * We proxy the rest of the Manta API under /.
	 */
	var methods = [ 'get', 'put', 'post', 'del', 'head' ];
	methods.forEach(function (method) {
		s[method]('/.*', init, maTaskApiManta);
	});

	s.on('expectContinue', function (request, response, route) {
		/*
		 * When we receive a request with "Expect: 100-continue", we run
		 * the route as normal, since this will necessarily be a
		 * proxying request.  This handler only exists to avoid Node's
		 * default behavior of immediately sending the "100 continue",
		 * which is wrong and leads to very nasty (missed wakeup) issues
		 * in the client.  We don't call writeContinue() until we have
		 * the upstream's "100 Continue".
		 */
		route.run(request, response);
	});
}

/*
 * Checks whether the stream associated with the given zone can accept Task
 * Control API requests right now.  If not, fail the given request.
 */
function maTaskApiValidate(request, response, next)
{
	var zone = request.maZone;

	if (zone.z_taskstream &&
	    zone.z_taskstream.s_state == maTaskStream.TASKSTREAM_S_RUNNING)
		return (true);

	next(new mod_restify.ConflictError('invalid zone state'));
	return (false);
}

/*
 * GET /my/jobs/task/task: fetch the task currently assigned to this zone.  By
 * default, this entry point always returns immediately, either with 200 plus
 * the task or 204 if no task is assigned right now.  The in-zone agent invokes
 * this with wait=true, which means to block until a task is available.
 */
function maTaskApiTask(request, response, next)
{
	var zone = request.maZone;
	var stream = zone.z_taskstream;

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
		 * knew about it.  In that case, we're introducing an additional
		 * delay here, which is unfortunate but shouldn't affect
		 * correctness (or latency of user jobs, unless all zones on
		 * this machine are saturated).
		 */
		request.log.warn('caught zone agent too early/too late');
		response.send(new mod_restify.ServiceUnavailableError());
		next();
		return;
	}

	if (stream !== undefined &&
	    stream.s_task !== undefined &&
	    stream.s_state == maTaskStream.TASKSTREAM_S_RUNNING) {
		maZoneHeartbeat(zone);

		if (maTaskStreamHasWork(stream) && !stream.s_pending) {
			response.send(maTaskStreamState(stream));
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
 * POST /my/jobs/task/commit: indicate that the given key has been successfully
 * processed.
 */
function maTaskApiCommit(request, response, next)
{
	var body = request.body || {};
	var zone = request.maZone;
	var stream = zone.z_taskstream;
	var group;
	var now, nkeys;

	if (!maTaskApiValidate(request, response, next))
		return;

	group = stream.s_group;

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
	stream.s_rqqueue.push(function (callback) {
		if (!maTaskApiValidate(request, response, callback))
			return;

		mod_assert.equal(stream.s_state,
		    maTaskStream.TASKSTREAM_S_RUNNING);

		var task = stream.s_task;

		if (!group.g_multikey || task.t_xinput.length > 0) {
			if (!body.hasOwnProperty('key')) {
				callback(new mod_restify.InvalidArgumentError(
				    '"key" required'));
				return;
			}

			var first;
			if (group.g_multikey)
				first = task.t_xinput[0]['key'];
			else
				first = task.t_record['value']['key'];

			if (body['key'] != first) {
				callback(new mod_restify.ConflictError('key "' +
				    body['key'] + '" is not the current key (' +
				    first + ')'));
				return;
			}
		}

		if (group.g_multikey && task.t_xinput.length > 0) {
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

			task.t_xinput = task.t_xinput.slice(nkeys);
			maTaskUpdate(task, stream.s_machine,
			    stream.s_noutput);
			callback();
			return;
		}

		task.t_stream = undefined;
		maTaskDone(task, stream.s_machine, stream.s_noutput, now);
		maTaskStreamAdvance(stream, callback);
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
 * POST /my/jobs/task/fail: indicate that the given task has failed
 */
function maTaskApiFail(request, response, next)
{
	var body = request.body || {};
	var zone = request.maZone;
	var stream = zone.z_taskstream;
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
	stream.s_rqqueue.push(function (callback) {
		if (!maTaskApiValidate(request, response, callback))
			return;

		mod_assert.equal(stream.s_state,
		    maTaskStream.TASKSTREAM_S_RUNNING);

		var task = stream.s_task;
		task.t_record['value']['machine'] = stream.s_machine;
		task.t_stream = undefined;
		maTaskError(task, now, error);
		maTaskStreamAdvance(stream, callback);
	}, function (sent) {
		if (!sent)
			response.send(204);
		next();
	});
}

/*
 * /:key: We proxy the entire Manta API here.  The only thing that we do
 * specially here is note objects that get created so we can "commit" them only
 * if the task successfully completes.
 */
function maTaskApiManta(request, response, next)
{
	var zone, key, group, stream, type;
	var iostream, proxyargs, reducer, continuefunc;

	if (!maTaskApiValidate(request, response, next))
		return;

	zone = request.maZone;
	stream = zone.z_taskstream;
	group = stream.s_group;
	key = mod_url.parse(request.url).pathname;
	mod_assert.equal(stream.s_state, maTaskStream.TASKSTREAM_S_RUNNING);

	maZoneHeartbeat(zone);

	type = request.headers['content-type'] || '';

	if (request.method == 'PUT' &&
	    type.indexOf('application/json') == -1 &&
	    type.indexOf('type=directory') == -1) {
		/*
		 * XXX consider what happens if we crash before saving this.
		 */
		reducer = request.headers['x-marlin-reducer'];
		iostream = request.headers['x-marlin-stream'] &&
		    request.headers['x-marlin-stream'] == 'stderr' ?
		    'stderr' : 'stdout';

		/*
		 * mcat uses the x-marlin-reference header to indicate that it's
		 * not actually creating a new file, but just marking it for
		 * output for this task.  In that case, we simply skip the proxy
		 * step.
		 */
		if (request.headers['x-marlin-reference']) {
			maTaskEmit(stream, stream.s_task, iostream, key,
			    reducer);
			response.send(204);
			next();
			return;
		}

		continuefunc = function () {
			maTaskEmit(stream, stream.s_task, iostream, key,
			    reducer);
		};
	}

	proxyargs = {
	    'request': request,
	    'response': response,
	    'continue': continuefunc,
	    'server': {
		'headers': {
		    'authorization': sprintf('Token %s', group.g_token)
		},
	        'host': maDnsCache.lookupv4(maMantaHost),
		'port': maMantaPort,
		'path': key
	    }
	};

	maCounters['mantarq_proxy_sent']++;

	mod_mautil.maHttpProxy(proxyargs, function () {
		maCounters['mantarq_proxy_return']++;
		next();
	});
}

function maTaskEmit(stream, task, iostream, key, reducer)
{
	if (stream.s_task != task) {
		stream.s_log.error('stream has moved on by the time %s key ' +
		    '"%s" was finished writing', stream, key);
		return;
	}

	if (iostream == 'stderr') {
		task.t_record['value']['stderr'] = key;
		return;
	}

	if (++stream.s_noutput <= 5) {
		task.t_record['value']['firstOutputs'].push({
		    'key': key,
		    'rIdx': reducer,
		    'timeCreated': mod_jsprim.iso8601(Date.now())
		});
		return;
	}

	var uuid = mod_uuid.v4();
	maMorayQueue.dirty('taskoutput', uuid, {
	    'jobId': stream.s_group.g_jobid,
	    'taskId': task.t_id,
	    'key': key,
	    'rIdx': reducer,
	    'timeCreated': mod_jsprim.iso8601(Date.now())
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
	return ([
	    'Global',
	    'Jobs',
	    'Requests',
	    'TaskGroups',
	    'TaskStreams',
	    'Zones'
	]);
}

function maKangListObjects(type)
{
	if (type == 'Global')
		return ([ 0 ]);

	if (type == 'Jobs')
		return (Object.keys(maJob));

	if (type == 'Requests')
		return (Object.keys(maRequests));

	if (type == 'TaskGroups')
		return (Object.keys(maTaskGroups));

	if (type == 'TaskStreams') {
		var rv = [];
		mod_jsprim.forEachKey(maTaskGroups, function (_, group) {
			mod_jsprim.forEachKey(group.g_streams,
			    function (__, stream) {
				rv.push(group.g_groupid + '|' + stream.s_id);
			    });
		});
		return (rv);
	}

	return (Object.keys(maZones));
}

function maKangGetObject(type, id)
{
	if (type == 'Global') {
		return ({
		    'tickStart': maTickStart,
		    'tickDone': maTickDone,
		    'nGroupsQueued': maTaskGroupsQueued.length,
		    'nZonesReady': maZonesReady.length,
		    'nReduceTasks': Object.keys(maTasksReduce).length,
		    'nTasks': maNTasks,
		    'nZones': maNZones,
		    'nZonesReserved': maNZonesReserved
		});
	}

	if (type == 'Jobs')
		return (maJobs[id].kangState());

	if (type == 'Requests') {
		var request = maRequests[id];

		return ({
		    'id': request.id,
		    'method': request.method,
		    'url': request.url,
		    'time': request.time
		});
	}

	if (type == 'TaskGroups')
		return (maTaskGroups[id].kangState());

	if (type == 'TaskStreams') {
		var parts = id.split('|', 2);
		return (maTaskGroups[parts[0]].g_streams[parts[1]].kangState());
	}

	return (maZones[id].httpState());
}

function maKangStats()
{
	return (maCounters);
}

main();
