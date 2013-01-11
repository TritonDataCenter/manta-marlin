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
 * This agent is tightly-coupled with the lackey that runs in each compute zone
 * and manages tasks execution in that zone.
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
var mod_child = require('child_process');
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

var maConfSchema = require('./schema');


/*
 * Global agent state
 */
var maAgent;				/* singleton agent instance */


/*
 * Static configuration
 */
var maCheckNTasks 		= true;	/* periodically verify task count */
var maZoneAutoReset 		= true;	/* reset zones upon group completion */
var maZoneSaveLogs 		= true;	/* save lackey logs before reset */

var maServerName		= 'MarlinAgent';
var maZeroByteFilename 		= '/var/run/marlin/.zero';
var maZoneLogRoot 		= '/var/smartdc/marlin/log/zones';

var maZoneLivenessInterval	= 15000;	/* ms before zone timeout */
var maMorayMaxRecords 		= 1000;		/* max results per request */
var maRingbufferSize 		= 1000;		/* ringbuffer records */
var maRequestTimeout 		= 300000;	/* local long poll timeout */

var maMorayOptions = {
    'limit': maMorayMaxRecords,
    'noCache': true,
    'sort': {
	'attribute': '_txn_snap',
	'order': 'ASC'
    }
};

var maLogStreams = [ {
    'stream': process.stdout,
    'level': process.env['LOG_LEVEL'] || 'info'
} ];


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

	if (!process.env['NO_ABORT_ON_CRASH']) {
		mod_panic.enablePanicOnCrash({
		    'skipDump': true,
		    'abortOnPanic': true
		});
	}

	mod_agent_zone.maZoneApiCallback(maTaskApiSetup);
	maAgent = new mAgent(process.argv[parser.optind()]);
	maAgent.init();
}

function usage(errmsg)
{
	if (errmsg)
		console.error(errmsg);

	console.error('usage: node agent.js [-o logfile] conffile');
	process.exit(2);
}

function mAgent(filename)
{
	var agent = this;
	var url;

	/*
	 * Configuration
	 */
	this.ma_conf = mod_mautil.readConf(this.ma_log, maConfSchema, filename);

	this.ma_server_uuid = this.ma_conf['instanceUuid'];

	url = mod_url.parse(this.ma_conf['manta']['url']);
	this.ma_manta_host = url['hostname'];
	this.ma_manta_port = url['port'] ? parseInt(url['port'], 10) : 80;

	url = mod_url.parse(this.ma_conf['moray']['url']);
	this.ma_moray_host = url['hostname'];
	this.ma_moray_port = parseInt(url['port'], 10);

	this.ma_bucket_names = {};
	mod_jsprim.forEachKey(this.ma_conf['buckets'], function (n, b) {
		agent.ma_bucket_names[b] = n;
	});

	mod_http.globalAgent.maxSockets =
	    this.ma_conf['tunables']['httpMaxSockets'] || 512;

	/*
	 * Helper objects
	 */
	this.ma_log = new mod_bunyan({
	    'name': maServerName,
	    'streams': maLogStreams
	});

	this.ma_log.info('configuration', this.ma_conf);

	/*
	 * Availability of Manta services relies on locating them via DNS as
	 * they're used.  Since this agent runs in the global zone of SDC
	 * compute nodes where DNS is not available, we do our own DNS lookups.
	 */
	this.ma_dns_waiters = [];	/* queue of streams waiting on DNS */
	this.ma_dns_cache = new mod_adnscache.AsyncDnsCache({
	    'log': this.ma_log.child({ 'component': 'DnsCache' }),
	    'nameServers': this.ma_conf['dns']['nameservers'].slice(0),
	    'triggerInterval': this.ma_conf['dns']['triggerInterval'],
	    'graceInterval': this.ma_conf['dns']['graceInterval'],
	    'onResolve': this.onDnsResolve.bind(this)
	});
	this.ma_dns_cache.add(this.ma_manta_host);
	this.ma_dns_cache.add(this.ma_moray_host);

	this.ma_task_poll = new mod_mamoray.PollState(
	    this.ma_conf['tunables']['timePoll'], maRingbufferSize);
	this.ma_task_throttle = new mod_mautil.Throttler(
	    this.ma_conf['tunables']['timePoll']);
	this.ma_job_throttle = new mod_mautil.Throttler(
	    this.ma_conf['tunables']['timeJobSave']);

	this.ma_moray_queue = new mod_mamoray.MorayWriteQueue({
	    'log': this.ma_log.child({ 'component': 'MorayQueue' }),
	    'client': function () { return (agent.ma_moray); },
	    'buckets': this.ma_conf['buckets'],
	    'maxpending': this.ma_conf['tunables']['maxPendingPuts']
	});

	this.ma_server = mod_restify.createServer({
	    'name': maServerName,
	    'log': this.ma_log.child({ 'component': 'HttpServer' })
	});

	this.ma_moray = undefined;	/* moray client */

	/*
	 * Dynamic state
	 */
	this.ma_ntasks = 0;		/* total nr of tasks */
	this.ma_nzones = 0;		/* total nr of zones */
	this.ma_nzones_reserved = 0;	/* total nr of reserved zones */
	this.ma_tick_start = undefined;	/* time the last tick started */
	this.ma_tick_done = undefined;	/* time the last tick ended */
	this.ma_moray_pending = false;	/* currently connecting */
	this.ma_counters = {
	    'streams_dispatched': 0,	/* streams dispatched to a zone */
	    'streams_done': 0,		/* streams completed */
	    'tasks_failed': 0,		/* individual tasks that failed */
	    'tasks_committed': 0,	/* individual tasks committed */
	    'zones_added': 0,		/* zones added to Marlin */
	    'zones_readied': 0,		/* zones transitioned to "ready" */
	    'zones_disabled': 0,	/* zones permanently disabled */
	    'mantarq_proxy_sent': 0,	/* requests forwarded to Manta */
	    'mantarq_proxy_return': 0	/* responses received from Manta */
	};

	this.ma_requests = {};		/* pending HTTP requests, by req_id */
	this.ma_init_barrier = mod_vasync.barrier();

	/*
	 * Tasks represent units of work.  Completed tasks are put on deathrow
	 * -- we need to keep them in-memory to deal with incoming writes, but
	 * we don't consider them like other tasks.  We also keep a separate
	 * index of active reduce tasks so we can iterate only those in order to
	 * fetch input records.
	 */
	this.ma_tasks = {};		/* known tasks, by taskid */
	this.ma_tasks_deathrow = {};	/* tasks awaiting final save */
	this.ma_tasks_reduce = {};	/* reduce tasks, by taskid */

	/*
	 * We must also keep track of active jobs, since that's where the actual
	 * description of work lives.
	 */
	this.ma_jobs = {};		/* known jobs, by jobid */

	/*
	 * As described above, tasks are collected into groups based on jobid
	 * and phase number so that we can run multiple tasks in the same group
	 * in a zone without resetting it in between.
	 */
	this.ma_taskgroups = {};	/* task groups, by group id */
	this.ma_taskgroups_queued = [];	/* ready-to-run task groups */

	/*
	 * We execute tasks in compute zones.
	 */
	this.ma_zones = {};		/* all zones, by zonename */
	this.ma_zones_ready = [];	/* ready-to-use zones */
}

mAgent.prototype.init = function ()
{
	var agent = this;
	var tick = function () { agent.tick(); };
	var interval = this.ma_conf['tunables']['timeTick'];

	this.ma_init_barrier.on('drain', function () {
		agent.ma_log.info('agent started');
		setInterval(tick, interval);
	});

	this.ma_init_barrier.start('sync');

	this.initDirs();
	this.initHttp();
	this.initZones();

	this.ma_init_barrier.done('sync');
};

/*
 * Create directories used to store log files.
 */
mAgent.prototype.initDirs = function ()
{
	this.ma_log.info('creating "%s"', maZoneLogRoot);
	mod_mkdirp.sync(maZoneLogRoot);

	this.ma_log.info('creating zero-byte "%s"', maZeroByteFilename);
	mod_mkdirp.sync(mod_path.dirname(maZeroByteFilename));
	mod_fs.writeFileSync(maZeroByteFilename, '');
};

/*
 * Setup and start the local HTTP server used for kang and control.
 */
mAgent.prototype.initHttp = function ()
{
	var agent = this;
	var server = this.ma_server;

	server.use(function (request, response, next) {
		agent.ma_requests[request.id] = request;
		next();
	});

	server.use(mod_restify.acceptParser(server.acceptable));
	server.use(mod_restify.queryParser());
	server.use(mod_restify.bodyParser({ 'mapParams': false }));

	server.on('uncaughtException', mod_mautil.maRestifyPanic);

	server.on('after', mod_restify.auditLogger({
	    'body': true,
	    'log': this.ma_log.child({ 'component': 'AuditLog' })
	}));

	server.on('after', function (request, response) {
		delete (agent.ma_requests[request.id]);
	});

	server.on('error', function (err) {
		agent.ma_log.fatal(err, 'failed to start server');
		process.exit(1);
	});

	server.post('/zones', maHttpZonesAdd);
	server.get('/zones', maHttpZonesList);
	server.post('/zones/:zonename/disable', maHttpZoneDisable);
	server.del('/zones/:zonename', maHttpZoneDelete);

	server.get('/kang/.*', mod_kang.knRestifyHandler({
	    'uri_base': '/kang',
	    'service_name': 'marlin',
	    'component': 'agent',
	    'ident': this.ma_server_uuid,
	    'version': '0',
	    'list_types': maKangListTypes,
	    'list_objects': maKangListObjects,
	    'get': maKangGetObject,
	    'stats': maKangStats
	}));

	var port = this.ma_conf['port'];

	this.ma_init_barrier.start('listen');

	server.listen(port, function () {
		agent.ma_log.info('server listening on port %d', port);
		agent.ma_init_barrier.done('listen');
	});
};

/*
 * Find the local zones configured for Marlin use and initialize them.
 */
mAgent.prototype.initZones = function ()
{
	var agent = this;
	var cmd = 'vmadm lookup tags.manta_role=marlincompute';

	this.ma_log.info('listing zones with "%s"', cmd);
	this.ma_init_barrier.start('zones');

	mod_child.exec(cmd, function (err, stdout, stderr) {
		if (err) {
			agent.ma_log.fatal(err,
			    'failed to read list of zones: %s', stderr);
			throw (err);
		}

		agent.ma_init_barrier.done('zones');

		var lines = stdout.toString('utf-8').split('\n');

		lines.forEach(function (line) {
			line = line.trim();

			if (line.length === 0 || line[0] == '#')
				return;

			agent.zoneAdd(line);
		});
	});
};

mAgent.prototype.onDnsResolve = function ()
{
	if (this.ma_dns_waiters.length === 0)
		return;

	this.ma_log.info('unblocking %d streams waiting on DNS',
	    this.ma_dns_waiters.length);

	var waiters = this.ma_dns_waiters;
	this.ma_dns_waiters = [];
	waiters.forEach(function (callback) { callback(); });
};

/*
 * Invoked once per timeTick milliseconds to potentially kick off a poll for
 * more records from Moray and a save for our existing records.  Each of these
 * actions is throttled so that it won't happen either while another one is
 * ongoing or if it's been too recent since the last one.
 */
mAgent.prototype.tick = function ()
{
	this.ma_tick_start = new Date();

	this.ma_dns_cache.update();

	if (!this.ma_moray) {
		this.ma_log.warn('tick skipped (no moray connection)');
		this.morayConnect();
		this.ma_tick_done = new Date();
		return;
	}

	if (!this.ma_job_throttle.tooRecent()) {
		this.ma_job_throttle.start();
		this.ma_job_throttle.done();
		this.jobsHeartbeat(this.ma_tick_start);
	}

	this.poll(this.ma_tick_start.getTime());
	this.zonesCheckLiveness(this.ma_moray);
	this.ma_moray_queue.flush();

	if (maCheckNTasks)
		this.checkTaskCount();

	this.deathrowReap();

	this.ma_tick_done = new Date();
};

/*
 * Ensure that we have a valid Moray client.  If there's one ready or being
 * initialized, we do nothing here.  Otherwise, we create a new one and start
 * initializing it.
 */
mAgent.prototype.morayConnect = function ()
{
	var agent = this;
	var client, ip;

	if (this.ma_moray !== undefined || this.ma_moray_pending)
		return;

	ip = this.ma_dns_cache.lookupv4(this.ma_moray_host);
	if (!ip) {
		this.ma_log.warn('no IP available for "%s"',
		    this.ma_moray_host);
		return;
	}

	this.ma_moray_pending = true;

	client = mod_moray.createClient({
	    'host': ip,
	    'port': this.ma_moray_port,
	    'log': this.ma_log.child({ 'component': 'MorayClient' }),
	    'reconnect': true,
	    'retry': this.ma_conf['moray']['reconnect']
	});

	client.on('error', function (err) {
		agent.ma_moray_pending = false;
		agent.ma_log.error(err, 'moray client error');
	});

	client.on('close', function () {
		agent.ma_log.error('moray client closed');
	});

	client.on('connect', function () {
		agent.ma_log.info('moray client connected');
		mod_assert.ok(agent.ma_moray === undefined ||
		    agent.ma_moray == client);
		agent.ma_moray = client;
		agent.ma_moray_pending = false;
	});
};

/*
 * Fetch updates to jobs, tasks, and taskinputs.  This is how we discover all
 * new work.
 */
mAgent.prototype.poll = function (now)
{
	var agent = this;
	var req;

	/*
	 * Fetch updates to job records.
	 */
	mod_jsprim.forEachKey(agent.ma_jobs, function (jobid, job) {
		if (job.j_cancelled)
			return;

		var filter = sprintf('(&(jobId=%s)%s)', jobid,
		    job.j_poll.filter());

		req = mod_mamoray.poll({
		    'client': agent.ma_moray,
		    'options': maMorayOptions,
		    'now': now,
		    'log': agent.ma_log,
		    'throttle': job.j_update_throttle,
		    'bucket': agent.ma_conf['buckets']['job'],
		    'filter': filter,
		    'onrecord': function (record) {
			    if (job.j_poll.record(record))
				    agent.onRecord(record);
		    }
		});

		job.j_poll.request(filter, req);
	});

	/*
	 * Fetch new and updated task records.
	 */
	var incremental_filter = this.ma_task_poll.filter();
	var cancelled_filter = incremental_filter.length > 0 ? '' :
	    '(!(state=cancelled))';
	var task_filter = sprintf(
	    '(&(server=%s)(!(state=done))(!(state=aborted))%s%s)',
	    agent.ma_server_uuid, incremental_filter, cancelled_filter);

	req = mod_mamoray.poll({
	    'client': agent.ma_moray,
	    'options': maMorayOptions,
	    'now': now,
	    'log': agent.ma_log,
	    'throttle': agent.ma_task_throttle,
	    'bucket': agent.ma_conf['buckets']['task'],
	    'filter': task_filter,
	    'onrecord': function (record) {
		    if (agent.ma_task_poll.record(record))
			    agent.onRecord(record);
	    }
	});

	agent.ma_task_poll.request(task_filter, req);

	/*
	 * Fetch new taskinput records for pending reduce tasks.
	 */
	mod_jsprim.forEachKey(agent.ma_tasks_reduce, function (taskid) {
		var task = agent.ma_tasks[taskid];

		if (agent.taskReadAllRecords(task)) {
			agent.ma_log.info('removing "%s" from reduce tasks',
			    taskid);
			delete (agent.ma_tasks_reduce[taskid]);
			return;
		}

		var filter = sprintf('(&(taskId=%s)%s)', task.t_id,
		    task.t_poll.filter());

		mod_mamoray.poll({
		    'client': agent.ma_moray,
		    'options': maMorayOptions,
		    'now': now,
		    'log': agent.ma_log,
		    'throttle': task.t_poll.p_throttle,
		    'bucket': agent.ma_conf['buckets']['taskinput'],
		    'filter': filter,
		    'onrecord': function (record) {
			if (task.t_poll.record(record))
			        agent.onRecord(record);
		    },
		    'ondone': function (_, start_time, count) {
			/* Wake up waiters blocked on this query. */
			if (task.t_stream !== undefined &&
			    agent.taskStreamHasWork(task.t_stream))
				maZoneWakeup(agent.ma_zones[
				    task.t_stream.s_machine]);
		    }
		});
	});
};

/*
 * Invoked during polling when new or updated records are discovered.
 */
mAgent.prototype.onRecord = function (record)
{
	var name, error, jobid, taskid;

	name = this.ma_bucket_names[record['bucket']];
	mod_assert.ok(mod_schema.sBktJsonSchemas.hasOwnProperty(name),
	    'poller returned record for an unknown bucket ' + record['bucket']);

	error = mod_jsprim.validateJsonObject(
	    mod_schema.sBktJsonSchemas[name], record['value']);
	if (error) {
		this.ma_log.warn(error, 'ignoring record (invalid)', record);
		return;
	}

	jobid = record['value']['jobId'];
	taskid = record['value']['taskId'];
	this.ma_log.debug('record: "%s" "%s" etag %s', name, record['key'],
	    record['_etag']);

	if (name == 'job') {
		if (!this.ma_jobs.hasOwnProperty(jobid)) {
			this.ma_log.warn('ignoring record (unknown job)',
			    record);
			return;
		}

		this.jobUpdated(this.ma_jobs[jobid], record);
		return;
	}

	if (name == 'taskinput') {
		if (!this.ma_tasks.hasOwnProperty(taskid)) {
			this.ma_log.warn('ignoring record (unknown task)',
			    record);
			return;
		}

		this.taskAppendKey(this.ma_tasks[taskid], record['value']);
		return;
	}

	mod_assert.equal(name, 'task',
	    'poller returned a moray object we didn\'t ask for: ' + name);

	if (this.ma_tasks_deathrow.hasOwnProperty(taskid)) {
		if (this.ma_moray_queue.pending('task', taskid)) {
			var task = this.ma_tasks_deathrow[taskid];

			if (task.t_record['_etag'] == record['_etag'])
				return;

			task.t_record['value']['timeCommitted'] =
			    record['value']['timeCommitted'];
			task.t_record['_etag'] = record['_etag'];
			this.ma_moray_queue.dirty('task', task.t_id,
			    task.t_record['value'],
			    { 'etag': task.t_record['_etag'] });
		}

		return;
	}

	if (this.ma_tasks.hasOwnProperty(taskid)) {
		this.taskUpdated(this.ma_tasks[taskid], record);
		return;
	}

	if (record['value']['state'] != 'cancelled' &&
	    (!this.ma_jobs.hasOwnProperty(record['value']['jobId']) ||
	    !this.ma_jobs[record['value']['jobId']].j_cancelled))
		this.taskCreate(record);
};

/*
 * Go through all jobs and trigger a task save for any job which hasn't had any
 * tasks updated in a while.  This tells the worker that we're still alive.
 */
mAgent.prototype.jobsHeartbeat = function (now)
{
	var agent = this;
	var log = this.ma_log;

	/*
	 * We keep track of the last time a task was dirtied for this job,
	 * including which task it was.  If we find a job whose last dirty time
	 * was too long ago, if the write is not still pending, we dirty another
	 * task.
	 */
	mod_jsprim.forEachKey(this.ma_jobs, function (jobid, job) {
		if (job.j_dirtied &&
		    now - job.j_dirtied < job.j_save_interval) {
			log.debug('job "%s": skipping save (too recent)',
			    job.j_id);
			return;
		}

		if (job.j_saving &&
		    agent.ma_moray_queue.pending('task', job.j_saving)) {
			log.debug('job "%s": skipping save (already pending)',
			    job.j_id, job.j_saving);
			return;
		}

		var task = agent.jobPickTask(job);

		if (task) {
			log.info('job "%s": saving via task',
			    job.j_id, task.t_id);
			agent.taskDirty(task);
		}
	});
};

/*
 * Returns any task from this job, if the job has any tasks.  Recall that our
 * mechanism of heartbeating to the worker is to update any task associated with
 * the job, which is why it doesn't matter which task it is.
 */
mAgent.prototype.jobPickTask = function (job)
{
	var group, stream;
	var groupid, streamid;

	for (groupid in job.j_groups) {
		group = this.ma_taskgroups[groupid];

		for (streamid in group.g_streams) {
			stream = group.g_streams[streamid];
			if (stream.s_task)
				return (stream.s_task);
		}

		mod_assert.ok(group.g_tasks.length > 0);
		return (group.g_tasks[0]);
	}
};

/*
 * Take a pass over all zones to see whether it's been too long since we've
 * heard from their lackey, in which case we time out the running task and
 * disable the zone.
 */
mAgent.prototype.zonesCheckLiveness = function ()
{
	var agent = this;
	var now = Date.now();

	mod_jsprim.forEachKey(this.ma_zones, function (zonename, zone) {
		var stream = zone.z_taskstream;

		if (!stream ||
		    stream.s_state != maTaskStream.TASKSTREAM_S_RUNNING)
			return;

		if (now - zone.z_last_contact <= maZoneLivenessInterval)
			return;

		/*
		 * A lackey timeout is a pretty serious failure.  We disable the
		 * zone.  Eventually we'll need alarms on this to make it clear
		 * that operator intervention is required to diagnose and clear
		 * the failure.  We could potentially even skip this by
		 * collecting core files dumped by the zone, logs from the zone,
		 * a ptree of the zone, and a core of any running lackey
		 * process, and then resetting the zone.
		 */
		zone.z_log.error('lackey has timed out');
		zone.z_failed = Date.now();

		agent.taskStreamAbort(stream, {
		    'code': 'EJ_INTERNAL',
		    'message': 'internal error',
		    'messageInternal': 'lackey timed out'
		});
	});
};

/*
 * In debug mode, we periodically check that the total number of tasks in the
 * system matches what we expect.
 */
mAgent.prototype.checkTaskCount = function ()
{
	var agent = this;
	var log = this.ma_log;

	var found = {};
	var maybe = {};
	var taskcount = 0;

	mod_jsprim.forEachKey(this.ma_tasks, function () { taskcount++; });

	if (taskcount != this.ma_ntasks) {
		log.fatal('checkTaskCount failed: ma_ntasks = %d, but ' +
		    'found %d tasks in ma_tasks', this.ma_ntasks, taskcount);
		process.abort();
	}

	mod_jsprim.forEachKey(this.ma_taskgroups, function (_, group) {
		group.g_tasks.forEach(function (task, i) {
			if (!agent.ma_tasks.hasOwnProperty(task.t_id)) {
				log.fatal('checkTaskCount failed: ' +
				    'found task "%s" on group "%s" idx %d ' +
				    'not in ma_tasks', task.t_id,
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
			    agent.ma_jobs[group.g_jobid].j_cancelled !==
			    undefined) {
				maybe[task.t_id] = true;
				return;
			}

			if (!agent.ma_tasks.hasOwnProperty(task.t_id)) {
				log.fatal('checkTaskCount ' +
				    'failed: found task "%s" on ' +
				    'stream "%s" not in ma_tasks',
				    task.t_id, stream.s_id);
				process.abort();
			}

			found[stream.s_task.t_id] = true;
		});
	});

	mod_jsprim.forEachKey(agent.ma_tasks, function (taskid) {
		if (!found.hasOwnProperty(taskid) &&
		    !maybe.hasOwnProperty(taskid)) {
			log.fatal('checkTaskCount failed: found task ' +
			    '"%s" on ma_tasks, but not in groups', taskid);
			process.abort();
		}

		delete (found[taskid]);
		delete (maybe[taskid]);
	});

	mod_jsprim.forEachKey(found, function (taskid) {
		log.fatal('checkTaskCount failed: found task "%s" ' +
		    'in groups, but not on ma_tasks', taskid);
		process.abort();
	});
};

mAgent.prototype.deathrowReap = function ()
{
	var agent = this;

	mod_jsprim.forEachKey(this.ma_tasks_deathrow, function (taskid, _) {
		if (!agent.ma_moray_queue.pending('task', taskid))
			delete (agent.ma_tasks_deathrow[taskid]);
	});
};


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
	    'groups': Object.keys(this.j_groups),
	    'cancelled': this.j_cancelled,
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
function maTask(record, agent)
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
	this.t_poll = new mod_mamoray.PollState(
	    agent.ma_conf['tunables']['timePoll'], maRingbufferSize);
}

/*
 * To preserve isolation, tasks are executed in dedicated compute zones that are
 * set back to a clean state in between tenants.  A set of tasks that may be
 * run in the same zone without a reset are called a "task group".  See the
 * big comment at the top of this file.
 */
function maTaskGroup(groupid, job, pi, log)
{
	/* Immutable job and task group state */
	this.g_groupid = groupid;	/* group identifier */
	this.g_jobid = job.j_id;	/* job identifier */
	this.g_phasei = pi;		/* phase number */
	this.g_log = log;

	/* filled in asynchronously when we retrieve the job record */
	this.g_token = undefined;	/* auth token */
	this.g_phase = undefined;	/* phase specification */
	this.g_map_keys = undefined;	/* whether to hyprlofs map keys */
	this.g_multikey = undefined;	/* whether this is a reduce phase */
	this.g_login = undefined;	/* user's login name */

	/* dynamic state */
	this.g_state = maTaskGroup.TASKGROUP_S_INIT;
	this.g_nrunning = 0;		/* number of tasks running */
	this.g_nstreams = 0;		/* number of streams running */
	this.g_streams = {};		/* set of concurrent streams */
	this.g_tasks = [];		/* queue of tasks to be run */
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

maTaskGroup.prototype.kangState = function (agent)
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
	    'share': agent.schedGroupShare(this)
	});
};

function maTaskStream(stream_id, group, machine)
{
	/* immutable state */
	this.s_id = stream_id;		/* stream identifier */
	this.s_group = group;		/* maTaskGroup object */
	this.s_machine = machine;	/* assigned zonename */

	/* helper objects */
	this.s_log = group.g_log.child({ 'component': 'Stream-' + stream_id });

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
	    'pendingUpdate': this.s_pending,
	    'taskStart': this.s_start,
	    'taskId': this.s_task ? this.s_task.t_id : undefined,
	    'taskNOutput': this.s_noutput
	});
};

/*
 * Given a stream, return an object representing the current state.  This will
 * be passed to the lackey and must contain all information necessary to execute
 * the current task and generate output.  For details on how the lackey uses
 * this information, see the comment in lackey.js.
 */
maTaskStream.prototype.streamState = function (agent)
{
	var group, task, rv, outbase, base;

	group = this.s_group;
	mod_assert.ok(group.g_phase !== undefined);

	task = this.s_task;
	mod_assert.ok(task !== undefined);

	rv = {};

	/*
	 * Start with the basics:
	 *
	 *    taskId			Task identifier
	 *
	 *    taskPhase			Full phase description, taken straight
	 *    				from the job record.  This tells the
	 *    				lackey exactly what to execute.
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
	 *    			each batch, the lackey will make additional
	 *    			requests to process the next set of keys.
	 *
	 *    taskInputDone	If true, then there will be no input keys after
	 *    			the ones specified by taskInputKeys.  If false,
	 *    			there may or may not be more keys, and the
	 *    			lackey will have to check again later to see if
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
		rv['taskInputDone'] = agent.taskReadAllRecords(task);
		rv['taskInputRemote'] = '/tmp/.marlin.sock';
	} else {
		rv['taskInputKeys'] = [ task.t_record['value']['key'] ];
		rv['taskInputDone'] = true;

		if (group.g_map_keys)
			rv['taskInputFile'] = mod_path.join(
			    agent.ma_zones[this.s_machine].z_manta_root,
			    rv['taskInputKeys'][0]);
	}

	return (rv);
};

/*
 * Create a new job record for the given jobid.  At this point, we don't know
 * anything about it except for its id, but we'll fetch the details when we next
 * poll.
 */
mAgent.prototype.jobCreate = function (jobid)
{
	this.ma_jobs[jobid] = new maJob(jobid,
	    this.ma_conf['tunables']['timeJobSave'],
	    this.ma_conf['tunables']['timePoll']);
};

/*
 * Update an existing job object based on a potentially updated record.
 */
mAgent.prototype.jobUpdated = function (job, record)
{
	var agent = this;
	var first = job.j_record === undefined;

	if (job.j_record && job.j_record['_etag'] == record['_etag'])
		return;

	job.j_record = record;

	if (record['value']['timeCancelled'] && !job.j_cancelled) {
		this.ma_log.info('job "%s": cancelled', job.j_id);
		job.j_cancelled = record['value']['timeCancelled'];

		mod_jsprim.forEachKey(job.j_groups, function (groupid) {
			agent.taskGroupCancel(agent.ma_taskgroups[groupid]);
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
		var group = agent.ma_taskgroups[groupid];
		agent.taskGroupSetJob(group, job);
	});
};

/*
 * Create a new task object for the given record.
 */
mAgent.prototype.taskCreate = function (record)
{
	var value, taskid, jobid, pi, groupid;
	var job, task, group;

	value = record['value'];
	taskid = value['taskId'];
	jobid = value['jobId'];
	pi = value['phaseNum'];
	groupid = jobid + '/' + pi;

	task = new maTask(record, this);
	this.ma_ntasks++;
	this.ma_tasks[taskid] = task;
	if (!record['value']['key'])
		this.ma_tasks_reduce[taskid] = true;

	if (this.ma_taskgroups.hasOwnProperty(groupid)) {
		this.taskGroupAppendTask(this.ma_taskgroups[groupid], task);
		return;
	}

	if (!this.ma_jobs.hasOwnProperty(jobid))
		this.jobCreate(jobid);

	job = this.ma_jobs[jobid];
	job.j_groups[groupid] = true;

	group = new maTaskGroup(groupid, job, pi, this.ma_log.child(
	    { 'component': 'Group-' + groupid }));
	this.ma_taskgroups[groupid] = group;
	group.g_log.debug('created group for job "%s" phase %d ' +
	    '(from task "%s")', jobid, pi, task.t_id);
	this.taskGroupAppendTask(group, task);

	if (job.j_record)
		this.taskGroupSetJob(group, job);
};

/*
 * Update an existing task object based on a potentially updated record.
 */
mAgent.prototype.taskUpdated = function (task, record)
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
	needdirty = this.ma_moray_queue.pending('task', task.t_id);

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
			this.taskStreamAbort(task.t_stream, {
			    'code': 'EJ_INTERNAL',
			    'message': 'internal abort',
			    'messageInternal': 'task cancelled externally'
			}, true);

			task.t_stream = undefined;
			this.taskRemove(task);
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

			this.taskRemove(task);

			if (group.g_nstreams === 0 &&
			    group.g_tasks.length === 0) {
				if (group.g_state ==
				    maTaskGroup.TASKGROUP_S_QUEUED)
					this.taskGroupCancel(group);
				else
					this.taskGroupRemove(group);
			}
		}
	}

	if (record['value']['timeInputDone'] && !task.t_cancelled &&
	    task.t_ninputs === undefined) {
		this.ma_log.info('task "%s": input done', task.t_id);

		task.t_ninputs = record['value']['nInputs'];

		/*
		 * Wake up waiters for zones waiting for task input to be
		 * complete, if we've also read all of the corresponding
		 * taskinput records.
		 */
		if (task.t_stream !== undefined &&
		    task.t_xinput.length === 0 && this.taskReadAllRecords(task))
			maZoneWakeup(this.ma_zones[task.t_stream.s_machine]);
	}

	if (needdirty)
		this.taskDirty(task);
};

/*
 * Returns true if all of the given task's input records have been read.
 */
mAgent.prototype.taskReadAllRecords = function (task)
{
	if (task.t_ninputs === undefined)
		return (false);

	if (task.t_ninputs < task.t_nread)
		this.ma_log.warn('task "%s": ninputs (%d) < nread (%d)',
		    task.t_id, task.t_ninputs, task.t_nread, task);

	return (task.t_ninputs <= task.t_nread);
};

/*
 * Returns true if there is work available for the lackey on this stream.  This
 * is true if the stream has any map task or if the stream has a reduce task
 * with at least one input object waiting or has finished reading all input
 * records.
 */
mAgent.prototype.taskStreamHasWork = function (stream)
{
	mod_assert.ok(stream.s_group);

	if (stream.s_task === undefined)
		return (false);

	var group = stream.s_group;
	var task = stream.s_task;

	if (!group.g_multikey)
		return (true);

	return (task.t_xinput.length > 0 || this.taskReadAllRecords(task));
};

/*
 * Remove a task from global state.  It's assumed that it's not referenced by
 * any other structures by this point.
 */
mAgent.prototype.taskRemove = function (task)
{
	this.ma_log.debug('task "%s": removing', task.t_id);
	mod_assert.ok(task.t_stream === undefined);

	delete (this.ma_tasks[task.t_id]);
	delete (this.ma_tasks_reduce[task.t_id]);
	this.ma_ntasks--;
};

/*
 * Append a new input key to the given reduce task.
 */
mAgent.prototype.taskAppendKey = function (task, keyinfo)
{
	this.ma_log.debug('task "%s": appending key', task.t_id, keyinfo);
	task.t_xinput.push(keyinfo);
	task.t_nread++;

	var group = task.t_group;

	if (group.g_phase && group.g_state == maTaskGroup.TASKGROUP_S_INIT) {
		group.g_state = maTaskGroup.TASKGROUP_S_QUEUED;
		this.ma_taskgroups_queued.push(group);
		this.schedIdle();
		return;
	}

	if (task.t_stream !== undefined && task.t_xinput.length == 1)
		maZoneWakeup(this.ma_zones[task.t_stream.s_machine]);
};

/*
 * Schedule an update for the given task's record in Moray.
 */
mAgent.prototype.taskDirty = function (task)
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
		if (this.ma_tasks.hasOwnProperty(task.t_id)) {
			this.taskRemove(task);
			this.ma_tasks_deathrow[task.t_id] = task;
		} else {
			this.ma_log.warn('dirtied task "%s" after removing it',
			    task.t_id, task.t_record, new Error('stack').stack);
		}
	}

	this.ma_moray_queue.dirty('task', task.t_id, task.t_record['value'],
	    { 'etag': task.t_record['_etag'] });

	/*
	 * Keep track of the last time each job is dirtied.  See jobsHeartbeat.
	 */
	var job = this.ma_jobs[task.t_record['value']['jobId']];
	if (!job)
		return;

	job.j_dirtied = Date.now();
	job.j_saving = task.t_id;
};

/*
 * Mark a task as failed with the given error.  The error object should contain
 * "code" and "message" fields, as well as "messageInternal" if the "code" is
 * EJ_INTERNAL.
 */
mAgent.prototype.taskError = function (task, nout, when, error)
{
	mod_assert.ok(task.t_record['value']['result'] === undefined);
	task.t_group.g_nrunning--;
	this.ma_counters['tasks_failed']++;

	task.t_record['value']['nOutputs'] = nout;
	task.t_record['value']['state'] = 'aborted';
	task.t_record['value']['result'] = 'fail';
	task.t_record['value']['timeDone'] = mod_jsprim.iso8601(when);
	task.t_record['value']['error'] = error;

	this.taskDirty(task);
};

/*
 * Mark a task as successfully completed with the given number of output keys.
 */
mAgent.prototype.taskDone = function (task, machine, nout, when)
{
	mod_assert.ok(task.t_record['value']['result'] === undefined);
	task.t_group.g_nrunning--;
	this.ma_counters['tasks_committed']++;

	task.t_record['value']['nOutputs'] = nout;
	task.t_record['value']['state'] = 'done';
	task.t_record['value']['result'] = 'ok';
	task.t_record['value']['timeDone'] = mod_jsprim.iso8601(when);
	task.t_record['value']['machine'] = machine;

	this.taskDirty(task);
};

/*
 * Mark the given task as running in machine "machine" and having emitted "nout"
 * inputs so far.
 */
mAgent.prototype.taskUpdate = function (task, machine, nout)
{
	mod_assert.ok(task.t_record['value']['result'] === undefined);

	if (task.t_record['value']['state'] == 'dispatched')
		task.t_record['value']['state'] = 'running';

	task.t_record['value']['nOutputs'] = nout;
	task.t_record['value']['machine'] = machine;

	this.taskDirty(task);
};

mAgent.prototype.taskStreamAbort = function (stream, error, skipTaskUpdate)
{
	var agent = this;

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
			agent.taskGroupError(stream.s_group, error);
		} else {
			var task = stream.s_task;

			if (!skipTaskUpdate &&
			    task.t_record['value']['result'] === undefined) {
				task.t_stream = undefined;
				agent.taskError(task, stream.s_noutput,
				    Date.now(), error);
			} else
				stream.s_group.g_nrunning--;
		}

		agent.taskStreamAdvance(stream, callback);
	}, function () {});
};

mAgent.prototype.taskGroupSetJob = function (group, job)
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
		this.ma_taskgroups_queued.push(group);
		this.schedIdle();
	}
};

/*
 * Invoked when we discover a new task for a group.  This just enqueues it in
 * the task group.  Streams will pick up queued tasks as zones become available.
 * We also poke the scheduler, since this may have changed our shares.
 */
mAgent.prototype.taskGroupAppendTask = function (group, task)
{
	group.g_log.debug('appending task "%s"', task.t_id);
	task.t_group = group;
	group.g_tasks.push(task);

	this.schedGroup(group);
};


/*
 * Task scheduling
 */

/*
 * Update the number of reserve zones based on the current total number of
 * zones.
 */
mAgent.prototype.schedUpdateReserve = function ()
{
	var nreserved = Math.floor(
	    this.ma_nzones *
	    this.ma_conf['tunables']['zoneReservePercent'] / 100);
	mod_assert.ok(nreserved <= this.ma_nzones);
	mod_assert.ok(nreserved >= 0);

	nreserved = Math.max(nreserved,
	    this.ma_conf['tunables']['zoneReserveMin']);
	this.ma_log.info('updating reserve to %d zones', nreserved);
	this.ma_nzones_reserved = nreserved;
};

/*
 * Given a task group, determine how many zones this group should be allocated.
 * See the comment at the top of this file for much more detail.
 */
mAgent.prototype.schedGroupShare = function (group)
{
	var ntasks, nzones;

	/*
	 * The number of running tasks (g_nrunning) is almost the same as the
	 * number of streams (g_nstreams), but during initialization the task
	 * that will be run on some stream may still be on g_tasks, in which
	 * case we would double-count it here.
	 */
	ntasks = group.g_nrunning + group.g_tasks.length;
	mod_assert.ok(ntasks <= this.ma_ntasks);

	nzones = Math.floor(
	    (this.ma_nzones - this.ma_nzones_reserved) *
	    (ntasks / this.ma_ntasks));

	return (Math.min(ntasks, Math.max(1, nzones)));
};

/*
 * Determines whether the given task group has more than its fair share of
 * zones.  If so, the caller may decide to repurpose one of those zones for some
 * other group.
 */
mAgent.prototype.schedGroupOversched = function (group)
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
	var nzones = this.schedGroupShare(group);
	mod_assert.ok(nzones > 0 && nzones <= this.ma_nzones);

	if (nzones == 1)
		return (group.g_nstreams > 1);

	return (group.g_nstreams - 1 > nzones);
};

/*
 * Dispatches queued task groups to available zones until we run out of work to
 * do it or zones in which to do it.
 */
mAgent.prototype.schedIdle = function ()
{
	var zone, group;

	while (this.ma_zones_ready.length > 0 &&
	    this.ma_taskgroups_queued.length > 0) {
		group = this.ma_taskgroups_queued.shift();

		mod_assert.equal(group.g_state, maTaskGroup.TASKGROUP_S_QUEUED);
		group.g_state = maTaskGroup.TASKGROUP_S_RUNNING;

		zone = this.ma_zones_ready.shift();
		this.schedDispatch(group, zone);
	}

	while (this.ma_zones_ready.length > this.ma_nzones_reserved) {
		/*
		 * This is the expensive part.  Generally, we wouldn't expect to
		 * be executing this loop a lot, since it only happens when
		 * zones switch from an overscheduled task group to an
		 * underscheduled one.  Besides that, this operation is at worse
		 * O(nzones).  But if we spend a lot of time here, we could
		 * cache the results.
		 */
		group = this.schedPickGroup();

		if (group === undefined)
			break;

		zone = this.ma_zones_ready.shift();
		this.schedDispatch(group, zone);
	}
};

/*
 * Finds the task group having the most "need" of another zone, as computed
 * based on the difference between desired and actual concurrency as a
 * proportion of actual concurrency.
 */
mAgent.prototype.schedPickGroup = function ()
{
	var agent = this;
	var bestGroup, bestValue;

	mod_jsprim.forEachKey(agent.ma_taskgroups, function (_, group) {
		var nzones, value;

		if (group.g_state != maTaskGroup.TASKGROUP_S_RUNNING)
			return;

		if (group.g_tasks.length === 0)
			return;

		mod_assert.ok(group.g_nstreams > 0);

		nzones = agent.schedGroupShare(group);
		if (nzones <= group.g_nstreams)
			return;

		value = (nzones - group.g_nstreams) / group.g_nstreams;
		if (bestValue === undefined || value > bestValue) {
			bestValue = value;
			bestGroup = group;
		}
	});

	return (bestGroup);
};

/*
 * Invoked when we think there might be additional work to do for a single
 * group that can be parallelized with a new stream.
 */
mAgent.prototype.schedGroup = function (group)
{
	if (this.ma_zones_ready.length === 0)
		return;

	if (group.g_nstreams > 0 &&
	    this.ma_zones_ready.length <= this.ma_nzones_reserved)
		return;

	if (group.g_tasks.length === 0)
		return;

	if (group.g_state == maTaskGroup.TASKGROUP_S_INIT)
		return;

	var zone = this.ma_zones_ready.shift();
	this.schedDispatch(group, zone);
};

/*
 * Invoked when scheduling a task group on a zone.  Creates a new stream and
 * sets it running.
 */
mAgent.prototype.schedDispatch = function (group, zone)
{
	var agent = this;
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
	    'arg': {
		'agent': agent,
		'stream': stream
	    },
	    'funcs': maTaskStreamStagesDispatch
	}, function (err) {
		stream.s_pipeline = undefined;

		if (!err)
			return;

		err = new VError(err, 'failed to dispatch task');
		agent.taskStreamAbort(stream, {
		    'code': 'EJ_INIT',
		    'message': err.message
		});
	});
};

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
function maTaskStreamSetProperties(arg, callback)
{
	var stream = arg.stream;
	var group = stream.s_group;
	var zone = arg.agent.ma_zones[stream.s_machine];
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
function maTaskStreamWaitDns(arg, callback)
{
	var agent = arg.agent;

	if (agent.ma_dns_cache.lookupv4(agent.ma_manta_host)) {
		callback();
		return;
	}

	agent.ma_log.warn('delaying stream dispatch because host "%s" has ' +
	    'not been resolved', agent.ma_manta_host);
	agent.ma_dns_waiters.push(function () {
		maTaskStreamWaitDns(arg, callback);
	});
}

/*
 * Loads the task's assets into its assigned zone.  "next" is invoked only if
 * there are no errors.
 */
function maTaskStreamLoadAssets(arg, callback)
{
	var agent = arg.agent;
	var stream = arg.stream;
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
	    'func': maTaskStreamLoadAsset.bind(null, agent, stream)
	}, function (err) {
		stream.s_load_assets = undefined;
		callback(err);
	});
}

/*
 * Loads one asset for the given task group into its compute zone.
 */
function maTaskStreamLoadAsset(agent, stream, asset, callback)
{
	var group = stream.s_group;
	var zone = agent.ma_zones[stream.s_machine];
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
			    'host': agent.ma_dns_cache.lookupv4(
				agent.ma_manta_host),
			    'port': agent.ma_manta_port,
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
function maTaskStreamDispatch(arg, callback)
{
	var agent = arg.agent;
	var stream = arg.stream;

	mod_assert.equal(stream.s_state, maTaskStream.TASKSTREAM_S_LOADING);

	stream.s_log.info('starting stream execution');
	agent.ma_counters['streams_dispatched']++;

	var zone = agent.ma_zones[stream.s_machine];
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_BUSY);
	zone.z_last_contact = Date.now();

	stream.s_rqqueue.push(function (subcallback) {
		mod_assert.equal(stream.s_state,
		    maTaskStream.TASKSTREAM_S_LOADING);
		stream.s_state = maTaskStream.TASKSTREAM_S_RUNNING;

		agent.taskStreamAdvance(stream, function () {
			subcallback();
			maZoneWakeup(zone);
		});
	}, callback);
}

/*
 * Wakes up any lackey requests currently polling on work to do.
 */
function maZoneWakeup(zone)
{
	var waiters = zone.z_waiters;
	zone.z_waiters = [];
	waiters.forEach(function (w) { w.wakeup(); });
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
mAgent.prototype.taskStreamAdvance = function (stream, callback)
{
	var agent = this;
	var now, task, group, key, zone;

	mod_assert.ok(!stream.s_pending,
	    'concurrent calls to maTaskStreamAdvance');

	if (stream.s_error !== undefined ||
	    this.ma_zones[stream.s_machine].z_quiesced !== undefined ||
	    stream.s_group.g_tasks.length === 0 ||
	    this.schedGroupOversched(stream.s_group)) {
		this.taskStreamCleanup(stream);
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
		this.taskError(task, stream.s_noutput, now, {
		    'code': 'EJ_NOENT',
		    'message': 'failed to load key'
		});
		this.taskStreamAdvance(stream, callback);
		return;
	}

	zone = this.ma_zones[stream.s_machine];
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
			agent.taskStreamAbort(stream, {
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
			agent.taskError(task, stream.s_noutput, new Date(), {
			    'code': 'EJ_NOENT',
			    'message': 'failed to load key',
			    'messageInternal': suberr.message
			});
			agent.taskStreamAdvance(stream, callback);
		});
	});
};

/*
 * Cancel a task group in any state.  It's assumed that no external state needs
 * to be updated (generally because the whole job has already been cancelled, so
 * it's not necessary to update all of the individual tasks).
 */
mAgent.prototype.taskGroupCancel = function (group)
{
	var agent = this;
	var i, tasks;

	group.g_log.info('cancelled in state "%s"', group.g_state);

	/* Clear out any queued tasks. */
	tasks = group.g_tasks;
	group.g_tasks = [];
	tasks.forEach(function (task) { agent.taskRemove(task); });

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
			agent.taskStreamAbort(stream, {
			    'code': 'EJ_CANCELLED',
			    'message': 'job cancelled'
			}, true);

			if (stream.s_task) {
				stream.s_task.t_stream = undefined;
				agent.taskRemove(stream.s_task);
			}
		});

		return;
	}

	if (group.g_state == maTaskGroup.TASKGROUP_S_QUEUED) {
		mod_assert.ok(mod_jsprim.isEmpty(group.g_streams));
		mod_assert.ok(group.g_nstreams === 0);

		for (i = 0; i < this.ma_taskgroups_queued.length; i++) {
			if (this.ma_taskgroups_queued[i] === group)
				break;
		}

		mod_assert.ok(i < this.ma_taskgroups_queued.length);
		this.ma_taskgroups_queued.splice(i, 1);

		group.g_state = maTaskGroup.TASKGROUP_S_INIT;
	}

	this.taskGroupRemove(group);
};

/*
 * Mark all tasks in a group as failed, as when assets could not be found.
 * XXX This could be handled better.  At the moment, we're going to go through a
 * lot of churn to figure this out, instead of failing the job completely.
 */
mAgent.prototype.taskGroupError = function (group, error)
{
	var agent = this;
	var now = new Date();
	var tasks = group.g_tasks;

	group.g_tasks = [];

	tasks.forEach(function (task) {
		++group.g_nrunning; /* XXX because taskError decrements it */
		agent.taskError(task, 0, now, error);
	});
};

/*
 * Removes all references to this task group.  It's assumed at this point that
 * no tasks currently reference the group and the group is not enqueued to run.
 */
mAgent.prototype.taskGroupRemove = function (group)
{
	mod_assert.ok(group.g_state != maTaskGroup.TASKGROUP_S_QUEUED);
	mod_assert.equal(group.g_tasks.length, 0);
	mod_assert.equal(this.ma_taskgroups[group.g_groupid], group);
	mod_assert.ok(mod_jsprim.isEmpty(group.g_streams));
	mod_assert.equal(group.g_nstreams, 0);
	mod_assert.equal(group.g_nrunning, 0);

	var job = this.ma_jobs[group.g_jobid];

	delete (job.j_groups[group.g_groupid]);
	delete (this.ma_taskgroups[group.g_groupid]);

	if (mod_jsprim.isEmpty(job.j_groups) && !job.j_cancelled)
		delete (this.ma_jobs[job.j_id]);

	group.g_log.info('removed');
};

/*
 * Called when the stream has been terminated, either because the task group has
 * finished, the zone has been repurposed for another task group, or the stream
 * has failed fatally.  Removes this stream immediately and resets the zone for
 * use by another group.
 */
mAgent.prototype.taskStreamCleanup = function (stream)
{
	var agent = this;
	var task, group, zone;

	group = stream.s_group;
	mod_assert.equal(group.g_streams[stream.s_id], stream);

	zone = this.ma_zones[stream.s_machine];

	if (stream.s_task !== undefined) {
		task = stream.s_task;
		if (!(task.t_cancelled ||
		    zone.z_quiesced ||
		    task.t_record['value']['result'] !== undefined ||
		    this.ma_jobs[task.t_group.g_jobid].j_cancelled)) {
			stream.s_log.fatal('stream cancelled with task ' +
			    'for unknown reason');
			process.abort();
		}

		if (this.ma_tasks.hasOwnProperty(task.t_id)) {
			stream.s_log.fatal('stream cancelled with task ' +
			    'still in maTasks');
			process.abort();
		}
	}

	this.ma_counters['streams_done']++;
	stream.s_log.info('stream terminated (state = "%s"), error = ',
	    stream.s_state, stream.s_error || 'no error');
	stream.s_state = maTaskStream.TASKSTREAM_S_DONE;

	delete (group.g_streams[stream.s_id]);
	if (--group.g_nstreams === 0) {
		if (group.g_tasks.length === 0) {
			this.taskGroupRemove(group);
		} else {
			/*
			 * This is an unusual situation where we took the last
			 * zone away from a group that has more tasks.  The
			 * stream may have failed fatally.  Whatever the reason,
			 * we re-enqueue this task group.
			 */
			group.g_state = maTaskGroup.TASKGROUP_S_QUEUED;
			this.ma_taskgroups_queued.push(group);
			process.nextTick(function () { agent.schedIdle(); });
		}
	}

	mod_assert.equal(zone.z_taskstream, stream);
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_BUSY);
	zone.z_taskstream = undefined;

	if (zone.z_quiesced !== undefined) {
		this.zoneDisable(zone, new VError('zone disabled by operator'));
		return;
	}

	if (zone.z_failed !== undefined) {
		this.zoneDisable(zone, new VError('zone failed'));
		return;
	}

	if (!maZoneAutoReset) {
		this.zoneDisable(zone, new VError('auto-reset is disabled'));
		return;
	}

	zone.z_state = mod_agent_zone.maZone.ZONE_S_UNINIT;

	if (!maZoneSaveLogs) {
		mod_agent_zone.maZoneMakeReady(zone, this.zoneReady.bind(this));
		return;
	}

	var outname = mod_path.join(maZoneLogRoot,
	    group.g_jobid + '.' + group.g_phasei + '.' + mod_uuid.v4());
	var logstream = mod_agent_zone.maZoneAgentLog(zone);
	var outstream = mod_fs.createWriteStream(outname);
	logstream.pipe(outstream);
	zone.z_log.info('copying lackey log to "%s"', outname);

	function onLogError(err) {
		zone.z_log.warn(err, 'failed to read log for copy');
		mod_agent_zone.maZoneMakeReady(zone,
		    agent.zoneReady.bind(agent));
		logstream.removeListener('end', onEnd);
		outstream.removeListener('error', onErr);
		outstream.destroy();
	}
	logstream.on('error', onLogError);

	function onEnd() {
		mod_agent_zone.maZoneMakeReady(zone,
		    agent.zoneReady.bind(agent));
		outstream.removeListener('error', onErr);
		logstream.removeListener('error', onLogError);
	}
	logstream.on('end', onEnd);

	function onErr(err) {
		zone.z_log.warn(err, 'failed to write log copy');
		mod_agent_zone.maZoneMakeReady(zone,
		    agent.zoneReady.bind(agent));
		logstream.removeListener('end', onEnd);
		logstream.removeListener('error', onLogError);
		logstream.destroy();
	}
	outstream.on('error', onErr);
};

/*
 * Invoked as a callback when the given zone transitions to the "ready" state
 * (or fails to do so).
 */
mAgent.prototype.zoneReady = function (zone, err, reason)
{
	var agent = this;

	if (err) {
		mod_assert.equal(zone.z_state,
		    mod_agent_zone.maZone.ZONE_S_DISABLED);
		this.zoneDisable(zone,
		    new VError(err, 'zone could not be made ready'));
		return;
	}

	if (zone.z_quiesced !== undefined) {
		this.zoneDisable(zone, new VError('zone disabled by operator'));
		return;
	}

	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_READY);

	var options = this.ma_conf['zoneDefaults'];
	mod_agent_zone.maZoneSet(zone, options, function (suberr) {
		if (suberr) {
			agent.zoneDisable(zone, new VError(suberr,
			    'failed to set properties'));
			return;
		}

		agent.ma_counters['zones_readied']++;
		agent.ma_zones_ready.push(zone);
		agent.schedIdle();
	});
};

mAgent.prototype.zoneDisable = function (zone, err)
{
	zone.z_state = mod_agent_zone.maZone.ZONE_S_DISABLED;
	this.ma_nzones--;
	this.ma_counters['zones_disabled']++;
	zone.z_log.error(err, 'zone removed from service');

	this.schedUpdateReserve();
	this.schedIdle();
};

mAgent.prototype.zoneAdd = function (zonename)
{
	var agent = this;
	var zone;

	if (this.ma_zones.hasOwnProperty(zonename)) {
		/*
		 * If this request identifies a zone that is currently disabled,
		 * we take this as a request to try to make it ready again.
		 */
		zone = this.ma_zones[zonename];
		if (zone.z_state == mod_agent_zone.maZone.ZONE_S_DISABLED) {
			zone.z_quiesced = undefined;
			this.ma_nzones++;
			mod_agent_zone.maZoneMakeReady(zone,
			    this.zoneReady.bind(this));
			this.schedUpdateReserve();
			return (null);
		}

		return (new Error(
		    'attempted to add duplicate zone ' + zonename));
	}

	agent.ma_log.info('adding zone "%s"', zonename);
	zone = mod_agent_zone.maZoneAdd(zonename,
	    this.ma_log.child({ 'component': 'Zone-' + zonename }));
	this.ma_zones[zone.z_zonename] = zone;
	this.ma_nzones++;
	this.ma_counters['zones_added']++;
	mod_agent_zone.maZoneMakeReady(zone, this.zoneReady.bind(this));
	this.schedUpdateReserve();
	return (null);
};


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
	error = maAgent.zoneAdd(zonename);

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
	var agent = maAgent;
	var rv = [];

	for (var zonename in agent.ma_zones)
		rv.push(agent.ma_zones[zonename].httpState());

	response.send(rv);
	next();
}

/* POST /zones/:zonename/disable */
function maHttpZoneDisable(request, response, next)
{
	var agent = maAgent;
	var zonename, zone;

	zonename = request.params['zonename'];

	if (!agent.ma_zones.hasOwnProperty(zonename)) {
		next(new mod_restify.NotFoundError());
		return;
	}

	zone = agent.ma_zones[zonename];

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

	for (var i = 0; i < agent.ma_zones_ready.length; i++) {
		if (agent.ma_zones_ready[i] == zone)
			break;
	}

	if (i < agent.ma_zones_ready.length)
		agent.ma_zones_ready.splice(i, 1);

	agent.zoneDisable(zone, new VError('zone disabled by operator'));
	response.send(204);
	next();
}

/*
 * DELETE /zones/:zonename
 */
function maHttpZoneDelete(request, response, next)
{
	var agent = maAgent;
	var zonename, zone;

	zonename = request.params['zonename'];

	if (!agent.ma_zones.hasOwnProperty(zonename)) {
		next(new mod_restify.NotFoundError());
		return;
	}

	zone = agent.ma_zones[zonename];

	if (zone.z_state != mod_agent_zone.maZone.ZONE_S_DISABLED) {
		next(new mod_restify.ConflictError('zone is not disabled'));
		return;
	}

	mod_agent_zone.maZoneRemove(zone, function (err) {
		if (err) {
			next(err);
			return;
		}

		agent.ma_log.info('removing zone "%s"', zonename);
		delete (agent.ma_zones[zonename]);
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

	/*
	 * By default, Node will destroy the socket in 2 minutes, and there's no
	 * reasoanble way to prevent that.  We use socket.setTimeout to set this
	 * timeout to a much larger value, and we use our own timer to report a
	 * 204 instead.
	 */
	var setTimer = function (request, response, next) {
		request.connection.setTimeout(2 * maRequestTimeout);

		request.maWaiter = new maZoneWaiter(request, response, next);

		request.maTimeout = setTimeout(function () {
			request.maTimeout = undefined;
			request.maWaiter.timedOut();
		}, maRequestTimeout);

		next();
	};

	var clearTimer = function (request, response, next) {
		if (request.maTimeout !== undefined)
			clearTimeout(request.maTimeout);

		next();
	};

	s.get('/my/jobs/task/task',
	    mod_restify.bodyParser({ 'mapParams': false }),
	    init, setTimer, maTaskApiTask, clearTimer);
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
 * the task or 204 if no task is assigned right now.  The lackey invokes this
 * with wait=true, which means to block until a task is available.
 */
function maTaskApiTask(request, response, next, nowait)
{
	var agent = maAgent;
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
		request.log.warn('caught lackey too early/too late');
		response.send(new mod_restify.ServiceUnavailableError());
		next();
		return;
	}

	if (stream !== undefined &&
	    stream.s_task !== undefined &&
	    stream.s_state == maTaskStream.TASKSTREAM_S_RUNNING) {
		maZoneHeartbeat(zone);

		if (agent.taskStreamHasWork(stream) && !stream.s_pending) {
			response.send(stream.streamState(agent));
			next();
			return;
		}
	}

	if (nowait || request.query['wait'] != 'true') {
		response.send(204);
		next();
		return;
	}

	zone.z_waiters.push(request.maWaiter);
}

function maZoneWaiter(request, response, next)
{
	this.w_request = request;
	this.w_response = response;
	this.w_next = next;
}

maZoneWaiter.prototype.wakeup = function (nowait)
{
	mod_assert.equal(-1, this.w_request.maZone.z_waiters.indexOf(this));
	maTaskApiTask(this.w_request, this.w_response, this.w_next, nowait);
};

maZoneWaiter.prototype.timedOut = function ()
{
	var which;

	this.w_request.log.debug('timed out waiting for work');

	which = this.w_request.maZone.z_waiters.indexOf(this);
	mod_assert.ok(which != -1);
	this.w_request.maZone.z_waiters.splice(which, 1);

	maTaskApiTask(this.w_request, this.w_response, this.w_next, true);
};

/*
 * POST /my/jobs/task/commit: indicate that the given key has been successfully
 * processed.
 */
function maTaskApiCommit(request, response, next)
{
	var agent = maAgent;
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
			agent.taskUpdate(task, stream.s_machine,
			    stream.s_noutput);
			callback();
			return;
		}

		task.t_stream = undefined;
		agent.taskDone(task, stream.s_machine, stream.s_noutput, now);
		agent.taskStreamAdvance(stream, callback);
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
	var agent = maAgent;
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
		agent.taskError(task, stream.s_noutput, now, error);
		agent.taskStreamAdvance(stream, callback);
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
	var agent = maAgent;
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
		iostream = request.headers['x-marlin-stream'];

		if (iostream != 'stderr' && iostream != 'stdout')
			iostream = undefined;

		/*
		 * mcat uses the x-marlin-reference header to indicate that it's
		 * not actually creating a new file, but just marking it for
		 * output for this task.  In that case, we simply skip the proxy
		 * step.
		 */
		if (request.headers['x-marlin-reference']) {
			agent.taskEmit(stream, stream.s_task, iostream, key,
			    reducer);
			response.send(204);
			next();
			return;
		}

		if (iostream !== undefined) {
			continuefunc = function () {
				agent.taskEmit(stream, stream.s_task, iostream,
				    key, reducer);
			};
		}
	}

	proxyargs = {
	    'request': request,
	    'response': response,
	    'continue': continuefunc,
	    'server': {
		'headers': {
		    'authorization': sprintf('Token %s', group.g_token)
		},
	        'host': agent.ma_dns_cache.lookupv4(agent.ma_manta_host),
		'port': agent.ma_manta_port,
		'path': key
	    }
	};

	agent.ma_counters['mantarq_proxy_sent']++;

	mod_mautil.maHttpProxy(proxyargs, function () {
		agent.ma_counters['mantarq_proxy_return']++;
		next();
	});
}

mAgent.prototype.taskEmit = function (stream, task, iostream, key, reducer)
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
	this.ma_moray_queue.dirty('taskoutput', uuid, {
	    'jobId': stream.s_group.g_jobid,
	    'taskId': task.t_id,
	    'key': key,
	    'rIdx': reducer,
	    'timeCreated': mod_jsprim.iso8601(Date.now())
	});
};

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
	var agent = maAgent;

	if (type == 'Global')
		return ([ 0 ]);

	if (type == 'Jobs')
		return (Object.keys(agent.ma_jobs));

	if (type == 'Requests')
		return (Object.keys(agent.ma_requests));

	if (type == 'TaskGroups')
		return (Object.keys(agent.ma_taskgroups));

	if (type == 'TaskStreams') {
		var rv = [];
		mod_jsprim.forEachKey(agent.ma_taskgroups,
		    function (_, group) {
			mod_jsprim.forEachKey(group.g_streams,
			    function (__, stream) {
				rv.push(group.g_groupid + '|' + stream.s_id);
			    });
		    });
		return (rv);
	}

	return (Object.keys(agent.ma_zones));
}

function maKangGetObject(type, id)
{
	var agent = maAgent;

	if (type == 'Global') {
		return ({
		    'tickStart': agent.ma_tick_start,
		    'tickDone': agent.ma_tick_done,
		    'nGroupsQueued': agent.ma_taskgroups_queued.length,
		    'nZonesReady': agent.ma_zones_ready.length,
		    'nReduceTasks': Object.keys(agent.ma_tasks_reduce).length,
		    'nTasks': agent.ma_ntasks,
		    'nZones': agent.ma_nzones,
		    'nZonesReserved': agent.ma_nzones_reserved
		});
	}

	if (type == 'Jobs')
		return (agent.ma_jobs[id].kangState());

	if (type == 'Requests') {
		var request = agent.ma_requests[id];

		return ({
		    'id': request.id,
		    'method': request.method,
		    'url': request.url,
		    'time': request.time
		});
	}

	if (type == 'TaskGroups')
		return (agent.ma_taskgroups[id].kangState(agent));

	if (type == 'TaskStreams') {
		var parts = id.split('|', 2);
		return (agent.ma_taskgroups[parts[0]].g_streams[
		    parts[1]].kangState());
	}

	return (agent.ma_zones[id].httpState());
}

function maKangStats()
{
	return (maAgent.ma_counters);
}

main();
