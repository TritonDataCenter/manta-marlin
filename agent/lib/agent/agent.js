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
var mod_fs = require('fs');
var mod_http = require('http');
var mod_os = require('os');
var mod_path = require('path');
var mod_url = require('url');
var mod_uuid = require('node-uuid');

var mod_bunyan = require('bunyan');
var mod_extsprintf = require('extsprintf');
var mod_getopt = require('posix-getopt');
var mod_jsprim = require('jsprim');
var mod_kang = require('kang');
var mod_kstat = require('kstat');
var mod_mkdirp = require('mkdirp');
var mod_panic = require('panic');
var mod_restify = require('restify');
var mod_semver = require('semver');
var mod_spawnasync = require('spawn-async');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var sprintf = mod_extsprintf.sprintf;
var statvfs = require('statvfs');
var VError = mod_verror.VError;

var mod_adnscache = require('./adnscache');
var mod_bus = require('../bus');
var mod_libmanta = require('libmanta');
var mod_mautil = require('../util');
var mod_meter = require('../meter');
var mod_provider = require('../provider');
var mod_schema = require('../schema');
var mod_agent_zone = require('./zone');
var mod_agent_zonepool = require('./zone-pool');

var maQueries = require('./queries');
var maConfSchema = require('./schema');
var maProviderDefinition = require('./provider');

var NANOSEC = 1e9;

/* jsl:import ../../../common/lib/errors.js */
require('../errors');

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

var maMorayMaxRecords 		= 1000;		/* max results per request */
var maRequestTimeout 		= 300000;	/* local long poll timeout */

var maLogStreams = [ {
    'stream': process.stdout,
    'level': process.env['LOG_LEVEL'] || 'info'
} ];

var maLackeyTimeoutError = {
    'code': EM_INTERNAL,
    'message': 'internal error',
    'messageInternal': 'lackey timed out'
};

var maOutOfDiskError = {
    'code': EM_USERTASK,
    'message': 'user task ran out of local disk space'
};

var maOutOfMemoryError = {
    'code': EM_USERTASK,
    'message': 'user task ran out of memory'
};

var maNoImageError = {
    'code': EM_INVALIDARGUMENT,
    'message': 'failed to dispatch task: requested image is not available'
};

var maNoMemError = {
    'code': EM_TASKINIT,
    'message': 'failed to dispatch task: not enough memory available'
};

var maNoDiskError = {
    'code': EM_TASKINIT,
    'message': 'failed to dispatch task: not enough disk space available'
};

var maKilledError = {
    'code': EM_TASKKILLED,
    'message': 'task killed (excessive resource usage)'
};

/*
 * Kstats filters.  The instance numbers of the corresponding kstats must
 * correspond to the zoneids of the corresponding zone.
 */
var maKstatFilters = {
    'cpu': {
	'module': 'zones',
	'class': 'zone_misc'
    },
    'memory': {
	'module': 'memory_cap'
    },
    'vfs': {
	'module': 'zone_vfs'
    },
    'zfs': {
	'module': 'zone_zfs'
    },
    'vnic0': {
	'module': 'link'
    }
};

/*
 * link:z${zoneid}_net0: we grab only the 64-bit bytes/packets/errors fields
 */
var maKstatFieldsPrune = {};
[
    'brdcstrcv', 'brdcstxmt', 'collisions', 'ifspeed', 'ipackets',
    'link_duplex', 'link_state', 'multircv', 'multixmt', 'norcvbuf',
    'noxmtbuf', 'obytes', 'oerrors', 'opackets', 'rbytes', 'unknowns'
].forEach(function (f) { maKstatFieldsPrune[f] = true; });

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

/*
 * All "global" state for the agent is linked to a singleton mAgent object.
 * Methods of this class make up most of the agent implementation.
 */
function mAgent(filename)
{
	var agent = this;
	var url;

	this.ma_log = new mod_bunyan({
	    'name': maServerName,
	    'streams': maLogStreams
	});

	this.ma_spawner = mod_spawnasync.createWorker({
	    'log': this.ma_log.child({ 'component': 'Spawner' })
	});

	/*
	 * Configuration
	 */
	this.ma_conf = mod_mautil.readConf(this.ma_log, maConfSchema, filename);
	this.ma_log.info('configuration', this.ma_conf);

	this.ma_server_uuid = this.ma_conf['instanceUuid'];
	this.ma_manta_compute_id = this.ma_conf['mantaComputeId'];
	this.ma_buckets = this.ma_conf['buckets'];
	this.ma_liveness_interval =
		this.ma_conf['tunables']['zoneLivenessCheck'];
	this.ma_bucketnames = {};
	mod_jsprim.forEachKey(this.ma_buckets, function (name, bucket) {
		agent.ma_bucketnames[bucket] = name;
	});
	this.ma_zone_idle_min = this.ma_conf['tunables']['timeZoneIdleMin'];
	this.ma_zone_idle_max = this.ma_conf['tunables']['timeZoneIdleMax'];
	mod_assert.ok(this.ma_zone_idle_max >= this.ma_zone_idle_min,
	    'timeZoneIdleMax cannot be less than timeZoneIdleMin');

	url = mod_url.parse(this.ma_conf['manta']['url']);
	this.ma_manta_host = url['hostname'];
	this.ma_manta_port = url['port'] ? parseInt(url['port'], 10) : 80;

	this.ma_kstat_readers = {};
	mod_jsprim.forEachKey(maKstatFilters, function (name, filter) {
		agent.ma_kstat_readers[name] = new mod_kstat.Reader(filter);
	});

	mod_http.globalAgent.maxSockets =
	    this.ma_conf['tunables']['httpMaxSockets'] || 512;

	/*
	 * Helper objects
	 */
	this.ma_logthrottle = new mod_mautil.EventThrottler(60 * 1000);
	this.ma_task_checkpoint = new mod_mautil.Throttler(
	    this.ma_conf['tunables']['timeTasksCheckpoint']);
	this.ma_task_checkpoint_freq = new mod_mautil.Throttler(5000);
	this.ma_dtrace = mod_provider.createProvider(maProviderDefinition);

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

	this.ma_bus = new mod_bus.createBus(this.ma_conf, {
	    'log': this.ma_log.child({ 'component': 'MorayBus' })
	});

	this.ma_bus_options = {
	    'limit': maMorayMaxRecords,
	    'timePoll': this.ma_conf['tunables']['timePoll']
	};

	this.ma_server = mod_restify.createServer({
	    'name': maServerName,
	    'log': this.ma_log.child({
		'component': 'HttpServer',
		'serializers': mod_restify.bunyan.serializers
	    })
	});

	/*
	 * Dynamic state
	 */

	/*
	 * Images and zones: each zone is deployed with a particular image,
	 * which we track in order to allow users to depend on image versions.
	 * We maintain a pool of zones for each image, incoming tasks are
	 * assigned to a pool based on their image dependency, and the scheduler
	 * state is maintained on this per-pool basis.
	 *
	 * The zone and image discovery process is asynchronous, since it
	 * involves calls to both imgadm and vmadm.  We store information about
	 * all images in ma_images, but only the supported ones (ones for which
	 * we have compute zones available) in ma_images_byvers, where the
	 * images are listed in priority order.
	 */
	this.ma_zonepools = {};		/* zone pools, by image uuid */
	this.ma_images = {};		/* image details, by image uuid */
	this.ma_images_byvers = [];	/* list of images in preference order */
	this.ma_discovering = false;	/* currently discovering zones */
	this.ma_discovery_pending = false;	/* queued discover */
	this.ma_discovery_callback = null;	/* invoke on discover done */

	this.ma_start = undefined;	/* agent start time */
	this.ma_tick_start = undefined;	/* time the last tick started */
	this.ma_tick_done = undefined;	/* time the last tick ended */
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
	this.ma_heartbeat = new mod_mautil.Throttler(
	    this.ma_conf['tunables']['timeHeartbeat']);

	/*
	 * See the block comment above for information on tasks, jobs, task
	 * groups, and zones.
	 */
	this.ma_jobs = {};		/* known jobs, by jobid */
	this.ma_taskgroups = {};	/* task groups, by group id */
	this.ma_taskgroups_queued = {};	/* ready-to-run task groups, by image */
	this.ma_tasks = {};		/* known tasks, by taskid */
	this.ma_ntasks = 0;		/* total nr of tasks */
	this.ma_zones = {};		/* all zones, by zonename */
	this.ma_tasks_failed = {};	/* failed reduce tasks */

	/*
	 * Memory management: we maintain a slop pool of DRAM, configured as a
	 * percentage of total physical memory on the system.  Memory from this
	 * pool is allocated to zones whose tasks request it (by specifying the
	 * "memory" property on the job phase).
	 */
	var pct = this.ma_conf['tunables']['zoneMemorySlopPercent'] / 100;
	this.ma_slopmem = Math.floor(pct * mod_os.totalmem() / 1024 / 1024);
	this.ma_slopmem_used = 0;

	/*
	 * Disk space management: we take a similar approach for managing disk
	 * space as well, though fetching the total filesystem size is
	 * asynchronous.
	 */
	this.ma_slopdisk = 0;
	this.ma_slopdisk_used = 0;
}

/*
 * Start the agent: initialize temporary directories, the local HTTP server,
 * local zones assigned to this agent, Moray subscriptions, and so on.
 */
mAgent.prototype.init = function ()
{
	var agent = this;
	var tick = function () { agent.tick(); };
	var interval = this.ma_conf['tunables']['timeTick'];

	this.ma_spawner.aspawn([
	    mod_path.join(__dirname, '../../sbin/mrlogadmconf.sh')
	], function (err, stdout, stderr) {
		if (err) {
			agent.ma_log.error(err,
			    'failed to set up logadm: "%s"', stderr);
		} else {
			agent.ma_log.info('updated logadm.conf');
		}
	});

	this.ma_dtrace.enable();
	this.ma_dtrace.fire('agent-started', function () {
		return ([ agent.ma_conf['mantaComputeId'] ]);
	});

	this.ma_init_barrier.start('sync');

	this.initDiskSlop();
	this.initDirs();
	this.initHttp();
	this.initZones();
	this.initPolls();

	this.ma_init_barrier.on('drain', function () {
		agent.ma_log.info('memory slop: %d MB', agent.ma_slopmem);
		agent.ma_log.info('disk slop: %d GB', agent.ma_slopdisk);
		agent.ma_log.info('agent started');
		agent.ma_start = mod_jsprim.iso8601(Date.now());
		agent.ma_conf['instanceGeneration'] = agent.ma_start;
		agent.ma_conf['agentGeneration'] = agent.ma_start;
		agent.meter('AgentStart', {});
		setInterval(tick, interval);
	});

	this.ma_init_barrier.done('sync');
};

/*
 * Figure out how much disk space we've got to work with.
 */
mAgent.prototype.initDiskSlop = function ()
{
	var agent = this;
	var path = '/zones';
	var pct = this.ma_conf['tunables']['zoneDiskSlopPercent'] / 100;

	this.ma_init_barrier.start('disk slop');
	statvfs(path, function (err, fsinfo) {
		if (err) {
			agent.ma_log.fatal('failed to statvfs "%s"', path);
			throw (err);
		}

		var fsbytes = fsinfo['blocks'] * fsinfo['frsize'];
		var fsgb = fsbytes / 1024 / 1024 / 1024;
		agent.ma_slopdisk = Math.floor(pct * fsgb);
		agent.ma_init_barrier.done('disk slop');
	});
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

	/* JSSTYLED */
	server.get(/^\/kang\/.*/, mod_kang.knRestifyHandler({
	    'uri_base': '/kang',
	    'service_name': 'marlin',
	    'component': 'agent',
	    'ident': this.ma_manta_compute_id,
	    'version': '0',
	    'list_types': maKangListTypes,
	    'list_objects': maKangListObjects,
	    'get': maKangGetObject,
	    'schema': maKangSchema,
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
	this.ma_init_barrier.start('zones discovery');
	this.ma_discovery_callback = function () {
		agent.ma_init_barrier.done('zones discovery');
	};
	this.zonesDiscover();
};

/*
 * Set up our Moray subscriptions (which drive the agent's work).
 */
mAgent.prototype.initPolls = function ()
{
	var agent = this;
	var queries = [
	    maQueries.aqTasksDispatched,
	    maQueries.aqTasksCancelled,
	    maQueries.aqTasksInputDone,
	    maQueries.aqTaskInputs
	];

	queries.forEach(function (queryconf) {
		agent.ma_bus.subscribe(
		    agent.ma_buckets[queryconf['bucket']],
		    queryconf['query'].bind(null, agent.ma_conf),
		    agent.ma_bus_options, agent.onRecord.bind(agent));
	});
};

mAgent.prototype.availMemSlop = function ()
{
	return (this.ma_slopmem - this.ma_slopmem_used);
};

mAgent.prototype.availDiskSlop = function ()
{
	return (this.ma_slopdisk - this.ma_slopdisk_used);
};

/*
 * Periodic activity
 */

/*
 * Invoked once per "timeTick" milliseconds to potentially kick off a poll for
 * more records from Moray, update our DNS cache, and check lackey liveness.
 * Each of these actions is throttled so that it won't happen either while
 * another one is ongoing or if it's been too recent since the last one.
 */
mAgent.prototype.tick = function ()
{
	var timestamp;

	this.ma_tick_start = new Date();
	timestamp = this.ma_tick_start.getTime();

	this.ma_dns_cache.update();
	this.ma_bus.poll(timestamp);
	this.ma_logthrottle.flush(timestamp);

	this.zonesCheckLiveness();

	if (!this.ma_heartbeat.tooRecent()) {
		this.heartbeat();
		this.schedHogsTick(timestamp);
	}

	if (!this.ma_task_checkpoint_freq.tooRecent()) {
		this.ma_task_checkpoint_freq.start();
		this.tasksCheckpoint(!this.ma_task_checkpoint.tooRecent());
		this.ma_task_checkpoint_freq.done();
	}

	if (maCheckNTasks)
		this.checkTaskCount();

	this.ma_tick_done = new Date();
};

mAgent.prototype.zoneMaybeTimeout = function (zone, now)
{
	var stream = zone.z_taskstream;

	/*
	 * We only expect heartbeats from lackeys actually assigned to
	 * execute a task stream.
	 */
	if (stream === undefined ||
	    stream.s_state != maTaskStream.TASKSTREAM_S_RUNNING)
		return (false);

	if (now - zone.z_last_contact <= this.ma_liveness_interval)
		return (false);

	/*
	 * If the task has already completed, we may just be waiting for
	 * the rest of the taskinputs to be processed.  We don't care
	 * about lackey liveness in this case.
	 */
	if (stream.s_task !== undefined && stream.s_task.t_done)
		return (false);

	/*
	 * If there's no work available on this stream, then it's
	 * still possible that we haven't assigned any work to it, in
	 * which case it won't be heartbeating.  This could happen for a
	 * reduce stream where the lackey completed one of the first
	 * tasks, but there's neither data available nor end-of-stream
	 * for another task, so the stream is running but the lackey has
	 * nothing to do.
	 */
	if (stream.s_group.g_multikey && !this.taskStreamHasWork(stream))
		return (false);

	return (true);
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

		if (stream === undefined || stream.s_statvfs ||
		    !agent.zoneMaybeTimeout(zone, now))
			return;

		stream.s_statvfs = true;
		statvfs(zone.z_root, function (err, fsinfo) {
			stream.s_statvfs = false;

			if (err)
				zone.z_log.warn(err,
				    'failed to check disk usage');

			/*
			 * Check whether this zone has advanced to another
			 * stream, or if any of the conditions above have
			 * changed.
			 */
			if (stream !== zone.z_taskstream ||
			    !agent.zoneMaybeTimeout(zone, now))
				return;

			var timestamp = Date.now();
			var taskerror;

			if (agent.streamSawAllocFailures(stream)) {
				/*
				 * One possible source of lackey timeouts is if
				 * the lackey ran out of memory.  We can detect
				 * this by looking for anonymous allocation
				 * failures in the zone.  If we find any, we'll
				 * report a more specific error and not bother
				 * taking the zone out of service.
				 */
				zone.z_log.error('lackey has timed out ' +
				    '(with anon allocation failures)');
				taskerror = maOutOfMemoryError;
			} else if (!err &&
			    fsinfo['bfree'] * fsinfo['frsize'] <
			    10 * 1024 * 1024) {
				/*
				 * Similarly, if we ran out of disk space, we
				 * can be reasonably sure that's what caused
				 * the lackey to fail.
				 */
				zone.z_log.error('lackey has timed out ' +
				    '(out of disk space)');
				taskerror = maOutOfDiskError;
			} else {
				/*
				 * A lackey timeout is a pretty serious failure.
				 * We disable the zone.  Eventually we'll need
				 * alarms on this to make it clear that operator
				 * intervention is required to diagnose and
				 * clear the failure.  We could potentially even
				 * skip this by collecting core files dumped by
				 * the zone, logs from the zone, a ptree of the
				 * zone, and a core of any running lackey
				 * process, and then resetting the zone.
				 */
				zone.z_log.error('lackey has timed out');
				taskerror = maLackeyTimeoutError;
				zone.z_failed = timestamp;
			}

			if (stream.s_task !== undefined)
				agent.taskMarkFailed(stream.s_task, timestamp,
				    taskerror);

			agent.taskStreamAbort(stream, taskerror);
		});
	});
};

mAgent.prototype.heartbeat = function ()
{
	var agent = this;

	this.ma_log.debug('heartbeat: start');
	this.ma_heartbeat.start();
	this.ma_bus.putBatch([
	    [ this.ma_buckets['health'], this.ma_manta_compute_id, {
		'component': 'agent',
		'instance': this.ma_manta_compute_id,
		'serverUuid': this.ma_server_uuid,
		'generation': this.ma_start
	    } ] ], {}, function (err) {
		agent.ma_heartbeat.done();

		if (err)
			agent.ma_log.warn(err, 'heartbeat: failed');
		else
			agent.ma_log.debug('heartbeat: done');
	    });
};

/*
 * Write out "checkpoint" metering records for tasks that are currently
 * executing.  If "force" is true, then we do this for all tasks.  Otherwise, we
 * only do this for tasks in jobs requesting frequent checkpointing.
 */
mAgent.prototype.tasksCheckpoint = function (force)
{
	var agent = this;

	this.ma_log.debug('tasks checkpoint: start', force);

	if (force)
		this.ma_task_checkpoint.start();

	mod_jsprim.forEachKey(this.ma_taskgroups, function (_1, group) {
		mod_jsprim.forEachKey(group.g_streams, function (_2, stream) {
			if (stream.s_task === undefined)
				return;

			if (!force && !stream.s_task.t_group.g_checkpoint)
				return;

			agent.taskMeter(stream.s_task, 'TaskCheckpoint',
			    stream.s_stats.checkpoint());
		});
	});

	this.ma_log.debug('tasks checkpoint: done', force);

	if (force)
		this.ma_task_checkpoint.done();
};

/*
 * In debug mode, we periodically check that the total number of tasks in the
 * system matches what we expect.
 */
mAgent.prototype.checkTaskCount = function ()
{
	var agent = this;
	var log = this.ma_log;
	var found, taskcount;

	taskcount = 0;
	mod_jsprim.forEachKey(this.ma_tasks, function () { taskcount++; });

	if (taskcount != this.ma_ntasks) {
		log.fatal('checkTaskCount failed: ma_ntasks = %d, but ' +
		    'found %d tasks in ma_tasks', this.ma_ntasks, taskcount);
		process.abort();
	}

	found = {};
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
			if (stream.s_task === undefined)
				return;

			var taskid = stream.s_task.t_id;
			if (!agent.ma_tasks.hasOwnProperty(taskid)) {
				log.fatal('checkTaskCount ' +
				    'failed: found task "%s" on ' +
				    'stream "%s" but not in ma_tasks',
				    taskid, stream.s_id);
				process.abort();
			}

			found[taskid] = true;
		});
	});

	mod_jsprim.forEachKey(agent.ma_tasks, function (taskid) {
		if (!found.hasOwnProperty(taskid)) {
			log.fatal('checkTaskCount failed: found task ' +
			    '"%s" on ma_tasks, but not in groups', taskid);
			process.abort();
		}
	});
};


/*
 * Event handlers
 */

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

mAgent.prototype.onRecord = function (record, barrier)
{
	var schema, error;

	this.ma_log.debug('record: "%s" "%s" etag %s',
	    record['bucket'], record['key'], record['_etag']);

	schema = mod_schema.sBktJsonSchemas[
	    this.ma_bucketnames[record['bucket']]];
	error = mod_jsprim.validateJsonObject(schema, record['value']);
	if (error) {
		if (!this.ma_logthrottle.throttle(sprintf(
		    'record %s/%s', record['bucket'], record['key'])))
			this.ma_log.warn(error, 'onRecord: validation error',
			    record);
		return;
	}

	if (record['bucket'] == this.ma_buckets['task']) {
		this.onRecordTask(record, barrier);
	} else {
		mod_assert.equal(record['bucket'],
		    this.ma_buckets['taskinput']);
		this.onRecordTaskInput(record, barrier);
	}
};

mAgent.prototype.onRecordTask = function (record, barrier)
{
	var taskid = record['value']['taskId'];
	var task, group;

	if (record['value']['state'] == 'dispatched' &&
	    record['value']['timeCancelled'] === undefined) {
		this.taskAccept(record, barrier);
		return;
	}

	/*
	 * If this task is not "dispatched" and we don't already know about it,
	 * then either it's from a previous crash (in which case the agent
	 * should notice and cancel it) or it's a result of a taskAccept for
	 * which we haven't received a Moray response yet.  In both cases, we
	 * just ignore the record.
	 */
	if (!this.ma_tasks.hasOwnProperty(taskid)) {
		if (!this.ma_logthrottle.throttle(sprintf(
		    'record %s/%s', record['bucket'], record['key'])))
			this.ma_log.warn('onRecord: ignoring record for ' +
			    'unknown task', record);
		return;
	}

	task = this.ma_tasks[taskid];
	group = task.t_group;

	/*
	 * If the task's internal state is "done", then there's nothing else for
	 * us to do here.  It's already finished or cancelled, we've very likely
	 * already written out our last update for it, and we'll resolve
	 * EtagConflict errors elsewhere.
	 */
	if (task.t_record['value']['state'] == 'done') {
		group.g_log.warn('onRecord: dropping task (already done)',
		    record);
		return;
	}

	/*
	 * Relatedly, since we mark a task's internal state "done" when we see
	 * that it's been cancelled, the task must not be cancelled now.
	 */
	mod_assert.ok(!task.t_cancelled);
	if (record['value']['timeCancelled'] !== undefined) {
		group.g_log.debug('task "%s": cancelled');

		task.t_cancelled = true;
		task.t_record['value']['timeCancelled'] =
		    record['value']['timeCancelled'];
		task.t_record['_etag'] = record['_etag'];
		barrier.start(task.t_id);
		this.taskMarkFailed(task, Date.now(), undefined, barrier);
		this.taskCancel(task);
		return;
	}

	if (record['value']['timeInputDoneRead'] !== undefined ||
	    record['value']['timeInputDone'] === undefined) {
		if (!this.ma_logthrottle.throttle(sprintf(
		    'record %s/%s', record['bucket'], record['key'])))
			this.ma_log.error('onRecord: unknown reason for match',
			    record);
		return;
	}

	task.t_group.g_log.info('task "%s": input done', task.t_id);
	task.t_record['value']['timeInputDone'] =
	    record['value']['timeInputDone'];
	task.t_record['value']['timeInputDoneRead'] =
	    mod_jsprim.iso8601(Date.now());
	task.t_record['value']['nInputs'] = record['value']['nInputs'];
	task.t_ninputs = record['value']['nInputs'];
	barrier.start(task.t_id);
	this.taskDirty(task, undefined, barrier);

	if (group.g_poolid && group.g_state == maTaskGroup.TASKGROUP_S_INIT) {
		group.g_state = maTaskGroup.TASKGROUP_S_QUEUED;
		this.ma_taskgroups_queued[group.g_poolid].push(group);
		this.schedIdlePool(group.g_poolid);
		return;
	}

	if (task.t_stream !== undefined && !task.t_done &&
	    task.t_xinput.length === 0 && this.taskReadAllRecords(task)) {
		this.zoneWakeup(this.ma_zones[task.t_stream.s_machine]);
		return;
	}

	if (task.t_done && this.taskReadAllRecords(task))
		this.taskAsyncDone(task);
};

mAgent.prototype.onRecordTaskInput = function (record, barrier)
{
	var taskid, task;

	/*
	 * If we don't know anything about the corresponding task, we ignore
	 * this record.  This is unlikely because task records are usually
	 * written when the job is picked up by the worker, while taskinput
	 * records cannot be written out until the worker has resolved the input
	 * object's user and location, so it's tough for the latter to beat the
	 * former.  But if this does happen, we just assume that we will read
	 * the task record shortly and will process this taskinput record on one
	 * of the next go-arounds.
	 */
	taskid = record['value']['taskId'];

	if (this.ma_tasks.hasOwnProperty(taskid)) {
		/*
		 * Append the input object to the corresponding task.  This will
		 * wake up anybody who's waiting on the result.
		 */
		task = this.ma_tasks[taskid];
		this.taskAppendKey(task, record['value']);
	} else if (this.ma_tasks_failed.hasOwnProperty(taskid)) {
		if (!this.ma_logthrottle.throttle(sprintf(
		    'task %s taskinput', taskid)))
			this.ma_log.warn('onRecord: found taskinput "%s" for ' +
			    'failed task "%s" (other messages throttled)',
			    record['key'], taskid);
	} else {
		if (!this.ma_logthrottle.throttle(sprintf(
		    'record %s/%s', record['bucket'], record['key'])))
			this.ma_log.warn('onRecord: dropping taskinput for ' +
			    'unknown task', record);
		return;
	}

	/*
	 * Update the record to indicate that we've read it so that we don't
	 * keep seeing this key.  Use the barrier to block subsequent queries
	 * for new taskinputs until we've saved this.  There's no reason this
	 * should ever fail with EtagConflict, so we don't handle that here.
	 */
	barrier.start(record['key']);
	record['value']['timeRead'] = mod_jsprim.iso8601(Date.now());
	this.ma_bus.putBatch([
	    [ record['bucket'], record['key'], record['value'],
		{ 'etag': record['_etag'] } ]
	], {}, function () { barrier.done(record['key']); });
};


/*
 * Job, task, stream, and group management
 */


/*
 * Move a newly-dispatched record to the "accepted" state.
 */
mAgent.prototype.taskAccept = function (record, barrier)
{
	/*
	 * There's no way we should already know about this task because we
	 * don't do anything with "dispatched" tasks except through this
	 * "create" code path, and we avoid requesting more dispatched tasks
	 * until we've finished updating the state of previous ones.
	 */
	var taskid = record['value']['taskId'];
	mod_assert.ok(!this.ma_tasks.hasOwnProperty(taskid));

	/*
	 * We don't actually do anything here until/unless the Moray update
	 * succeeds.  That way if we fail with EtagConflict, we can ignore the
	 * error and just deal with this task the next time around (as though
	 * we'd never seen it before).
	 *
	 * Although the task could have timeCancelled set, we let the normal
	 * flow handle that: it'll take an extra round trip, but the resulting
	 * code is simpler.
	 */
	var agent = this;
	barrier.start(taskid);
	record['value']['state'] = 'accepted';
	record['value']['timeAccepted'] = mod_jsprim.iso8601(Date.now());
	this.ma_bus.putBatch([
	    [ record['bucket'], record['key'], record['value'],
	      { 'etag': record['_etag'] } ] ],
	    {}, function (err) {
		barrier.done(taskid);

		if (!err)
			agent.taskAcceptFinish(record);
	    });
};

/*
 * Finish the task accept process by creating a new task object for the given
 * record.  This creates a task group and job record if needed.
 */
mAgent.prototype.taskAcceptFinish = function (record)
{
	var task, jobid, pi, groupid;
	var job, group;

	task = new maTask(record, this);
	this.ma_tasks[record['value']['taskId']] = task;
	this.ma_ntasks++;

	jobid = record['value']['jobId'];
	pi = record['value']['phaseNum'];
	groupid = jobid + '/' + pi;
	if (this.ma_taskgroups.hasOwnProperty(groupid)) {
		this.taskGroupAppendTask(this.ma_taskgroups[groupid], task);
		return;
	}

	if (!this.ma_jobs.hasOwnProperty(jobid))
		this.jobCreate(jobid);
	job = this.ma_jobs[jobid];
	job.j_groups[groupid] = true;

	group = new maTaskGroup(groupid, job, pi,
	    this.ma_log.child({ 'component': 'Group-' + groupid }));
	group.g_log.debug('created group for job "%s" phase %d ' +
	    '(from task "%s")', jobid, pi, task.t_id);
	this.ma_taskgroups[groupid] = group;
	this.taskGroupAppendTask(group, task);

	if (job.j_record)
		this.taskGroupSetJob(group, job);
};

/*
 * Create a new job object for the given jobid.  At this point we don't know
 * anything about it except for its id, but we'll fetch the details here.
 */
mAgent.prototype.jobCreate = function (jobid)
{
	var agent = this;
	var job, bucket, query;

	job = new maJob(jobid);
	this.ma_jobs[jobid] = job;

	bucket = this.ma_buckets['job'];
	query = maQueries.aqJob['query'].bind(null, jobid);
	this.ma_bus.oneshot(bucket, query, this.ma_bus_options,
	    function (record) {
		job.j_record = record;

		mod_jsprim.forEachKey(job.j_groups, function (groupid) {
			var group = agent.ma_taskgroups[groupid];
			agent.taskGroupSetJob(group, job);
		});
	    });
};

/*
 * Returns the slop memory required for each stream in this task group.
 */
mAgent.prototype.taskGroupMemSlop = function (group)
{
	if (group.g_phase['memory'] === undefined)
		return (0);

	return (Math.max(0, group.g_phase['memory'] -
	    this.ma_conf['zoneDefaults']['max_physical_memory']));
};

/*
 * Returns the slop disk required for each stream in this task group.
 */
mAgent.prototype.taskGroupDiskSlop = function (group)
{
	if (group.g_phase['disk'] === undefined)
		return (0);

	return (Math.max(0, group.g_phase['disk'] -
	    this.ma_conf['zoneDefaults']['quota']));
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
	this.ma_dtrace.fire('task-enqueued',
	    function () { return ([ group.g_jobid, task.t_id,
	        task.t_record['value'] ]); });
	this.schedGroup(group);
};

/*
 * Fill in the task group with details from the job record, including the phase
 * execution details, the owner's login, the effective user's credentials, and
 * so on.  This is invoked exactly once per group, either upon creation (if the
 * job record is already available) or when the job record is fetched.
 */
mAgent.prototype.taskGroupSetJob = function (group, job)
{
	mod_assert.ok(job.j_record !== undefined);
	mod_assert.ok(job.j_groups.hasOwnProperty(group.g_groupid));
	mod_assert.equal(group.g_state, maTaskGroup.TASKGROUP_S_INIT);

	group.g_token = job.j_record['value']['authToken'];
	group.g_phase = job.j_record['value']['phases'][group.g_phasei];
	group.g_map_keys = group.g_phase['type'] == 'map' ||
	    group.g_phase['type'] == 'storage-map';
	group.g_multikey = group.g_phase['type'] == 'reduce';
	group.g_login = job.j_record['value']['auth']['login'];
	group.g_owner = job.j_record['value']['owner'];
	group.g_intermediate = group.g_phasei <
	    job.j_record['value']['phases'].length - 1;
	group.g_checkpoint = job.j_record['value']['options'] &&
	    job.j_record['value']['options']['frequentCheckpoint'];
	group.g_poolid = this.imageLookup(group.g_phase['image']);
	group.g_domainid = job.j_record['value']['worker'];

	if (group.g_poolid === null) {
		group.g_log.warn(maNoImageError['message']);
		this.taskGroupError(group, maNoImageError);
		return;
	}

	mod_assert.ok(this.ma_taskgroups_queued.hasOwnProperty(group.g_poolid));

	if (!group.g_multikey ||
	    group.g_tasks[0].t_xinput.length > 0 ||
	    group.g_tasks[0].t_ninputs !== undefined) {
		group.g_state = maTaskGroup.TASKGROUP_S_QUEUED;
		this.ma_taskgroups_queued[group.g_poolid].push(group);
		this.schedIdlePool(group.g_poolid);
	}
};

/*
 * Append a new input object to the given reduce task.
 */
mAgent.prototype.taskAppendKey = function (task, keyinfo)
{
	if (this.taskReadAllRecords(task)) {
		this.ma_log.error('dropping key for task "%s" because ' +
		    'we\'ve already read all %d keys', task.t_id,
		    task.t_ninputs);
		return;
	}

	this.ma_log.debug('task "%s": appending key', task.t_id, keyinfo);
	task.t_xinput.push(keyinfo);
	task.t_nread++;

	var group = task.t_group;
	this.ma_dtrace.fire('taskinput-enqueued', function () {
	    return ([ group.g_jobid, task.t_id, task.t_record['value'],
	        keyinfo ]);
	});

	if (group.g_poolid && group.g_state == maTaskGroup.TASKGROUP_S_INIT) {
		group.g_state = maTaskGroup.TASKGROUP_S_QUEUED;
		this.ma_taskgroups_queued[group.g_poolid].push(group);
		this.schedIdle(group.g_poolid);
		return;
	}

	if (task.t_stream !== undefined && task.t_xinput.length == 1)
		this.zoneWakeup(this.ma_zones[task.t_stream.s_machine]);

	if (task.t_done && this.taskReadAllRecords(task))
		this.taskAsyncDone(task);
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

	if (!stream.s_group.g_multikey)
		return (true);

	var task = stream.s_task;
	return (task.t_xinput.length > 0 || this.taskReadAllRecords(task));
};

/*
 * Returns kstats for the stream's zone.  This may only be called while the zone
 * is known to be running.
 */
mAgent.prototype.streamStats = function (stream)
{
	var zonename = stream.s_machine;
	var zone = this.ma_zones[zonename];
	var zoneid = zone.zoneid();
	var rv = {};
	var k, r, j, filter, stats, err;

	for (k in this.ma_kstat_readers) {
		r = this.ma_kstat_readers[k];

		if (k != 'vnic0')
			filter = { 'instance': zoneid };
		else
			filter = { 'name': 'z' + zoneid + '_net0' };

		stats = r.read(filter);

		mod_assert.ok(Array.isArray(stats));
		if (stats.length != 1) {
			err = new VError('failed to read kstat "%s" for ' +
			    'zoneid %d (zonename "%s"): got %d instead of 1',
			    k, zoneid, zonename, stats.length);
			stream.s_log.error(err);
			return (err);
		}

		if (stats[0]['data']['zonename'] != zonename) {
			err = new VError('failed to read kstat "%s" for ' +
			    'zoneid %d (zonename "%s"): got stat for zone ' +
			    '"%s" instead', k, zoneid, zonename,
			    stats[0]['data']['zonename']);
			stream.s_log.error(err);
			return (err);
		}

		rv[k] = stats[0];

		for (j in rv[k]['data']) {
			if (maKstatFieldsPrune[j])
				delete (rv[k]['data'][j]);
		}
	}

	return (rv);
};

mAgent.prototype.meter = function (eventname, record)
{
	record['event'] = eventname;
	record['computeAudit'] = true;
	record['serverUuid'] = this.ma_server_uuid;
	record['mantaComputeId'] = this.ma_manta_compute_id;
	this.ma_log.info(record);
};

mAgent.prototype.taskMeter = function (task, eventname, record)
{
	record['jobId'] = task.t_group.g_jobid;
	record['owner'] = task.t_group.g_owner;
	record['phase'] = task.t_group.g_phasei;
	record['taskId'] = task.t_id;
	if (record.hasOwnProperty('metricsCumul'))
		record['metricsCumul']['config_disk'] =
		    this.taskGroupDiskSlop(task.t_group) +
		    this.ma_conf['zoneDefaults']['quota'];
	this.meter(eventname, record);
};

/*
 * Returns whether the given stream stats indicates any anonymous allocation
 * failures occurred.
 */
mAgent.prototype.streamSawAllocFailures = function (stream)
{
	var stats = stream.s_stats.stats();
	return ((!(stats instanceof Error)) &&
	    stats['memory']['data']['anon_alloc_fail'] > 0);
};

/*
 * Cancel the execution of the given task.  This just deals with the mechanics
 * of stopping a currently running task or unscheduling a queued one.  It's
 * assumed that the task record has already been updated to indicate that the
 * task is now done.
 */
mAgent.prototype.taskCancel = function (task)
{
	mod_assert.ok(task.t_record['value']['result'] !== undefined ||
	    task.t_abandoned);

	if (task.t_stream !== undefined) {
		this.taskStreamAbort(task.t_stream, {
		    'code': EM_INTERNAL,
		    'message': 'internal abort',
		    'messageInternal': 'task cancelled externally'
		});
		return;
	}

	/*
	 * The task never started running.  Remove it from its group, and remove
	 * the group if the group itself not running.
	 */
	var group = task.t_group;
	var i = group.g_tasks.indexOf(task);
	if (i != -1) {
		group.g_tasks.splice(i, 1);
		group.g_log.debug('task "%s": removed from queue', task.t_id);
	}

	this.taskRemove(task);

	if (group.g_nstreams === 0 && group.g_tasks.length === 0) {
		if (group.g_state == maTaskGroup.TASKGROUP_S_QUEUED)
			this.taskGroupRemoveQueued(group);
		this.taskGroupRemove(group);
	}
};

/*
 * Like taskCancel, except that we will not attempt to write any more updates
 * for this task because it has already been stolen from us.
 */
mAgent.prototype.taskAbandon = function (err, task)
{
	if (task.t_abandoned)
		return;

	this.ma_log.error(err, 'abandoning task "%s"', task.t_id);
	task.t_abandoned = true;

	if (task.t_record['value']['result'] === undefined)
		this.taskCancel(task);
};

/*
 * For a given task which is currently running and which has just now been
 * marked "failed" or "ok", remove it from the system and advance the stream to
 * the next task.  This function deals with the mechanics of stopping the task
 * and advancing the stream and assumes that the caller has already updated the
 * task's record to indicate that it's no longer running.
 *
 * NOTE: This function must *only* be invoked from contexts in which
 * taskStreamAdvance may be invoked.  See the comment on taskStreamAdvance.
 */
mAgent.prototype.taskDoneRunning = function (task, callback)
{
	mod_assert.ok(task.t_stream !== undefined);
	mod_assert.ok(task.t_stream.s_task == task);
	mod_assert.ok(task.t_record['value']['result'] !== undefined ||
	    task.t_abandoned);

	var group, stream;

	group = task.t_group;
	group.g_nrunning--;

	stream = task.t_stream;
	task.t_stream = undefined;
	this.taskRemove(task);
	this.taskStreamAdvance(stream, callback);
};

/*
 * Update a task's record to indicate that it has failed with the given error.
 * The error object should contain "code" and "message" fields, as well as
 * "messageInternal" if the "code" is InternalError.  The error object may be
 * omitted for cases where we don't actually need to write out an error (e.g.,
 * when the task has been cancelled), but we still need to mark the state
 * "done").
 */
mAgent.prototype.taskMarkFailed = function (task, when, error, barrier)
{
	var uuid, errvalue, extra;

	this.ma_counters['tasks_failed']++;

	if (task.t_stream === undefined)
		task.t_record['value']['nOutputs'] = 0;
	else
		task.t_record['value']['nOutputs'] = task.t_stream.s_noutput;

	task.t_record['value']['state'] = 'done';
	task.t_record['value']['result'] = 'fail';
	task.t_record['value']['timeDone'] = mod_jsprim.iso8601(when);

	if (error !== undefined) {
		task.t_record['value']['errorCode'] = error['code'];

		uuid = mod_uuid.v4();

		errvalue = {
		    'errorId': uuid,
		    'jobId': task.t_group.g_jobid,
		    'domain': task.t_group.g_domainid,
		    'phaseNum': task.t_group.g_phasei,
		    'errorCode': error['code'],
		    'errorMessage': error['message'],
		    'errorMessageInternal': error['messageInternal'],

		    'input': task.t_record['value']['input'],
		    'p0input': task.t_record['value']['p0input'],

		    'taskId': task.t_id,
		    'server': this.ma_conf['instanceUuid'],
		    'mantaComputeId': this.ma_conf['mantaComputeId'],
		    'machine': task.t_stream ?
			task.t_stream.s_machine : undefined,
		    'stderr': task.t_stream ?
			task.t_stream.s_stderr : undefined,
		    'core': task.t_stream ?
			task.t_stream.s_core : undefined,

		    'prevRecordType': 'task',
		    'prevRecordId': task.t_id
		};

		extra = [ this.ma_buckets['error'], uuid, errvalue ];
	}

	this.taskDirty(task, extra, barrier);

	if (task.t_group.g_multikey && !this.taskReadAllRecords(task)) {
		this.ma_log.warn('reduce task "%s": zone released before ' +
		    'task has read all of its records', task.t_id);
		this.ma_tasks_failed[task.t_id] = true;
	}
};

/*
 * Update a task's record to indicate that it has successfully completed on
 * machine "machine" with "nout" output keys.
 */
mAgent.prototype.taskMarkOk = function (task, machine, nout, when)
{
	this.ma_counters['tasks_committed']++;

	task.t_record['value']['nOutputs'] = nout;
	task.t_record['value']['state'] = 'done';
	task.t_record['value']['result'] = 'ok';
	task.t_record['value']['timeDone'] = mod_jsprim.iso8601(when);
	task.t_record['value']['machine'] = machine;

	/*
	 * We can't want save this task with result == 'ok' until/unless we've
	 * written out all output tasks.
	 */
	if (task.t_nout_pending === 0)
		this.taskDirty(task);
	else
		this.ma_log.debug('waiting for task\'s outputs to save',
		    task.t_id);
};

/*
 * Schedule an update for the given task's Moray record.  A related record may
 * also be specified, in which case the two will be written atomically (as when
 * a task is written with result "fail" along with an "error" record).
 */
mAgent.prototype.taskDirty = function (task, related, barrier)
{
	var agent = this;
	var records;

	records = [
	    [ this.ma_buckets['task'], task.t_id,
	      task.t_record['value'], { 'etag': task.t_record['_etag'] } ] ];
	if (related !== undefined)
		records.push(related);

	this.ma_bus.putBatch(records, {
	    'retryConflict': function (oldrec, newrec) {
		return (mod_bus.mergeRecords([
			'nInputs',
			'timeCancelled',
			'timeDispatchDone',
			'timeInputDone'
		    ], [
			'machine',
			'nOutputs',
			'result',
			'errorCode',
			'state',
			'timeInputDoneRead',
			'timeAccepted',
			'timeStarted',
			'timeDone'
		    ], oldrec['value'], newrec['value']));
	    }
	}, function (err) {
		if (err)
			agent.taskAbandon(err, task);

		if (barrier !== undefined)
			barrier.done(task.t_id);
	});
};

/*
 * Emit an output object from the given task.
 */
mAgent.prototype.taskEmitOutput = function (stream, task, iostream, key,
    reducer, callback)
{
	var agent = this;

	if (task === undefined || stream.s_task != task) {
		stream.s_log.error('stream has moved on by the time %s key ' +
		    '"%s" was finished writing', iostream, key);
		if (callback)
			callback();
		return;
	}

	this.ma_dtrace.fire('taskoutput-emitted', function () {
	    return ([ stream.s_group.g_jobid, task.t_id, task.t_record['value'],
	        iostream, key ]);
	});

	if (iostream == 'stderr') {
		stream.s_stderr = key;
		if (callback)
			callback();
		return;
	}

	if (iostream == 'core') {
		stream.s_core = key;
		if (callback)
			callback();
		return;
	}

	++stream.s_noutput;
	++task.t_nout_pending;
	this.ma_bus.putBatch([ [
	    this.ma_buckets['taskoutput'], mod_uuid.v4(), {
		'jobId': task.t_record['value']['jobId'],
		'taskId': task.t_record['value']['taskId'],
		'domain': task.t_record['value']['domain'],
		'phaseNum': task.t_record['value']['phaseNum'],
		'intermediate': stream.s_group.g_intermediate,
		'output': key,
		'input': task.t_record['value']['input'],
		'p0input': task.t_record['value']['p0input'],
		'rIdx': reducer,
		'timeCreated': mod_jsprim.iso8601(Date.now())
	    } ] ], {}, function () {
		if (--task.t_nout_pending === 0 &&
		    task.t_record['value']['result'] == 'ok')
			agent.taskDirty(task);
		if (callback)
			callback();
	    });
};

/*
 * See maTaskApiCommit.
 */
mAgent.prototype.taskCommit = function (stream, body, now, callback)
{
	var task = stream.s_task;
	var group = stream.s_group;
	var nkeys;

	if (body['key']) {
		if ((!group.g_multikey &&
		    task.t_record['value']['input'] != body['key']) ||
		    (group.g_multikey && (task.t_xinput.length === 0 ||
		     task.t_xinput[0]['input'] != body['key']))) {
			callback(new mod_restify.InvalidArgumentError(
			    sprintf('"%s" is not the current object',
			    body['key'])));
			return;
		}

		/*
		 * Reduce tasks may commit either a set of keys (identified by
		 * "key" and "nkeys"), in which case the task isn't actually
		 * done yet (even if these were the last keys in it), or no
		 * keys, which indicates that it's done with everything.  To
		 * make things more confusing, they can report success before
		 * reading everything, in which case this agent continues
		 * process taskinputs and doesn't repurpose the zone until
		 * that's done.
		 *
		 * Map tasks fall through, since any commit indicates
		 * completion.
		 */
		if (group.g_multikey) {
			nkeys = parseInt(body['nkeys'], 10);
			if (isNaN(nkeys)) {
				callback(new mod_restify.InvalidArgumentError(
				    '"nkeys" must be a valid integer'));
			} else {
				task.t_xinput = task.t_xinput.slice(nkeys);
				callback();
			}
			return;
		}
	}

	/*
	 * A commit with no key specified (or a map commit that fell through
	 * above) means that the task has finished successfully.  Map tasks are
	 * necessarily done, as are reduce tasks that have read all inputs.  But
	 * reduce tasks that haven't finished reading all inputs will be left in
	 * the zone until we finish reading them all.  This greatly simplifies
	 * the processing of this presumably uncommon case, since otherwise we'd
	 * need to invalidate newly written taskinput records.
	 */
	if (!group.g_multikey || this.taskReadAllRecords(task)) {
		this.taskMarkOk(task, stream.s_machine, stream.s_noutput, now);
		this.taskDoneRunning(task, callback);
	} else {
		task.t_stream.s_log.info('reduce task "%s" finished before ' +
		    'reading all of its input', task.t_id);
		task.t_done = true;
		callback();
	}
};

/*
 * Invoked after a reduce task that previously completed before reading all of
 * its inputs has finally read all of its inputs.  We can finally mark it done.
 */
mAgent.prototype.taskAsyncDone = function (task)
{
	mod_assert.ok(task.t_done);
	mod_assert.ok(task.t_group.g_multikey);
	mod_assert.ok(task.t_stream !== undefined);
	mod_assert.ok(task.t_stream.s_task == task);

	if (task.t_cancelled || task.t_abandoned)
		return;

	var agent = this;
	var stream = task.t_stream;
	var now = new Date();

	stream.s_log.info('reduce task "%s" asyncDone', task.t_id);
	stream.s_rqqueue.push(function (callback) {
		if (task.t_async_error !== undefined)
			agent.taskMarkFailed(task, now, task.t_async_error);
		else
			agent.taskMarkOk(task, stream.s_machine,
			    stream.s_noutput, now);
		agent.taskDoneRunning(task, callback);
	}, function () {});
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
	this.ma_ntasks--;
};

/*
 * Abort the given task stream with the specified error.  This cancels any
 * currently running task and triggers a stream advance that will cause it to be
 * cleaned up.  (It's expected that the current task, if any, has already been
 * marked failed or does not need to be marked failed.)
 *
 * This function may be invoked from arbitrary contexts because it schedules
 * the abort after any pending taskStreamAdvance operations have completed.
 */
mAgent.prototype.taskStreamAbort = function (stream, error)
{
	var agent = this;

	stream.s_log.warn('issued stream abort', error);
	stream.s_rqqueue.push(function (callback) {
		if (stream.s_state == maTaskStream.TASKSTREAM_S_DONE) {
			stream.s_log.warn('ignoring stream abort because ' +
			    'the stream is already done', error);
			return;
		}

		stream.s_log.warn('executing stream abort', error);
		stream.s_error = error;
		if (stream.s_task !== undefined)
			agent.taskDoneRunning(stream.s_task, callback);
		else
			agent.taskStreamAdvance(stream, callback);
	}, function () {});
};

/*
 * Mark all tasks in a group as failed, as when assets could not be found or the
 * requested image is not available.
 * TODO: If there's a failure initializing the assets for a phase, we currently
 * will keep retrying many times, which is probably a big waste.  We may want to
 * abort the whole job in this case.
 */
mAgent.prototype.taskGroupError = function (group, error)
{
	var agent = this;
	var now = new Date();
	var tasks = group.g_tasks;

	group.g_tasks = [];

	tasks.forEach(function (task) {
		agent.taskMarkFailed(task, now, error);
		agent.taskRemove(task);
	});

	if (group.g_nstreams === 0) {
		if (group.g_state == maTaskGroup.TASKGROUP_S_QUEUED)
			this.taskGroupRemoveQueued(group);
		this.taskGroupRemove(group);
	}
};

/*
 * Dequeue and remove a task group in the QUEUED state.
 */
mAgent.prototype.taskGroupRemoveQueued = function (group)
{
	var i;

	mod_assert.equal(group.g_state, maTaskGroup.TASKGROUP_S_QUEUED);
	mod_assert.equal(group.g_tasks.length, 0);
	mod_assert.ok(mod_jsprim.isEmpty(group.g_streams));
	mod_assert.ok(group.g_nstreams === 0);

	var queue = this.ma_taskgroups_queued[group.g_poolid];
	for (i = 0; i < queue.length; i++) {
		if (queue[i] === group)
			break;
	}

	mod_assert.ok(i < queue.length);
	queue.splice(i, 1);

	group.g_state = maTaskGroup.TASKGROUP_S_INIT;
};

/*
 * Removes all references to this task group.  It's assumed at this point that
 * no tasks currently reference the group and the group is not enqueued to run.
 */
mAgent.prototype.taskGroupRemove = function (group)
{
	mod_assert.ok(group.g_state != maTaskGroup.TASKGROUP_S_QUEUED);
	mod_assert.equal(group.g_tasks.length, 0);
	mod_assert.ok(mod_jsprim.isEmpty(group.g_streams));
	mod_assert.equal(group.g_nstreams, 0);
	mod_assert.equal(group.g_nrunning, 0);
	mod_assert.equal(group.g_idle.length, 0);

	var job = this.ma_jobs[group.g_jobid];
	if (job !== undefined)
		delete (job.j_groups[group.g_groupid]);

	if (this.ma_taskgroups[group.g_groupid] !== undefined) {
		mod_assert.equal(this.ma_taskgroups[group.g_groupid], group);
		delete (this.ma_taskgroups[group.g_groupid]);
	}

	if (job !== undefined &&
	    mod_jsprim.isEmpty(job.j_groups) && !job.j_cancelled)
		delete (this.ma_jobs[job.j_id]);

	group.g_log.info('removed');
};


/*
 * Task scheduling
 */

mAgent.prototype.schedHogsMark = function (poolid, timestamp)
{
	var agent = this;

	mod_jsprim.forEachKey(this.ma_taskgroups, function (_, group) {
		if (group.g_poolid != poolid)
			return;

		/*
		 * If any zones have used up their idle time, clean them up now.
		 */
		var i, stream;
		for (i = 0; i < group.g_idle.length; i++) {
			stream = group.g_idle[i];

			mod_assert.ok(stream.s_idle_start !== undefined);
			mod_assert.ok(stream.s_idle_time !== undefined);

			if (timestamp <=
			    stream.s_idle_start + stream.s_idle_time)
				continue;

			stream.s_log.info('idle time expired');
			group.g_idle.splice(i--, 1);
			agent.taskStreamCleanup(stream);
		}

		var oversched = agent.schedGroupOversched(group);
		if (group.g_hog === undefined && oversched) {
			group.g_log.warn('marking group as hog (pool busy)');
			group.g_hog = timestamp;
		} else if (group.g_hog !== undefined && !oversched) {
			group.g_log.warn('unmarking group as hog (undersched)');
			group.g_hog = undefined;
		}
	});
};

mAgent.prototype.schedHogsClear = function (poolid)
{
	mod_jsprim.forEachKey(this.ma_taskgroups, function (_, group) {
		if (group.g_poolid == poolid && group.g_hog !== undefined) {
			group.g_log.warn('unmarking group as hog ' +
			    '(pool cleared)');
			group.g_hog = undefined;
		}
	});
};

mAgent.prototype.schedHogsTick = function (timestamp)
{
	var agent = this;
	var grace = this.ma_conf['tunables']['timeHogGrace'];
	var steal = this.ma_conf['tunables']['timeHogKill'];

	mod_jsprim.forEachKey(this.ma_zonepools, function (poolid) {
		agent.schedHogsMark(poolid, timestamp);
	});

	mod_jsprim.forEachKey(this.ma_taskgroups, function (_, group) {
		if (group.g_hog === undefined)
			return;

		if (group.g_idle.length === 0) {
			if (timestamp - group.g_hog < grace)
				return;

			if (group.g_hog_last !== undefined &&
			    timestamp - group.g_hog_last < steal)
				return;
		}

		/*
		 * We'll take away as many idle streams as we need to at once,
		 * but we only take away non-idle streams at a rate of once per
		 * "steal" interval.
		 */
		var count, i;
		count = Math.min(agent.schedGroupOversched(group),
		    group.g_idle.length + 1);
		mod_assert.ok(count > 0);
		for (i = 0; i < count; i++)
			agent.schedHogKill(group, timestamp);
	});
};

mAgent.prototype.schedHogKill = function (group, timestamp)
{
	var stream, latest;

	/*
	 * In order to be a hog, it must be over its share.  All groups get a
	 * share of at least one zone, so there must be more than one stream
	 * here.
	 */
	mod_assert.ok(group.g_nstreams > 1);

	/*
	 * We prefer to kill streams in the following order:
	 * (1) a stream which is idle
	 * (2) a stream which is initializing
	 * (3) the stream whose current task has been running for the least
	 *     amount of time
	 */
	if (group.g_idle.length > 0) {
		stream = group.g_idle.shift();
		stream.s_idle_start = undefined;
		stream.s_idle_time = undefined;
	} else {
		mod_jsprim.forEachKey(group.g_streams, function (sid, astream) {
			if (astream.s_task === undefined) {
				stream = astream;
				latest = Infinity;
			} else if (latest === undefined ||
			    astream.s_start > latest) {
				stream = astream;
				latest = astream.s_start;
			}
		});
	}

	mod_assert.ok(stream !== undefined);
	group.g_hog_last = timestamp;
	if (stream.s_task !== undefined) {
		this.taskMarkFailed(stream.s_task, timestamp, maKilledError);
		this.ma_dtrace.fire('task-killed', function () {
			return ([ stream.s_task.t_group.g_jobid,
			    stream.s_task.t_id,
			    stream.s_task.t_record['value'] ]);
		});
		this.taskStreamAbort(stream, maKilledError);
	} else {
		this.taskStreamAbort(stream, {
		    'code': 'NoError',
		    'message': 'killed idle stream'
		});
	}
};

/*
 * Given a task group, determine how many zones this group should be allocated.
 * See the comment at the top of this file for much more detail.
 */
mAgent.prototype.schedGroupShare = function (group)
{
	var ntasks, capacity, nzones;

	/*
	 * The number of running tasks (g_nrunning) is almost the same as the
	 * number of streams (g_nstreams), but during initialization the task
	 * that will be run on some stream may still be on g_tasks, in which
	 * case we would double-count it here.  We also don't want to count idle
	 * streams.
	 */
	ntasks = group.g_nrunning + group.g_tasks.length;
	mod_assert.ok(ntasks <= this.ma_ntasks);
	if (ntasks === 0)
		return (0);

	capacity = this.ma_zonepools[group.g_poolid].unreservedCapacity();
	nzones = Math.floor(capacity * (ntasks / this.ma_ntasks));
	return (Math.min(ntasks, Math.max(1, nzones)));
};

/*
 * Determines how many zones the given task group should give up because it has
 * been allocated more zones than we now want it to have.  In general, we only
 * consider a group overscheduled if zones are in contention; if nobody else has
 * work to do, there's no reason to take zones away from a group.
 */
mAgent.prototype.schedGroupOversched = function (group)
{
	var ntasks, pool, capacity, nzones;

	/*
	 * If there's no work to do on the whole system, then nobody's
	 * overscheduled.  (See above.)
	 */
	if (this.ma_ntasks === 0)
		return (0);

	/*
	 * Since we allow groups to have idle zones when the system is
	 * uncontended, being overscheduled is not the same as having more zones
	 * than your share.  Rather, each group's share is a minimum number of
	 * zones it should have, and that's what the group will get when the
	 * system is saturated.  But a group is not actually overscheduled
	 * unless it has more zones than whatever's left over when all other
	 * groups' minimums are met.
	 *
	 * In order to avoid two equal-share groups thrashing over the last of
	 * an odd number of zones, you actually have to be more than one zone
	 * over your share to be considered overscheduled.
	 */
	ntasks = this.ma_ntasks - (group.g_nrunning + group.g_tasks.length);
	mod_assert.ok(ntasks >= 0 && ntasks <= this.ma_ntasks);

	pool = this.ma_zonepools[group.g_poolid];
	capacity = pool.unreservedCapacity();
	nzones = Math.ceil(capacity * (ntasks / this.ma_ntasks));
	return (Math.max(0, group.g_nstreams - 1 - (capacity - nzones)));
};

/*
 * Dispatches queued task groups to available zones until we run out of work to
 * do it or zones in which to do it.
 */
mAgent.prototype.schedIdle = function ()
{
	var agent = this;
	mod_jsprim.forEachKey(this.ma_zonepools,
	    function (poolid) { agent.schedIdlePool(poolid); });
};

mAgent.prototype.schedIdlePool = function (poolid, zoneadded)
{
	var agent = this;
	var pool = this.ma_zonepools[poolid];
	var startedfull, queue, zone, group;

	startedfull = pool.saturated();
	queue = agent.ma_taskgroups_queued[poolid];

	while (pool.hasZoneReady() && queue.length > 0) {
		group = queue.shift();
		mod_assert.equal(group.g_state,
		    maTaskGroup.TASKGROUP_S_QUEUED);

		if (agent.taskGroupMemSlop(group) >
		    agent.availMemSlop()) {
			group.g_state = maTaskGroup.TASKGROUP_S_INIT;
			agent.taskGroupError(group, maNoMemError);
			continue;
		}

		if (agent.taskGroupDiskSlop(group) >
		    agent.availDiskSlop()) {
			group.g_state = maTaskGroup.TASKGROUP_S_INIT;
			agent.taskGroupError(group, maNoDiskError);
			continue;
		}

		group.g_state = maTaskGroup.TASKGROUP_S_RUNNING;
		zone = agent.ma_zones[pool.zonePick()];
		agent.schedDispatch(group, zone);
	}

	while (!pool.saturated()) {
		/*
		 * This is the expensive part.  Generally, we wouldn't
		 * expect to be executing this loop a lot, since it only
		 * happens when zones switch from an overscheduled task
		 * group to an underscheduled one.  Besides that, this
		 * operation is at worse O(nzones).  But if we spend a
		 * lot of time here, we could cache the results.
		 */
		group = agent.schedPickGroup();
		if (group === undefined)
			break;

		zone = agent.ma_zones[pool.zonePick()];
		agent.schedDispatch(group, zone);
	}

	if (!zoneadded && !startedfull && pool.saturated()) {
		agent.ma_log.warn('pool "%s": now saturated', poolid);
		agent.schedHogsMark(poolid, Date.now());
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

		if (agent.taskGroupMemSlop(group) > agent.availMemSlop())
			return;

		if (agent.taskGroupDiskSlop(group) > agent.availDiskSlop())
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
	if (group.g_tasks.length === 0)
		return;

	if (group.g_state == maTaskGroup.TASKGROUP_S_INIT)
		return;

	var agent, stream, pool, zone;

	if (group.g_idle.length > 0) {
		agent = this;
		stream = group.g_idle.shift();
		stream.s_log.info('taking idle stream for new task');
		stream.s_rqqueue.push(function (callback) {
			agent.taskStreamAdvance(stream, function () {
				callback();
				agent.zoneWakeup(
				    agent.ma_zones[stream.s_machine]);
			});
		});
		return;
	}

	pool = this.ma_zonepools[group.g_poolid];

	if (!pool.hasZoneReady())
		return;

	if (group.g_nstreams > 0 && pool.saturated())
		return;

	if (this.taskGroupMemSlop(group) > this.availMemSlop())
		return;

	if (this.taskGroupDiskSlop(group) > this.availDiskSlop())
		return;

	zone = this.ma_zones[pool.zonePick()];
	this.schedDispatch(group, zone);
	if (pool.saturated()) {
		this.ma_log.warn('pool "%s": now saturated', group.g_poolid);
		this.schedHogsMark(group.g_poolid, Date.now());
	}
};

/*
 * Invoked when scheduling a task group on a zone.  Creates a new stream and
 * sets it running.
 */
mAgent.prototype.schedDispatch = function (group, zone)
{
	var agent = this;
	var memslop, diskslop, streamid, stream;

	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_READY);
	mod_assert.ok(zone.z_taskstream === undefined);
	mod_assert.equal(group.g_state, maTaskGroup.TASKGROUP_S_RUNNING);

	memslop = this.taskGroupMemSlop(group);
	mod_assert.ok(memslop <= this.availMemSlop());

	diskslop = this.taskGroupDiskSlop(group);
	mod_assert.ok(diskslop <= this.availDiskSlop());

	streamid = group.g_groupid + '/' + zone.z_zonename;
	stream = new maTaskStream(this, streamid, group, zone.z_zonename);
	stream.s_state = maTaskStream.TASKSTREAM_S_LOADING;

	agent.ma_dtrace.fire('sched-stream-created', function () {
		return ([ group.g_jobid, '' + group.g_phasei,
		    zone.z_zonename ]);
	});

	zone.z_state = mod_agent_zone.maZone.ZONE_S_BUSY;
	zone.z_taskstream = stream;

	group.g_streams[streamid] = stream;
	group.g_nstreams++;

	this.ma_slopmem_used += memslop;
	this.ma_slopdisk_used += diskslop;

	stream.s_log.info('created stream for job "%s" phase %d (memslop %d, ' +
	    'diskslop %d, group concurrency now %d)', group.g_jobid,
	    group.g_phasei, memslop, diskslop, group.g_nstreams);

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
		    'code': EM_TASKINIT,
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

	if (group.g_phase.hasOwnProperty('memory')) {
		options['max_physical_memory'] = group.g_phase['memory'];
		options['max_swap'] = group.g_phase['memory'];
	}

	if (group.g_phase.hasOwnProperty('disk'))
		options['quota'] = group.g_phase['disk'];

	if (mod_jsprim.isEmpty(options))
		callback();
	else
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
	 * TODO A very large number of assets here could cause us to use lots of
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
	var zone, stats;

	mod_assert.equal(stream.s_state, maTaskStream.TASKSTREAM_S_LOADING);

	stream.s_log.info('starting stream execution');
	agent.ma_counters['streams_dispatched']++;

	zone = agent.ma_zones[stream.s_machine];
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_BUSY);

	stats = stream.s_stats.read();
	if (!(stats instanceof Error) &&
	    stats['memory']['data']['anon_alloc_fail'] > 0)
		stats = new VError(
		    'zone started out with non-zero anon allocation failures');
	if (stats instanceof Error) {
		callback(stats);
		return;
	}

	stream.s_stats.zero(stats);
	zone.z_last_contact = Date.now();

	stream.s_rqqueue.push(function (subcallback) {
		mod_assert.equal(stream.s_state,
		    maTaskStream.TASKSTREAM_S_LOADING);
		stream.s_state = maTaskStream.TASKSTREAM_S_RUNNING;

		agent.taskStreamAdvance(stream, function () {
			subcallback();
			agent.zoneWakeup(zone);
		});
	}, callback);
}

/*
 * Invoked after processing each task (successfully or not) to advance the
 * stream to the next task.  If this task is part of a map group, this operation
 * also updates the zone's hyprlofs mappings.  Although this function operates
 * asynchronously and those asynchronous operations can fail, the "advance"
 * operation itself never fails.  If a hyprlofs operation fails, we determine
 * the scope of the failure (the task or the entire stream) and update the
 * stream state accordingly.  The caller will never get an error in its
 * callback.
 *
 * All calls must be funneled through the task group's work queue to ensure that
 * there is only ever one "advance" operation ongoing for a given task stream.
 */
mAgent.prototype.taskStreamAdvance = function (stream, callback)
{
	var agent = this;
	var stop, now, task, group, taskvalue, zone;
	var rootkeypath, localkeypath;

	mod_assert.ok(!stream.s_pending,
	    'concurrent calls to maTaskStreamAdvance');

	if (stream.s_task !== undefined) {
		task = stream.s_task;
		mod_assert.ok(task.t_record['value']['result'] !== undefined ||
		    task.t_abandoned);
		mod_assert.ok(!this.ma_tasks.hasOwnProperty(task.t_id));
		mod_assert.ok(task.t_stream === undefined);
		stream.s_task = undefined;

		this.taskMeter(task, 'TaskDone', stream.s_stats.checkpoint());
		stream.s_stats.zero();
		this.ma_dtrace.fire('task-done', function () {
		    return ([ task.t_group.g_jobid, task.t_id,
		        task.t_record['value'] ]);
		});
	}

	stop = stream.s_error !== undefined ||
	    this.ma_zones[stream.s_machine].z_quiesced !== undefined ||
	    this.schedGroupOversched(stream.s_group) > 0;

	if (stop || stream.s_group.g_tasks.length === 0) {
		if (stream.s_error &&
		    stream.s_error['code'] == EM_TASKINIT) {
			stream.s_log.warn('failed to initialize stream; ' +
			    'assuming task group failure');
			this.taskGroupError(stream.s_group, stream.s_error);
		}

		if (stop)
			this.taskStreamCleanup(stream);
		else
			this.taskStreamIdle(stream);

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
	stream.s_stderr = undefined;
	stream.s_core = undefined;
	stream.s_start = undefined;
	stream.s_idle_start = undefined;
	stream.s_idle_time = undefined;
	stream.s_failprob = undefined;
	this.taskMeter(task, 'TaskStart', {});
	this.ma_dtrace.fire('task-dispatched', function () {
	    return ([ group.g_jobid, task.t_id, task.t_record['value'] ]);
	});

	task.t_record['value']['timeStarted'] = mod_jsprim.iso8601(now);

	if (!group.g_map_keys) {
		stream.s_start = now;
		callback();
		return;
	}

	taskvalue = task.t_record['value'];

	if (taskvalue['input'][0] != '/') {
		/*
		 * This should be impossible because it would have been caught
		 * by the worker, but we handle it defensively.
		 */
		this.taskMarkFailed(task, now, {
		    'code': EM_INVALIDARGUMENT,
		    'message': 'malformed object name'
		});
		this.taskDoneRunning(task, callback);
		return;
	}

	localkeypath = taskvalue['input'].substr(1);

	if (taskvalue['objectid'] == '/dev/null') {
		rootkeypath = maZeroByteFilename;
	} else {
		rootkeypath = sprintf('/zones/%s/root/manta/%s/%s',
		taskvalue['zonename'], taskvalue['account'],
		taskvalue['objectid']);
	}

	stream.s_pending = true;
	zone = this.ma_zones[stream.s_machine];
	zone.z_hyprlofs.removeAll(function (err) {
		mod_assert.ok(stream.s_pending);
		var error;

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
			error = {
			    'code': EM_INTERNAL,
			    'message': 'internal error',
			    'messageInternal': sprintf(
				'failed to unmap hyprlofs: %s', err.message)
			};
			stream.s_log.error(error['messageInternal']);
			agent.taskMarkFailed(stream.s_task);
			agent.taskStreamAbort(stream, error);
			callback();
			return;
		}

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
			 * user specified an input object that doesn't exist, in
			 * which case we simply fail this task and advance
			 * again.  Even this should be extremely unlikely since
			 * we don't actually remove objects from storage nodes
			 * until at least hours, if not days, after they're
			 * deleted from the metadata tier, so this task would
			 * have to have been queued for quite some time.
			 * Nevertheless, it could happen if a user was allocated
			 * only a single zone, but had many objects to process,
			 * each of which took a very long time.
			 *
			 * Other possibilities for this error include metadata
			 * corruption (i.e., the indexing tier said the object
			 * was here when it isn't) or some persistent problem
			 * adding mappings, either of which would likely require
			 * engineering support.
			 */
			suberr = new VError(suberr, 'failed to load %j from %s',
			    taskvalue, rootkeypath);
			stream.s_log.error(suberr);
			agent.taskMarkFailed(task, new Date(), {
			    'code': EM_RESOURCENOTFOUND,
			    'message': 'failed to load object',
			    'messageInternal': suberr.message
			});
			agent.taskDoneRunning(task, callback);
		});
	});
};

/*
 * Called when the task stream has become idle because there's no more work to
 * do, but is otherwise able to keep processing tasks.  This allows us to hold
 * the stream open for a bit while we wait for more tasks to arrive.
 */
mAgent.prototype.taskStreamIdle = function (stream)
{
	var group = stream.s_group;
	var idletime = Math.floor(Math.random() *
	    (this.ma_zone_idle_max - this.ma_zone_idle_min) +
	    this.ma_zone_idle_min);

	stream.s_log.info('stream idle', idletime);
	if (idletime === 0) {
		this.taskStreamCleanup(stream);
		return;
	}

	stream.s_idle_start = Date.now();
	stream.s_idle_time = idletime;
	group.g_idle.push(stream);
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
	var group, zone;

	stream.s_log.info('stream terminated (state = "%s"), error = ',
	    stream.s_state, stream.s_error || 'no error');
	mod_assert.ok(stream.s_task === undefined);
	stream.s_state = maTaskStream.TASKSTREAM_S_DONE;
	this.ma_counters['streams_done']++;

	group = stream.s_group;
	mod_assert.equal(group.g_streams[stream.s_id], stream);
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
			this.ma_taskgroups_queued[group.g_poolid].push(group);
			process.nextTick(
			    function () { agent.schedIdle(group.g_poolid); });
		}
	}

	zone = this.ma_zones[stream.s_machine];
	mod_assert.equal(zone.z_taskstream, stream);
	zone.z_taskstream = undefined;

	var logpath = mod_path.join(maZoneLogRoot,
	    group.g_jobid + '.' + group.g_phasei + '.' + mod_uuid.v4());
	this.zoneReset(zone, logpath);
};


/*
 * Zone management
 */

/*
 * Given the "image" property specified by a user, return the uuid of the
 * appropriate image to use.
 */
mAgent.prototype.imageLookup = function (image)
{
	/*
	 * We only support the string form of the "image" property today, and
	 * it's validated long before it gets here.
	 */
	var i, imageinfo;

	if (image === undefined) {
		if (this.ma_images_byvers.length === 0)
			return (null);

		return (this.ma_images_byvers[0]);
	}

	mod_assert.equal('string', typeof (image));

	for (i = 0; i < this.ma_images_byvers.length; i++) {
		imageinfo = this.ma_images[this.ma_images_byvers[i]];
		if (mod_semver.satisfies(imageinfo['version'], image))
			return (this.ma_images_byvers[i]);
	}

	return (null);
};

mAgent.prototype.zonesDiscover = function ()
{
	if (this.ma_discovering) {
		/*
		 * If there's already a discovery operation outstanding, don't
		 * start a new one.  Record that another one is wanted to handle
		 * the case where the caller added a new zone that might be
		 * missed by the outstanding discovery.
		 */
		this.ma_discovery_pending = true;
		return;
	}

	var agent = this;
	this.ma_log.info('kicking off zone discovery');
	this.ma_discovery_pending = false;
	this.ma_discovering = true;

	mod_agent_zonepool.discoverZones(this.ma_log, function (err, zones) {
		if (err) {
			agent.zonesDiscoverFini(err);
			return;
		}

		for (var i = 0; i < zones.length; i++) {
			if (!agent.ma_images.hasOwnProperty(
			    zones[i]['image']))
				break;
		}

		if (i == zones.length)
			agent.zonesDiscoverFini(null, zones);
		else
			agent.zonesRefreshImages(function (err2) {
				agent.zonesDiscoverFini(err2, zones);
			});
	});
};

mAgent.prototype.zonesRefreshImages = function (callback)
{
	mod_assert.ok(this.ma_discovering);

	var agent = this;
	mod_agent_zonepool.listImages(this.ma_log, function (err, images) {
		if (err) {
			callback(err);
			return;
		}

		images.forEach(function (imageinfo) {
			agent.ma_images[imageinfo['uuid']] = imageinfo;
		});
		callback();
	});
};

mAgent.prototype.zonesDiscoverFini = function (err, zones)
{
	var agent = this;
	var usedimages;

	this.ma_discovering = null;

	if (err) {
		/*
		 * There are two contexts where this could have
		 * happened: initialization and compute zone deployment.
		 * Blowing up in either case isn't very helpful, and
		 * it's definitely wrong once we're already up.  The
		 * real answer here is that the deployment tools should
		 * time out if the new zone doesn't appear soon enough.
		 */
		this.ma_log.error(err, 'failed to discover zones');
		return;
	}

	/*
	 * By the time we get here, we've identified all of the compute zones
	 * and updated this.ma_images with the name and version of any images
	 * that the compute zones might be using.  Now figure out which images
	 * we've actually got zones for, create any zone pools that don't
	 * already exist, and add any zones that we don't already know about.
	 */
	usedimages = {};
	zones.forEach(function (zoneinfo) {
		usedimages[zoneinfo['image']] = zoneinfo['zonename'];
	});

	mod_jsprim.forEachKey(usedimages, function (image, example_zone) {
		if (!agent.ma_images[image]) {
			/*
			 * This should be impossible since we listed all images
			 * after fetching the list of compute zones.
			 */
			agent.ma_log.error('found zone "%s" with image "%s"' +
			    'but no matching image', example_zone, image);
			return;
		}

		if (!agent.ma_zonepools.hasOwnProperty(image)) {
			agent.ma_taskgroups_queued[image] = [];
			agent.ma_zonepools[image] =
			    new mod_agent_zonepool.ZonePool({
			        'log': agent.ma_log.child({
				    'component': 'Pool-' + image
				}),
				'tunables': agent.ma_conf['tunables']
			    });
		}
	});

	zones.forEach(function (zoneinfo) {
		var zonename = zoneinfo['zonename'];
		var image = zoneinfo['image'];
		if (!agent.ma_zones.hasOwnProperty(zonename) &&
		    agent.ma_zonepools.hasOwnProperty(image))
			agent.zoneAdd(zonename, image);
	});

	/*
	 * Finally, create a list of supported images sorted in order of our
	 * preference for using them.  We prefer newer images first.
	 */
	this.ma_images_byvers = Object.keys(usedimages);
	this.ma_images_byvers.sort(function (a, b) {
		return (mod_semver.rcompare(
		    agent.ma_images[a]['version'],
		    agent.ma_images[b]['version']));
	});

	this.ma_log.info('zone discovery complete');

	if (this.ma_discovery_callback) {
		var callback = this.ma_discovery_callback;
		this.ma_discovery_callback = null;
		callback();
	}

	if (this.ma_discovery_pending)
		this.zonesDiscover();
};

mAgent.prototype.zoneReset = function (zone, logpath)
{
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_BUSY);

	var agent = this;
	var finish, logstream, outstream;

	finish = function () {
		mod_agent_zone.maZoneMakeReady(zone,
		    agent.zoneReady.bind(agent));
		agent.ma_dtrace.fire('zone-reset-start',
		    function () { return ([ zone.z_zonename ]); });
	};

	if (zone.z_quiesced !== undefined) {
		this.zoneDisable(zone, new VError('zone disabled by operator'));
		return;
	}

	if (!maZoneAutoReset) {
		this.zoneDisable(zone, new VError('auto-reset is disabled'));
		return;
	}

	if (zone.z_failed !== undefined) {
		var pool = this.ma_zonepools[zone.z_image];
		var maxdisabled = Math.floor(pool.nzones() *
		    this.ma_conf['tunables']['zoneDisabledMaxPercent'] / 100);

		if (pool.ndisabled() + 1 <= maxdisabled) {
			this.zoneDisable(zone,
			    new VError('zone failed (lackey timed out)'));
			return;
		}

		zone.z_log.error(new VError('zone failed'), 'zone failed, ' +
		    'but cannot remove from service because too many ' +
		    'zones in this pool have been removed already');
	}

	zone.z_state = mod_agent_zone.maZone.ZONE_S_UNINIT;

	if (!maZoneSaveLogs) {
		finish();
		return;
	}

	zone.z_log.info('copying lackey log to "%s"', logpath);
	logstream = mod_agent_zone.maZoneAgentLog(zone);
	outstream = mod_fs.createWriteStream(logpath);
	logstream.pipe(outstream);

	function onLogError(err) {
		zone.z_log.warn(err, 'failed to read log for copy');
		logstream.removeListener('end', onEnd);
		outstream.removeListener('error', onErr);
		outstream.destroy();
		finish();
	}
	logstream.on('error', onLogError);

	function onEnd() {
		outstream.removeListener('error', onErr);
		logstream.removeListener('error', onLogError);
		finish();
	}
	logstream.on('end', onEnd);

	function onErr(err) {
		zone.z_log.warn(err, 'failed to write log copy');
		logstream.removeListener('end', onEnd);
		logstream.removeListener('error', onLogError);
		logstream.destroy();
		finish();
	}
	outstream.on('error', onErr);
};

/*
 * Wakes up any lackey requests currently polling on work to do.
 */
mAgent.prototype.zoneWakeup = function (zone)
{
	var waiters = zone.z_waiters;
	zone.z_waiters = [];
	waiters.forEach(function (w) { w.wakeup(); });
};

/*
 * Invoked as a callback when the given zone transitions to the "ready" state
 * (or fails to do so).
 */
mAgent.prototype.zoneReady = function (zone, err)
{
	var agent = this;
	var oldmem, olddisk, options;

	this.ma_dtrace.fire('zone-reset-done', function () {
		return ([ zone.z_zonename, err ? err.message : '' ]);
	});

	if (err) {
		mod_assert.equal(zone.z_state,
		    mod_agent_zone.maZone.ZONE_S_DISABLED);
		this.zoneDisable(zone, new VError(err, 'zone not ready'));
		return;
	}

	if (zone.z_quiesced !== undefined) {
		this.zoneDisable(zone, new VError('zone disabled by operator'));
		return;
	}

	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_READY);
	oldmem = zone.z_options['max_physical_memory'];
	olddisk = zone.z_options['quota'];
	options = this.ma_conf['zoneDefaults'];
	mod_agent_zone.maZoneSet(zone, options, function (suberr) {
		if (suberr) {
			agent.zoneDisable(zone, new VError(suberr,
			    'failed to set properties'));
			return;
		}

		/*
		 * If this zone was given additional physical memory, reclaim it
		 * now.  Note that we're just giving back whatever we knew the
		 * zone was given; if this is the first time the zone was
		 * readied after an agent crash, we won't know about any extra
		 * memory it had here, but that's okay because we also won't
		 * remember that that memory was allocated in the first place.
		 */
		var giveback;
		giveback = oldmem - options['max_physical_memory'];
		if (giveback > 0) {
			zone.z_log.info('giving back %d MB of mem', giveback);
			mod_assert.ok(giveback <= agent.ma_slopmem_used);
			agent.ma_slopmem_used -= giveback;
		}

		/*
		 * Ditto for disk.
		 */
		giveback = olddisk - options['quota'];
		if (giveback > 0) {
			zone.z_log.info('giving back %d GB of disk', giveback);
			mod_assert.ok(giveback <= agent.ma_slopdisk_used);
			agent.ma_slopdisk_used -= giveback;
		}

		agent.ma_counters['zones_readied']++;
		var pool = agent.ma_zonepools[zone.z_image];
		var startedfull = pool.saturated();
		pool.zoneMarkReady(zone.z_zonename);
		agent.schedIdlePool(zone.z_image, true);
		if (startedfull && !pool.saturated()) {
			agent.ma_log.info('pool "%s": now has capacity',
			    zone.z_image);
			agent.schedHogsClear(zone.z_image);
		}
	});
};

mAgent.prototype.zoneDisable = function (zone, err)
{
	zone.z_state = mod_agent_zone.maZone.ZONE_S_DISABLED;
	this.ma_counters['zones_disabled']++;
	zone.z_log.error(err, 'zone removed from service');

	zone.z_disabled_time = new Date();
	zone.z_disabled_error = err;

	this.ma_zonepools[zone.z_image].zoneMarkDisabled(zone.z_zonename);
	this.schedIdle();
};

mAgent.prototype.zoneEnable = function (zone)
{
	mod_assert.equal(zone.z_state, mod_agent_zone.maZone.ZONE_S_DISABLED);
	zone.z_quiesced = undefined;
	this.ma_zonepools[zone.z_image].zoneMarkEnabled(zone.z_zonename);
	mod_agent_zone.maZoneMakeReady(zone, this.zoneReady.bind(this));
	this.ma_dtrace.fire('zone-reset-start',
	    function () { return ([ zone.z_zonename ]); });
};

mAgent.prototype.zoneRemovePool = function (image)
{
	var agent = this;
	var pool, groups, i;

	this.ma_log.info('removing zone pool "%s"', image);

	mod_assert.ok(this.ma_zonepools.hasOwnProperty(image));
	pool = this.ma_zonepools[image];
	mod_assert.ok(pool.nzones() === 0);
	groups = this.ma_taskgroups_queued[image];
	i = this.ma_images_byvers.indexOf(image);
	mod_assert.ok(i !== -1);

	delete (this.ma_zonepools[image]);
	delete (this.ma_taskgroups_queued[image]);
	this.ma_images_byvers.splice(i, 1);
	groups.forEach(function (group) {
		agent.taskGroupError(group, maNoImageError);
	});
};

/*
 * Register a zone for use by this agent.  This zone must already exist -- it's
 * just new to us.  The caller is responsible for ensuring that this zone
 * doesn't already exist in this agent and that the appropriate zone pool has
 * been created already.
 */
mAgent.prototype.zoneAdd = function (zonename, image)
{
	var zone;

	mod_assert.ok(!this.ma_zones.hasOwnProperty(zonename));
	mod_assert.ok(this.ma_zonepools.hasOwnProperty(image));
	this.ma_log.info('adding zone "%s"', zonename);
	zone = mod_agent_zone.maZoneAdd(zonename, this.ma_spawner,
	    this.ma_log.child({ 'component': 'Zone-' + zonename }));
	zone.z_image = image;
	this.ma_zones[zonename] = zone;
	this.ma_counters['zones_added']++;
	this.ma_zonepools[image].zoneAdd(zonename, zone);
	mod_agent_zone.maZoneMakeReady(zone, this.zoneReady.bind(this));
	this.ma_dtrace.fire('zone-reset-start',
	    function () { return ([ zone.z_zonename ]); });
};

/*
 * HTTP entry points
 */

/* POST /zones */
function maHttpZonesAdd(request, response, next)
{
	var zonename, zone;

	zonename = request.query['zonename'];
	if (zonename && maAgent.ma_zones.hasOwnProperty(zonename)) {
		/*
		 * If this request identifies a zone that is currently disabled,
		 * we take this as a request to try to make it ready again.
		 */
		zone = maAgent.ma_zones[zonename];
		if (zone.z_state == mod_agent_zone.maZone.ZONE_S_DISABLED)
			maAgent.zoneEnable(zone);
	} else {
		maAgent.zonesDiscover();
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
	if (zone.z_state == mod_agent_zone.maZone.ZONE_S_READY) {
		mod_assert.ok(zone.z_taskstream === undefined);
		agent.ma_zonepools[zone.z_image].zoneUnready(zonename);
		agent.zoneDisable(zone,
		    new VError('zone disabled by operator'));
	} else if ((zone.z_state == mod_agent_zone.maZone.ZONE_S_BUSY ||
	    zone.z_state == mod_agent_zone.maZone.ZONE_S_UNINIT) &&
	    !zone.z_quiesced) {
		zone.z_quiesced = Date.now();
	}

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

		var pool = agent.ma_zonepools[zone.z_image];

		agent.ma_log.info('removing zone "%s"', zonename);
		pool.zoneRemove(zonename);
		delete (agent.ma_zones[zonename]);
		response.send(204);
		next();

		if (pool.nzones() === 0)
			agent.zoneRemovePool(zone.z_image);
	});
}


/*
 * Task Control API entry points
 */

function maTaskApiSetup(zone, s)
{
	s.pre(mod_restify.pre.sanitizePath());
	s.use(mod_restify.requestLogger());
	s.use(mod_restify.queryParser());

	s.on('uncaughtException', mod_mautil.maRestifyPanic);
	s.on('after', mod_restify.auditLogger({ 'log': zone.z_log }));

	var accept = mod_restify.acceptParser(s.acceptable);

	/*
	 * We prepend the body parser to our own control APIs, but not the ones
	 * that we forward to Manta, since those stream potentially large
	 * amounts of arbitrary data.
	 */
	var init = function (request, response, next) {
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

		maTaskApiTask(request, response, function (_, waiting) {
			if (!waiting)
				next();
		});
	};

	var clearTimer = function (request, response, next) {
		if (request.maTimeout !== undefined)
			clearTimeout(request.maTimeout);

		next();
	};

	var writeContinue = function (request, response, next) {
		if (request.headers['expect'] === '100-continue')
			response.writeContinue();

		next();
	};

	s.get('/my/jobs/task/task',
	    accept, writeContinue, init, setTimer, clearTimer);
	s.post('/my/jobs/task/commit',
	    accept,
	    writeContinue,
	    mod_restify.bodyParser({ 'mapParams': false }),
	    init, maTaskApiCommit);
	s.post('/my/jobs/task/fail',
	    accept,
	    writeContinue,
	    mod_restify.bodyParser({ 'mapParams': false }),
	    init, maTaskApiFail);
	s.post('/my/jobs/task/perturb',
	    accept,
	    writeContinue,
	    mod_restify.bodyParser(),
	    init, maTaskApiPerturb);
	s.post('/my/jobs/task/live',
	    accept, writeContinue, init, maTaskApiLive);

	/*
	 * We proxy the rest of the Manta API under /.
	 */
	var methods = [ 'get', 'put', 'post', 'del', 'head' ];
	methods.forEach(function (method) {
		/* JSSTYLED */
		s[method](/.*/, init, maTaskApiManta);
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
function maTaskApiTask(request, response, callback, nowait)
{
	var agent = maAgent;
	var zone = request.maZone;
	var state;
	var stream = zone.z_taskstream;

	request.log.debug({
		nowait: nowait || false,
		zone: zone.z_zonename
	}, 'maTaskApiTask: entered');

	if (zone.z_state == mod_agent_zone.maZone.ZONE_S_UNINIT) {
		/*
		 * This zone isn't ready.  The only case where this should be
		 * possible is during zone reset after we've closed the HTTP
		 * server, when we could see a request that Node accepted before
		 * we shut down the server.
		 */
		request.log.debug({
			zone: zone.z_zonename
		}, 'maTaskApiTask: zone shutting down');
		response.send(new mod_restify.GoneError('zone shutting down'));
		callback();
		return;
	}

	if (stream !== undefined &&
	    stream.s_task !== undefined &&
	    stream.s_state == maTaskStream.TASKSTREAM_S_RUNNING) {
		maZoneHeartbeat(zone);

		if (!stream.s_task.t_done &&
		    agent.taskStreamHasWork(stream) && !stream.s_pending) {
			state = stream.streamState(agent);
			request.log.debug({
				state: state,
				zone: zone.z_zonename
			}, 'maTaskApiTask: streaming state');
			response.send(state);
			callback();
			return;
		}
	}

	if (nowait || request.query['wait'] != 'true') {
		request.log.debug({
			zone: zone.z_zonename
		}, 'maTaskApiTask: not waiting');
		response.send(204);
		callback();
		return;
	}

	request.log.debug({
		zone: zone.z_zonename
	}, 'maTaskApiTask: waiting for task');
	zone.z_waiters.push(request.maWaiter);
	callback(null, true);
}

function maZoneWaiter(request, response, next)
{
	this.w_request = request;
	this.w_response = response;
	this.w_next = next;
}

maZoneWaiter.prototype.wakeup = function (nowait)
{
	var self = this;
	this.w_request.log.debug({
		nowait: nowait,
		zone: self.w_request.maZone.z_zonename
	}, 'maZoneWaiter: wakeup');

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
	var now;

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
	stream.s_rqqueue.push(function (callback) {
		if (!maTaskApiValidate(request, response, callback))
			return;

		mod_assert.equal(stream.s_state,
		    maTaskStream.TASKSTREAM_S_RUNNING);
		agent.taskCommit(stream, body, now, callback);
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

	if (!maTaskApiValidate(request, response, next))
		return;

	maZoneHeartbeat(zone);

	/*
	 * Make sure the error is valid: it must have string fields for "code"
	 * and "message".
	 */
	if (!body['error'])
		error = {};
	else
		error = body['error'];

	if (!error['code'] || typeof (error['code']) != 'string')
		error['code'] = EM_USERTASK;

	if (!error['message'] || typeof (error['message']) != 'string')
		error['message'] = 'no message given';

	/*
	 * If the zone reports that there were anonymous allocation failures,
	 * append a note to the recorded error message.  This greatly helps
	 * debugging programs which don't gracefully handle allocation failures.
	 */
	if (agent.streamSawAllocFailures(stream))
		error['message'] +=
		    ' (WARNING: ran out of memory during execution)';

	now = new Date();

	/*
	 * If the error code is EM_TASKINIT, we short-circuit this whole path
	 * and just abort the task stream.  This indicates a failure to even set
	 * up the zone (e.g., the user's "init" script failed), and we want to
	 * tear down this stream immediately.
	 */
	if (error['code'] == EM_TASKINIT) {
		stream.s_task.t_record['value']['machine'] = stream.s_machine;
		agent.taskMarkFailed(stream.s_task, now, error);
		agent.taskStreamAbort(stream, error);
		response.send(204);
		next();
		return;
	}

	/*
	 * See comments in maTaskApiCommit.
	 */
	stream.s_rqqueue.push(function (callback) {
		if (!maTaskApiValidate(request, response, callback))
			return;

		mod_assert.equal(stream.s_state,
		    maTaskStream.TASKSTREAM_S_RUNNING);

		var task = stream.s_task;
		task.t_record['value']['machine'] = stream.s_machine;

		if (!task.t_group.g_multikey ||
		    agent.taskReadAllRecords(task)) {
			agent.taskMarkFailed(task, now, error);
			agent.taskDoneRunning(task, callback);
		} else {
			task.t_done = true;
			task.t_async_error = error;
			callback();
		}
	}, function (sent) {
		if (!sent)
			response.send(204);
		next();
	});
}

/*
 * POST /my/jobs/task/perturb: configure upstream API requests to fail with 500
 * errors with some probability for the duration of this task.  This API is
 * undocumented and unstable.
 */
function maTaskApiPerturb(request, response, next)
{
	var p = request.params['p'];
	var stream;

	if (!maTaskApiValidate(request, response, next))
		return;

	if (!isFinite(p)) {
		next(new mod_restify.InvalidArgumentError(
		    '"p" is not a number'));
		return;
	}

	p = parseFloat(p);
	if (isNaN(p) || p < 0 || p > 1) {
		next(new mod_restify.InvalidArgumentError(
		    '"p" is out of range [0, 1]'));
		return;
	}

	stream = request.maZone.z_taskstream;
	stream.s_failprob = p;
	response.send(204);
	next();
}

/*
 * /:key: We proxy the entire Manta API here.  The only thing that we do
 * specially here is note objects that get created so we can "commit" them only
 * if the task successfully completes.
 */
function maTaskApiManta(request, response, next)
{
	var agent = maAgent;
	var zone, key, decode_err, group, stream, type;
	var iostream, proxyargs, reducer, r, donefunc;

	if (!maTaskApiValidate(request, response, next))
		return;

	zone = request.maZone;
	stream = zone.z_taskstream;
	maZoneHeartbeat(zone);

	reducer = request.headers['x-manta-reducer'];
	iostream = request.headers['x-manta-stream'];
	if (iostream != 'stderr' && iostream != 'stdout' && iostream != 'core')
		iostream = undefined;

	if (reducer !== undefined) {
		r = parseInt(reducer, 10);
		if (isNaN(r) || r < 0 || r >= mod_schema.sMaxReducers) {
			next(new mod_restify.InvalidArgumentError(
			    'invalid reducer'));
			return;
		}
	}

	if (stream.s_failprob !== undefined &&
	    Math.random() < stream.s_failprob) {
		next(new mod_restify.InternalServerError('injected failure'));
		return;
	}

	mod_libmanta.normalizeMantaPath({
	    'path': mod_url.parse(request.url).pathname
	}, function (err, nkey) {
		decode_err = err;
		key = nkey;
	});

	if (decode_err) {
		next(new mod_restify.InvalidArgumentError(
		    'bad object name: ' + decode_err.message));
		return;
	}

	mod_assert.ok(key);

	request.pause();
	stream.s_outqueue.push(function (qcallback) {
		if (!maTaskApiValidate(request, response, qcallback))
			return;

		mod_assert.equal(stream.s_state,
		    maTaskStream.TASKSTREAM_S_RUNNING);
		group = stream.s_group;
		type = request.headers['content-type'] || '';

		if (request.method == 'PUT' &&
		    !mod_mautil.isMantaDirectory(type)) {
			/*
			 * mcat uses the x-manta-reference header to indicate
			 * that it's not actually creating a new file, but just
			 * marking it for output for this task.  In that case,
			 * we simply skip the proxy step, and we consider this
			 * request ongoing until the Moray write completes in
			 * order to limit how many of these can be in-flight at
			 * once.  (This is to avoid mcat bombs.)
			 * TODO consider what happens if we crash before saving
			 * this.
			 */
			if (request.headers['x-manta-reference']) {
				agent.taskEmitOutput(stream, stream.s_task,
				    iostream, key, reducer, function () {
					qcallback();
				});

				response.send(204);
				next();
				return;
			}

			/*
			 * Otherwise, we consider the request done as soon as
			 * we get the response from upstream.  We're effectively
			 * assuming that on average, Manta won't be faster than
			 * our own Moray shard.
			 */
			if (iostream !== undefined) {
				donefunc = function () {
					agent.taskEmitOutput(stream,
					    stream.s_task, iostream, key,
					    reducer);
				};
			}
		}

		proxyargs = {
		    'request': request,
		    'response': response,
		    'server': {
			'headers': {
			    'authorization': sprintf('Token %s', group.g_token)
			},
			'host': agent.ma_dns_cache.lookupv4(
			    agent.ma_manta_host),
			'port': agent.ma_manta_port
		    }
		};

		if (iostream === 'stdout' && group.g_intermediate)
			proxyargs['server']['headers'][
			    'x-durability-level'] = 1;

		agent.ma_counters['mantarq_proxy_sent']++;

		mod_mautil.maHttpProxy(proxyargs, function (err, res) {
			if (donefunc && !err && res && res.statusCode < 400)
				donefunc();
			agent.ma_counters['mantarq_proxy_return']++;
			next();
			qcallback();
		});
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
 * Classes
 */

/*
 * We keep track of job records because the job definition specifies how to run
 * individual tasks and so that we can cancel pending tasks when a job is
 * cancelled.
 */
function maJob(jobid)
{
	this.j_id = jobid;
	this.j_record = undefined;		/* last received job record */
	this.j_groups = {};			/* pending task groups */
}

maJob.prototype.kangState = function ()
{
	return ({
	    'jobid': this.j_id,
	    'record': this.j_record,
	    'groups': Object.keys(this.j_groups)
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
	this.t_abandoned = false;		/* task stolen by worker */
	this.t_nout_pending = 0;		/* nr of pending taskoutputs */
	this.t_done = false;			/* task ended */
	this.t_async_error = undefined;		/* for tasks done early */
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
	this.g_login = undefined;	/* authz user's login name */
	this.g_owner = undefined;	/* owning user's uuid */
	this.g_intermediate = undefined; /* outputs are intermediate */
	this.g_checkpoint = false;	/* checkpoint frequently */
	this.g_poolid = undefined;	/* zone pool to pick from (image) */
	this.g_domainid = undefined;	/* worker domain id */

	/* dynamic state */
	this.g_state = maTaskGroup.TASKGROUP_S_INIT;
	this.g_nrunning = 0;		/* number of tasks running */
	this.g_nstreams = 0;		/* number of streams running */
	this.g_streams = {};		/* set of concurrent streams */
	this.g_idle = [];		/* idle streams */
	this.g_tasks = [];		/* queue of tasks to be run */
	this.g_hog = undefined;		/* time since started hogging */
	this.g_hog_last = undefined;	/* last time we stole a zone */
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
	    'intermediate': this.g_intermediate,
	    'mapKeys': this.g_map_keys,
	    'multiKey': this.g_multikey,
	    'ntasks': this.g_tasks.length,
	    'nrunning': this.g_nrunning,
	    'nstreams': this.g_nstreams,
	    'nidle': this.g_idle.length,
	    'hog': this.g_hog,
	    'share': this.g_poolid ? agent.schedGroupShare(this) : 'n/a'
	});
};

function maTaskStream(agent, stream_id, group, machine)
{
	var stream = this;
	var tunables = agent.ma_conf['tunables'];

	/* immutable state */
	this.s_id = stream_id;		/* stream identifier */
	this.s_group = group;		/* maTaskGroup object */
	this.s_machine = machine;	/* assigned zonename */

	/* helper objects */
	this.s_log = group.g_log.child({ 'component': 'Stream-' + stream_id });
	this.s_stats = new mod_meter.StatAccumulator({
	    'read': function () { return (agent.streamStats(stream)); }
	});

	/* dynamic state */
	this.s_state = maTaskStream.TASKSTREAM_S_LOADING;
	this.s_load_assets = undefined;		/* assets vasync cookie */
	this.s_pipeline = undefined;		/* dispatch vasync cookie */
	this.s_error = undefined;		/* stream error */
	this.s_statvfs = false;

	/* used to serialize "commit" and "fail" */
	this.s_pending = false;

	this.s_rqqueue = mod_vasync.queuev({
	    'concurrency': 1,
	    'worker': function rqqfunc(task, callback) { task(callback); }
	});

	this.s_outqueue = mod_vasync.queuev({
	    'concurrency': tunables['maxPendingOutputsPerTask'],
	    'worker': function outqfunc(task, callback) { task(callback); }
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
	this.s_stderr = undefined;		/* current stderr object */
	this.s_core = undefined;		/* current core file */
	this.s_failprob = undefined;		/* p of injecting failure */
	this.s_idle_start = undefined;		/* time stream began idling */
	this.s_idle_time = undefined;		/* max time to idle stream */
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
	    'idle': this.s_idle_start !== undefined,
	    'vasyncAssets': this.s_load_assets,
	    'vasyncPipeline': this.s_pipeline,
	    'error': this.s_error,
	    'pendingUpdate': this.s_pending,
	    'taskStart': this.s_start,
	    'taskNOutput': this.s_noutput,
	    'taskId': this.s_task ? this.s_task.t_id : undefined,
	    'taskNInputs': this.s_task ? this.s_task.t_ninputs: undefined,
	    'taskNRead': this.s_task ? this.s_task.t_nread : undefined
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
	 *    jobId			Job identifier
	 *
	 *    rIdx			Reducer number, if any
	 *
	 *    taskId			Task identifier
	 *
	 *    taskPhase			Full phase description, taken straight
	 *    				from the job record.  This tells the
	 *    				lackey exactly what to execute.
	 *
	 *    login			User Login
	 */
	rv['jobId'] = task.t_record['value']['jobId'];
	rv['rIdx'] = task.t_record['value']['rIdx'];
	rv['taskId'] = task.t_id;
	rv['taskPhase'] = group.g_phase;
	rv['login'] = group.g_login;

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
	 *
	 *    taskCoreBase		Suggested base name for core files
	 */
	outbase = mod_path.join('/', group.g_login, 'jobs',
	    group.g_jobid, 'stor');

	if (group.g_multikey)
		base = 'reduce';
	else if (task.t_record['value']['p0input'])
		base = task.t_record['value']['p0input'];
	else {
		base = task.t_record['value']['input'];

		if (mod_jsprim.startsWith(base, outbase))
			base = base.substr(outbase.length);
	}

	rv['taskErrorKey'] = mod_path.join(outbase,
	    base + '.' + group.g_phasei + '.err.' + task.t_id);
	rv['taskOutputKey'] = mod_path.join(outbase,
	    base + '.' + group.g_phasei + '.' + task.t_id);
	rv['taskOutputBase'] = mod_path.join(outbase,
	    base + '.' + group.g_phasei + '.');
	rv['taskCoreBase'] = mod_path.join(outbase,
	    'cores', '' + group.g_phasei) + '/';

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
		    function (k) { return (k['input']); });
		rv['taskInputDone'] = agent.taskReadAllRecords(task) &&
		    task.t_xinput.length === 0;
		rv['taskInputRemote'] = sprintf('/var/run/.marlin.%s.sock',
		    this.s_machine);
	} else {
		rv['taskInputKeys'] = [ task.t_record['value']['input'] ];
		rv['taskInputDone'] = true;

		if (group.g_map_keys)
			rv['taskInputFile'] = mod_path.join(
			    agent.ma_zones[this.s_machine].z_manta_root,
			    rv['taskInputKeys'][0]);
	}

	return (rv);
};

/*
 * Kang (introspection) entry points
 */

function maKangListTypes()
{
	return ([
	    'agent',
	    'job',
	    'request',
	    'taskgroup',
	    'taskstream',
	    'zone',
	    'zonepool'
	].concat(maAgent.ma_bus.kangListTypes()));
}

function maKangSchema(type)
{
	if (type == 'agent') {
		return ({
		    'summaryFields': [
			'nZones',
			'nZonesDisabled',
			'nZonesReserved',
			'nZonesReady',
			'nGroupsQueued',
			'nTasks'
		    ]
		});
	}

	if (type == 'job') {
		return ({
		    'summaryFields': [ 'jobid' ]
		});
	}

	if (type == 'request') {
		return ({
		    'summaryFields': [ 'id', 'method', 'url', 'time' ]
		});
	}

	if (type == 'taskgroup') {
		return ({
		    'summaryFields': [
			'jobid',
			'phasei',
			'phase',
			'login',
			'multiKey',
			'ntasks',
			'nrunning',
			'nstreams',
			'share'
		    ]
		});
	}

	if (type == 'taskstream') {
		return ({
		    'summaryFields': [
			'jobid',
			'phasei',
			'machine',
			'state',
			'error',
			'pendingUpdate',
			'taskStart',
			'taskNOutput',
			'taskId',
			'taskNInputs',
			'taskNRead'
		    ]
		});
	}

	if (type == 'zone') {
		return ({
		    'summaryFields': [
			'state',
			'sockfd',
			'nwaiters',
			'timeout',
			'pendingCommand'
		    ]
		});
	}

	if (type == 'zonepool')
		return ({ 'summaryFields': [] });

	return (maAgent.ma_bus.kangSchema(type));
}

function maKangListObjects(type)
{
	var agent = maAgent;

	if (type == 'agent')
		return ([ maAgent.ma_conf['instanceUuid'] ]);

	if (type == 'job')
		return (Object.keys(agent.ma_jobs));

	if (type == 'request')
		return (Object.keys(agent.ma_requests));

	if (type == 'taskgroup')
		return (Object.keys(agent.ma_taskgroups));

	if (type == 'taskstream') {
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

	if (type == 'zone')
		return (Object.keys(agent.ma_zones));

	if (type == 'zonepool')
		return (Object.keys(agent.ma_zonepools));

	return (maAgent.ma_bus.kangListObjects(type));
}

function maKangGetObject(type, id)
{
	var agent = maAgent;

	if (type == 'agent') {
		return ({
		    'tickStart': agent.ma_tick_start,
		    'tickDone': agent.ma_tick_done,
		    'nTasks': agent.ma_ntasks,
		    'slopDiskTotal': agent.ma_slopdisk,
		    'slopDiskUsed': agent.ma_slopdisk_used,
		    'slopMemTotal': agent.ma_slopmem,
		    'slopMemUsed': agent.ma_slopmem_used,
		    'images': agent.ma_images_byvers.map(
		         function (u) { return (agent.ma_images[u]); })
		});
	}

	if (type == 'job')
		return (agent.ma_jobs[id].kangState());

	if (type == 'request') {
		var request = agent.ma_requests[id];

		return ({
		    'id': request.id,
		    'method': request.method,
		    'url': request.url,
		    'time': request.time
		});
	}

	if (type == 'taskgroup')
		return (agent.ma_taskgroups[id].kangState(agent));

	if (type == 'taskstream') {
		var parts = id.split('|', 2);
		return (agent.ma_taskgroups[parts[0]].g_streams[
		    parts[1]].kangState());
	}

	if (type == 'zone')
		return (agent.ma_zones[id].httpState());

	if (type == 'zonepool')
		return (agent.ma_zonepools[id].kangState());

	return (agent.ma_bus.kangGetObject(type, id));
}

function maKangStats()
{
	return (maAgent.ma_counters);
}

main();
