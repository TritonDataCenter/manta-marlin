/*
 * lib/worker/worker.js: job worker implementation
 */

/*
 * Each Marlin deployment includes a fleet of job workers that are responsible
 * for managing the distributed execution of Marlin jobs.  The core of each
 * worker is a loop that looks for new and abandoned jobs, divides each job into
 * chunks called tasks, assigns these tasks to individual compute nodes,
 * monitors each node's progress, and collects the results.  While individual
 * workers are not resource-intensive, a fleet is used to support very large
 * numbers of jobs concurrently and to provide increased availability in the
 * face of failures and partitions.
 *
 * Jobs and tasks are represented as records within Moray instances, which are
 * themselves highly available.  At any given time, a job is assigned to at most
 * one worker, and this assignment is stored in the job's record in Moray.
 * Workers do not maintain any state which cannot be reconstructed from the
 * state stored in Moray.  This makes it possible for workers to pick up jobs
 * abandoned by other workers which have failed or become partitioned from the
 * Moray ring.  In order to detect such failures, workers must update job
 * records on a regular basis (even if there's no substantial state change) so
 * that failure to update the job record indicates that a worker has failed.
 *
 * All communication among the workers, compute nodes, and the web tier (through
 * which jobs are submitted and monitored) goes through Moray.
 *
 * Jobs run through the following states:
 *
 *                              +
 *                              | Discover new or abandoned job
 *                              v
 *                         UNASSIGNED
 *                              |
 *                              | Successfully write assignment record
 *                  + --- INITIALIZING
 *                  |           |
 *                  |           | Load current job state
 *                  v           v
 *                  + <----  RUNNING
 *                  |           |
 *                  |           | Last phase completes or
 *                  |           | job encounters fatal failure
 *    Job dropped   |           v
 *    because lock  |       FINISHING
 *    was lost      |           |
 *                  |           | Final save completes
 *                  |           v
 *                  +-------> Remove job from internal state
 *
 *
 * LIFETIME OF A TWO-PHASE MAP JOB
 *
 * (1) Job submission: user submit jobs via the manta front door (muskie).
 *     Muskie creates a "job" record that includes the specification of what to
 *     do in each phase, the effective credentials of the user (including uuid,
 *     login, and authentication token), and various metadata about the job
 *     (time created and the like).
 *
 * (2) Job assignment: some time very shortly later, one of several job workers
 *     finds the job and does an etag-conditional PUT to set "worker" to the
 *     worker's uuid.  Many workers may attempt this, but only one may succeed.
 *     The others forget about the job.
 *
 * (3) Job input submission: the user submits inputs for the job via muskie.
 *     For each input, muskie creates a "jobinput" record that includes the
 *     object's name.  The worker runs through the pipeline described under
 *     "Dispatch pipeline" below, which results in either an error or a task
 *     assigned to a particular agent.
 *
 * (4) Task execution: for each task issued, the corresponding agent picks up
 *     the task and immediately writes back state = "accepted".  Some time
 *     later, the agent actually starts running the task.  As output objects are
 *     emitted (as via "mpipe"), a "taskoutput" record is written out for each
 *     output object.
 *
 * (5) Task completion: when the task completes, the agent updates the task
 *     record with state = "done".  If the task completes successfully, the
 *     number of emitted output objects is recorded, along with result = "ok".
 *     Otherwise, a separate "error" object is included in the same transaction
 *     as the task update, and the task's result = "fail".
 *
 * (6) Task commit: the worker sees the task has been updated with state =
 *     "done".  If the task failed, then the worker updates the task *and* all
 *     taskoutput records with "committed" and "propagated" to true, but "valid"
 *     = false.  If the task completed successfully, the worker writes a
 *     transaction that sets "committed" and "valid" to true on the task *and*
 *     all the taskoutput records.  Note that in both cases, the taskoutput
 *     updates are a server-side mass update, since there could be an
 *     arbitrarily large number of them.
 *
 * (7) Task propagation: for each "taskoutput" record committed but not marked
 *     propagated in (4f), all of step (4) is repeated, except that in (4b), the
 *     worker sets propagated = true on the "taskoutput" record (instead of a
 *     "jobinput" record).
 *
 * (8) Ending job input: some time later, the user indicates to muskie that the
 *     job's input stream has ended.  When this happens, muskie updates the
 *     "job" record to indicate that inputDone = true.
 *
 * (9) Job completion: When the job completes, the worker updates the job record
 *     to set state = "done".  The job is complete only when:
 *
 *         (a) The "job" record indicates that the input stream has ended.
 *
 *         (b) There are zero "jobinput" records with propagated = false.
 *
 *         (c) There are zero "task" records with committed = false (which
 *             includes dispatched, accepted, and done tasks).  This also means
 *             there are zero "taskoutput" records with committed = false.
 *
 *         (d) There are zero "taskoutput" records with committed = true and
 *             propagated = false.
 *
 *
 * CLIENT OPERATIONS
 *
 * Clients need to list outputs and errors.
 *
 * o To list outputs, fetch the job record to determine the number of phases,
 *   and then query moray for taskoutputs for job $jobid, phase $nphases - 1
 *   where committed = true and valid = true.
 *
 * o To list errors, query moray for error objects for job $jobid.
 *
 *
 * JOB CANCELLATION
 *
 * If a job is cancelled while it's still executing:
 *
 * (1) Muskie receives the job cancellation request and updates the "job" record
 *     to set cancelled = true.
 *
 * (2) The worker sees that cancelled = true and state != "done" and writes a
 *     transaction that sets the job's state = "done" as well as cancelled =
 *     "true" for all of the job's tasks with state != "done".  These are done
 *     in a transaction, and the latter is a server-side mass update.
 *
 * (3) The agents process task cancellation as described below.
 *
 *
 * TASK CANCELLATION
 *
 * Individual tasks may be cancelled, either because of a job cancellation or
 * because the worker decides to time out an agent.  In that case, the task's
 * record is updated to set cancelled = true, and the agent immediately forgets
 * about the task.
 *
 *
 * REDUCE TASKS
 *
 * The above example explained how basic map jobs work, but reduce tasks are
 * more complex.  Recall that each phase can have 0 or more reduce tasks, each
 * task can have an arbitrary number of inputs, and an arbitrary number of
 * outputs, and the input stream may be ended before or after the reduce task
 * has already started running.
 *
 * The differences between reduce tasks and map tasks are:
 *
 * (1) For each reduce task in the entire job, a "task" record is written out
 *     for a randomly selected agent when the job is first initialized.  The
 *     agent may start executing the reduce task immediately (though in practice
 *     should wait until there's at least one input object ready).  The
 *     execution will continue (blocking if necessary) until the task's
 *     inputDone is true and all inputs have been read.
 *
 * (2) Instead of dispatching a "task" for each input object, the worker
 *     dispatches a "taskinput" record.  When the agent sees it, instead of
 *     setting state = "accepted", it sets read = true.
 *
 * (3) The worker and agent must coordinate to keep track of when the input for
 *     each reducer is complete.  This condition is exactly equivalent to the
 *     "job done" condition described in (9) above, except it must only be true
 *     for phases 0 ... i - 1, where i = the reducer's phase.
 *
 * The detailed list of queries used for polling can be found in
 * lib/{worker,agent}/queries.js.
 */

var mod_assert = require('assert');
var mod_extsprintf = require('extsprintf');
var mod_fs = require('fs');
var mod_path = require('path');

var mod_libmanta = require('libmanta');
var mod_jsprim = require('jsprim');
var mod_manta = require('manta');
var mod_mkdirp = require('mkdirp');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_bus = require('../bus');
var mod_locator = require('./locator');
var mod_schema = require('../schema');
var mod_mautil = require('../util');
var mod_provider = require('../provider');

var VError = mod_verror.VError;
var sprintf = mod_extsprintf.sprintf;
var mwConfSchema = require('./schema');
var mwProviderDefinition = require('./provider');
var Throttler = mod_mautil.Throttler;

var wQueries = require('./queries');

/* jsl:import ../errors.js */
require('../errors');

/* Public interface */
exports.mwConfSchema = mwConfSchema;
exports.mwWorker = Worker;

/* Static configuration */
var mwTmpfileRoot = '/var/tmp/marlin';

/*
 * Maintains state for a single job.  The logic for this class lives in the
 * worker class.  Arguments include:
 *
 *    conf	worker configuration
 *
 *    log	bunyan-style logger
 *
 *    record	initial job record, as returned from Moray
 */
function JobState(args)
{
	var j = args['record']['value'];
	var tunables = args['conf']['tunables'];

	this.j_job = j;				/* in-moray job record */
	this.j_id = j['jobId'];			/* immutable job id */
	this.j_log = args['log'];		/* job-specific logger */
	this.j_etag = args['record']['_etag'];	/* last known etag */

	this.j_state = 'unassigned';		/* current state (see above) */
	this.j_state_time = new Date();		/* time of last state change */
	this.j_dropped = undefined;		/* time the job was dropped */
	this.j_input_fully_read = false;	/* all inputs have been read */
	this.j_cancelled = j['timeCancelled'];	/* time job was cancelled */

	this.j_nlocates = 0;			/* nr of pending locates */
	this.j_nauths = 0;			/* nr of pending auths */
	this.j_ndeletes = 0;			/* nr of records to delete */

	this.j_phases = j['phases'].map(
	    function (phase) { return (new JobPhase(phase)); });

	this.j_save_throttle = new Throttler(tunables['timeJobSave']);
	this.j_save = new mod_mautil.SaveGeneration();
	this.j_save_barrier = undefined;
	this.j_init_barrier = undefined;
	this.j_last_input = undefined;

	/*
	 * We only poll for job inputs with our own workerid.  Once the workerid
	 * has been assigned, muskie will fill it in for new job inputs, but we
	 * must periodically mark it for existing inputs in case muskie wrote
	 * (or is writing) some inputs before the workerid was assigned.
	 */
	this.j_mark_inputs = new Throttler(tunables['timeMarkInputs']);
}

JobState.prototype.debugState = function ()
{
	return ({
	    'record': this.j_job,
	    'state': this.j_state,
	    'state_time': this.j_state_time,
	    'dropped': this.j_dropped,
	    'input_fully_read': this.j_input_fully_read,
	    'cancelled': this.j_cancelled,
	    'phases': this.j_phases,
	    'save_throttle': this.j_save_throttle,
	    'save_gen': this.j_save,
	    'nlocates': this.j_nlocates,
	    'nauths': this.j_nauths
	});
};

/*
 * Stores runtime state about each phase in a job.
 */
function JobPhase(phase)
{
	this.p_type = phase['type'];		/* phase type from job record */
	this.p_ndispatches = 0;			/* nr of pending dispatches */
	this.p_nuncommitted = undefined;	/* nr of uncommitted tasks */
	this.p_nunpropagated = undefined;	/* nr of unpropagated outputs */
	this.p_nretryneeded = undefined;	/* nr of tasks needing retry */

	if (this.p_type != 'reduce')
		return;

	this.p_reducers = new Array(
	    phase.hasOwnProperty('count') ? phase['count'] : 1);

	for (var i = 0; i < this.p_reducers.length; i++) {
		this.p_reducers[i] = {
		    'r_task': undefined,	/* task record */
		    'r_ninput': 0,		/* total nr of input keys */
		    'r_nissued': 0,		/* nr of inputs issued */
		    'r_done': false		/* task already finished */
		};
	}
}

/*
 * Stores information about a specific task within a job.
 */
function JobTask(jobid, pi, taskid)
{
	this.t_id = taskid;		/* unique task identifier */
	this.t_etag = undefined;	/* last received etag for this task */
	this.t_value = {		/* authoritative task record */
	    'jobId': jobid,
	    'taskId': taskid,
	    'phaseNum': pi
	};
}

/*
 * Agent health management: the worker is responsible for monitoring agent
 * health and handling tasks that have been assigned to agents which have either
 * restarted or just gone out to lunch.  In both cases, we'll use per-job
 * server-side updates to set state=done and result=fail, which will trigger
 * other queries to find these tasks and handle them just like any other errors.
 */
function AgentState(record)
{
	this.a_last = Date.now();
	this.a_record = record;
	this.a_timedout = false;
	this.a_warning = false;
}

/*
 * Worker health management: workers are also responsible for monitoring the
 * health of other workers and taking over for them when they disappear.
 */
function WorkerState(record)
{
	this.ws_last = Date.now();
	this.ws_record = record;
	this.ws_operating = [];
	this.ws_timedout = false;
}

function JobCacheState()
{
	this.jc_checking = false;
	this.jc_timecancelled = undefined;
	this.jc_throttle = undefined;
	this.jc_job = undefined;
}

/*
 * Manages all jobs owned by a single Marlin worker.  Arguments include:
 *
 *    conf		Configuration object matching the above schema.
 *
 *    log		Bunyan-style logger instance
 */
function Worker(args)
{
	var worker = this;
	var conf, error;

	conf = args['conf'];
	mod_assert.ok(args.hasOwnProperty('log'), '"log" is required');
	mod_assert.equal('object', typeof (conf), '"conf" must be an object');

	args['log'].info('worker configuration', conf);
	error = mod_jsprim.validateJsonObject(mwConfSchema, conf);
	if (error) {
		args['log'].fatal(error, 'invalid configuration');
		throw (error);
	}

	/* immutable configuration */
	this.w_uuid = conf['instanceUuid'];
	this.w_conf = mod_jsprim.deepCopy(conf);
	this.w_buckets = this.w_conf['buckets'];
	this.w_max_retries = conf['tunables']['maxTaskRetries'];
	this.w_max_pending_locates = conf['tunables']['maxPendingLocates'];
	this.w_max_pending_auths = conf['tunables']['maxPendingAuths'];
	this.w_max_pending_deletes = conf['tunables']['maxPendingDeletes'];
	this.w_time_tick = conf['tunables']['timeTick'];
	this.w_tmproot = mwTmpfileRoot + '-' + this.w_uuid;
	this.w_names = {};
	this.w_storage_map = {};

	mod_jsprim.forEachKey(this.w_buckets,
	    function (name, bucket) { worker.w_names[bucket] = name; });

	/* helper objects */
	this.w_log = args['log'].child(
	    { 'component': 'Worker-' + this.w_uuid });

	this.w_bus = mod_bus.createBus(conf, {
	    'log': this.w_log.child({ 'component': 'MorayBus' })
	});

	/*
	 * We sort query results by oldest first to avoid starving out small
	 * jobs when large ones are overwhelming us (i.e., generating more
	 * records per poll than we can handle).
	 */
	this.w_bus_options = {
	    'sort': {
	        'attribute': '_mtime',
		'order': 'ASC'
	    },
	    'limit': conf['tunables']['maxRecordsPerQuery'],
	    'timePoll': conf['tunables']['timePoll']
	};

	this.w_locator = mod_locator.createLocator(args['conf'], {
	    'log': this.w_log.child({ 'component': 'MantaLocator' }),
	    'storage_map': this.w_storage_map
	});

	this.w_manta = mod_manta.createClient({
	    'connectTimeout': conf['manta']['connectTimeout'],
	    'url': conf['manta']['url'],
	    'log': this.w_log.child({ 'component': 'MantaClient' }),
	    'sign': mod_mautil.mantaSignNull
	});

	this.w_mahi = undefined;
	this.w_logthrottle = new mod_mautil.EventThrottler(60 * 1000);
	this.w_dtrace = mod_provider.createProvider(mwProviderDefinition);

	/* throttling state */
	this.w_pending_locates = 0;		/* nr of pending locate ops */
	this.w_pending_auths = 0;		/* nr of pending auth ops */
	this.w_pending_deletes = 0;		/* nr of pending delete ops */

	/* control configuration */
	this.w_timeout = undefined;		/* JS timeout handle */
	this.w_ticker = this.tick.bind(this);	/* timeout function */
	this.w_subscrips = {};			/* moray bus subscriptions */
	this.w_init_barrier = undefined;
	this.w_init_pipeline = undefined;
	this.w_init_to = undefined;
	this.w_record_handlers = {
	    'domain': this.onRecordDomain.bind(this),
	    'health': this.onRecordHealth.bind(this),
	    'job': this.onRecordJob.bind(this),
	    'jobinput': this.onRecordJobInput.bind(this),
	    'storage': this.onMantaStorage.bind(this),
	    'task': this.onRecordTask.bind(this),
	    'taskinput': this.onRecordTaskInput.bind(this),
	    'taskoutput': this.onRecordTaskOutput.bind(this)
	};

	/* global dynamic state */
	this.w_jobs = {};			/* all jobs, by jobId */
	this.w_agents = {};			/* known agents */

	/* debugging state */
	this.w_worker_start = undefined;	/* time worker started */
	this.w_tick_start = undefined;		/* time last tick started */
	this.w_tick_done = undefined;		/* time last tick finished */
	this.w_stats = {			/* stat counters */
	    'asgn_failed': 0,			/* failed job assignments */
	    'asgn_restart': 0			/* jobs picked up on restart */
	};

	/*
	 * See dispStart.
	 */
	this.w_auths_out = [];		/* outgoing "auth" dispatches */
	this.w_auths_in = [];		/* incoming "auth" dispatches */
	this.w_locates_out = [];	/* outgoing "locate" dispatches */
	this.w_locates_in = [];		/* incoming "locate" dispatches */

	/*
	 * To avoid issuing concurrent requests for the same information, we
	 * keep track of pending auth and locate requests and "piggy-back"
	 * concurrent requests on each one.
	 */
	this.w_auths_pending = {};	/* pending auth, by user */
	this.w_locates_pending = {};	/* pending locates, by internal key */

	/*
	 * Finally, we also keep track of pending "delete" requests.
	 */
	this.w_deletes_out = [];	/* outgoing "delete" requests */
	this.w_deletes = {};		/* pending "delete" requests */

	/* worker health monitoring */
	this.w_allworkers = {};		/* all known workers */
	this.w_alldomains = {};		/* all known domains */
	this.w_ourdomains = {};		/* domains we're operating */
	this.w_heartbeat = new Throttler(conf['tunables']['timeHeartbeat']);

	/*
	 * In order to deal with a corner case when cancelling jobs and aborting
	 * reduce tasks, we need to keep track of some jobs even after they've
	 * finished.  The value in this object is a JobCacheState object.  See
	 * onRecordForUnknownJob for details.
	 */
	this.w_jobs_cached = {};		/* cache of finished jobs */
}

Worker.prototype.debugState = function ()
{
	return ({
	    'conf': this.w_conf,
	    'nLocs': this.w_pending_locates,
	    'nAuths': this.w_pending_auths,
	    'nDels': this.w_pending_deletes,
	    'tStart': this.w_worker_start,
	    'tTickStart': this.w_tick_start,
	    'tTickDone': this.w_tick_done,
	    'agents': Object.keys(this.w_agents),
	    'workers': Object.keys(this.w_allworkers),
	    'nAuthsIn': this.w_auths_in.length,
	    'nAuthsOut': this.w_auths_out.length,
	    'nLocIn': this.w_locates_in.length,
	    'nLocOut': this.w_locates_out.length
	});
};

/*
 * Start the worker: connect to Moray and start looking for work to do.
 */
Worker.prototype.start = function ()
{
	var worker = this;

	mod_assert.ok(this.w_worker_start === undefined);
	this.w_worker_start = new Date();
	this.w_init_barrier = mod_vasync.barrier();

	this.w_dtrace.enable();
	this.w_log.info('enabled dtrace provider');
	this.w_dtrace.fire('worker-startup',
	    function () { return ([ worker.w_uuid ]); });

	this.initTmp();
	this.initMoray();
	this.initRedis();

	mod_assert.ok(!mod_jsprim.isEmpty(this.w_init_barrier.pending));
	this.w_init_barrier.on('drain', function () {
		clearInterval(worker.w_init_to);
		worker.w_init_to = undefined;

		if (!worker.w_alldomains.hasOwnProperty(worker.w_uuid)) {
			worker.w_log.info('first time starting up');
			worker.w_bus.putBatch([ [
			    worker.w_buckets['domain'],
			    worker.w_uuid,
			    {
			        'domainId': worker.w_uuid,
				'operatedBy': worker.w_uuid,
				'wantTransfer': ''
			    } ] ], {}, function (err) {
				if (err) {
					worker.w_log.fatal(err,
					    'failed to create our domain');
					throw (err);
				}
			    });
		}

		process.nextTick(worker.w_ticker);
		worker.w_log.info('worker started');
	});
};

Worker.prototype.initTmp = function ()
{
	var worker = this;
	var files;

	mod_assert.ok(mod_jsprim.startsWith(this.w_tmproot, '/var/tmp/marlin'));

	this.w_log.info('initializing "%s"', this.w_tmproot);
	mod_mkdirp.sync(this.w_tmproot);

	files = mod_fs.readdirSync(this.w_tmproot);
	files.forEach(function (file) {
		worker.w_log.info('removing "%s"', file);
		mod_fs.unlinkSync(mod_path.join(worker.w_tmproot, file));
	});
};

Worker.prototype.initMoray = function ()
{
	var worker = this;

	this.w_log.info('initializing moray buckets');
	this.w_init_barrier.start('moray');
	this.w_bus.initBuckets(this.w_conf['buckets'], function (err) {
		if (!err) {
			worker.w_log.info('moray buckets okay');
			worker.initPoll();
			worker.w_init_barrier.done('moray');
			return;
		}

		worker.w_log.error(err, 'failed to init buckets ' +
		    '(will retry)');
		worker.w_init_barrier.start('moray-retry-wait');
		worker.w_init_barrier.done('moray');
		setTimeout(function () {
			worker.initMoray();
			worker.w_init_barrier.done('moray-retry-wait');
		}, 10000);
	});
};

Worker.prototype.initRedis = function ()
{
	var worker = this;
	var conf, mahi;

	this.w_log.info('initializing mahi', this.w_conf['auth']);
	this.w_init_barrier.start('mahi');

	conf = {};
	for (var k in this.w_conf['auth'])
		conf[k] = this.w_conf['auth'][k];
	conf['log'] = this.w_log.child({ 'component': 'MahiClient' });

	this.w_mahi = mahi = mod_libmanta.createMahiClient(conf);

	var onError, onClose, onConnect;
	onError = function (err) {
		worker.w_mahi.removeListener('close', onClose);
		worker.w_mahi.removeListener('connect', onConnect);

		worker.w_log.error(err, 'failed to connect to mahi ' +
		    '(will retry)');
		worker.w_init_barrier.start('mahi-retry-wait');
		worker.w_init_barrier.done('mahi');
		setTimeout(function () {
			worker.initRedis();
			worker.w_init_barrier.done('mahi-retry-wait');
		}, 10000);
	};

	onClose = function () {
		worker.w_log.error('mahi connection closed');
	};

	onConnect = function () {
		worker.w_log.info('mahi connected');
		worker.w_mahi.removeListener('error', onError);
		worker.w_init_barrier.done('mahi');
	};

	mahi.once('error', onError);
	mahi.once('close', onClose);
	mahi.once('connect', onConnect);
};

Worker.prototype.initPoll = function ()
{
	var worker = this;

	/*
	 * We fetch agent health, worker health, domain, and manta storage
	 * records before starting other queries so that we don't try to process
	 * jobs with an incomplete set of these records, which could lead to
	 * erroneous transient failures.
	 */
	var queries = [
	    wQueries.wqAgentHealth,
	    wQueries.wqWorkerHealth,
	    wQueries.wqDomains,
	    wQueries.wqMantaStorage
	];

	queries.forEach(function (queryconf) {
		var query, bucket, options, sid;

		bucket = worker.w_buckets[queryconf['bucket']];
		query = queryconf['query'].bind(null, worker.w_conf);
		options = queryconf['options'] ?
		    queryconf['options'](worker.w_conf) :
		    worker.w_bus_options;
		sid = worker.w_bus.subscribe(bucket, query, options,
		    worker.onRecord.bind(worker));
		worker.w_subscrips[queryconf['name']] = sid;
		worker.w_init_barrier.start(queryconf['name']);
		worker.w_bus.fence(sid, function () {
			worker.w_init_barrier.done(queryconf['name']);
		});
	});

	this.w_init_to = setInterval(
	    function () { worker.w_bus.poll(Date.now()); }, 1000);
};

Worker.prototype.domainStart = function (domainid)
{
	var worker = this;
	var qconf, query, bucket, count;

	if (this.w_ourdomains[domainid] == 'pending' ||
	    this.w_ourdomains[domainid] == 'removed') {
		this.w_log.warn(
		    'domain "%s": domain start already pending', domainid);
		return;
	}

	this.w_log.info('domain "%s": starting processing', domainid);
	mod_assert.ok(!this.w_ourdomains.hasOwnProperty(domainid));
	this.w_ourdomains[domainid] = 'pending';

	/*
	 * Load all jobs we already own before subscribing to related records
	 * that might reference those jobs.
	 */
	qconf = wQueries.wqJobsOwned;
	query = qconf['query'].bind(null, this.w_conf, domainid);
	bucket = this.w_buckets[qconf['bucket']];
	count = 0;
	this.w_bus.oneshot(bucket, query, this.w_bus_options,
	    function (record, barrier) {
		worker.jobCreate(record, barrier);
		count++;
	    },
	    function () {
		worker.w_log.info('domain "%s": found %d existing jobs',
		    domainid, count);
		if (worker.w_ourdomains[domainid] != 'pending') {
			worker.w_ourdomains[domainid] = {};
			worker.domainStop(domainid);
			return;
		}

		var queries = [
		    wQueries.wqJobsCancelled,
		    wQueries.wqJobsInputEnded,
		    wQueries.wqJobInputs,
		    wQueries.wqJobTasksDone,
		    wQueries.wqJobTasksNeedingDelete,
		    wQueries.wqJobTaskInputsNeedingDelete,
		    wQueries.wqJobTasksNeedingRetry,
		    wQueries.wqJobTaskInputsNeedingRetry,
		    wQueries.wqJobTaskOutputsUnpropagated
		];

		if (domainid == worker.w_uuid)
			queries.unshift(wQueries.wqJobsCreated);

		worker.w_ourdomains[domainid] = {};
		queries.forEach(function (queryconf) {
			var options = queryconf['options'] ?
			    queryconf['options'](worker.w_conf) :
			    worker.w_bus_options;
			worker.w_ourdomains[domainid][queryconf['name']] =
			    worker.w_bus.subscribe(
			    worker.w_buckets[queryconf['bucket']],
			    queryconf['query'].bind(null, worker.w_conf,
			    domainid), options, worker.onRecord.bind(worker));
		});
	    });
};

Worker.prototype.domainStop = function (domainid)
{
	if (this.w_ourdomains[domainid] == 'pending' ||
	    this.w_ourdomains[domainid] == 'removed') {
		/*
		 * There's a domainStart operation still pending that will
		 * invoke us again when it completes.
		 */
		this.w_ourdomains = 'removed';
		return;
	}

	mod_assert.equal('object', typeof (this.w_ourdomains[domainid]));
	this.w_log.info('domain "%s": stopping processing', domainid);

	var worker = this;
	var k;
	for (k in this.w_ourdomains[domainid]) {
		this.w_bus.unsubscribe(this.w_ourdomains[domainid][k]);
	}

	delete (this.w_ourdomains[domainid]);

	mod_jsprim.forEachKey(this.w_jobs, function (jobid, job) {
		if (job.j_job['worker'] == domainid)
			worker.jobRemove(job);
	});
};

Worker.prototype.domainTakeover = function (domainid, barrier, barrierid)
{
	var worker = this;
	var record, bucket, key, value, options;

	if (!this.w_alldomains.hasOwnProperty(domainid)) {
		this.w_log.warn('domain "%s": would takeover, but ' +
		    'domain was not found', domainid);
		return;
	}

	record = this.w_alldomains[domainid];
	this.w_log.info('domain "%s": taking over (from %s)', domainid,
	    record['value']['operatedBy'] || 'nobody');
	bucket = record['bucket'];
	key = record['key'];
	value = mod_jsprim.deepCopy(record['value']);
	value['operatedBy'] = this.w_uuid;
	options = { 'etag': record['_etag'] };
	barrier.start(barrierid);
	this.w_bus.putBatch([ [ bucket, key, value, options ] ], {},
	    function (err) {
		barrier.done(barrierid);

		if (err) {
			worker.w_log.warn(err,
			    'domain "%s": takeover failed', domainid);
		} else {
			worker.w_log.info('domain "%s": takeover ok', domainid);
			worker.domainStart(domainid);
		}
	    });
};

Worker.prototype.domainFailback = function (domainid, barrier)
{
	var worker = this;
	var record, bucket, key, value, options;

	mod_assert.ok(this.w_ourdomains.hasOwnProperty(domainid));
	mod_assert.ok(this.w_alldomains.hasOwnProperty(domainid));
	record = this.w_alldomains[domainid];
	mod_assert.equal(record['value']['operatedBy'], this.w_uuid);

	this.w_log.info('domain "%s": failing back', domainid);
	worker.domainStop(domainid);

	bucket = record['bucket'];
	key = record['key'];
	value = mod_jsprim.deepCopy(record['value']);
	value['operatedBy'] = value['wantTransfer'];
	value['wantTransfer'] = '';
	options = { 'etag': record['_etag'] };

	barrier.start(domainid);
	this.w_bus.putBatch([ [ bucket, key, value, options ] ], {},
	    function (err) {
		barrier.done(domainid);

		if (err) {
			worker.w_log.error(err, 'domain "%s": failback failed',
			    domainid);
			worker.domainStart(domainid);
		} else {
			worker.w_log.info('domain "%s": failback ok', domainid);
		}
	    });
};

Worker.prototype.domainRequestFailback = function (domainid, barrier)
{
	var worker = this;
	var record, bucket, key, value, options;

	mod_assert.ok(!this.w_ourdomains.hasOwnProperty(domainid));
	mod_assert.ok(this.w_alldomains.hasOwnProperty(domainid));
	record = this.w_alldomains[domainid];
	mod_assert.ok(record['value']['operatedBy'] != this.w_uuid);

	bucket = record['bucket'];
	key = record['key'];
	value = mod_jsprim.deepCopy(record['value']);
	value['wantTransfer'] = this.w_uuid;
	options = { 'etag': record['_etag'] };

	barrier.start(domainid);
	this.w_bus.putBatch([ [ bucket, key, value, options ] ], {},
	    function (err) {
		barrier.done(domainid);

		if (err) {
			worker.w_log.error(err,
			    'domain "%s": failback requests failed', domainid);
			worker.domainStart(domainid);
		} else {
			worker.w_log.info('domain "%s": failback request ok',
			    domainid);
		}
	    });
};

/*
 * The heart of the job worker: this function is invoked periodically to poll
 * Moray, evaluate timeouts, and evaluate job state.
 */
Worker.prototype.tick = function ()
{
	var now, jobid;

	this.w_timeout = undefined;
	this.w_tick_start = new Date();
	now = this.w_tick_start.getTime();

	/* Poll Moray for updates. */
	this.w_bus.poll(now);

	/* Process queued and outgoing messages. */
	this.processQueues();

	/* Check whether each job needs to be saved. */
	for (jobid in this.w_jobs)
		this.jobTick(this.w_jobs[jobid]);

	this.w_logthrottle.flush(now);
	this.heartbeat();

	this.w_tick_done = new Date();
	this.w_timeout = setTimeout(this.w_ticker, this.w_time_tick);
};

Worker.prototype.heartbeat = function ()
{
	var worker = this;

	if (this.w_heartbeat.tooRecent())
		return;

	this.w_log.debug('heartbeat start');
	this.w_heartbeat.start();
	this.w_bus.putBatch([ [ this.w_buckets['health'], this.w_uuid, {
	    'component': 'worker',
	    'instance': this.w_uuid,
	    'generation': this.w_worker_start
	} ] ], {}, function (err) {
		worker.w_heartbeat.done();

		if (!err)
			worker.w_log.debug('heartbeat done');
		else
			worker.w_log.warn(err, 'heartbeat failed');
	});
};

Worker.prototype.onRecord = function (record, barrier)
{
	var schema, error, handler;

	this.w_log.debug('record: "%s" "%s" etag %s',
	    record['bucket'], record['key'], record['_etag']);

	schema = mod_schema.sBktJsonSchemas[this.w_names[record['bucket']]];
	error = mod_jsprim.validateJsonObject(schema, record['value']);
	if (error) {
		if (!this.w_logthrottle.throttle(sprintf(
		    'record %s/%s', record['bucket'], record['key'])))
			this.w_log.warn(error,
			    'onRecord: validation error', record);
		return;
	}

	handler = this.w_record_handlers[this.w_names[record['bucket']]];
	mod_assert.ok(handler !== undefined);
	handler(record, barrier);
};

/*
 * Handle an incoming job record.  There are only four kinds of job records that
 * can get here, corresponding to the four statically configured polls on the
 * "jobs" bucket defined at the top of this file: newly created jobs, abandoned
 * jobs, jobs whose input has been marked complete, and jobs which have been
 * cancelled.  We won't see any other updates to a job, since we're not
 * querying for them.  This may sound brittle, but even if we saw such updates,
 * we wouldn't know how to merge the update with any local changes that we've
 * accumulated, so this really cannot be allowed.  If this were to happen today
 * (as by a bug or errant administrative action), our attempts to update the job
 * would fail with EtagConflict errors, and we'd eventually have to give up.
 */
Worker.prototype.onRecordJob = function (record, barrier)
{
	var job;

	/*
	 * If we don't already know about this job, then this must be new,
	 * abandoned, or one one of ours and we've just crashed.  Attempt to
	 * assign it to ourselves.
	 */
	if (!this.w_jobs.hasOwnProperty(record['value']['jobId'])) {
		this.jobCreate(record, barrier);
		return;
	}

	if ((job = this.jobForRecord(record)) === null)
		return;

	/*
	 * It should be impossible to receive updated job records that match our
	 * own etag because we only poll for records in specific states that
	 * require our attention and we never write records for which we haven't
	 * made as much forward progress as we can.
	 */
	if (job.j_etag == record['_etag'])
		job.j_log.warn('onRecord: found record with up-to-date etag',
		    record);

	/*
	 * If there's a pending save for some other purpose, ignore this update.
	 * We'll catch it again after the other save completes.
	 */
	if (job.j_save_barrier !== undefined) {
		job.j_log.warn('onRecord: skipping job update (already saving)',
		    record);
		return;
	}

	if (record['value']['timeCancelled'] !== undefined &&
	    record['value']['state'] != 'done') {
		job.j_log.info('job cancelled');
		job.j_cancelled = record['value']['timeCancelled'];

		job.j_etag = record['_etag'];
		job.j_job['state'] = 'done';
		job.j_job['timeCancelled'] = job.j_cancelled;
		job.j_job['timeDone'] = mod_jsprim.iso8601(Date.now());

		job.j_save_barrier = barrier;
		job.j_save.markDirty();
		barrier.start('save job ' + job.j_id);

		this.jobTransition(job, 'running', 'finishing');
		return;
	}

	/*
	 * As described above, there are only four possible reasons a record
	 * should be able to get here, and we've already checked for three of
	 * them.  If this isn't a record whose input has just been marked done,
	 * we've messed up something in the way we query for data, and there's
	 * not much we can do in this situation.
	 */
	if (record['value']['timeInputDone'] === undefined ||
	    record['value']['timeInputDoneRead'] !== undefined) {
		if (!this.w_logthrottle.throttle(sprintf(
		    'record %s/%s', record['bucket'], record['key'])))
			job.j_log.warn('onRecord: unknown reason for ' +
			    'record match', record);
		return;
	}

	job.j_log.info('job input completed');
	job.j_etag = record['_etag'];
	job.j_job['timeInputDone'] = record['value']['timeInputDone'];
	job.j_job['timeInputDoneRead'] = mod_jsprim.iso8601(Date.now());

	job.j_save_barrier = barrier;
	job.j_save.markDirty();
	barrier.start('save job ' + job.j_id);
	this.jobInputEnded(job);
};

Worker.prototype.onRecordJobInput = function (record, barrier)
{
	var job, dispatch, phase;

	if ((job = this.jobForRecord(record)) === null)
		return;

	if (job.j_input_fully_read) {
		job.j_log.error('found job input after having ' +
		    'fully read all inputs');
		return;
	}

	mod_assert.ok(record['value']['nextRecordType'] === undefined);
	mod_assert.ok(record['value']['nextRecordId'] === undefined);

	job.j_job['stats']['nInputsRead']++;
	if (job.j_last_input !== undefined)
		job.j_last_input = Date.now();

	dispatch = {
	    'd_id': record['bucket'] + '/' + record['key'],
	    'd_job': job,
	    'd_pi': 0,
	    'd_origin': record,
	    'd_objname': mod_path.normalize(record['value']['input']),
	    'd_p0objname': record['value']['input'],
	    'd_barrier': barrier,
	    'd_ri': undefined,

	    'd_auths': [],
	    'd_locates': [],
	    'd_login': undefined,
	    'd_account': undefined,
	    'd_objname_internal': undefined,
	    'd_objectid': undefined,
	    'd_locations': undefined,
	    'd_time': undefined,
	    'd_error': undefined
	};

	phase = job.j_phases[0];
	if (phase.p_reducers !== undefined)
		dispatch.d_ri = Math.floor(
		    Math.random() * phase.p_reducers.length);

	this.dispStart(dispatch);
};

Worker.prototype.onRecordTask = function (record, barrier)
{
	var job, phase, now;

	if ((job = this.jobForRecord(record)) === null)
		return;

	phase = job.j_phases[record['value']['phaseNum']];
	now = mod_jsprim.iso8601(Date.now());

	if (record['value']['state'] != 'done') {
		if (!this.w_logthrottle.throttle(sprintf(
		    'record %s/%s', record['bucket'], record['key'])))
			job.j_log.error('onRecord: unexpected task', record);
	} else if (record['value']['timeCommitted'] === undefined) {
		this.onRecordTaskCommit(record, barrier, job, phase, now);
	} else if (record['value']['wantRetry'] &&
	    record['value']['timeRetried'] === undefined) {
		this.onRecordTaskRetry(record, barrier, job, phase, now);
	} else if (record['value']['wantInputRemoved'] &&
	    record['value']['timeInputRemoved'] === undefined) {
		this.taskRecordCleanup(record, barrier, job, job.j_job, now);
	} else if (!this.w_logthrottle.throttle(sprintf(
	    'record %s/%s', record['bucket'], record['key']))) {
		job.j_log.error('onRecord: unexpected task', record);
	}
};

Worker.prototype.onRecordTaskCommit = function (record, barrier, job, phase,
    now)
{
	var worker = this;
	var tochanges, allchanges, uuid, errvalue;
	var whichstat, reducer;
	var nretries = 0;
	var nerrors = 0;
	var nunpropagated = 0;
	var ndeletes = 0;
	var njoboutputs = 0;

	job.j_log.debug('committing task "%s"', record['key']);

	tochanges = {};
	tochanges['timeCommitted'] = now;
	record['value']['timeCommitted'] = now;

	allchanges = [];

	if (record['value']['result'] == 'ok') {
		whichstat = 'nTasksCommittedOk';

		if (record['value']['phaseNum'] == job.j_phases.length - 1) {
			njoboutputs = record['value']['nOutputs'];
			tochanges['valid'] = true;
			tochanges['timePropagated'] = now;
		} else {
			nunpropagated = record['value']['nOutputs'];
			phase.p_nunpropagated += nunpropagated;
		}
	} else {
		whichstat = 'nTasksCommittedFail';

		tochanges['timePropagated'] = now;

		/*
		 * On a normal, non-retryable failure, the agent issues an
		 * error.  For abandoned tasks, we're responsible for emitting
		 * the error here and determining whether to retry.  For
		 * retryable tasks, we just set wantRetry.
		 */
		if (record['value']['timeAbandoned'] !== undefined) {
			uuid = mod_uuid.v4();
			record['value']['nextRecordType'] = 'error';
			record['value']['nextRecordId'] = uuid;
			record['value']['wantRetry'] =
			    record['value']['nattempts'] === undefined ||
			    record['value']['nattempts'] <= this.w_max_retries;

			if (record['value']['wantRetry']) {
				phase.p_nretryneeded++;
				nretries = 1;
			} else {
				nerrors = 1;
			}

			errvalue = {
			    'errorId': uuid,
			    'jobId': job.j_id,
			    'domain': job.j_job['worker'],
			    'phaseNum': record['value']['phaseNum'],
			    'errorCode': EM_INTERNAL,
			    'errorMessage': 'internal error',
			    'errorMessageInternal': 'agent timed out',
			    'input': record['value']['input'],
			    'p0input': record['value']['p0input'],
			    'prevRecordType': 'task',
			    'prevRecordId': record['value']['taskId'],
			    'retried': record['value']['wantRetry'],
			    'timeCommitted': now
			};
			allchanges.push([ 'put', this.w_buckets['error'],
			    uuid, errvalue ]);
			this.w_dtrace.fire('error-dispatched',
			    function () { return ([ job.j_id, errvalue ]); });
		} else if (record['value']['errorCode'] &&
		    record['value']['errorCode'] == EM_TASKKILLED &&
		    (record['value']['nattempts'] === undefined ||
		    record['value']['nattempts'] <= this.w_max_retries)) {
			phase.p_nretryneeded++;
			nretries = 1;
			record['value']['wantRetry'] = true;
			allchanges.push([ 'update', this.w_buckets['error'],
			    sprintf('(taskId=%s)', record['value']['taskId']),
			    { 'retried': true, 'timeCommitted': now } ]);
		} else {
			nerrors = 1;
			allchanges.push([ 'update', this.w_buckets['error'],
			    sprintf('(taskId=%s)', record['value']['taskId']),
			    { 'retried': false, 'timeCommitted': now } ]);
		}
	}

	/*
	 * Now's the time to schedule cleanup of anonymous intermediate objects
	 * created by this job.  By "anonymous", we mean objects that the user
	 * has not explicitly given a name to.  (Named objects are not
	 * automatically removed.)  By "intermediate", we mean objects created
	 * by this job that are not final output objects, which we identify as
	 * the inputs to all non-phase-0 tasks.  These may be specified in the
	 * task itself (for map tasks), or they may be specified by a number of
	 * taskinput records (for reduce tasks).
	 *
	 * If this task failed and was retried, there's nothing to do here,
	 * since we're not ready to remove the input objects yet.
	 *
	 * Otherwise, if this is a map task, we mark wantInputRemoved=true when
	 * we save it here.  If this is a reduce task, we do a server-side mass
	 * update to set wantInputRemoved=true on the taskinput records.  We
	 * have separate queries that scan for such records, delete the
	 * corresponding object if it's not anonymous, and then set
	 * timeInputRemoved.  (This has to be two steps for the taskinput case
	 * because we don't have those records at the moment.  Separating these
	 * two steps for the task case allows us to write this commit and
	 * process other commits without blocking on the delete to actually
	 * happen.)
	 */
	if (record['value']['phaseNum'] > 0 &&
	    (record['value']['result'] == 'ok' ||
	    !record['value']['wantRetry'])) {
		if (phase.p_type == 'reduce') {
			if (record['value']['nInputs'] !== undefined) {
				ndeletes = record['value']['nInputs'];
			} else {
				/*
				 * If we didn't end input yet, we don't know
				 * exactly how many taskinput records there are.
				 * There's at least r_nissued, and at most
				 * r_ninput, and the difference are in flight.
				 */
				reducer = phase.p_reducers[
				    record['value']['rIdx']];
				ndeletes = reducer.r_nissued;
			}
			allchanges.push([ 'update', this.w_buckets['taskinput'],
			    sprintf('(taskId=%s)', record['value']['taskId']),
			    { 'wantInputRemoved': true } ]);
		} else {
			ndeletes = 1;
			record['value']['wantInputRemoved'] = true;
		}

		job.j_ndeletes += ndeletes;
	}

	allchanges.push([ 'put', record['bucket'], record['key'],
	    record['value'], { 'etag': record['_etag'] } ]);
	allchanges.push([ 'update', this.w_buckets['taskoutput'],
	      sprintf('(taskId=%s)', record['key']), tochanges ]);

	if (phase.p_type == 'reduce')
		phase.p_reducers[record['value']['rIdx']].r_done = true;

	this.w_dtrace.fire('task-committed', function () {
	    return ([ job.j_id, record['key'], record['value'] ]);
	});

	barrier.start('task ' + record['key']);
	this.w_bus.batch(allchanges, {
	    'retryConflict': function () {
		/*
		 * See the comment in taskRetryReduce.  The bus may think
		 * there's a possibility for a conflict here if we issue this
		 * write while the initial task dispatch write is still pending.
		 * The conflict cannot actually happen, since the etag we're
		 * using here is newer in that case, but the bus has no way to
		 * know that and blows up if it sees us making a dependent write
		 * without handling this error.  (This is hard to see, since it
		 * requires the agent making at least two round-trips before our
		 * write completes, but it has been seen in production.)
		 *
		 * In order to handle other errors, we avoid committing changes
		 * to the job stats above until after we know the write
		 * completes.  We can't avoid changes to the various per-job and
		 * per-phase counters because these need to keep track of the
		 * fact that there's work outstanding or we might end the job
		 * early, so we make those changes and unwind them upon failure.
		 */
		return (new Error('conflict while committing task'));
	    }
	}, function (err) {
		if (!err) {
			job.j_job['stats']['nJobOutputs'] += njoboutputs;
			job.j_job['stats'][whichstat]++;
			job.j_job['stats']['nErrors'] += nerrors;
			job.j_job['stats']['nRetries'] += nretries;
			phase.p_nuncommitted--;
			worker.jobPropagateEnd(job, null);
		} else {
			job.j_log.warn(err, 'unwinding state changes after ' +
			    'error committing task');

			job.j_ndeletes -= ndeletes;
			mod_assert.ok(job.j_ndeletes >= 0);

			phase.p_nunpropagated -= nunpropagated;
			mod_assert.ok(phase.p_nunpropagated >= 0);

			if (nretries > 0) {
				phase.p_nretryneeded--;
				mod_assert.ok(phase.p_nretryneeded > 0);
			}
		}

		barrier.done('task ' + record['key']);
	});
};

Worker.prototype.onRecordTaskRetry = function (record, barrier, job, phase, now)
{
	if (record['value']['input'] === undefined)
		this.taskRetryReduce(record, barrier, job, phase, now);
	else
		this.taskRetryMap(record, barrier, job, phase, now);
};

Worker.prototype.taskRetryReduce = function (record, barrier, job, phase, now)
{
	var worker = this;
	var task = this.taskCreate(job, record['value']['phaseNum'], now);
	var value = task.t_value;
	var instance = mod_jsprim.randElt(Object.keys(this.w_agents));
	var agent = this.w_agents[instance];

	job.j_log.info('retrying reduce task "%s" as "%s"',
	    record['key'], task.t_id);

	value['rIdx'] = record['value']['rIdx'];
	value['mantaComputeId'] = agent.a_record['value']['instance'];
	value['agentGeneration'] = agent.a_record['value']['generation'];
	value['nattempts'] = record['value']['nattempts'] ?
	    record['value']['nattempts'] + 1 : 2;

	if (record['value']['timeInputDone']) {
		value['timeInputDone'] = record['value']['timeInputDone'];
		value['nInputs'] = record['value']['nInputs'];
	}

	record['value']['timeRetried'] = now;

	this.w_dtrace.fire('task-dispatched', function () {
	    return ([ task.t_value['jobId'], task.t_id, value ]);
	});

	barrier.start('task ' + record['value']['taskId']);
	this.w_bus.batch([ [
	    'put',
	    record['bucket'],
	    task.t_id,
	    value
	], [
	    'put',
	    record['bucket'],
	    record['key'],
	    record['value'],
	    { 'etag': record['_etag'] }
	], [
	    'update',
	    this.w_buckets['taskinput'],
	    sprintf('(taskId=%s)', record['key']),
	    {
		'retryTaskId': value['taskId'],
		'retryMantaComputeId': value['mantaComputeId'],
		'retryAgentGeneration': value['agentGeneration']
	    }
	] ], {
	    'retryConflict': function () {
		/*
		 * We specify this 'retryConflict' member that always returns an
		 * error in order to explicitly indicate to the bus that we do
		 * handle EtagConflict errors, even though we "handle" them by
		 * just failing.  This works because we know this operation will
		 * be retried as part of the normal polling loop.
		 *
		 * If we didn't specify this function, the bus assumes that we
		 * don't handle this error at all, and it will blow an assertion
		 * if there's ever another write to the same object pending,
		 * since this subsequent operation will necessarily fail in a
		 * way that could have been predicted.  We can wind up in this
		 * situation if the write that set wantRetry = true is still
		 * outstanding, which is possible even though we made it here
		 * because we may see the results of that write before the
		 * operation itself returns.
		 *
		 * One could argue that assertion in the bus is too aggressive
		 * since it catches legitimate behavior in this case, but it
		 * has caught several real bugs, so at this point it's more
		 * valuable to explicitly override the check here than to rip it
		 * out.  In short, think of this like casting a return value to
		 * void in C: it has no effect except to indicate to tools that
		 * we've explicitly considered this error case, which allows
		 * tools to continue checking for legitimate bugs.
		 */
		return (new Error('conflict while retrying reduce task ' +
		    '(will try again)'));
	    }
	}, function (err, etags) {
		barrier.done('task ' + record['value']['taskId']);

		if (err) {
			job.j_log.error(err, 'failed to retry reduce task "%s"',
			    task.t_id);
			return;
		}

		task.t_etag = etags['etags'][0]['etag'];
		mod_assert.ok(task.t_etag !== undefined);
		phase.p_reducers[value['rIdx']].r_task = task;
		worker.taskPostDispatchCheck(task);

		phase.p_nretryneeded--;
		worker.jobPropagateEnd(job, null);
		worker.jobTick(job);
	});
};

Worker.prototype.taskRetryMap = function (record, barrier, job, phase, now)
{
	var dispatch;

	dispatch = {
	    'd_id': record['bucket'] + '/' + record['key'],
	    'd_job': job,
	    'd_pi': record['value']['phaseNum'],
	    'd_origin': record,
	    'd_objname': mod_path.normalize(record['value']['input']),
	    'd_p0objname': record['value']['p0input'],
	    'd_barrier': barrier,
	    'd_ri': record['value']['rIdx'],

	    'd_auths': [],
	    'd_locates': [],
	    'd_login': undefined,
	    'd_account': undefined,
	    'd_objname_internal': undefined,
	    'd_objectid': undefined,
	    'd_locations': undefined,
	    'd_time': undefined,
	    'd_error': undefined
	};

	this.dispStart(dispatch);
	phase.p_nretryneeded--;
};

Worker.prototype.onRecordTaskInput = function (record, barrier)
{
	var jc, jobrec, job, now, value;
	var wantremove;

	/*
	 * There are two reasons to find a taskinput record: either
	 * wantInputRemoved is set, or wantRetry is set.  In both cases, we
	 * usually still have the job around, but it's possible in the
	 * wantInputRemoved case that the job has already been marked "done", in
	 * which case we have to check w_jobs_cached for a record.  If this
	 * record is not found or invalid, jobForRecord() will take care of
	 * handling the error (by fetching the job record, logging the error, or
	 * whatever.)
	 */
	wantremove = record['value']['wantInputRemoved'] &&
	    record['value']['timeInputRemoved'] === undefined;
	now = mod_jsprim.iso8601(new Date());

	if (wantremove) {
		jc = this.w_jobs_cached[record['value']['jobId']];
		if (jc !== undefined && !jc.jc_checking &&
		    !jc.jc_timecancelled) {
			job = null;
			jobrec = jc.jc_job;
			this.w_log.warn('cleaning up taskinput record ' +
			    '%s for removed job', record['key']);
		} else {
			job = this.jobForRecord(record);
			if (job === null)
				return;
			jobrec = job.j_job;
		}

		this.taskRecordCleanup(record, barrier, job, jobrec, now);
		return;
	}

	record['value']['timeRetried'] = now;
	value = {
	    'taskInputId': mod_uuid.v4(),
	    'jobId': record['value']['jobId'],
	    'taskId': record['value']['retryTaskId'],
	    'domain': record['value']['domain'],
	    'mantaComputeId': record['value']['retryMantaComputeId'],
	    'agentGeneration': record['value']['retryAgentGeneration'],
	    'input': record['value']['input'],
	    'p0input': record['value']['p0input'],
	    'account': record['value']['account'],
	    'objectid': record['value']['objectid'],
	    'servers': record['value']['servers'],
	    'timeDispatched': now,
	    'prevRecordType': 'taskinput',
	    'prevRecordId': record['key']
	};

	barrier.start('taskinput ' + value['taskInputId']);
	this.w_bus.batch([ [
	    'put',
	    record['bucket'],
	    record['key'],
	    record['value'],
	    { 'etag': record['_etag'] }
	], [
	    'put',
	    record['bucket'],
	    value['taskInputId'],
	    value
	] ], {}, function () {
		barrier.done('taskinput ' + value['taskInputId']);
	});
};

Worker.prototype.onRecordTaskOutput = function (record, barrier)
{
	var job, pi, phase, dispatch;

	if ((job = this.jobForRecord(record)) === null)
		return;

	if (record['value']['timeCommitted'] === undefined ||
	    record['value']['timePropagated'] !== undefined) {
		job.j_log.error('onRecord: unexpected taskoutput', record);
		return;
	}

	if (record['value']['phaseNum'] == job.j_phases.length - 1) {
		job.j_log.error('onRecord: unexpected taskoutput', record);
		return;
	}

	mod_assert.ok(record['value']['nextRecordType'] === undefined);
	mod_assert.ok(record['value']['nextRecordId'] === undefined);

	pi = record['value']['phaseNum'];
	if (pi >= job.j_phases.length) {
		job.j_log.error('onRecord: invalid taskoutput', record);
		return;
	}

	phase = job.j_phases[pi + 1];
	dispatch = {
	    'd_id': record['bucket'] + '/' + record['key'],
	    'd_job': job,
	    'd_pi': record['value']['phaseNum'] + 1,
	    'd_origin': record,
	    'd_objname': mod_path.normalize(record['value']['output']),
	    'd_p0objname': record['value']['p0input'],
	    'd_barrier': barrier,
	    'd_ri': undefined,

	    'd_auths': [],
	    'd_locates': [],
	    'd_login': undefined,
	    'd_account': undefined,
	    'd_objname_internal': undefined,
	    'd_objectid': undefined,
	    'd_locations': undefined,
	    'd_time': undefined,
	    'd_error': undefined
	};

	if (phase.p_type == 'reduce') {
		if (record['value']['rIdx'] !== undefined)
			dispatch.d_ri = record['value']['rIdx'];
		else
			dispatch.d_ri = Math.floor(
			    Math.random() * phase.p_reducers.length);

		if (record['value']['rIdx'] >= phase.p_reducers.length) {
			dispatch.d_error = {
			    'code': EM_INVALIDARGUMENT,
			    'message': sprintf('reducer "%d" specified, but ' +
				'only %d reducers exist', dispatch.d_ri,
				phase.p_reducers.length)
			};
		}
	}

	this.dispStart(dispatch);
};

Worker.prototype.jobForRecord = function (record)
{
	if (!this.w_jobs.hasOwnProperty(record['value']['jobId']))
		return (this.onRecordForUnknownJob(record));

	var job = this.w_jobs[record['value']['jobId']];

	if (job.j_dropped !== undefined) {
		this.w_log.warn('onRecord: dropping record for dropped job',
		    record);
		return (null);
	}

	if (job.j_state != 'running') {
		if (!this.w_logthrottle.throttle(
		    'job ' + job.j_id + ' initializing'))
			job.j_log.warn('onRecord: dropping record in job ' +
			    'state "%s"', job.j_state, record);
		return (null);
	}

	mod_assert.equal(job.j_state, 'running');
	return (job);
};

Worker.prototype.onRecordForUnknownJob = function (record)
{
	var worker = this;
	var jobid = record['value']['jobId'];
	var dologthrottle = this.w_logthrottle.throttle(
	    sprintf('%s for unknown job %s', record['bucket'], jobid));
	var jc;

	/*
	 * The only way we should be able to see a record for a job that we
	 * don't know about is if the job itself was cancelled, in which case we
	 * remove it from our global state, but it's still possible that new
	 * jobinputs or taskoutputs are written by other components that don't
	 * have timeJobCancelled set.  We always log a warning for this case,
	 * but if the job really is cancelled, we also trigger a server-side
	 * update to set timeJobCancelled on these records to avoid catching
	 * this one in the future.
	 *
	 * Of course, it's also possible that we don't know whether this job is
	 * cancelled (perhaps because we crashed at some point), and we don't
	 * want to paper over some other bug.  In that case, we explicitly fetch
	 * the job record before checking this.
	 */
	if (record['bucket'] == this.w_buckets['jobinput'] ||
	    record['bucket'] == this.w_buckets['task'] ||
	    record['bucket'] == this.w_buckets['taskinput'] ||
	    record['bucket'] == this.w_buckets['taskoutput']) {
		if (!this.w_jobs_cached.hasOwnProperty(jobid)) {
			this.w_log.warn('onRecord: dropping record for ' +
			    'unknown job (fetching record)', record);
			jc = new JobCacheState();
			jc.jc_checking = true;
			this.w_jobs_cached[jobid] = jc;
			this.w_bus.oneshot(this.w_buckets['job'],
			    function () { return ('(jobId=' + jobid + ')'); },
			    this.w_bus_options, function (rec) {
				jc.jc_job = rec['value'];
				jc.jc_checking = false;

				if (rec['value']['timeCancelled']) {
					jc.jc_throttle = new Throttler(30000);
					jc.jc_timecancelled = rec['value'][
					    'timeCancelled'];
					worker.jobCancelRecords(jobid);
				}
			    });
		} else if (this.w_jobs_cached[jobid].jc_checking) {
			if (!dologthrottle)
				this.w_log.warn('onRecord: dropping record ' +
				    'for unknown job (check pending)', record);
		} else if (this.w_jobs_cached[jobid].jc_timecancelled) {
			if (!dologthrottle)
				this.w_log.warn('onRecord: dropping record ' +
				    'for cancelled job', record);
			this.jobCancelRecords(jobid);
		} else {
			if (!dologthrottle)
				this.w_log.error('onRecord: dropping record ' +
				    'for gone, non-cancelled job', record);
		}
	} else if (!dologthrottle) {
		this.w_log.warn('onRecord: dropping record for unknown job',
		    record);
	}

	return (null);
};

Worker.prototype.jobCancelRecords = function (jobid)
{
	var worker = this;
	var jc, batch;

	jc = this.w_jobs_cached[jobid];
	mod_assert.ok(!jc.jc_checking);
	mod_assert.ok(jc.jc_timecancelled);
	if (jc.jc_throttle.tooRecent())
		return;

	batch = [ [ 'update', this.w_buckets['task'],
	    sprintf('(&(jobId=%s)(!(timeCancelled=*)))', jobid),
	    { 'timeCancelled': jc.jc_timecancelled } ] ];
	[ 'jobinput', 'taskinput', 'taskoutput' ].forEach(function (b) {
		batch.push([ 'update', worker.w_buckets[b],
		    sprintf('(&(jobId=%s)(!(timeCancelled=*)))', jobid),
		    { 'timeJobCancelled': jc.jc_timecancelled } ]);
	});

	this.w_log.info('job "%s": cancelling job records', jobid);
	jc.jc_throttle.start();
	this.w_bus.batch(batch, {}, function (err) {
		jc.jc_throttle.done();
		if (err)
			worker.w_log.warn(err,
			    'job "%s": failed to cancel records', jobid);
		else
			worker.w_log.info(
			    'job "%s": cancelled job records', jobid);
	});
};

/*
 * Invoked with both "task" and "taskinput" records with wantInputRemoved = true
 * and timeInputRemoved undefined.  Checks if the input itself is anonymous,
 * removes the object if so, and writes timeInputRemoved regardless.
 */
Worker.prototype.taskRecordCleanup = function (record, barrier, job, jobrec,
    now)
{
	var worker = this;
	var bucket = record['bucket'];
	var keep, jobid;

	mod_assert.ok(bucket == this.w_buckets['task'] ||
	    bucket == this.w_buckets['taskinput']);
	mod_assert.equal(typeof (record['value']['input']), 'string');
	mod_assert.ok(record['value']['wantInputRemoved']);

	barrier.start(record['key']);

	/*
	 * If the object is not anonymous, don't remove it.  Just update the
	 * state so we know there's no more work to do.
	 */
	keep = jobrec['options'] && jobrec['options']['keepIntermediate'];
	jobid = record['value']['jobId'];
	if (job !== null)
		mod_assert.ok(jobid === job.j_id);
	if (keep || !keyIsAnonymous(record['value']['input'], jobid)) {
		this.taskRecordCleanupFini(record, barrier, job, jobrec,
		    now, null);
		return;
	}

	this.w_deletes_out.push({
	    'token': jobrec['authToken'],
	    'key': record['value']['input'],
	    'callback': function (err) {
		now = mod_jsprim.iso8601(new Date());
		worker.taskRecordCleanupFini(record, barrier, job, jobrec,
		    now, err);
	    }
	});
};

Worker.prototype.taskRecordCleanupFini = function (record, barrier, job,
    jobrec, now, delete_err)
{
	/*
	 * If we failed, just clear the barrier without updating the record's
	 * state so that we'll try again the next time around.
	 */
	if (delete_err) {
		barrier.done(record['key']);
		return;
	}

	var worker = this;
	record['value']['timeInputRemoved'] = now;

	this.w_bus.putBatch([ [
	    record['bucket'],
	    record['key'],
	    record['value'],
	    { 'etag': record['_etag'] }
	] ], {
	    'retryConflict': function () {
		/*
		 * See the comment in taskRetryReduce.  Like in many other
		 * places, it's possible for Moray response delays to trigger
		 * what would look to the bus like an unhandled conflict error,
		 * though in fact this would never happen.
		 */
		return (new Error('conflict while marking task for cleanup'));
	    }
	}, function (err) {
		if (err) {
			worker.w_log.warn(err,
			    'failed to mark task for cleanup');
		} else if (job !== null) {
			mod_assert.ok(job.j_ndeletes > 0);
			if (--job.j_ndeletes === 0)
				worker.jobTick(job);
		}

		barrier.done(record['key']);
	});
};

Worker.prototype.onRecordDomain = function (record, barrier)
{
	var domainid, oldrecord, oldoperator, newoperator, ws, i;

	/*
	 * Update the reverse mapping of worker -> domains operated based on the
	 * latest content of this record.  Also update w_alldomains with the
	 * latest copy of this record in case we later want to modify it.
	 */
	domainid = record['key'];
	newoperator = record['value']['operatedBy'];
	if (this.w_alldomains.hasOwnProperty(domainid)) {
		oldrecord = this.w_alldomains[domainid];
		oldoperator = oldrecord['value']['operatedBy'];
		if (oldoperator !== newoperator &&
		    this.w_allworkers.hasOwnProperty(oldoperator)) {
			ws = this.w_allworkers[oldoperator];
			i = ws.ws_operating.indexOf(domainid);
			if (i != -1)
				ws.ws_operating.splice(i, 1);
		}
	}

	if (this.w_allworkers.hasOwnProperty(newoperator)) {
		ws = this.w_allworkers[newoperator];
		if (ws.ws_operating.indexOf(domainid) == -1)
			ws.ws_operating.push(domainid);
	}

	this.w_alldomains[domainid] = record;

	/*
	 * With our internal state updated, take action based on the changes.
	 */
	if (newoperator == this.w_uuid &&
	    !this.w_ourdomains.hasOwnProperty(domainid)) {
		this.w_log.info('domain "%s": reclaiming for ourselves',
		    domainid);
		this.domainTakeover(domainid, barrier, domainid);
	} else if (!newoperator && domainid === this.w_uuid) {
		this.w_log.info('domain "%s": claiming for ourselves',
		    domainid);
		this.domainTakeover(domainid, barrier, domainid);
	} else if (this.w_ourdomains.hasOwnProperty(domainid)) {
		if (newoperator !== this.w_uuid) {
			this.w_log.warn('domain "%s" was stolen (now ' +
			    'operated by "%s")', domainid, newoperator);
			this.domainStop(domainid);
		} else if (record['value']['wantTransfer']) {
			this.w_log.info('domain "%s": %s requested failback',
			    domainid, record['value']['wantTransfer']);
			this.domainFailback(domainid, barrier);
		}
	} else if (domainid == this.w_uuid) {
		this.w_log.info('domain "%s": requesting failback', domainid);
		this.domainRequestFailback(domainid, barrier);
	}
};

Worker.prototype.onRecordHealth = function (record, barrier)
{
	if (record['value']['component'] == 'agent')
		this.onAgentHealth(record);
	else
		this.onWorkerHealth(record, barrier);
};

Worker.prototype.onAgentHealth = function (newrec)
{
	var instance, agent, oldrec, now, tunables;

	instance = newrec['value']['instance'];
	if (!this.w_agents.hasOwnProperty(instance)) {
		this.w_log.info('agent "%s": discovered', instance);
		this.w_agents[instance] = new AgentState(newrec);
		this.agentStarted(instance);
		return;
	}

	agent = this.w_agents[instance];
	oldrec = agent.a_record;
	agent.a_record = newrec;
	now = Date.now();
	tunables = this.w_conf['tunables'];

	if (oldrec['value']['generation'] != newrec['value']['generation']) {
		this.w_log.info('agent "%s": restarted', instance);
		agent.a_last = now;
		agent.a_timedout = false;
		agent.a_warning = false;
		this.agentStarted(instance);
		return;
	}

	if (oldrec['_mtime'] != newrec['_mtime']) {
		/*
		 * The record has been written -- that's a heartbeat.  If we
		 * previously reported that this agent was gone, report that
		 * it's now back.
		 */
		if (agent.a_timedout || agent.a_warning)
			this.w_log.info('agent "%s": came back', instance);
		agent.a_last = now;
		agent.a_timedout = false;
		agent.a_warning = false;
		return;
	}

	/*
	 * At this point, we went at least one poll cycle without seeing a
	 * heartbeat.  We ignore just one missed cycle for cases where we poll
	 * multiple times frequently (as on startup).  After two, we log a
	 * warning.  After timeAgentTimeout, we time out the agent.
	 */
	if (now - agent.a_last < tunables['timeAgentPoll'])
		return;

	agent.a_warning = true;
	if (now - agent.a_last < tunables['timeAgentTimeout']) {
		mod_assert.ok(!agent.a_timedout);
		this.w_log.warn('agent "%s": missed heartbeat', instance);
		return;
	}

	if (!agent.a_timedout) {
		agent.a_timedout = true;
		this.w_log.error('agent "%s": timed out', instance);
		this.w_dtrace.fire('agent-timeout',
		    function () { return ([ instance ]); });
	}

	/*
	 * We continue executing this while the agent appears out to lunch in
	 * case there were tasks dispatched in flight the first time around.
	 */
	this.agentTimeout(instance);
};

Worker.prototype.onWorkerHealth = function (record, barrier)
{
	var instance, ws, oldrec, now;

	instance = record['value']['instance'];
	if (instance == this.w_uuid)
		return;

	if (!this.w_allworkers.hasOwnProperty(instance)) {
		this.w_log.info('worker "%s": discovered', instance);
		this.w_allworkers[instance] = new WorkerState(record);
		return;
	}

	ws = this.w_allworkers[instance];
	oldrec = ws.ws_record;
	now = Date.now();
	if (oldrec['_mtime'] != record['_mtime']) {
		/* This worker is alive, since the health record was updated. */
		if (ws.ws_timedout) {
			this.w_log.info('worker "%s": came back', instance);
			ws.ws_timedout = false;
		}
		ws.ws_last = now;
		ws.ws_record = record;
	} else if (now - ws.ws_last >
	    this.w_conf['tunables']['timeWorkerAbandon']) {
		/*
		 * It's been too long since this health record has been updated.
		 * We (and the other workers still running) will attempt to pick
		 * up this worker's load.  Each time through, we pick up one of
		 * the domains he's still operating in attempt to spread out the
		 * load.  We use the barrier to avoid queueing multiple attempts
		 * to takeover the same domain.
		 */
		if (!ws.ws_timedout) {
			this.w_log.info('worker "%s": timed out', instance);
			this.w_dtrace.fire('worker-timeout',
			    function () { return ([ instance ]); });
			ws.ws_timedout = true;
		}

		if (ws.ws_operating.length > 0)
			this.domainTakeover(mod_jsprim.randElt(ws.ws_operating),
			    barrier, instance);
	}
};

Worker.prototype.onMantaStorage = function (newrec)
{
	var v = newrec['value'];
	this.w_storage_map[v['manta_storage_id']] = v;
};

Worker.prototype.eachRunningJob = function (func)
{
	mod_jsprim.forEachKey(this.w_jobs, function (_, job) {
		if (job.j_dropped ||
		    job.j_state == 'unassigned' || job.j_state == 'finishing')
			return;

		func(job);
	});
};

/*
 * Invoked when we detect that an agent has restarted to finish off any tasks
 * from a previous instance of that agent.  We also invoke this when we discover
 * a new agent in case, as it's always safe to do this.
 */
Worker.prototype.agentStarted = function (instance)
{
	var worker = this;
	var agent = this.w_agents[instance];
	var generation = agent.a_record['value']['generation'];
	var filter = sprintf('(!(agentGeneration=%s))', generation);
	var timestamp = mod_jsprim.iso8601(new Date());

	/*
	 * We don't technically need to do this on a per-job basis, but it keeps
	 * the queries themselves small and avoids the headaches of dealing with
	 * workers stomping on each others' jobs.
	 */
	this.w_log.info('agent "%s": failing stale tasks for all jobs',
	    instance, filter);
	this.eachRunningJob(function (job) {
		worker.agentFailTasks(instance, job, timestamp, filter);
	});
};

/*
 * Invoked when we decide that an agent has been AWOL for too long to wait for.
 * We abort any tasks that this agent was responsible for.  This sounds like
 * what we do on agent restart (see agentStarted), but the difference is that we
 * abort all unfinished tasks, not just ones that we *know* the agent isn't
 * working on.  As a result, the filter's a little different, and it's
 * inadvisable to invoke this unless we're fairly sure the agent has gone away.
 */
Worker.prototype.agentTimeout = function (instance)
{
	var worker = this;
	var timestamp = mod_jsprim.iso8601(new Date());

	/*
	 * See agentStarted for why we do this per-job instead of globally.
	 */
	this.w_log.info('agent "%s": failing unfinished tasks for all jobs',
	    instance);
	this.eachRunningJob(function (job) {
		worker.agentFailTasks(instance, job, timestamp, '');
	});
};

Worker.prototype.agentFailTasks = function (instance, job, timestamp, extra)
{
	var worker = this;
	var bucket, filter, changes;

	/*
	 * All we actually need to do is set state=done and result=fail on each
	 * of these.  After that it will be picked up by the same query that
	 * handles failures recorded by the agent.
	 */
	bucket = this.w_buckets['task'];
	filter = sprintf('(&(jobId=%s)(!(state=done))(mantaComputeId=%s)%s)',
	    job.j_id, instance, extra);
	changes = {
	    'state': 'done',
	    'result': 'fail',
	    'timeDone': timestamp,
	    'timeAbandoned': timestamp
	};

	this.w_log.debug('agent "%s": job "%s": failing tasks',
	    instance, job.j_id, extra);
	worker.w_bus.batch([
	    [ 'update', bucket, filter, changes ] ], {}, function () {
		worker.w_log.debug('agent "%s": job "%s": done failing tasks',
		    instance, job.j_id, extra);
	    });
};

/*
 * Invoked when we receive a new job record that we don't already own.
 */
Worker.prototype.jobCreate = function (record, barrier)
{
	var jobid, job;

	jobid = record['value']['jobId'];

	if (record['value']['worker'] == this.w_uuid) {
		this.w_log.info('resuming our own job: %s', jobid);
		this.w_stats['asgn_restart']++;
	} else if (record['value']['worker'] !== undefined) {
		this.w_log.info('attempting to steal job "%s" from "%s"',
		    jobid, record['value']['worker']);
	} else {
		this.w_log.info('attempting to take new job "%s"', jobid);
	}

	mod_assert.ok(!this.w_jobs.hasOwnProperty(jobid));
	job = this.w_jobs[jobid] = new JobState({
	    'conf': this.w_conf,
	    'log': this.w_log.child({ 'component': 'Job-' + jobid }),
	    'record': record
	});

	/*
	 * Attempt to move this job from "unassigned" state to "initializing" by
	 * updating the Moray job record to have job['worker'] == our uuid.
	 */
	mod_assert.equal(job.j_state, 'unassigned');

	if (!job.j_job['timeAssigned']) {
		job.j_job['timeAssigned'] = mod_jsprim.iso8601(Date.now());
		job.j_job['worker'] = this.w_uuid;
		job.j_job['stats'] = {
		    'nAssigns': 1,
		    'nErrors': 0,
		    'nRetries': 0,
		    'nInputsRead': 0,
		    'nJobOutputs': 0,
		    'nTasksDispatched': 0,
		    'nTasksCommittedOk': 0,
		    'nTasksCommittedFail': 0
		};
	} else {
		job.j_job['stats']['nAssigns']++;

		if (job.j_job['stats']['nRetries'] === undefined)
			job.j_job['stats']['nRetries'] = 0;
	}

	job.j_job['state'] = 'running';

	job.j_save.markDirty();
	job.j_save_barrier = barrier;
	barrier.start('save job ' + job.j_id);

	this.jobSave(job);
};

Worker.prototype.jobAssigned = function (job)
{
	var worker = this;
	var barrier, queries, queryconf, query;

	this.w_dtrace.fire('job-assigned',
	    function () { return ([ job.j_id, job.j_job ]); });
	this.jobTransition(job, 'unassigned', 'initializing');
	barrier = job.j_init_barrier = mod_vasync.barrier();

	/*
	 * In order to know when the job is finished, we need to know how many
	 * uncommitted tasks, tasks needing retry, and committed, unpropagated
	 * taskoutputs there are.  Since we're the only component that writes
	 * new tasks and commits taskoutputs, these numbers cannot go up except
	 * by our own action, and if both of these numbers are zero, then there
	 * cannot be any outstanding work.  We need this same information on a
	 * per-phase basis in order to know when individual reducers' inputs are
	 * done.
	 */
	job.j_phases.forEach(function (_, i) {
		queryconf = wQueries.wqCountJobTasksUncommitted;
		query = queryconf['query'].bind(null, i, job.j_id);
		barrier.start('phase ' + i + ' tasks');
		worker.w_bus.count(worker.w_buckets[queryconf['bucket']],
		    query, worker.w_bus_options, function (c) {
			barrier.done('phase ' + i + ' tasks');
			mod_assert.equal(typeof (c), 'number');
			job.j_phases[i].p_nuncommitted = c;
		    });

		queryconf = wQueries.wqCountJobTasksNeedingRetry;
		query = queryconf['query'].bind(null, i, job.j_id);
		barrier.start('phase ' + i + ' tasks needing retry');
		worker.w_bus.count(worker.w_buckets[queryconf['bucket']],
		    query, worker.w_bus_options, function (c) {
			barrier.done('phase ' + i + ' tasks needing retry');
			mod_assert.equal(typeof (c), 'number');
			job.j_phases[i].p_nretryneeded = c;
		    });

		if (i == job.j_phases.length - 1) {
			job.j_phases[i].p_nunpropagated = 0;
			return;
		}

		queryconf = wQueries.wqCountJobTaskOutputsUnpropagated;
		query = queryconf['query'].bind(null, i, job.j_id);
		barrier.start('phase ' + i + ' taskoutputs');
		worker.w_bus.count(worker.w_buckets[queryconf['bucket']],
		    query, worker.w_bus_options, function (c) {
			barrier.done('phase ' + i + ' taskoutputs');
			mod_assert.equal(typeof (c), 'number');
			job.j_phases[i].p_nunpropagated = c;
		    });
	});

	/*
	 * For the same reasons we need the above per-phase counters, we also
	 * need a per-job count of objects that still need to be deleted for
	 * this job.
	 */
	queries = [
	    wQueries.wqCountJobTasksNeedingDelete,
	    wQueries.wqCountJobTaskInputsNeedingDelete
	];
	queries.forEach(function (q) {
		query = q['query'].bind(null, job.j_id);
		barrier.start(q['name']);
		worker.w_bus.count(worker.w_buckets[q['bucket']],
		    query, worker.w_bus_options, function (c) {
			barrier.done(q['name']);
			job.j_ndeletes += c;
		    });
	});

	/*
	 * To maintain accurate counters, we have to load these at job startup.
	 */
	queries = [
	    wQueries.wqCountErrors,
	    wQueries.wqCountRetries,
	    wQueries.wqCountInputsRead,
	    wQueries.wqCountOutputs,
	    wQueries.wqCountTasksDispatched,
	    wQueries.wqCountTasksCommittedOk,
	    wQueries.wqCountTasksCommittedFail
	];
	queries.forEach(function (q) {
		var stat;
		mod_assert.ok(mod_jsprim.startsWith(q['name'], 'count '));
		stat = q['name'].substr('count '.length);
		mod_assert.ok(job.j_job['stats'].hasOwnProperty(stat));

		query = q['query'].bind(null, job.j_id,
		    job.j_phases.length - 1);
		barrier.start(q['name']);
		worker.w_bus.count(worker.w_buckets[q['bucket']],
		    query, worker.w_bus_options, function (c) {
			mod_assert.equal(typeof (c), 'number');

			if (job.j_job['stats'][stat] !== c) {
				job.j_log.info('updating stat "%s" ' +
				    'from %d to %d', stat,
				    job.j_job['stats'][stat], c);
				job.j_job['stats'][stat] = c;
			}

			barrier.done(q['name']);
		    });
	});

	/*
	 * We also need to know the full set of reduce tasks already dispatched.
	 */
	queryconf = wQueries.wqJobTasksReduce;
	query = queryconf['query'].bind(null, job.j_id);
	barrier.start('fetch reduce tasks');
	this.w_bus.oneshot(worker.w_buckets[queryconf['bucket']], query,
	    this.w_bus_options, function (record) {
		worker.jobLoadReduceTask(job, record, barrier);
	    },
	    function () { barrier.done('fetch reduce tasks'); });

	barrier.on('drain', function () {
		if (job.j_dropped) {
			job.j_log.warn(
			    'found job dropped after load completed');
			return;
		}

		worker.jobTransition(job, 'initializing', 'running');
		worker.jobLoaded(job);
	});

	/*
	 * Finally, we need to make sure that any tasks with stale
	 * agentGeneration values are dealt with.  We don't have to finish
	 * before we start running the job, though.  This logic relies on the
	 * fact that we don't start picking up jobs until we've gotten each
	 * agents' last health report.
	 */
	job.j_log.info('failing stale tasks on all agents');
	var timestamp = mod_jsprim.iso8601(new Date());
	mod_jsprim.forEachKey(this.w_agents, function (instance, agent) {
		var filter = sprintf('(!(agentGeneration=%s))',
		    agent.a_record['value']['generation']);
		worker.agentFailTasks(instance, job, timestamp, filter);
	});
};

Worker.prototype.jobLoadReduceTask = function (job, record, barrier)
{
	var value, pi, phase, ri, reducer;

	value = record['value'];
	pi = value['phaseNum'];
	if (pi >= job.j_phases.length) {
		job.j_log.error('invalid reduce task', record);
		return;
	}

	phase = job.j_phases[pi];
	ri = value['rIdx'];
	if (ri >= phase.p_reducers.length) {
		job.j_log.error('invalid reduce task', record);
		return;
	}

	reducer = phase.p_reducers[ri];
	job.j_log.info('loading reduce task', value['taskId']);
	reducer.r_task = new JobTask(job.j_id, pi, value['taskId']);
	reducer.r_task.t_etag = record['_etag'];
	reducer.r_task.t_value = value;
	reducer.r_done = record['value']['state'] == 'done';

	/*
	 * In order to maintain an accurate count of the taskinputs dispatched
	 * for each reduce task, we need to know how many have already been
	 * dispatched for this task.
	 */
	var queryconf = wQueries.wqCountReduceTaskInputs;
	barrier.start('count task ' + value['taskId'] + ' inputs');
	this.w_bus.count(this.w_buckets[queryconf['bucket']],
	    queryconf['query'].bind(null, value['taskId']),
	    this.w_bus_options, function (c) {
		mod_assert.equal(typeof (c), 'number');
		reducer.r_ninput = c;
		reducer.r_nissued = c;
		barrier.done('count task ' + value['taskId'] + ' inputs');
	    });
};

Worker.prototype.jobError = function (job, pi, timestamp, code, message,
    messageInternal)
{
	var uuid = mod_uuid.v4();
	var value = {
	    'errorId': uuid,
	    'jobId': job.j_id,
	    'domain': job.j_job['worker'],
	    'phaseNum': pi,
	    'errorCode': code,
	    'errorMessage': message,
	    'retried': false,
	    'timeCommitted': timestamp
	};

	job.j_job['stats']['nErrors']++;

	if (messageInternal)
		value['errorMessageInternal'] = messageInternal;

	this.w_dtrace.fire('error-dispatched',
	    function () { return ([ job.j_id, value ]); });
	this.w_bus.putBatch([ [ this.w_buckets['error'], uuid, value ] ]);
};

Worker.prototype.jobLoaded = function (job)
{
	var worker = this;

	/*
	 * If this job's input stream was already marked done (and we've already
	 * read that fact), then do one more pass to make sure we've found
	 * everything.  If timeInputDoneRead isn't set, then we haven't
	 * processed the end-of-input yet (possibly because it hasn't happened
	 * yet) and this will be triggered sometime later.
	 */
	job.j_last_input = Date.now();
	if (job.j_job['timeInputDoneRead'] !== undefined)
		this.jobInputEnded(job);

	/*
	 * Dispatch any reduce tasks that haven't already been dispatched.
	 */
	var now = mod_jsprim.iso8601(Date.now());

	job.j_phases.forEach(function (phase, pi) {
		if (!phase.p_reducers)
			return;

		phase.p_reducers.forEach(function (reducer, ri) {
			if (mod_jsprim.isEmpty(worker.w_agents)) {
				worker.jobError(job, pi, now,
				    EM_SERVICEUNAVAILABLE,
				    sprintf('no servers available for ' +
					'reducer %d in phase %d', ri, pi));
				return;
			}

			if (reducer.r_task !== undefined)
				return;

			var task, value;
			var instance = mod_jsprim.randElt(
			    Object.keys(worker.w_agents));

			task = worker.taskCreate(job, pi, now);
			reducer.r_task = task;

			value = task.t_value;
			value['rIdx'] = ri;
			value['mantaComputeId'] = worker.w_agents[
			    instance].a_record['value']['instance'];
			value['agentGeneration'] = worker.w_agents[
			    instance].a_record['value']['generation'];
			value['nattempts'] = 1;

			worker.w_dtrace.fire('task-dispatched', function () {
			    return ([ task.t_value['jobId'], task.t_id,
			        task.t_value ]);
			});

			job.j_log.info('dispatching reduce task: ' +
			    'phase %d rIdx %d', pi, ri);
			worker.w_bus.putBatch([ [ worker.w_buckets['task'],
			    task.t_id, value ] ], {}, function (err, etags) {
				task.t_etag = etags['etags'][0]['etag'];
				mod_assert.ok(task.t_etag !== undefined);
				worker.taskPostDispatchCheck(task);
			});
		});
	});
};

/*
 * Invoked to completely remove a job from this worker.
 */
Worker.prototype.jobRemove = function (job)
{
	if (job.j_state == 'unassigned')
		this.w_stats['asgn_failed']++;

	job.j_dropped = new Date();
	delete (this.w_jobs[job.j_id]);

	job.j_log.info('job removed');

	if (job.j_save_throttle.ongoing())
		job.j_log.info('job removed with pending save operation');

	if (job.j_nauths !== 0)
		job.j_log.info('job removed with %d pending auth requests',
		    job.j_nauths);

	if (job.j_nlocates !== 0)
		job.j_log.info('job removed with %d pending locate requests',
		    job.j_nlocates);
};

/*
 * Common function to transition from state S1 to state S2.
 */
Worker.prototype.jobTransition = function (job, s1, s2)
{
	if (s1 !== undefined)
		mod_assert.equal(job.j_state, s1);

	job.j_log.info('transitioning states from %s to %s', s1, s2);
	job.j_state = s2;
	job.j_state_time = new Date();

	this.jobTick(job);
};

/*
 * Invoked periodically while processing the job to check for expired timeouts.
 */
Worker.prototype.jobTick = function (job)
{
	if (job.j_dropped)
		return;

	if (job.j_state == 'unassigned')
		return;

	/*
	 * If it's been too long since the last job save, mark the job dirty.
	 * After that, if the job is dirty and we're not already trying to save
	 * it, save it now.
	 */
	if (!job.j_save_throttle.tooRecent() && !job.j_save.dirty())
		job.j_save.markDirty();

	if (!job.j_save_throttle.ongoing() && job.j_save.dirty())
		this.jobSave(job);

	if (job.j_state == 'initializing' || job.j_state == 'finishing')
		/* We'll get kicked out of this state asynchronously. */
		return;

	mod_assert.equal(job.j_state, 'running');

	if (!job.j_input_fully_read && !job.j_mark_inputs.tooRecent())
		this.jobMarkInputs(job);

	if (this.jobDone(job)) {
		job.j_save.markDirty();
		job.j_job['timeDone'] = mod_jsprim.iso8601(new Date());
		job.j_job['state'] = 'done';
		this.jobTransition(job, 'running', 'finishing');
		this.w_dtrace.fire('job-done',
		    function () { return ([ job.j_id, job.j_job ]); });
		return;
	}

	var timeout = this.w_conf['tunables']['timeJobIdleClose'];
	if (job.j_last_input !== undefined && timeout) {
		var now = Date.now();
		if (now - job.j_last_input > timeout) {
			job.j_log.warn('ending input (idle for %sms)',
			    now - job.j_last_input);
			mod_assert.ok(job.j_job['timeInputDone'] === undefined);
			job.j_job['timeInputDone'] = mod_jsprim.iso8601(now);
			job.j_save.markDirty();
			job.j_last_input = undefined;
			if (!job.j_save_throttle.ongoing())
				this.jobSave(job);
		}
	}
};

Worker.prototype.jobInputEnded = function (job)
{
	var worker = this;
	var domainid = job.j_job['worker'];
	var sid = this.w_ourdomains[domainid][wQueries.wqJobInputs['name']];

	this.w_dtrace.fire('job-input-done',
	    function () { return ([ job.j_id, job.j_job ]); });
	job.j_last_input = undefined;
	this.jobMarkInputs(job, function () {
		worker.w_bus.fence(sid, function () {
			job.j_log.info('finished reading job inputs');
			job.j_input_fully_read = true;
			worker.w_dtrace.fire('job-inputs-read',
			    function () { return ([ job.j_id ]); });
			worker.jobPropagateEnd(job, null);
			worker.jobTick(job);
		});
	});
};

Worker.prototype.jobMarkInputs = function (job, forcecb)
{
	var worker, bucket, filter, changes;

	worker = this;
	bucket = this.w_buckets['jobinput'];
	filter = sprintf('(&(jobId=%s)(!(domain=*)))', job.j_id);
	changes = { 'domain': job.j_job['worker'] };

	this.w_dtrace.fire('job-mark-inputs-start',
	    function () { return ([ job.j_id ]); });

	job.j_log.debug('marking inputs');
	if (!forcecb)
		job.j_mark_inputs.start();
	this.w_bus.batch([
	    [ 'update', bucket, filter, changes ] ], {}, function () {
		worker.w_dtrace.fire('job-mark-inputs-done',
		    function () { return ([ job.j_id ]); });
		job.j_log.debug('marked inputs');
		if (!forcecb)
			job.j_mark_inputs.done();
		else
			forcecb();
	});
};

Worker.prototype.jobSave = function (job)
{
	var worker = this;

	mod_assert.ok(!job.j_save_throttle.ongoing());
	job.j_save_throttle.start();
	job.j_log.debug('saving job (%j)', job.j_save);
	job.j_save.saveStart();

	var options = {};

	if (job.j_state != 'unassigned') {
		/*
		 * This write is time-sensitive, so ignore the throttle.  We
		 * don't ignore the throttle for assignment writes since they're
		 * not time-critical, and if a write gets delayed by the
		 * throttle, perhaps this worker really is too busy to take this
		 * job.
		 */
		options['ignoreThrottle'] = true;

		/*
		 * The job record is only ever modified by ourselves and muskie,
		 * but surprisingly, even in a functioning system with no
		 * outages or errors, it's impossible for us to always know what
		 * the expected job etag should be if we both modify the record
		 * at nearly the same time.  We'll see the muskie write as part
		 * of a poll operation, and we'll see the etag from our own
		 * write in the "put" response, but we could see these in-order
		 * or out-of-order with respect to what happened in the
		 * database, and there's no way for us to know which happened
		 * second.  Our only choice is to fetch-and-merge on an
		 * EtagConflict error.
		 */
		options['retryConflict'] = function (oldrec, newrec) {
			return (mod_bus.mergeRecords([
				'timeCancelled',
				'timeInputDone'
			], [
				/*
				 * These are all the fields that we *might* have
				 * changed.  Regrettably, this is necessary to
				 * know what changes to ignore between the old
				 * record and the new record.  If this list
				 * becomes a burden to maintain, we could track
				 * it automatically by keeping track of the
				 * original in-moray state and diffing that
				 * against what we're trying to write.
				 */
				'state',
				'timeAssigned',
				'timeInputDone',
				'timeInputDoneRead',
				'timeDone',
				'stats'
			], oldrec['value'], newrec['value']));
		};
	}

	var records = [ [
	    'put',
	    this.w_buckets['job'],
	    job.j_id,
	    mod_jsprim.deepCopy(job.j_job),
	    { 'etag': job.j_etag }
	] ];

	/*
	 * If we're cancelling this job, make sure to also cancel the other
	 * associated job records.
	 */
	if (job.j_state == 'finishing' && job.j_job['state'] == 'done' &&
	    job.j_job['timeCancelled'] !== undefined) {
		/*
		 * Tasks are cancelled directly to stop execution.  We write
		 * this on all tasks, committed, done, or not, because we're
		 * going to stop processing any updates and we don't want to
		 * keep finding them.
		 */
		records.push([ 'update', this.w_buckets['task'],
		    sprintf('(&(jobId=%s))', job.j_id),
		    { 'timeCancelled': job.j_job['timeCancelled'] } ]);

		/*
		 * We mark timeJobCancelled on jobinputs, taskoutputs, and
		 * taskinputs so that our polling doesn't keep finding them.
		 */
		records.push([ 'update', this.w_buckets['jobinput'],
		    sprintf('(&(jobId=%s))', job.j_id),
		    { 'timeJobCancelled': job.j_job['timeCancelled'] } ]);
		records.push([ 'update', this.w_buckets['taskoutput'],
		    sprintf('(&(jobId=%s))', job.j_id),
		    { 'timeJobCancelled': job.j_job['timeCancelled'] } ]);
		records.push([ 'update', this.w_buckets['taskinput'],
		    sprintf('(&(jobId=%s))', job.j_id),
		    { 'timeJobCancelled': job.j_job['timeCancelled'] } ]);
	}

	this.w_bus.batch(records, options, function (err) {
		job.j_save_throttle.done();

		if (err) {
			job.j_save.saveFailed();
			if (job.j_state != 'unassigned')
				job.j_log.error(err,
				    'failed to save job record');
			worker.jobRemove(job);
		} else {
			job.j_save.saveOk();
		}

		if (job.j_save_barrier !== undefined) {
			job.j_save_barrier.done('save job ' + job.j_id);
			job.j_save_barrier = undefined;
		}

		if (job.j_state == 'unassigned' &&
		    job.j_dropped === undefined) {
			if (err) {
				job.j_log.warn('failed to assign job');
				worker.jobRemove(job);
			} else {
				job.j_log.info('successfully assigned job');
				worker.jobAssigned(job);
			}
		}

		if (job.j_state == 'finishing' && !job.j_save.dirty())
			worker.jobRemove(job);
	});
};

/*
 * Returns true iff the job is complete.
 */
Worker.prototype.jobDone = function (job)
{
	var pi;

	if (job.j_state != 'running' && job.j_state != 'finishing')
		return (false);

	if (!job.j_input_fully_read)
		return (false);

	if (job.j_nlocates > 0 || job.j_nauths > 0 || job.j_ndeletes > 0)
		return (false);

	for (pi = 0; pi < job.j_phases.length; pi++) {
		if (job.j_phases[pi].p_ndispatches > 0 ||
		    job.j_phases[pi].p_nuncommitted > 0 ||
		    job.j_phases[pi].p_nunpropagated > 0 ||
		    job.j_phases[pi].p_nretryneeded > 0)
			return (false);
	}

	return (true);
};

/*
 * Most of the work done by this service goes through one of several queues
 * documented in Worker above.  This function is invoked periodically to
 * process work on each of these queues.
 */
Worker.prototype.processQueues = function ()
{
	var now, key, job;
	var dispatch;
	var queries = [];
	var worker = this;

	now = mod_jsprim.iso8601(new Date());

	while (this.w_pending_auths < this.w_max_pending_auths &&
	    this.w_auths_out.length > 0) {
		dispatch = this.w_auths_out.shift();
		this.w_pending_auths++;
		this.dispResolveUser(dispatch);
	}

	while (this.w_auths_in.length > 0) {
		this.w_pending_auths--;

		dispatch = this.w_auths_in.shift();
		job = dispatch.d_job;
		job.j_nauths--;

		if (job.j_dropped) {
			dispatch.d_barrier.done(dispatch.d_id);
			dispatch.d_job.j_phases[dispatch.d_pi].p_ndispatches--;
			continue;
		}

		if (dispatch.d_error === undefined && !isAuthorized(
		    job, dispatch.d_account, dispatch.d_objname)) {
			dispatch.d_error = {
			    'code': EM_AUTHORIZATION,
			    'message': sprintf('permission denied: "%s"',
				dispatch.d_objname)
			};
		}

		if (dispatch.d_error !== undefined)
			this.dispError(dispatch);
		else
			this.dispLocate(dispatch);
	}

	if (this.w_pending_locates < this.w_max_pending_locates &&
	    this.w_locates_out.length > 0) {
		queries = this.w_locates_out.splice(0,
		    this.w_max_pending_locates - this.w_pending_locates);
		this.w_pending_locates += queries.length;
		this.w_locator.locate(queries.map(
		    function (disp) { return (disp.d_objname_internal); }),
		    this.dispLocateResponse.bind(this, queries));
		queries.forEach(function (disp) {
			worker.w_dtrace.fire('locate-start', function () {
				return ([ disp.d_objname_internal ]);
			});
		});
	}

	while (this.w_locates_in.length > 0) {
		dispatch = this.w_locates_in.shift();
		job = dispatch.d_job;
		job.j_nlocates--;

		if (job.j_dropped) {
			dispatch.d_barrier.done(dispatch.d_id);
			dispatch.d_job.j_phases[dispatch.d_pi].p_ndispatches--;
			continue;
		}

		dispatch.d_time = now;
		this.dispDispatch(dispatch);
	}

	while (this.w_pending_deletes < this.w_max_pending_deletes &&
	    this.w_deletes_out.length > 0) {
		key = this.w_deletes_out.shift();
		this.doDelete(key, now);
	}

	/*
	 * By this point, we should have processed all incoming records, and
	 * either the output queues should be empty or we've reached internal
	 * limits.
	 */
	mod_assert.equal(this.w_locates_in.length, 0);
	mod_assert.equal(this.w_auths_in.length, 0);
};

/*
 * Create a new "task" record for phase "pi" of job "job".
 */
Worker.prototype.taskCreate = function (job, pi, now)
{
	var task;

	mod_assert.ok(pi >= 0 && pi < job.j_phases.length);

	task = new JobTask(job.j_id, pi, mod_uuid.v4());
	task.t_value['state'] = 'dispatched';
	task.t_value['timeDispatched'] = now;
	task.t_value['domain'] = job.j_job['worker'];

	job.j_phases[pi].p_nuncommitted++;
	job.j_job['stats']['nTasksDispatched']++;

	return (task);
};

/*
 * Invoked after we've successfully written out a task to check whether it's
 * still valid.  This deals with a race between issuing the task for an agent
 * with a given agentGeneration concurrently with having discovered that that
 * agent has restarted (and its agentGeneration changed).  Without this check,
 * we might never update these records.
 */
Worker.prototype.taskPostDispatchCheck = function (task)
{
	var taskvalue, instance, agent;
	var job, filter, timestamp;

	taskvalue = task.t_value;
	instance = taskvalue['mantaComputeId'];
	agent = this.w_agents[instance];
	mod_assert.ok(agent !== undefined);

	if (agent.a_record['value']['generation'] ==
	    taskvalue['agentGeneration'])
		return;

	job = this.w_jobs[task.t_value['jobId']];
	mod_assert.ok(job !== undefined);
	job.j_log.info('issued task "%s" with already stale generation',
	    task.t_id);

	filter = sprintf('(!(agentGeneration=%s))',
	    agent.a_record['value']['generation']);
	timestamp = mod_jsprim.iso8601(new Date());
	this.agentFailTasks(instance, job, timestamp, filter);
};

/*
 * Dispatch pipeline
 *
 * When new input records are found, either jobinputs for phase 0 or taskoutputs
 * for subsequent phases, the worker runs through the following pipeline to
 * authorize access, locate the record, and assign work.  In all cases, we'll
 * process the input record and write out either a new "task" record assigning
 * work to an agent or an "error" record explaining why that wasn't possible.
 * In both cases, we also update the original record to indicate that it has
 * been processed, and these two updates happen in a Moray transaction.
 *
 * The pipeline steps include:
 *
 *     (1) Resolve the object's owner's uuid and do a simple access check: if
 *         the job owner's uuid matches the uuid corresponding to the login in
 *         the object name, or the object's name refers to a public directory,
 *         access is granted.  Otherwise, emit an error.
 *
 *     (2) Query the Moray ring to locate the object.  If the object does not
 *         exist, emit an error.  If the object exists in at least one location,
 *         emit a new "task" record with state "dispatched" assigned to the
 *         agent corresponding to one of the locations selected at random.  If
 *         the object is zero bytes in length, then it exists nowhere but may be
 *         processed anywhere, so write the task record assigned to any agent
 *         selected at random.
 *
 * We manage the state of a dispatch request through this pipeline using
 * the following fields:
 *
 *    d_id		unique identifier for this dispatch
 *
 *    d_job, d_pi	corresponding job and phase number
 *
 *    d_origin		origin jobinput or taskoutput record
 *
 *    d_objname		normalized user-facing object name (includes login)
 *
 *    d_p0objname	user-facing name of phase-0 object that led to
 *    			this dispatch, if available.  We track this
 *    			through the job for debugging purposes.
 *
 *    d_barrier		vasync barrier used to notify upon completion of
 *    			processing
 *
 *    d_ri		designated reducer index (reduce only)
 *
 * During execution of this pipeline we'll fill in these fields in roughly the
 * this order:
 *
 *    d_login			object owner's login
 *    (see dispResolveUser)	(derived directly from d_objname)
 *
 *    d_account			object owner's account uuid
 *    (see dispResolveUser)
 *
 *    d_objname_internal	object name as known to Moray
 *    (see dispResolveUser)	(derived by replacing d_login
 *    				with d_account in d_objname)
 *
 *    d_auths			piggy-backed auth requests
 *    (see dispResolveUser)
 *
 *    d_objectid		unique object identifier, or
 *    (see dispLocate)		'/dev/null' for zero-byte objects
 *
 *    d_locations		locations where this object is stored
 *    (see dispLocate)		("mantaComputeId", "zonename" tuples)
 *
 *    d_locates			piggy-backed locate requests
 *    (see dispLocate)
 *
 *    d_time			dispatch time
 *    (see dispDispatch)
 *
 *    d_error			error object
 *    (various)
 */

Worker.prototype.dispStart = function (dispatch)
{
	dispatch.d_barrier.start(dispatch.d_id);
	dispatch.d_job.j_phases[dispatch.d_pi].p_ndispatches++;

	if (dispatch.d_error !== undefined) {
		this.dispError(dispatch);
		return;
	}

	var job = dispatch.d_job;
	job.j_nauths++;
	job.j_log.debug('resolve "%s": new request', dispatch.d_objname);
	this.w_auths_out.push(dispatch);
};

Worker.prototype.dispResolveUser = function (dispatch)
{
	var worker = this;
	var login;

	login = mod_mautil.pathExtractFirst(dispatch.d_objname);
	if (!login) {
		dispatch.d_error = {
		    'code': EM_RESOURCENOTFOUND,
		    'message': sprintf('malformed object name: "%s"',
			dispatch.d_objname)
		};

		this.w_auths_in.push(dispatch);
		return;
	}

	if (this.w_auths_pending.hasOwnProperty(login)) {
		this.w_log.debug('piggy-backing auth "%s" onto existing ' +
		    'request', login);
		this.w_auths_pending[login].d_auths.push(dispatch);
		return;
	}

	this.w_dtrace.fire('auth-start', function () { return ([ login ]); });
	this.w_auths_pending[login] = dispatch;
	this.w_mahi.userFromLogin(login, function (err, record) {
		worker.w_dtrace.fire('auth-done',
		    function () { return ([ login, err ? err.name : '' ]); });
		mod_assert.equal(worker.w_auths_pending[login], dispatch);
		delete (worker.w_auths_pending[login]);

		if (err && err['name'] != 'UserDoesNotExistError') {
			dispatch.d_error = {
			    'code': EM_INTERNAL,
			    'message': err.message
			};
		} else if (err) {
			dispatch.d_error = {
			    'code': EM_RESOURCENOTFOUND,
			    'message': sprintf('no such object: "%s"',
				dispatch.d_objname)
			};
		} else {
			dispatch.d_login = login;
			dispatch.d_account = record['uuid'];
		}

		worker.w_auths_in.push(dispatch);

		dispatch.d_auths.forEach(function (odispatch) {
			odispatch.d_error = dispatch.d_error;
			odispatch.d_login = dispatch.d_login;
			odispatch.d_account = dispatch.d_account;
			worker.w_auths_in.push(odispatch);
		});

		dispatch.d_auths = null;
		worker.processQueues();
	});
};

/*
 * Enqueue a request to locate key "key", triggered by record "ent".
 */
Worker.prototype.dispLocate = function (dispatch)
{
	var job = dispatch.d_job;
	var objname;

	job.j_nlocates++;
	objname = pathSwapFirst(dispatch.d_objname, dispatch.d_account);
	dispatch.d_objname_internal = objname;

	if (this.w_locates_pending.hasOwnProperty(objname)) {
		this.w_log.debug('piggy-backing locate "%s" onto existing ' +
		    'request', objname);
		this.w_locates_pending[objname].d_locates.push(dispatch);
		return;
	}

	this.w_locates_pending[objname] = dispatch;
	this.w_locates_out.push(dispatch);
	job.j_log.debug('locate "%s": enqueued new request', objname);
};

/*
 * Handle an incoming "locate" response.
 */
Worker.prototype.dispLocateResponse = function (dispatches, err, locations)
{
	var dispatch, iobjname, i, code, message;
	var worker = this;

	for (i = 0; i < dispatches.length; i++) {
		dispatch = dispatches[i];
		iobjname = dispatch.d_objname_internal;

		mod_assert.equal(this.w_locates_pending[iobjname], dispatch);
		delete (this.w_locates_pending[iobjname]);

		this.w_log.debug('locate response for "%s"', iobjname);

		if (locations[iobjname]['error']) {
			code = locations[iobjname]['error']['code'];
			message = sprintf('%s: "%s"',
			    locations[iobjname]['error']['message'],
			    dispatch.d_objname);
			this.w_dtrace.fire('locate-done', function () {
				return ([ dispatch.d_objname_internal, code ]);
			});
			if (code != EM_RESOURCENOTFOUND)
				this.w_log.warn(locations[iobjname]['error'],
				    'error locating object "%s"', iobjname);
			if (code == EM_INTERNAL) {
				dispatch.d_error = {
				    'code': EM_INTERNAL,
				    'message': 'internal error',
				    'messageInternal': message
				};
			} else {
				dispatch.d_error = {
				    'code': code,
				    'message': message
				};
			}
		} else {
			dispatch.d_objectid = locations[iobjname].length > 0 ?
			    locations[iobjname][0]['objectid'] : '/dev/null';
			dispatch.d_locations = locations[iobjname].filter(
			    function (loc) {
				return (worker.w_agents.hasOwnProperty(
				    loc['mantaComputeId']));
			    });
			this.w_dtrace.fire('locate-done', function () {
				return ([ dispatch.d_objname_internal, '' ]);
			});
		}

		this.w_locates_in.push(dispatch);
		this.w_pending_locates--;

		dispatch.d_locates.forEach(function (odispatch) {
			odispatch.d_error = dispatch.d_error;
			odispatch.d_objectid = dispatch.d_objectid;
			odispatch.d_locations = dispatch.d_locations;
			worker.w_locates_in.push(odispatch);
		});

		dispatch.d_locates = null;
	}

	this.processQueues();
};

Worker.prototype.dispDispatch = function (dispatch)
{
	if (dispatch.d_error !== undefined)
		this.dispError(dispatch);
	else if (dispatch.d_job.j_phases[dispatch.d_pi].p_type == 'reduce')
		this.dispReduce(dispatch);
	else
		this.dispMap(dispatch);
};

/*
 * Implementation of keyDispatch() for map phases.  This is simple because we
 * always dispatch a new task.
 */
Worker.prototype.dispMap = function (dispatch)
{
	var worker = this;
	var which, instance, zonename, task, value, origin_value, phase;
	var isretry, locs;

	/*
	 * Figure out where to run this task.  For non-zero-byte objects, run
	 * the task on one of the systems where the object is stored.
	 * Otherwise, pick a compute instance at random that we know about.
	 */
	if (dispatch.d_objectid == '/dev/null' &&
	    !mod_jsprim.isEmpty(this.w_agents)) {
		instance = mod_jsprim.randElt(Object.keys(this.w_agents));
		zonename = '';
	} else if (dispatch.d_objectid != '/dev/null' &&
	    dispatch.d_locations.length > 0) {
		locs = dispatch.d_locations;

		if (dispatch.d_origin['bucket'] == this.w_buckets['task']) {
			/*
			 * This is a retry.  Prefer a location that's different
			 * than the one we just tried.
			 */
			locs = locs.filter(function (loc) {
				var v = dispatch.d_origin['value'];
				return (loc['mantaComputeId'] !=
				    v['mantaComputeId']);
			});

			if (locs.length === 0)
				locs = dispatch.d_locations;
		}

		which = mod_jsprim.randElt(locs);
		instance = which['mantaComputeId'];
		zonename = which['zonename'];
	} else {
		dispatch.d_error = {
		    'code': EM_SERVICEUNAVAILABLE,
		    'message': 'no servers available to run task'
		};
		this.dispError(dispatch);
		return;
	}

	task = this.taskCreate(dispatch.d_job, dispatch.d_pi, dispatch.d_time);
	value = task.t_value;
	value['input'] = dispatch.d_objname;
	value['p0input'] = dispatch.d_pi === 0 ? dispatch.d_objname :
	    dispatch.d_origin['value']['p0input'];
	value['account'] = dispatch.d_account;
	value['objectid'] = dispatch.d_objectid;
	value['mantaComputeId'] = this.w_agents[instance].
	    a_record['value']['instance'];
	value['agentGeneration'] = this.w_agents[instance].
	    a_record['value']['generation'];
	value['zonename'] = zonename;

	if (dispatch.d_origin['bucket'] == this.w_buckets['jobinput']) {
		value['prevRecordType'] = 'jobinput';
		isretry = false;
	} else if (dispatch.d_origin['bucket'] ==
	    this.w_buckets['taskoutput']) {
		value['prevRecordType'] = 'taskoutput';
		isretry = false;
	} else {
		mod_assert.equal(dispatch.d_origin['bucket'],
		    this.w_buckets['task']);
		mod_assert.ok(dispatch.d_origin['value']['wantRetry']);
		value['prevRecordType'] = 'task';
		isretry = true;
	}

	value['prevRecordId'] = dispatch.d_origin['key'];
	origin_value = dispatch.d_origin['value'];

	if (!isretry) {
		value['nattempts'] = 1;
		origin_value['nextRecordType'] = 'task';
		origin_value['nextRecordId'] = task.t_id;
		origin_value['timePropagated'] = dispatch.d_time;
	} else {
		value['nattempts'] = origin_value['nattempts'] ?
		    origin_value['nattempts'] + 1 : 2;
		origin_value['timeRetried'] = dispatch.d_time;
	}

	this.w_dtrace.fire('task-dispatched', function () {
	    return ([ task.t_value['jobId'], task.t_id, task.t_value ]);
	});

	if (dispatch.d_pi > 0)
		phase = dispatch.d_job.j_phases[dispatch.d_pi - 1];

	this.w_bus.putBatch([ [
	    this.w_buckets['task'],
	    task.t_id,
	    value
	], [
	    dispatch.d_origin['bucket'],
	    dispatch.d_origin['key'],
	    origin_value,
	    { 'etag': dispatch.d_origin['_etag'] }
	] ], {
	    'retryConflict': function () {
		/*
		 * See the retryConflict function in taskRetryReduce.  The only
		 * legitimate way this can happen here is for the same reason it
		 * can happen there: the "write" that set wantRetry = true is
		 * still pending.
		 */
		mod_assert.ok(isretry);
		return (new Error('conflict while retrying map task ' +
		    '(will try again)'));
	    }
	}, function (err) {
		dispatch.d_barrier.done(dispatch.d_id);
		dispatch.d_job.j_phases[dispatch.d_pi].p_ndispatches--;

		if (dispatch.d_pi > 0 && !isretry)
			phase.p_nunpropagated--;

		worker.jobPropagateEnd(dispatch.d_job, null);
		worker.jobTick(dispatch.d_job);
		worker.taskPostDispatchCheck(task);
	});
};

/*
 * Implementation of keyDispatch() for reduce phases.  Write a taskinput record
 * for this key.
 */
Worker.prototype.dispReduce = function (dispatch)
{
	var worker = this;
	var job, pi, uuid;
	var task, reducer, phase, value, origin_value, ndels;

	job = dispatch.d_job;
	pi = dispatch.d_pi;
	mod_assert.equal(job.j_phases[pi].p_type, 'reduce');

	phase = job.j_phases[pi];
	mod_assert.ok(dispatch.d_ri !== undefined);
	mod_assert.ok(dispatch.d_ri < phase.p_reducers.length);
	reducer = phase.p_reducers[dispatch.d_ri];

	if (reducer.r_task === undefined) {
		dispatch.d_error = {
		    'code': EM_INTERNAL,
		    'message': 'internal error',
		    'messageInternal': 'no reduce task available'
		};
		this.dispError(dispatch);
		return;
	}

	task = reducer.r_task;
	uuid = mod_uuid.v4();
	value = {
	    'taskInputId': uuid,
	    'jobId': job.j_id,
	    'taskId': task.t_id,
	    'domain': job.j_job['worker'],
	    'mantaComputeId': task.t_value['mantaComputeId'],
	    'agentGeneration': worker.w_agents[
		task.t_value['mantaComputeId']].a_record['value']['generation'],
	    'input': dispatch.d_objname,
	    'p0input': dispatch.d_p0objname,
	    'account': dispatch.d_account,
	    'objectid': dispatch.d_objectid,
	    'servers': dispatch.d_locations.map(function (l) {
		return ({
		    'mantaComputeId': l['mantaComputeId'],
		    'zonename': l['zonename']
		});
	    }),
	    'timeDispatched': dispatch.d_time,
	    'prevRecordType': dispatch.d_origin['bucket'] ==
		this.w_buckets['jobinput'] ? 'jobinput' : 'taskoutput',
	    'prevRecordId': dispatch.d_origin['key']
	};

	/*
	 * This is a little odd, but if the reducer has already been marked
	 * "done", then we still issue this taskinput, but with wantInputRemoved
	 * set (if appropriate).  This causes intermediate data to be cleaned
	 * up.  We also set timeRead = true to avoid bothering the agent about
	 * this.  Note that if we ever retried errors that can cause the task to
	 * end early, this logic would need to be updated to avoid removing the
	 * intermediate object if the task is being retried.
	 */
	if (reducer.r_done) {
		if (pi === 0) {
			this.dispAbort(dispatch);
			return;
		}

		value['wantInputRemoved'] = true;
		value['timeRead'] = dispatch.d_time;
		ndels = 1;
	} else {
		ndels = 0;
	}

	origin_value = dispatch.d_origin['value'];
	origin_value['nextRecordType'] = 'taskinput';
	origin_value['nextRecordId'] = uuid;
	origin_value['timePropagated'] = dispatch.d_time;

	reducer.r_ninput++;
	job.j_ndeletes += ndels;

	this.w_dtrace.fire('taskinput-dispatched', function () {
	    return ([ task.t_value['jobId'], task.t_id, value ]);
	});

	this.w_bus.putBatch([ [
	    this.w_buckets['taskinput'], uuid, value
	], [
	    dispatch.d_origin['bucket'],
	    dispatch.d_origin['key'],
	    origin_value,
	    { 'etag': dispatch.d_origin['_etag'] }
	] ], {}, function (err) {
		if (!err)
			reducer.r_nissued++;
		else
			job.j_ndeletes -= ndels;

		dispatch.d_barrier.done(dispatch.d_id);
		dispatch.d_job.j_phases[dispatch.d_pi].p_ndispatches--;

		if (dispatch.d_pi > 0)
			job.j_phases[dispatch.d_pi - 1].p_nunpropagated--;

		worker.jobPropagateEnd(job, null);
		worker.jobTick(job);
	});
};

Worker.prototype.dispError = function (dispatch)
{
	var worker = this;
	var job, uuid, value, origin_value;

	if (dispatch.d_time === undefined)
		dispatch.d_time = mod_jsprim.iso8601(Date.now());

	job = dispatch.d_job;
	uuid = mod_uuid.v4();
	value = {
	    'errorId': uuid,
	    'jobId': job.j_id,
	    'domain': job.j_job['worker'],
	    'phaseNum': dispatch.d_pi,
	    'errorCode': dispatch.d_error['code'],
	    'errorMessage': dispatch.d_error['message'],
	    'input': dispatch.d_objname,
	    'p0input': dispatch.d_pi === 0 ? dispatch.d_objname :
		dispatch.d_origin['value']['p0input'],
	    'prevRecordType': dispatch.d_origin['bucket'] ==
		this.w_buckets['jobinput'] ? 'jobinput' : 'taskoutput',
	    'prevRecordId': dispatch.d_origin['key'],
	    'retried': false,
	    'timeCommitted': dispatch.d_time
	};

	if (dispatch.d_error['messageInternal'] !== undefined)
		value['errorMessageInternal'] =
		    dispatch.d_error['messageInternal'];

	origin_value = dispatch.d_origin['value'];
	origin_value['nextRecordType'] = 'error';
	origin_value['nextRecordId'] = uuid;
	origin_value['timePropagated'] = dispatch.d_time;

	this.w_dtrace.fire('error-dispatched',
	    function () { return ([ job.j_id, value ]); });

	this.w_bus.putBatch([ [
	    this.w_buckets['error'], uuid, value
	], [
	    dispatch.d_origin['bucket'],
	    dispatch.d_origin['key'],
	    origin_value,
	    { 'etag': dispatch.d_origin['_etag'] }
	] ], {}, function (err) {
		if (dispatch.d_pi > 0)
			job.j_phases[dispatch.d_pi - 1].p_nunpropagated--;

		dispatch.d_barrier.done(dispatch.d_id);
		dispatch.d_job.j_phases[dispatch.d_pi].p_ndispatches--;
		job.j_job['stats']['nErrors']++;

		worker.jobPropagateEnd(dispatch.d_job, null);
		worker.jobTick(dispatch.d_job);
	});
};

/*
 * Like dispError, in that we're not dispatching a next-phase record due to an
 * error condition, except that we don't even dispatch an error either.  This is
 * used when we can't dispatch a taskinput to a reducer because it's already
 * failed, but there's already an error for the reducer and we don't want to
 * also see tons of errors for every input we failed to dispatch to the reducer.
 * We still need to update the origin record so we don't keep trying to dispatch
 * something.
 */
Worker.prototype.dispAbort = function (dispatch)
{
	var worker = this;
	var job, origin_value;

	if (dispatch.d_time === undefined)
		dispatch.d_time = mod_jsprim.iso8601(Date.now());

	job = dispatch.d_job;
	origin_value = dispatch.d_origin['value'];
	origin_value['nextRecordType'] = 'none';
	origin_value['nextRecordId'] = 'none';
	origin_value['timePropagated'] = dispatch.d_time;

	this.w_bus.putBatch([ [
	    dispatch.d_origin['bucket'],
	    dispatch.d_origin['key'],
	    origin_value,
	    { 'etag': dispatch.d_origin['_etag'] }
	] ], {}, function (err) {
		if (dispatch.d_pi > 0)
			job.j_phases[dispatch.d_pi - 1].p_nunpropagated--;

		dispatch.d_barrier.done(dispatch.d_id);
		dispatch.d_job.j_phases[dispatch.d_pi].p_ndispatches--;
		worker.jobPropagateEnd(dispatch.d_job, null);
		worker.jobTick(dispatch.d_job);
	});
};

Worker.prototype.doDelete = function (delete_request, now)
{
	var key = delete_request['key'];
	var token = delete_request['token'];

	if (this.w_deletes[key]) {
		this.w_log.debug('delete "%s": already pending', key);
		return;
	}

	var worker = this;
	this.w_deletes[key] = now;
	worker.w_pending_deletes++;

	var options = {
	    'headers': {
		'authorization': sprintf('Token %s', token)
	    }
	};

	this.w_dtrace.fire('delete-start', function () { return ([ key ]); });
	this.w_manta.unlink(key, options, function (err) {
		worker.w_pending_deletes--;
		delete (worker.w_deletes[key]);

		if (err && err['name'] == 'ResourceNotFoundError')
			err = null;

		if (err) {
			worker.w_dtrace.fire('delete-done',
			    function () { return ([ key, err.name ]); });
			worker.w_log.error(err, 'delete "%s": failed', key);
		} else {
			worker.w_dtrace.fire('delete-done',
			    function () { return ([ key, '' ]); });
			worker.w_log.debug('delete "%s": okay', key);
		}

		delete_request['callback'](err);
	});
};

/*
 * Examines the state of a job's phases to determine whether end-of-input should
 * be propagated.  This is invoked when we discover that a job's input key
 * stream has been ended by the user so that we can propagate that end-of-input
 * to any waiting reduce tasks.  It's also invoked when a task completes without
 * producing output to see if that should trigger the end of a subsequent reduce
 * task.
 */
Worker.prototype.jobPropagateEnd = function (job, now)
{
	var phase, pi;

	if (!job.j_input_fully_read)
		return;

	/*
	 * This works by iterating the phases and determining whether there is
	 * or may in the future be more work to do.  If so, bail out, as some
	 * future event will cause us to reevaluate this.  If not, propagate
	 * end-of-input.
	 */
	for (pi = 0; pi < job.j_phases.length; pi++) {
		phase = job.j_phases[pi];

		if (phase.p_ndispatches > 0 ||
		    phase.p_nuncommitted > 0 ||
		    phase.p_nunpropagated > 0 ||
		    phase.p_nretryneeded > 0)
			/* Tasks still running. */
			break;
	}

	if (pi < job.j_phases.length &&
	    job.j_phases[pi].p_reducers !== undefined &&
	    job.j_phases[pi].p_ndispatches === 0) {
		if (now === null)
			now = mod_jsprim.iso8601(new Date());
		this.reduceEndInput(job, pi, now);
	}
};

Worker.prototype.reduceEndInput = function (job, i, now)
{
	var worker = this;
	var phase = job.j_phases[i];

	mod_assert.ok(phase.p_reducers !== undefined);
	phase.p_reducers.forEach(function (reducer) {
		var task = reducer.r_task;
		if (task.t_value['timeInputDone'])
			return;

		if (task.t_value['timeAbandoned'] !== undefined) {
			job.j_log.info('skipping input-done for abandoned ' +
			    'reduce task "%s"', task.t_id);
			return;
		}

		job.j_log.info('marking input done for ' +
		    'reduce phase %d (task %s)', i, task.t_id);
		task.t_value['timeInputDone'] = now;
		task.t_value['nInputs'] = reducer.r_ninput;
		worker.w_dtrace.fire('task-input-done', function () {
			return ([ job.j_id, task.t_id, task.t_value ]);
		});

		var options = { 'etag': task.t_etag };
		var abandoned = false;

		worker.w_bus.putBatch([
		    [ worker.w_buckets['task'], task.t_id, task.t_value,
		    options ] ], {
			'retryConflict': function (oldrec, newrec) {
				if (oldrec['value']['timeAbandoned'] !==
				    undefined) {
					abandoned = true;
					return (new VError('task was ' +
					    'abandoned while we tried to ' +
					    'mark it done'));
				}

				/*
				 * The only thing that may be written to this
				 * task behind our back is "state", which may
				 * now be "accepted" instead of "dispatched".
				 * (It's also possible to write "timeCancelled",
				 * but that should result in a failure here
				 * anyway.)
				 */
				return (mod_bus.mergeRecords([ 'timeAccepted',
				    'state' ], ['timeInputDone', 'nInputs' ],
				    oldrec['value'], newrec['value']));
			}
		    }, function (err, etags) {
			if (!err) {
				task.t_etag = etags['etags'][0]['etag'];
				mod_assert.ok(task.t_etag !== undefined);
				return;
			}

			if (abandoned)
				return;

			worker.jobError(job, i, now, EM_INTERNAL,
			    'internal error ending reduce task input',
			    'error ending reduce task input: ' + err.message);
		    });
	});
};

/*
 * Kang (introspection) entry points
 */

Worker.prototype.kangListTypes = function ()
{
	return (this.w_bus.kangListTypes().concat([ 'job', 'worker' ]));
};

Worker.prototype.kangListObjects = function (type)
{
	if (type == 'worker')
		return ([ this.w_uuid ]);

	if (type == 'job')
		return (Object.keys(this.w_jobs));

	return (this.w_bus.kangListObjects(type));
};

Worker.prototype.kangGetObject = function (type, id)
{
	if (type == 'worker')
		return (this.debugState());

	if (type == 'job')
		return (this.w_jobs[id].debugState());

	return (this.w_bus.kangGetObject(type, id));
};

Worker.prototype.kangSchema = function (type)
{
	if (type == 'job') {
		return ({
		    'summaryFields': [
			'state',
			'input_done',
			'input_fully_read',
			'cancelled',
			'nlocates',
			'nauths'
		    ]
		});
	}

	if (type == 'worker') {
		return ({
		    'summaryFields': [
			'nLocs',
			'nAuths',
			'nDels'
		    ]
		});
	}

	return (this.w_bus.kangSchema(type));
};

Worker.prototype.kangStats = function ()
{
	return (mod_jsprim.deepCopy(this.w_stats));
};


/*
 * Return a string constructed by swapping the first component of "path" with
 * "newfirst".  "path" should have previously been normalized, and it should
 * contain at least two components.  This is used to change a key from the
 * "/:account_uuid/path" form to the "/:login/path" form or vice versa.
 */
function pathSwapFirst(path, newfirst)
{
	var i, j;
	i = path.indexOf('/');
	j = path.indexOf('/', i + 1);

	if (i == -1 || j == -1)
		return (null);

	return ('/' + newfirst + path.substr(j));
}

/*
 * Returns true if the given key represents an "anonymous" intermediate object
 * created by Marlin.  These keys can be identified because they're stored
 * under "/:login/jobs/:jobid/stor" rather than "/:login/stor".
 */
function keyIsAnonymous(key, jobid)
{
	var re = new RegExp('^/[^/]+/jobs/' + jobid + '/stor/');
	return (re.test(key));
}

function isAuthorized(job, account, key)
{
	/*
	 * Common case: user is allowed access to their own objects.
	 */
	if (account == job.j_job['auth']['uuid'])
		return (true);

	/*
	 * Operators are allowed to access anyone's objects.
	 */
	if (job.j_job['auth']['groups'].indexOf('operators') != -1)
		return (true);

	/*
	 * Anyone is allowed to access '/anybody_else/public'.
	 */
	var i, j, k;
	if ((i = key.indexOf('/')) !== 0 ||
	    (j = key.indexOf('/', i + 1)) == -1 ||
	    (k = key.indexOf('/', j + 1)) == -1)
		return (false);

	return (key.substr(j + 1, k - j - 1) == 'public');
}
