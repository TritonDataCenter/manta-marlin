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
 * which jobs are submitted and monitored) goes through Moray.  The Moray
 * interface is abstracted out so that it can be replaced with an alternative
 * mechanism for testing.
 *
 * Jobs run through the following states:
 *
 *                              +
 *                              | Discover new or abandoned job
 *                              v
 *                         UNASSIGNED
 *                              |
 *                              | Successfully write assignment record
 *                              v
 *                  +---- INITIALIZING
 *                  |           |
 *                  |           | Finish retrieving all existing records
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
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_util = require('util');

var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_locator = require('./locator');
var mod_mamoray = require('../moray');
var mod_moray = require('./moray');
var mod_schema = require('../schema');
var mod_mautil = require('../util');


/* Public interface */
exports.mwConfSchema = mwConfSchema;
exports.mwWorker = Worker;


/*
 * Configuration file JSON schema
 */
var mwConfSchema = {
    'type': 'object',
    'properties': {
	'instanceUuid': mod_schema.sStringRequiredNonEmpty,
	'port': mod_schema.sTcpPortRequired,
	'moray': {
	    'required': true,
	    'type': 'object',
	    'properties': {
		'indexing': {
		    'required': true,
		    'type': 'object',
		    'properties': {
			'urls': {
			    'required': true,
			    'type': 'array',
			    'items': mod_schema.sStringRequiredNonEmpty,
			    'minItems': 1
			}
		    }
		},
		'storage': {
		    'required': true,
		    'type': 'object',
		    'properties': {
			'url': mod_schema.sStringRequiredNonEmpty
		    }
		}
	    }
	},
	'locator': {
	    'type': 'string',
	    'enum': [ 'test', 'mock', 'manta' ]
	},
	'buckets': {
	    'required': true,
	    'type': 'object',
	    'properties': {
		'job': mod_schema.sStringRequiredNonEmpty,
		'jobinput': mod_schema.sStringRequiredNonEmpty,
		'task': mod_schema.sStringRequiredNonEmpty,
		'taskinput': mod_schema.sStringRequiredNonEmpty,
		'taskoutput': mod_schema.sStringRequiredNonEmpty
	    }
	},
	'tunables': {
	    'maxPendingLocates': mod_schema.sIntervalRequired,
	    'maxPendingPuts': mod_schema.sIntervalRequired,
	    'timeJobAbandon': mod_schema.sIntervalRequired,
	    'timeJobSave': mod_schema.sIntervalRequired,
	    'timePoll': mod_schema.sIntervalRequired,
	    'timeTaskAbandon': mod_schema.sIntervalRequired,
	    'timeTick': mod_schema.sIntervalRequired
	}
    }
};


/*
 * Bucket poll configuration.  Each entry below describes how to poll a
 * particular Moray bucket.  The two fields for each bucket are:
 *
 *	cache		If true, cache the etags and mtimes of the last object
 *			seen for each key and emit only those objects whose
 *			mtimes have changed.
 *
 *	mkfilter	Function which takes a reference to the worker and
 *			returns a filter suitable for passing to "findObjects".
 */
var mwPollConfig = {
    'job':		{ 'cache': true,   'mkfilter': pfJob		},
    'jobinput': 	{ 'cache': false,  'mkfilter': pfJobInput	},
    'task':		{ 'cache': true,   'mkfilter': pfTask		},
    'taskinput':	{ 'cache': false,  'mkfilter': pfTaskInput	},
    'taskoutput':	{ 'cache': false,  'mkfilter': pfTaskOutput	}
};

function pfJob(worker)
{
	return (worker.w_moray.filterJobs(
	    worker.w_uuid, worker.w_time_job_abandon));
}

function pfJobInput(worker)
{
	var watch;

	watch = [];
	mod_jsprim.forEachKey(worker.w_jobs, function (jobid, job) {
		if (job.j_input_done === undefined)
			watch.push(jobid);
	});

	return (worker.w_moray.filterJobInputs(watch));
}

function pfTask(worker)
{
	var watch = [];

	mod_jsprim.forEachKey(worker.w_jobs, function (jobid, job) {
		if (job.j_state == 'initializing' ||
		    job.j_state == 'running' ||
		    job.j_state == 'finishing')
			watch.push(jobid);
	});

	return (worker.w_moray.filterTasks(watch));
}

function pfTaskInput(worker)
{
	var watch = [];

	mod_jsprim.forEachKey(worker.w_jobs, function (jobid, job) {
		if (job.j_state == 'initializing')
			watch.push(jobid);
	});

	return (worker.w_moray.filterTaskInputs(watch));
}

function pfTaskOutput(worker)
{
	var watch = [];

	mod_jsprim.forEachKey(worker.w_jobs, function (jobid, job) {
		if (job.j_state == 'initializing' ||
		    job.j_state == 'running')
			watch.push(jobid);
	});

	return (worker.w_moray.filterTaskOutputs(watch));
}


/*
 * Maintains state for a single job.  The logic for this class lives in the
 * worker class.  Arguments include:
 *
 *    log	bunyan-style logger
 *
 *    record	initial job record, as returned from Moray
 */
function WorkerJobState(args)
{
	var j = args['record']['value'];

	this.j_job = j;				/* in-moray job record */
	this.j_id = j['jobId'];			/* immutable job id */
	this.j_log = args['log'];		/* job-specific logger */

	this.j_state = 'unassigned';		/* current state (see above) */
	this.j_state_time = new Date();		/* time of last state change */
	this.j_dropped = undefined;		/* time the job was dropped */
	this.j_input_done = j['timeInputDone'];	/* time input was completed */
	this.j_cancelled = j['timeCancelled'];	/* time job was cancelled */

	this.j_tasks = {};			/* set of all tasks */
	this.j_phases = j['phases'].map(
	    function (phase) { return (new WorkerJobPhase(phase)); });

	this.j_save_throttle =
	    new mod_mautil.Throttler(this.j_time_job_save);
	this.j_save = new mod_mautil.SaveGeneration();

	/*
	 * Before initializing the job, we'll retrieve all of its records in all
	 * Marlin-related buckets.  We store these records in j_init_records
	 * until we have them all.  We store the set of buckets we haven't
	 * finished loading in j_init_waiting.
	 */
	this.j_init_start = undefined;

	this.j_init_records = {
	    'jobinput': {},
	    'task': {},
	    'taskinput': {},
	    'taskoutput': {}
	};

	this.j_init_waiting = {
	    'jobinput': undefined,
	    'task': undefined,
	    'taskinput': undefined,
	    'taskoutput': undefined
	};
}

WorkerJobState.prototype.debugState = function ()
{
	return ({
	    'record': this.j_job,
	    'state': this.j_state,
	    'state_time': this.j_state_time,
	    'dropped': this.j_dropped,
	    'input_done': this.j_input_done,
	    'cancelled': this.j_cancelled,
	    'phases': this.j_phases,
	    'save_throttle': this.j_save_throttle,
	    'save_gen': this.j_save,
	    'init_start': this.j_init_start,
	    'init_records': this.j_init_records,
	    'init_waiting': this.j_init_waiting
	});
};


/*
 * Stores runtime state about each phase in a job.
 */
function WorkerJobPhase(phase)
{
	this.p_type = phase['type'];	/* phase type (same as in job record) */
	this.p_npending = 0;		/* number of uncommitted tasks */
	this.p_task = null;		/* task record (reduce phases only) */
	this.p_ninput = 0;		/* total number of input keys */
}


/*
 * Stores information about a specific task within a job.
 */
function WorkerTask(jobid, pi, timeDispatched)
{
	var taskid = mod_uuid.v4();

	this.t_id = taskid;		/* unique task identifier */
	this.t_xoutputs = [];		/* output keys NOT inside task record */
	this.t_record = undefined;	/* last received record for this task */
	this.t_value = {		/* authoritative task record */
	    'jobId': jobid,
	    'taskId': taskid,
	    'phaseNum': pi,
	    'state': 'dispatched',
	    'timeDispatched': timeDispatched
	};
}


/*
 * Stores current state associated with remote agents.
 */
function WorkerAgent()
{
	this.a_last = 0;	/* time (msec since epoch) of last contact */
	this.a_tasks = {};	/* set of uncompleted tasks assigned to agent */
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
	var worker, conf, error;

	worker = this;
	conf = args['conf'];
	mod_assert.ok(args.hasOwnProperty('log'), '"log" is required');
	mod_assert.equal('object', typeof (conf), '"conf" must be an object');

	args['log'].info('worker configuration', args['conf']);
	error = mod_jsprim.validateJsonObject(mwConfSchema, args['conf']);
	if (error) {
		args['log'].fatal(error, 'invalid configuration');
		throw (error);
	}

	/* immutable configuration */
	this.w_uuid = conf['instanceUuid'];
	this.w_conf = mod_jsprim.deepCopy(conf);
	this.w_buckets = this.w_conf['buckets'];
	this.w_max_pending_locates = conf['tunables']['maxPendingLocates'];
	this.w_max_pending_puts = conf['tunables']['maxPendingPuts'];
	this.w_time_tick = conf['tunables']['timeTick'];
	this.w_time_poll = conf['tunables']['timePoll'];
	this.w_time_job_abandon = conf['tunables']['timeJobAbandon'];
	this.w_time_job_save = conf['tunables']['timeJobSave'];
	this.w_time_task_abandon = conf['tunables']['timeTaskAbandon'];

	this.w_names = {};
	mod_jsprim.forEachKey(this.w_buckets, function (name, bucket) {
		worker.w_names[bucket] = name;
	});

	/* helper objects */
	this.w_log = args['log'].child({
	    'component': 'worker-' + this.w_uuid
	});

	this.w_locator = mod_locator.createLocator(args['conf'], {
	    'log': this.w_log.child({ 'component': 'manta-locator' })
	});

	this.w_moray = mod_moray.createMoray(args['conf'], {
	    'log': this.w_log.child({ 'component': 'moray-client' })
	});

	this.w_cache = new mod_mamoray.MorayEtagCache();
	this.w_cache.on('record', worker.onRecord.bind(this));
	this.w_polls = {};

	mod_jsprim.forEachKey(mwPollConfig, function (name, genconf) {
		worker.w_polls[name] = {
		    'throttle': new mod_mautil.Throttler(worker.w_time_poll),
		    'cache': genconf['cache'] ? worker.w_cache : null,
		    'mkfilter': genconf['mkfilter'].bind(null, worker)
		};
	});

	/* global dynamic state */
	this.w_pending_locates = 0;		/* nr of pending locate ops */
	this.w_pending_puts = 0;		/* nr of pending put ops */
	this.w_worker_start = undefined;	/* time worker started */
	this.w_worker_stopped = undefined;	/* time worker stopped */
	this.w_tick_start = undefined;		/* time last tick started */
	this.w_tick_done = undefined;		/* time last tick finished */
	this.w_timeout = undefined;		/* JS timeout handle */
	this.w_jobs = {};			/* all jobs, by jobId */

	this.w_stats = {			/* stat counters */
	    'asgn_failed': 0,			/* failed job assignments */
	    'asgn_restart': 0			/* jobs picked up on restart */
	};

	/*
	 * Set of remote agents that we know about.  We keep track of assigned
	 * tasks and the last time we've received an update from each one so
	 * that we can quickly deal with agents going AWOL.  See WorkerAgent.
	 */
	this.w_agents = {};

	/*
	 * Set of all pending tasks.  See WorkerTask.
	 */
	this.w_tasks = {};

	/*
	 * We keep track of pending work on several queues.  A given input key
	 * basically goes through the system in the following order.
	 */
	this.w_jobinputs_in = [];	/* incoming "job input" records */

	/*
	 * Outgoing locate requests are specified simply by Manta key (as a
	 * string).  The responses are enqueued on w_locates_in as an object:
	 *
	 *	lr_key		Manta key located
	 *
	 *	lr_objectid	Manta object uuid
	 *
	 *	lr_locations	Array of locations, each an object:
	 *
	 *	    lrl_server		physical server uuid
	 *
	 *	    lrl_zonename	mako zone uuid
	 *
	 * The pending requests are stored as objects in w_locates with:
	 *
	 *	l_key		Manta key to be located
	 *
	 *	l_origins	List of jobinput or taskoutput records that are
	 *			waiting on the result of this request
	 */
	this.w_locates_out = [];	/* outgoing "locate" requests */
	this.w_locates_in = [];	/* incoming "locate" responses */
	this.w_locates = {};		/* pending "locate" requests */

	/*
	 * We keep track of queued and pending save operations in w_records,
	 * indexed by bucket and then key.  The contents is an object with:
	 *
	 *    r_queued		time when the record was queued for write
	 *
	 *    r_issued		time when the write request was issued
	 *
	 *    r_attempts	number of previous failed attempts
	 *
	 *    r_value		value to save into Moray
	 *
	 *    r_options		options to pass through to putObject
	 *
	 *    r_callback	callback to be invoked when the operation
	 *    			completes successfully.  Failures will be
	 *    			retried without invoking the callback.
	 *
	 * Queued requests are stored on w_records_out as objects with:
	 *
	 *    q_bucket, q_key	identifies the corresopnding record in
	 *    			w_records.
	 *
	 * This design does support modifying a dirty object by simply updating
	 * the value that will be written, but it does not allow modifying an
	 * object with a pending write.  There's no general way to resolve
	 * conflicts between local changes and remote updates, and the overall
	 * Marlin design deliberately avoids needing to do so except in simple
	 * cases.
	 */
	this.w_records = {};
	mod_jsprim.forEachKey(this.w_buckets, function (_, bucket) {
		this.w_records[bucket] = {};
	});
	this.w_records_out = [];	/* outgoing record writes */
	this.w_tasks_in = [];		/* incoming "task" record updates */
	this.w_taskoutputs_in = [];	/* incoming "task output" records */
}

Worker.prototype.debugState = function ()
{
	return ({
	    'conf': this.w_conf,
	    'pending_locates': this.w_pending_locates,
	    'pending_puts': this.w_pending_puts,
	    'worker_start': this.w_worker_start,
	    'worker_stopped': this.w_worker_stopped,
	    'tick_start': this.w_tick_start,
	    'tick_done': this.w_tick_done,
	    'agents': this.w_agents,
	    'njobinputs_in': this.w_jobinputs_in.length,
	    'ntasks_in': this.w_tasks_in.length,
	    'ntaskoutputs_in': this.w_taskoutputs_in.length,
	    'nlocates_in': this.w_locates_in.length,
	    'nlocates_out': this.w_locates_out.length,
	    'nrecords_out': this.w_records_out.length
	});
};

/*
 * Kang (introspection) entry point.
 */
Worker.prototype.kangStats = function (callback)
{
	return (mod_jsprim.deepCopy(this.w_stats));
};

/*
 * Fetch stats.  This call is used by the tests, but is asynchronous so that it
 * can be replaced in the future with a call to a remote worker.
 */
Worker.prototype.stats = function (callback)
{
	var stats = mod_jsprim.deepCopy(this.w_stats);
	process.nextTick(function () { callback(null, stats); });
	return (undefined);
};

/*
 * Start the worker: connect to Moray and start looking for work to do.
 */
Worker.prototype.start = function ()
{
	var log = this.w_log;
	var next = this.tick.bind(this);

	mod_assert.ok(this.w_worker_start === undefined);
	this.w_worker_start = new Date();

	log.info('initializing moray');
	this.w_moray.setup(function (err) {
		if (err) {
			log.fatal(err, 'failed to initialize moray');
			throw (err);
		}

		log.info('starting worker');
		process.nextTick(next);
	});
};

/*
 * Stop the worker: stop polling on Moray and invoke "callback" when all pending
 * operations have stopped.
 */
Worker.prototype.stop = function (callback)
{
	this.w_log.info('shutting down worker');
	this.w_worker_stopped = new Date();

	if (this.w_timeout) {
		clearTimeout(this.w_timeout);
		this.w_timeout = undefined;
	}

	if (callback)
		this.w_moray.drain(callback);
};

/*
 * The heart of the job worker: this function is invoked periodically to poll
 * Moray, evaluate timeouts, and evaluate job state.
 */
Worker.prototype.tick = function ()
{
	var now, worker, jobid, name;

	this.w_timeout = undefined;
	this.w_tick_start = new Date();
	now = this.w_tick_start.getTime();
	worker = this;

	/* Poll Moray for updates. */
	for (name in this.w_polls)
		this.poll(name, now);

	/* Check whether each job needs to be saved. */
	for (jobid in this.w_jobs)
		this.jobTick(this.w_jobs[jobid]);

	/* Process incoming messages and send queued outgoing messages. */
	this.processQueues();

	/* Look for agents that we haven't heard from in a while. */
	mod_jsprim.forEachKey(this.w_agents, function (host, agent) {
		var ntasks = 0;

		if (mod_jsprim.isEmpty(agent.a_tasks) || !agent.a_last ||
		    now - agent.a_last <= worker.w_time_task_abandon)
			return;

		delete (worker.w_agents[host]);

		mod_jsprim.forEachKey(agent.a_tasks, function (taskid) {
			ntasks++;
			worker.taskTimedOut(worker.w_tasks[taskid], now);
		});

		worker.w_log.info('agent "%s" has timed out (no response in ' +
		    '%d ms); cancelling %d tasks', now - agent.a_last, ntasks);
	});

	this.w_tick_done = new Date();
	this.w_timeout = setTimeout(this.tick.bind(this), this.w_time_tick);
};

/*
 * Poll for updates to the named bucket.
 */
Worker.prototype.poll = function (name, now)
{
	var bucket, pollconf, throttle, filter, worker, req;

	bucket = this.w_buckets[name];
	pollconf = this.w_polls[name];
	throttle = pollconf['throttle'];

	if (throttle.tooRecent())
		return;

	filter = pollconf['mkfilter']();
	if (!filter)
		return;

	throttle.start();
	worker = this;
	req = this.w_moray.findObjects(bucket, filter, { 'noCache': true });

	req.on('error', function (err) {
		throttle.done();
		worker.w_log.warn(err, 'failed to poll "%s"', name);
	});

	req.on('record', function (record) {
		if (pollconf['cache'])
			worker.onRecord(record, true);
		else
			pollconf['cache'].record(record);
	});

	req.on('end', function () {
		throttle.done();
		worker.onSearchDone(name, now);
	});
};

/*
 * Invoked to process an incoming Moray record.
 */
Worker.prototype.onRecord = function (record, changed)
{
	var name, error, job;

	name = this.w_names[record['bucket']];

	/*
	 * If this is a task record from a remote agent, updated our last
	 * contact time for that agent.  We do this before just about anything
	 * else, even if we end up rejecting the record later for some other
	 * reason.
	 */
	if (name == 'task' && record['value']['server']) {
		var server = record['value']['server'];
		if (!this.w_agents.hasOwnProperty(server))
			this.w_agents[server] = new WorkerAgent();

		if (record['_mtime'] > this.w_agents[server].a_last)
			this.w_agents[server].a_last = record['_mtime'];
	}

	/*
	 * Having updated the last contact time, we don't care about records
	 * whose contents haven't actually changed since we last saw them.
	 */
	if (!changed)
		return;

	error = mod_jsprim.validateJsonObject(
	    mod_schema.sBktJsonSchemas[record['bucket']], record['value']);
	if (error) {
		this.w_log.warn(error, 'onRecord: validation error', record);
		return;
	}

	if (name == 'job') {
		this.jobUpdate(record);
		return;
	}

	if (!this.w_jobs.hasOwnProperty(record['value']['jobId'])) {
		this.w_log.warn('onRecord: no such jobId', record);
		return;
	}

	job = this.w_jobs[record['value']['jobId']];
	if (job.j_state == 'unassigned' || job.j_state == 'finishing') {
		job.j_log.info('onRecord: ignoring update in job state "%s"',
		    job.j_state, record);
		return;
	}

	/*
	 * While a job is still in the "initializing" state, buffer all incoming
	 * records until all they've all been received.
	 */
	if (job.j_state == 'initializing') {
		job.j_init_records[name][record['key']] = record;
		return;
	}

	mod_assert.ok(job.j_state == 'running');

	if (name == 'taskinput') {
		job.j_log.warn('onRecord: ignoring update in job state "%s"',
		    job.j_state, record);
		return;
	}

	if (name == 'jobinput')
		this.w_jobinputs_in.push(record);
	else if (name == 'task')
		this.w_tasks_in.push(record);
	else if (name == 'taskoutput')
		this.w_taskoutputs_in.push(record);
};

/*
 * Invoked when a Moray search request completes.  This only affects jobs in the
 * "initializing" state, which need to know when they've loaded all existing
 * state for a job.
 */
Worker.prototype.onSearchDone = function (name, start)
{
	var worker = this;

	if (name == 'job')
		return;

	mod_jsprim.forEachKey(this.w_jobs, function (jobid, job) {
		if (job.j_state !== 'initializing')
			return;

		if (start <= job.j_init_start)
			return;

		job.j_log.info('read all "%s" records', name);
		delete (job.j_init_waiting[name]);

		if (mod_jsprim.isEmpty(job.j_init_waiting))
			worker.jobInit(job);
	});
};

/*
 * Invoked when we receive a new job record that we don't already own.
 */
Worker.prototype.jobCreate = function (record)
{
	var job;

	job = record['value'];
	mod_assert.ok(!this.w_jobs.hasOwnProperty(job['jobId']));

	if (job['worker'] == this.w_uuid) {
		this.w_log.info('resuming our own job: %s', job['jobId']);
		this.w_stats['asgn_restart']++;
	}

	this.w_jobs[job['jobId']] = new WorkerJobState({
	    'record': record,
	    'log': this.w_log.child({ 'component': 'job-' + job['jobId'] })
	});

	this.jobAssign(this.w_jobs[job['jobId']]);
};

/*
 * Process an incoming Moray record for a job.
 */
Worker.prototype.jobUpdate = function (record)
{
	var jobid, job;

	jobid = record['value']['jobId'];

	if (!this.w_jobs.hasOwnProperty(jobid)) {
		this.jobCreate(record);
		return;
	}

	/*
	 * We don't want to clobber job.j_job because we may have various pieces
	 * of dirty state in our local copy.  Fortunately, the only state
	 * updates we can receive from elsewhere are that the job was cancelled
	 * or that its input was completed.  We check for these specific cases
	 * and update both our internal state as well as our copy of the full
	 * job record to make sure we don't clobber these changes on the next
	 * save.
	 */
	job = this.w_jobs[jobid];
	job.j_etag = record['_etag'];

	if (job.j_state != 'unassigned' &&
	    record['value']['worker'] != this.w_uuid) {
		/* We've lost the lock! Drop the job immediately. */
		job.j_log.error('job lock lost (now assigned to %j)',
		    record['value']['worker']);
		this.jobRemove(job);
	}

	if (job.j_cancelled) {
		job.j_log.warn('ignoring update (job cancelled');
		return;
	}

	if (record['value']['timeCancelled']) {
		job.j_cancelled = record['value']['timeCancelled'];
		job.j_job['timeCancelled'] = job.j_cancelled;
		job.j_log.info('job cancelled');
		this.jobRemove(job);
		return;
	}

	if (job.j_input_done === undefined &&
	    record['value']['timeInputDone']) {
		job.j_input_done = record['value']['timeInputDone'];
		job.j_job['timeInputDone'] = job.j_input_done;
		job.j_log.info('job input completed');
		this.jobTick(job);
		return;
	}
};

/*
 * Invoked to completely remove a job from this worker.
 */
Worker.prototype.jobRemove = function (job)
{
	var npending, taskid;

	if (job.j_state == 'unassigned')
		this.w_stats['asgn_failed']++;

	job.j_dropped = new Date();
	delete (this.w_jobs[job.j_id]);
	job.j_log.info('job removed');

	if (job.j_save_throttle.ongoing())
		job.j_log.info('job removed with pending save operation');

	this.w_cache.remove(this.w_buckets['job'], this.j_id);

	npending = 0;
	for (taskid in job.j_tasks) {
		if (this.writePending('task', taskid))
			npending++;

		delete (this.w_tasks[taskid]);
		this.w_cache.remove(this.w_buckets['task'], taskid);
	}

	if (npending > 0)
		job.j_log.info('job removed with %d pending task saves',
		    npending);
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
	mod_assert.ok(job.j_dropped === undefined);

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

	if (this.jobDone(job)) {
		job.j_save.markDirty();
		this.jobTransition(job, 'running', 'finishing');
	}
};

/*
 * Attempt to move this job from "unassigned" state to "initializing" by
 * updating the Moray job record to have job['worker'] == our uuid.
 */
Worker.prototype.jobAssign = function (job)
{
	mod_assert.equal(this.j_state, 'unassigned');

	if (job.j_job['worker']) {
		job.j_log.info('attempting to steal job from %s',
		    job.j_job['worker']);
	} else {
		job.j_log.info('attempting to take unassigned job');
	}

	job.j_job['state'] = 'running';
	job.j_job['worker'] = this.w_uuid;
	job.j_save.markDirty();
	this.jobSave(job);
};

Worker.prototype.jobSave = function (job)
{
	var worker = this;

	mod_assert.ok(!job.j_save_throttle.ongoing());
	job.j_save_throttle.start();
	job.j_log.debug('saving job (%j)', job.j_save);
	job.j_save.saveStart();

	job.j_moray.putObject(this.w_buckets['job'], job.j_id,
	    job.j_job, { 'etag': job.j_etag }, function (err) {
		if (err) {
			/*
			 * The most likely failures here are transient failures
			 * to connect to Moray and conflict errors resulting
			 * from concurrent modifications by muskie.  In both
			 * cases, we simply retry again.  The conflict case
			 * should be resolved when we see the updated job
			 * record from Moray and update our own copy and etag.
			 */
			job.j_log.warn(err, 'failed to save job');
			job.j_save.saveFailed();
		} else {
			job.j_save.saveOk();
			job.j_log.info('saved job (%j)', job.j_save);
		}

		job.j_save_throttle.done();

		if (job.j_state == 'unassigned' && !job.j_dropped) {
			if (err) {
				job.j_log.warn('failed to assign job');
				worker.jobRemove(job);
			} else {
				job.j_log.info('successfully assigned job');
				job.j_init_start = Date.now();
				worker.jobTransition(
				    job, 'unassigned', 'initializing');
			}
		}

		if (job.j_state == 'finishing' && !job.j_save.dirty())
			worker.jobRemove(job);
	});
};

/*
 * Invoked when we've finished loading all existing state on this job.
 */
Worker.prototype.jobInit = function (job)
{
	mod_assert.ok(mod_jsprim.isEmpty(job.j_init_waiting));
	mod_assert.equal(job.j_state, 'initializing');

	/* XXX compute the current state based on what's here. */

	this.jobTransition(job, 'initializing', 'running');
};

/*
 * Returns true iff the job is complete.
 */
Worker.prototype.jobDone = function (job)
{
	var pi;

	if (job.j_state != 'running' && job.j_state != 'finishing')
		return (false);

	if (!job.j_input_done)
		return (false);

	for (pi = 0; pi < job.j_phases.length; pi++) {
		if (job.j_phases[pi].p_npending > 0)
			return (false);
	}

	return (true);
};

Worker.prototype.enqueue = function (name, key, value, options, callback)
{
	var bucket, c;

	bucket = this.w_buckets[name];
	c = this.w_records[bucket];

	/*
	 * It's illegal to enqueue a write for a record which is either already
	 * dirty or for which a write is already pending.  See above for
	 * details.
	 */
	mod_assert.ok(!c.hasOwnProperty(key),
	    'attempted write of dirty ' + name);

	c[key] = {
	    'r_queued': Date.now(),
	    'r_issued': undefined,
	    'r_attempts': 0,
	    'r_bucket': bucket,
	    'r_key': key,
	    'r_value': value,
	    'r_options': options || {},
	    'r_callback': callback
	};

	this.w_records_out.push({ 'q_bucket': bucket, 'q_key': key });
};

Worker.prototype.reenqueue = function (ent)
{
	ent.r_attempts++;
	ent.r_queued = Date.now();
	ent.r_issued = undefined;

	this.w_records_out.push({
	    'q_bucket': ent.r_bucket,
	    'q_key': ent.r_key
	});
};

Worker.prototype.writePending = function (name, key)
{
	var bucket = this.w_buckets[name];
	return (this.w_records[bucket].hasOwnProperty(key));
};

/*
 * Most of the work done by this service goes through one of several queues
 * documented in Worker above.  This function is invoked periodically to
 * process work on each of these queues.
 */
Worker.prototype.processQueues = function ()
{
	var now, ent, key, loc, lreq, i, task, len, job;
	var changedtasks = {};
	var queries = [];

	now = mod_jsprim.iso8601(new Date());

	while (this.w_jobinputs_in.length > 0) {
		ent = this.w_jobinputs_in.shift();
		key = ent['value']['key'];
		this.keyLocate(key, ent);
	}

	while (this.w_locates_in.length > 0) {
		loc = this.w_locates_in.shift();
		lreq = this.w_locates[loc.lr_key];
		mod_assert.ok(lreq);

		for (i = 0; i < lreq.l_origins.length; i++) {
			ent = lreq.l_origins[i];
			this.keyDispatch(ent, loc, now);
		}

		delete (this.w_locates[loc.lr_key]);
	}

	while (this.w_tasks_in.length > 0) {
		ent = this.w_tasks_in.shift();
		task = this.w_tasks[ent['value']['taskId']];

		if (!task) {
			this.w_log.warn(
			    'task record references unknown task', ent);
			continue;
		}

		/*
		 * If we've got a pending write for this task already, then
		 * we've either timed it out, cancelled it, or we've got a
		 * pending update to timeInputDone.  In the first two cases, we
		 * can ignore the external update because we've already given up
		 * on this task.  In the last case, the only relevant external
		 * update is a state change to "aborted", in which case we
		 * want to clobber our own change.
		 */
		if (this.writePending('task', task.t_id) &&
		    (ent['value']['timeInputDone'] ||
		    !task.t_value['timeInputDone'] ||
		    ent['value']['state'] != 'aborted')) {
			job.j_log.warn('task "%s": ignoring update because ' +
			    'a write is already pending', task.t_id, ent);
			continue;
		}

		task.t_record = ent;
		task.t_value = ent['value'];
		changedtasks[task.t_id] = true;
	}

	while (this.w_taskoutputs_in.length > 0) {
		ent = this.w_taskoutputs_in.shift();
		task = this.w_tasks[ent['value']['taskId']];

		if (!task) {
			this.w_log.warn(
			    'taskoutput record references unknown task', ent);
			continue;
		}

		/* See above. */
		if (this.writePending('task', task.t_id)) {
			job.j_log.warn('task "%s": ignoring new taskoutput ' +
			    'because a task write is already pending',
			    task.t_id, ent);
			continue;
		}

		task.t_xoutputs.push(ent['value']);
		changedtasks[task.t_id] = true;
	}

	for (key in changedtasks) {
		task = this.w_tasks[key];
		job = this.w_jobs[task.t_value['jobId']];
		len = task.t_xoutputs.length +
		    task.t_value['firstOutputs'].length;

		/*
		 * Wait until the task is finished running and, if successful,
		 * we've collected all of its output.
		 */
		if (task.t_value['state'] == 'running')
			continue;

		if (task.t_value['state'] == 'done' &&
		    task.t_value['nOutputs'] > len)
			continue;

		/*
		 * For now, we commit the result in both the "aborted" and
		 * "done" cases.  Muskie should report errors from committed,
		 * aborted tasks as well as output keys from committed, done
		 * tasks in the final phase.  When we later add retries, we
		 * won't necessarily commit here if the task is aborted.
		 */
		mod_assert.ok(task.t_value['state'] == 'aborted' ||
		    (task.t_value['state'] == 'done' &&
		    task.t_value['nOutputs'] <= len));

		if (task.t_value['nOutputs'] < len)
			job.j_log.warn(
			    'task "%s" emitted extra output keys', task.t_id);

		this.taskCommit(task, now);
	}

	if (this.w_pending_locates < this.w_max_pending_locates &&
	    this.w_locates_out.length > 0) {
		queries = this.w_locates_out.splice(0,
		    this.w_max_pending_locates - this.w_pending_locates);
		this.w_pending_locates += queries.length;
		this.w_locator.locate(queries,
		    this.onLocate.bind(this, queries));
	}

	while (this.w_pending_puts < this.w_max_pending_puts &&
	    this.w_records_out.length > 0) {
		ent = this.w_records_out.shift();
		this.recordSave(ent);
	}

	/*
	 * By this point, we should have processed all incoming records, and
	 * either the output queues should be empty or we've reached internal
	 * limits.
	 */
	mod_assert.equal(this.w_jobinputs_in.length, 0);
	mod_assert.equal(this.w_locates_in.length, 0);
	mod_assert.equal(this.w_tasks_in.length, 0);
	mod_assert.equal(this.w_taskoutputs_in.length, 0);
	mod_assert.ok(this.w_locates_out.length === 0 ||
	    this.w_pending_locates === this.w_max_pending_locates);
	mod_assert.ok(this.w_records_out.length === 0 ||
	    this.w_pending_puts === this.w_max_pending_puts);
};

/*
 * Enqueue a request to locate key "key", triggered by record "ent".
 */
Worker.prototype.keyLocate = function (key, ent)
{
	if (this.w_locates.hasOwnProperty(key)) {
		this.w_locates[key].l_origins.push(ent);
		return;
	}

	this.w_locates[key] = {
	    'l_key': key,
	    'l_origins': [ ent ]
	};

	this.w_locates_out.push(key);
};

/*
 * Create a new "task" record for phase "pi" of job "job".
 */
Worker.prototype.taskCreate = function (job, pi, now)
{
	mod_assert.ok(pi >= 0 && pi < job.j_phases.length);

	var task = new WorkerTask(job.j_id, pi, now);
	job.j_phases[pi].p_npending++;
	job.j_tasks[task.t_id] = true;
	this.w_tasks[task.t_id] = task;
	return (task);
};

Worker.prototype.taskAssignServer = function (task, server)
{
	mod_assert.equal(task.t_value['server'], server);

	if (!this.w_agents.hasOwnProperty(server))
		this.w_agents[server] = new WorkerAgent();

	if (!this.w_agents[server].a_last)
		this.w_agents[server].a_last = Date.now();

	this.w_agents[server].a_tasks[task.t_id] = true;
};

Worker.prototype.taskTimedOut = function (task, now)
{
	mod_assert.ok(task.t_value['timeCancelled'] === undefined);
	task.t_value['timeCancelled'] = mod_jsprim.iso8601(now);

	if (this.writePending('task', task.t_id)) {
		/*
		 * If the write has not been issued, great.  We'll write out the
		 * timeCancelled update when we write out whatever else we're
		 * changing.  If the write has already been issued, we need to
		 * queue another one after this one completes. XXX
		 */
		return;
	}

	this.enqueue('task', task.t_id, task.t_value);
};

/*
 * Dispatch a key to its map or reduce phase.  "source" describes the record
 * that generated this key (either a job input record or a task record from a
 * previous phase), and implicitly indicates for which phase this key is now
 * an input key.  The key itself is specified via "loc", a location result.
 */
Worker.prototype.keyDispatch = function (source, loc, now)
{
	var job, pi, err;

	job = this.w_jobs[source['value']['jobId']];
	mod_assert.ok(job);

	if (source['bucket'] == this.w_bucket['jobinput']) {
		pi = 0;
	} else {
		mod_assert.equal(source['bucket'], this.w_bucket['task']);
		pi = source['value']['phaseNum'] + 1;
		mod_assert.ok(pi < job.j_phases.length - 1);
	}

	if (loc.lr_locations.length === 0) {
		err = {
		    'code': 'EJ_NOENT',
		    'message': 'input key does not exist: "' + loc.lr_key + '"'
		};
	}

	if (job.j_phases[pi].p_type == 'reduce')
		this.keyDispatchReduce(job, pi, loc, now, err);
	else
		this.keyDispatchMap(job, pi, loc, now, err);
};

/*
 * Implementation of keyDispatch() for map phases.  This is simple, because we
 * always dispatch a new task.
 */
Worker.prototype.keyDispatchMap = function (job, pi, loc, now, err)
{
	var task, record, server;

	task = this.taskCreate(job, pi, now);
	record = task.t_value;

	if (err) {
		record['error'] = err;
		record['state'] = 'aborted';
		record['timeDone'] = now;
	} else {
		server = mod_jsprim.randElt(loc.lr_locations);
		record['key'] = loc.lr_key;
		record['objectid'] = loc.lr_objectid;
		record['server'] = server.lrl_server;
		record['zonename'] = server.lrl_zonename;
		this.taskAssignServer(task, server.lrl_server);
	}

	this.enqueue('task', task.t_id, record);
};

/*
 * Implementation of keyDispatch() for reduce phases.  If a task for this phase
 * hasn't yet been dispatched, do so now.  Then write a taskinput record for
 * this key.
 */
Worker.prototype.keyDispatchReduce = function (job, pi, loc, now, err)
{
	var task, record, phase;

	mod_assert.equal(job.j_phases[pi].p_type, 'reduce');

	if (job.j_phases[pi].p_task === null) {
		task = this.taskCreate(job, pi, now);
		job.j_phases[pi].p_task = task;
		record = task.t_value;
		this.enqueue('task', task.t_id, record);

		if (mod_jsprim.isEmpty(this.w_agents)) {
			record['state'] = 'aborted';
			record['server'] = undefined;
			record['error'] = {
			    'code': 'EJ_NORESOURCES',
			    'message': 'no servers available to run task'
			};
		} else {
			record['server'] = mod_jsprim.randElt(
			    Object.keys(this.w_agents));
		}
	}

	phase = job.j_phases[pi];
	task = phase.p_task;
	if (task.t_state == 'aborted' || this.writePending('task', task.t_id))
		return;

	if (err) {
		this.enqueue('taskoutput', mod_uuid.v4(), {
		    'jobId': job.j_id,
		    'taskId': task.t_id,
		    'error': err,
		    'timeCreated': now
		});

		return;
	}

	job.j_phases[pi].p_ninput++;

	this.enqueue('taskinput', mod_uuid.v4(), {
	    'jobId': job.j_id,
	    'taskId': task.t_id,
	    'key': loc.lr_key,
	    'objectid': loc.lr_objectid,
	    'servers': loc.lr_locations.map(function (l) {
		return ({
		    'server': l.lrl_server,
		    'zonename': l.lrl_zonename
		});
	    })
	});

	if ((pi === 0 && !job.j_input_done) ||
	    job.j_phases[pi - 1].p_npending > 0 ||
	    task.t_value['timeInputDone'])
		return;

	task.t_value['timeInputDone'] = now;
	task.t_value['nInputs'] = phase.p_ninput;
	this.enqueue('task', task.t_id, task.t_value);
};

/*
 * Commit the given task.  If the task succeeded and isn't for the last phase of
 * the job, then propagate the outputs as inputs to the next phase.
 */
Worker.prototype.taskCommit = function (task, now)
{
	var record, job, i;

	record = task.t_value;
	record['timeCommitted'] = now;

	this.enqueue('task', task.t_id, record);

	job = this.w_jobs[record['jobId']];
	job.j_phases[record['phaseNum']].p_npending--;

	if (record['result'] != 'ok' ||
	    record['phaseNum'] == job.j_phases.length - 1)
		return;

	for (i = 0; i < record['firstOutputs'].length; i++)
		this.keyLocate(record['firstOutputs'][i]['key'], task.t_record);

	for (i = 0; i < task.t_xoutputs.length; i++)
		this.keyLocate(task.t_xoutputs[i]['key'], task.t_record);
};

/*
 * Dispatch a Moray putObject request and handle the response.
 */
Worker.prototype.recordSave = function (ent)
{
	var worker = this;

	var bucket = ent.q_bucket;
	var key = ent.q_key;
	var qent = this.w_records[bucket][key];
	var value = qent.r_value;
	var options = qent.r_options;

	if (!this.w_jobs.hasOwnProperty(value['jobId'])) {
		this.w_log.warn('save %s/%s: job %s no longer exists',
		    bucket, key, value['jobId']);
		delete (this.w_records[bucket][key]);
		return;
	}

	mod_assert.ok(qent.r_issued === undefined);
	qent.r_issued = Date.now();

	this.w_pending_puts++;
	this.w_moray.putObject(bucket, key, value, options, function (err) {
		worker.w_pending_puts--;

		if (err) {
			/*
			 * XXX Distinguish between transient and intrinsic
			 * failures.
			 */
			worker.w_log.warn(err, 'failed to save "%s" "%s"',
			    bucket, key, value, options);
			worker.reenqueue(qent);
			return;
		}

		delete (worker.w_records[bucket][key]);
		worker.w_log.debug('saved "%s" "%s"', bucket, key);

		if (qent['callback'])
			qent['callback']();
	});
};

/*
 * Handle an incoming "locate" response.
 */
Worker.prototype.onLocate = function (keys, err, locations)
{
	var key, i;
	var worker = this;

	if (err) {
		/*
		 * This should indicate a transient failure (like a failure to
		 * connect to Moray), not an intrinsic one (like ENOENT).  As a
		 * result, we should retry later.
		 */
		this.w_log.warn(err,
		    'failed to locate "%s" (will retry)', keys);
		this.w_locates_out = this.w_locates_out.concat(keys);
		return;
	}

	for (i = 0; i < keys.length; i++) {
		key = keys[i];

		if (!locations.hasOwnProperty(key)) {
			this.w_log.warn('locate result missing value for "%s"',
			    key);
			this.w_locates_out.push(key);
			continue;
		}

		if (locations[key].length === 0) {
			this.w_locates_in.push({
			    'lr_key': key,
			    'lr_objectid': undefined,
			    'lr_locations': []
			});
			continue;
		}

		locations[key].forEach(function (loc) {
			if (worker.w_agents.hasOwnProperty(loc['host']))
				return;

			worker.w_agents[loc['host']] = new WorkerAgent();
		});

		this.w_locates_in.push({
			'lr_key': key,
			'lr_objectid': locations[key][0]['objectid'],
			'lr_locations': locations[key].map(function (l) {
			    return ({
				'lrl_server': l['host'],
				'lrl_zonename': l['zonename']
			    });
			})
		});
	}
};
