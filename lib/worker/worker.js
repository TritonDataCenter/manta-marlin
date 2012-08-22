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
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_util = require('util');

var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_locator = require('./locator');
var mod_moray = require('./moray');
var mod_schema = require('../schema');
var mod_mautil = require('../util');

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
	    'timeJobPoll': mod_schema.sIntervalRequired,
	    'timeJobSave': mod_schema.sIntervalRequired,
	    'timeTaskPoll': mod_schema.sIntervalRequired,
	    'timeTaskAbandon': mod_schema.sIntervalRequired,
	    'timeTick': mod_schema.sIntervalRequired
	}
    }
};


/* Public interface */
exports.mwConfSchema = mwConfSchema;
exports.mwWorker = mwWorker;


function SaveGeneration()
{
	this.s_dirty = 0;
	this.s_saved = 0;
	this.s_pending = undefined;
}

SaveGeneration.prototype.markDirty = function ()
{
	this.s_dirty++;
};

SaveGeneration.prototype.saveStart = function ()
{
	mod_assert.ok(this.s_pending === undefined,
	    'attempted concurrent saves');
	this.s_pending = this.s_dirty;
};

SaveGeneration.prototype.saveOk = function ()
{
	mod_assert.ok(this.s_pending !== undefined,
	    'no save operation pending');
	this.s_saved = this.s_pending;
};

SaveGeneration.prototype.saveFailed = function ()
{
	mod_assert.ok(this.s_pending !== undefined,
	    'no save operation pending');
	this.s_pending = undefined;
};

SaveGeneration.prototype.dirty = function ()
{
	return (this.s_dirty > this.s_saved);
};

SaveGeneration.prototype.pending = function ()
{
	return (this.s_pending !== undefined);
};

/*
 * Each MorayPoller instance keeps track of several queries that should be
 * polled periodically, each with their own throttle and filter-generator
 * function.  This object also keeps track of objects' mtimes and etags so that
 * events are only emitted when an object's mtime changes and the consumer can
 * tell if the actual contents have changed.
 */
function mwMorayPoller(args)
{
	this.mp_log = args['log'];
	this.mp_moray = args['moray'];
	this.mp_objects = {};
	this.mp_polls = {};
}

mod_util.inherits(mwMorayPoller, mod_events.EventEmitter);

/*
 * Add a new query on the given bucket.
 */
mwMorayPoller.prototype.addPoll = function (name, bucket, interval, immutable,
    mkfilter)
{
	mod_assert.ok(!this.mp_objects.hasOwnProperty(bucket));

	if (!immutable)
		this.mp_objects[bucket] = {};

	this.mp_polls[bucket] = {
	    'event': name,
	    'mkfilter': mkfilter,
	    'throttle': new mod_mautil.Throttler(interval)
	};
};

/*
 * Remove an object from the cache.
 */
mwMorayPoller.remove = function (bucket, key)
{
	delete (this.mp_objects[bucket][key]);
};

/*
 * Queries Moray for all polls added with "addPoll" whose interval has expired.
 */
mwMorayPoller.prototype.poll = function ()
{
	var poller = this;
	mod_jsprim.forEachKey(this.mp_polls, function (bucket, conf) {
		if (!conf['throttle'].tooRecent())
			poller.pollBucket(bucket, conf);
	});
};

mwMorayPoller.prototype.pollBucket = function (bucket, conf)
{
	var now, poller, objects, log, filter, throttle, req;

	now = Date.now();
	poller = this;
	objects = this.mp_objects[bucket];
	log = this.mp_log;
	filter = conf['filter']();
	throttle = conf['throttle'];

	if (!filter) {
		log.debug('skipping "%s": no filter was specified', bucket);
		return;
	}

	throttle.start();
	req = this.mp_moray.findObjects(bucket, filter, { 'noCache': true });

	req.on('error', function (err) {
		throttle.done();
		log.warn(err, 'failed to poll "%s"', bucket);
	});

	req.on('record', function (record) {
		if (objects) {
			var saw = objects[record['key']];

			objects[record['key']] = {
			    'mtime': record['_mtime'],
			    'etag': record['_etag']
			};

			/*
			 * If the record is completely unchanged, don't emit an
			 * event.  Ideally, we'd be able to construct our
			 * queries to avoid returning such records.
			 */
			if (saw && saw['mtime'] === record['_mtime'] &&
			    saw['etag'] === record['_etag'])
				return;
		}

		/*
		 * We emit an event if either the mtime or the etag has changed
		 * because the caller may care if the record has been written
		 * even if its contents haven't changed.  For convenience, we
		 * provide an argument to indicate whether the contents have
		 * also changed.
		 */
		poller.emit('record', conf['event'], record,
		    !saw || record['_etag'] !== saw['etag']);
	});

	req.on('end', function () {
		throttle.done();
		poller.emit('end', conf['event'], now);
	});
};


/*
 * Maintains state for a single job.  Jobs run through the following states:
 *
 *                              +
 *                              | Discover new job (or abandoned job)
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
function mwJob(args)
{
	/* static state */
	this.j_job = args['record']['value'];
	this.j_id = this.j_job['jobId'];
	this.j_log = args['log'];

	/* dynamic state */
	this.j_state = 'unassigned';		/* current state */
	this.j_state_time = new Date();		/* time of last state change */
	this.j_dropped = undefined;		/* time the job was dropped */
	this.j_init_start = undefined;		/* time we started init */
	this.j_init_waiting = {
		'jobinput': undefined,
		'task': undefined,
		'taskinput': undefined,
		'taskoutput': undefined
	};
	this.j_init = {
		'jobinput': {},
		'task': {},
		'taskinput': {},
		'taskoutput': {}
	};
	this.j_input_done = this.j_job['timeInputDone'];
	this.j_cancelled = this.j_job['timeCancelled'];
	this.j_tasks = {};			/* set of all tasks */

	/* save state */
	this.j_save_throttle = new mod_mautil.Throttler(this.j_time_job_save);
	this.j_save = new SaveGeneration();

	/*
	 * For each job's phases, we keep track of:
	 *
	 *    p_type		phase type (same as job input)
	 *
	 *    p_npending	number of uncommitted tasks in this phase
	 *
	 *    p_task		task record (reduce only)
	 *
	 *    p_ninput		total number of input keys (reduce only)
	 */
	this.j_phases = this.j_job['phases'].map(function (jobphase) {
		var rv = {
		    'p_type': jobphase['type'],
		    'p_npending': 0
		};

		if (rv.p_type == 'reduce') {
			rv.p_task = null;
			rv.p_ninput = 0;
		}

		return (rv);
	});
}

mwJob.prototype.debugState = function ()
{
	return ({
	    'record': this.j_job,
	    'state': this.j_state,
	    'state_time': this.j_state_time,
	    'dropped': this.j_dropped,
	    'init_start': this.j_init_start,
	    'init_waiting': this.j_init_waiting,
	    'init': this.j_init,
	    'input_done': this.j_input_done,
	    'cancelled': this.j_cancelled,
	    'save_throttle': this.j_save_throttle,
	    'save_gen': this.j_save,
	    'phases': this.j_phases
	});
};


/*
 * Manages all jobs owned by a single Marlin worker.  Arguments for this class
 * include:
 *
 *    conf		Configuration object matching the above schema.
 *
 *    log		Bunyan-style logger instance
 */
function mwWorker(args)
{
	var conf, error;

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
	this.mw_uuid = conf['instanceUuid'];
	this.mw_conf = mod_jsprim.deepCopy(conf);
	this.mw_buckets = this.mw_conf['buckets'];
	this.mw_max_pending_locates = conf['tunables']['maxPendingLocates'];
	this.mw_max_pending_puts = conf['tunables']['maxPendingPuts'];
	this.mw_time_tick = conf['tunables']['timeTick'];
	this.mw_time_job_abandon = conf['tunables']['timeJobAbandon'];
	this.mw_time_job_poll = conf['tunables']['timeJobPoll'];
	this.mw_time_job_save = conf['tunables']['timeJobSave'];
	this.mw_time_task_abandon = conf['tunables']['timeTaskAbandon'];
	this.mw_time_task_poll = conf['tunables']['timeTaskPoll'];

	/* helper objects */
	this.mw_log = args['log'].child({
	    'component': 'worker-' + this.mw_uuid
	});
	this.mw_locator = mod_locator.createLocator(args['conf'], {
	    'log': this.mw_log.child({ 'component': 'manta-locator' })
	});
	this.mw_moray = mod_moray.createMoray(args['conf'], {
	    'log': this.mw_log.child({ 'component': 'moray-client' })
	});

	this.mw_poll = new mwMorayPoller({
	    'log': this.mw_log.child({ 'component': 'moray-poller' }),
	    'moray': this.mw_moray
	});

	this.mw_poll.on('record', this.onRecord.bind(this));
	this.mw_poll.on('end', this.onSearchDone.bind(this));

	this.mw_poll.addPoll('job', this.mw_buckets['job'],
	    this.mw_time_job_poll, false, this.pollJobFilter.bind(this));
	this.mw_poll.addPoll('jobinput', this.mw_buckets['jobinput'],
	    this.mw_time_job_poll, true, this.pollJobInputFilter.bind(this));
	this.mw_poll.addPoll('task', this.mw_bucket['task'],
	    this.mw_time_task_poll, false, this.pollTaskFilter.bind(this));
	this.mw_poll.addPoll('taskinput', this.mw_bucket['taskinput'],
	    this.mw_time_task_poll, true, this.pollTaskInputFilter.bind(this));
	this.mw_poll.addPoll('taskoutput', this.mw_bucket['taskoutput'],
	    this.mw_time_task_poll, true, this.pollTaskOutputFilter.bind(this));

	/* global dynamic state */
	this.mw_pending_locates = 0;		/* nr of pending locate ops */
	this.mw_pending_puts = 0;		/* nr of pending put ops */
	this.mw_worker_start = undefined;	/* time worker started */
	this.mw_worker_stopped = undefined;	/* time worker stopped */
	this.mw_tick_start = undefined;		/* time last tick started */
	this.mw_tick_done = undefined;		/* time last tick finished */
	this.mw_timeout = undefined;		/* JS timeout handle */
	this.mw_jobs = {};			/* all jobs, by jobId */

	this.mw_stats = {			/* stat counters */
	    'asgn_failed': 0,			/* failed job assignments */
	    'asgn_restart': 0			/* jobs picked up on restart */
	};

	/*
	 * We keep track of tasks by remote agent and the last time we've
	 * received an update from each agent so that we can quickly deal with
	 * agents going AWOL.  Each of these objects has:
	 *
	 *    a_last		Time (milliseconds since epoch) of last contact
	 *
	 *    a_tasks		Set of uncompleted tasks assigned to this host
	 */
	this.mw_agents = {};

	/*
	 * We keep track of running tasks, each an object with:
	 *
	 *	t_id		unique task identifier
	 *
	 *	t_record	last record received for the task itself
	 *
	 *	t_value		last value saved or received for the task itself
	 *
	 *	t_mtime		last Moray mtime for the task record itself
	 *
	 *	t_xoutputs	array of output keys NOT included in the record
	 *			itself
	 */
	this.mw_tasks = {};

	/*
	 * We keep track of pending work on several queues.  A given input key
	 * basically goes through the system in the following order.
	 */
	this.mw_jobinputs_in = [];	/* incoming "job input" records */

	/*
	 * Outgoing locate requests are specified simply by Manta key (as a
	 * string).  The responses are enqueued on mw_locates_in as an object:
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
	 * The pending requests are stored as objects in mw_locates with:
	 *
	 *	l_key		Manta key to be located
	 *
	 *	l_origins	List of jobinput or taskoutput records that are
	 *			waiting on the result of this request
	 */
	this.mw_locates_out = [];	/* outgoing "locate" requests */
	this.mw_locates_in = [];	/* incoming "locate" responses */
	this.mw_locates = {};		/* pending "locate" requests */

	/*
	 * We keep track of queued and pending save operations in mw_records,
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
	 * Queued requests are stored on mw_records_out as objects with:
	 *
	 *    q_bucket, q_key	identifies the corresopnding record in
	 *    			mw_records.
	 *
	 * This design does support modifying a dirty object by simply updating
	 * the value that will be written, but it does not allow modifying an
	 * object with a pending write.  There's no general way to resolve
	 * conflicts between local changes and remote updates, and the overall
	 * Marlin design deliberately avoids needing to do so except in simple
	 * cases.
	 */
	this.mw_records = {};
	mod_jsprim.forEachKey(this.mw_buckets, function (_, bucket) {
		this.mw_records[bucket] = {};
	});
	this.mw_records_out = [];	/* outgoing record writes */
	this.mw_tasks_in = [];		/* incoming "task" record updates */
	this.mw_taskoutputs_in = [];	/* incoming "task output" records */
}

mwWorker.prototype.debugState = function ()
{
	return ({
	    'conf': this.mw_conf,
	    'pending_locates': this.mw_pending_locates,
	    'pending_puts': this.mw_pending_puts,
	    'worker_start': this.mw_worker_start,
	    'worker_stopped': this.mw_worker_stopped,
	    'tick_start': this.mw_tick_start,
	    'tick_done': this.mw_tick_done,
	    'agents': this.mw_agents,
	    'njobinputs_in': this.mw_jobinputs_in.length,
	    'ntasks_in': this.mw_tasks_in.length,
	    'ntaskoutputs_in': this.mw_taskoutputs_in.length,
	    'nlocates_in': this.mw_locates_in.length,
	    'nlocates_out': this.mw_locates_out.length,
	    'nrecords_out': this.mw_records_out.length
	});
};

/*
 * Returns stats.  This asynchronous call is used for testing only, and could be
 * replaced in the future with a call to a remote worker.
 */
mwWorker.prototype.stats = function (callback)
{
	var stats = mod_jsprim.deepCopy(this.mw_stats);

	if (arguments.length === 0)
		return (stats);

	process.nextTick(function () { callback(null, stats); });
	return (undefined);
};

mwWorker.prototype.start = function ()
{
	var log = this.mw_log;
	var next = this.tick.bind(this);

	mod_assert.ok(this.mw_worker_start === undefined);
	this.mw_worker_start = new Date();

	log.info('initializing moray');
	this.mw_moray.setup(function (err) {
		if (err) {
			log.fatal(err, 'failed to initialize moray');
			throw (err);
		}

		log.info('starting worker');
		process.nextTick(next);
	});
};

mwWorker.prototype.stop = function (callback)
{
	this.mw_log.info('shutting down worker');
	this.mw_worker_stopped = new Date();

	if (this.mw_timeout) {
		clearTimeout(this.mw_timeout);
		this.mw_timeout = undefined;
	}

	if (callback)
		this.mw_moray.drain(callback);
};

/*
 * The heart of the job worker, this function scans Moray for various state
 * updates and then invokes "tick" on each of our existing jobs to reevaluate
 * their states.
 */
mwWorker.prototype.tick = function ()
{
	var now, jobid, worker;

	this.mw_tick_start = new Date();
	this.mw_timeout = undefined;

	now = this.mw_tick_start.getTime();
	this.mw_poll.poll();

	for (jobid in this.mw_jobs)
		this.jobTick(this.mw_jobs[jobid]);

	this.processQueues();

	worker = this;
	mod_jsprim.forEachKey(this.mw_agents, function (host, entry) {
		var ntasks = 0;

		if (!entry.a_last ||
		    now - entry.a_last <= worker.mw_time_task_abandon)
			return;

		delete (worker.mw_agents[host]);
		mod_jsprim.forEachKey(entry.a_tasks, function (taskid) {
			ntasks++;
			worker.taskTimedOut(worker.mw_tasks[taskid], now);
		});

		worker.mw_log.info('agent "%s" has timed out (no response in ' +
		    '%d ms); cancelling %d tasks', now - entry.a_last, ntasks);
	});

	this.mw_tick_done = new Date();
	this.mw_timeout = setTimeout(this.tick.bind(this), this.mw_time_tick);
};

/*
 * The poll(Bucket)Filter family of functions return the Moray filter used to
 * query for updates to that bucket.  They all use function on the mw_moray
 * field, since the filter itself is implementation-specific.
 */

mwWorker.prototype.pollJobFilter = function ()
{
	return (this.mw_moray.filterJobs(
	    this.mw_uuid, this.mw_time_job_abandon));
};

mwWorker.prototype.pollJobInputFilter = function ()
{
	var watch;

	watch = [];
	mod_jsprim.forEachKey(this.mw_jobs, function (jobid, job) {
		if (job.j_input_done === undefined)
			watch.push(jobid);
	});

	return (this.mw_moray.filterJobInputs(watch));
};

mwWorker.prototype.pollTaskFilter = function ()
{
	var watch = [];

	mod_jsprim.forEachKey(this.mw_jobs, function (jobid, job) {
		if (job.j_state == 'initializing' ||
		    job.j_state == 'running' ||
		    job.j_state == 'finishing')
			watch.push(jobid);
	});

	return (this.mw_moray.filterTasks(watch));
};

mwWorker.prototype.pollTaskInputFilter = function ()
{
	var watch = [];

	mod_jsprim.forEachKey(this.mw_jobs, function (jobid, job) {
		if (job.j_state == 'initializing')
			watch.push(jobid);
	});

	return (this.mw_moray.filterTaskInputs(watch));
};

mwWorker.prototype.pollTaskOutputFilter = function ()
{
	var watch = [];

	mod_jsprim.forEachKey(this.mw_jobs, function (jobid, job) {
		if (job.j_state == 'initializing' ||
		    job.j_state == 'running')
			watch.push(jobid);
	});

	return (this.mw_moray.filterTaskOutputs(watch));
};

/*
 * Invoked to process an incoming Moray record.
 */
mwWorker.prototype.onRecord = function (name, record, changed)
{
	var obj = record['value'];
	var jobid = obj['jobId'];
	var job;

	/*
	 * If this is a task record from a remote agent, updated our last
	 * contact time for that agent.  We do this before just about anything
	 * else, even if we end up rejecting the record later for some other
	 * reason.
	 */
	if (name == 'task' && record['value']['server']) {
		var server = record['value']['server'];
		if (!this.mw_agents.hasOwnProperty(server))
			this.mw_agents[server] = {
			    'a_last': 0,
			    'a_tasks': {}
			};

		if (record['_mtime'] > this.mw_agents[server].a_last)
			this.mw_agents[server].a_last = Date.now();
	}

	/*
	 * At this point we don't care about records that haven't changed.
	 */
	if (!changed)
		return;

	/* Do some basic validation on the incoming object. */
	if (!jobid) {
		this.mw_log.warn('%s: has no jobId', name, record);
		return;
	}

	if (name == 'job') {
		this.jobUpdate(record);
		return;
	}

	if (!this.mw_jobs.hasOwnProperty(jobid)) {
		this.mw_log.warn('%s: no such jobId: "%s"', name, jobid);
		return;
	}

	job = this.mw_jobs[jobid];
	if (job.j_state == 'unassigned' ||
	    (job.j_state == 'finishing' && name != 'task')) {
		job.j_log.warn('ignoring "%s" update in state "%s"',
		    name, job.j_state);
		return;
	}

	/*
	 * While a job is still in the "uninitialized" state, buffer all
	 * incoming updates until all records have been received.
	 */
	if (job.j_state == 'initializing') {
		job.j_init[name][record['key']] = record;
		return;
	}

	mod_assert.ok(job.j_state == 'running' ||
	    (job.j_state == 'finishing' && name == 'task'));

	if (name == 'taskinput') {
		job.j_log.warn('ignoring "%s" update in state "%s"',
		    name, job.j_state);
		return;
	}

	if (name == 'jobinput')
		this.mw_jobinputs_in.push(record);
	else if (name == 'task')
		this.mw_tasks_in.push(record);
	else if (name == 'taskoutput')
		this.mw_taskoutputs_in.push(record);

	this.jobTick(job);
};

/*
 * Invoked when a Moray search request completes.  This only affects jobs in the
 * "initializing" state, which need to know when they've loaded all existing
 * state for a job.
 */
mwWorker.prototype.onSearchDone = function (name, start)
{
	var worker = this;

	if (name == 'job')
		return;

	mod_jsprim.forEachKey(this.mw_jobs, function (jobid, job) {
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
mwWorker.prototype.jobCreate = function (record)
{
	var job;

	job = record['value'];
	mod_assert.ok(!this.mw_jobs.hasOwnProperty(job['jobId']));

	if (job['worker'] == this.mw_uuid) {
		this.mw_log.info('resuming our own job: %s', job['jobId']);
		this.mw_stats['asgn_restart']++;
	}

	this.mw_jobs[job['jobId']] = new mwJob({
	    'record': record,
	    'log': this.mw_log.child({ 'component': 'job-' + job['jobId'] })
	});

	this.jobTick(this.mw_jobs[job['jobId']]);
};

/*
 * Process an incoming Moray record for a job.
 */
mwWorker.prototype.jobUpdate = function (record)
{
	var jobid, job;

	jobid = record['value']['jobId'];

	if (!this.mw_jobs.hasOwnProperty(jobid)) {
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
	job = this.mw_jobs[jobid];
	job.j_etag = record['_etag'];

	if (job.j_state != 'unassigned' &&
	    record['value']['worker'] != this.mw_uuid) {
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
		return;
	}
};

/*
 * Invoked to completely remove a job from this worker.
 */
mwWorker.prototype.jobRemove = function (job)
{
	var npending, taskid;

	if (job.j_state == 'unassigned')
		this.mw_stats['asgn_failed']++;

	job.j_dropped = new Date();
	delete (this.mw_jobs[job.j_id]);
	job.j_log.info('job removed');

	if (job.j_save_throttle.ongoing())
		job.j_log.info('job removed with pending save operation');

	this.mw_poll.remove(this.mw_buckets['job'], this.j_id);

	npending = 0;
	for (taskid in job.j_tasks) {
		if (this.writePending('task', taskid))
			npending++;

		delete (this.mw_tasks[taskid]);
		this.mw_poll.remove(this.mw_buckets['task'], taskid);
	}

	if (npending > 0)
		job.j_log.info('job removed with %d pending task saves',
		    npending);
};

/*
 * Common function to transition from state S1 to state S2.
 */
mwWorker.prototype.jobTransition = function (job, s1, s2)
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
mwWorker.prototype.jobTick = function (job)
{
	mod_assert.ok(job.j_dropped === undefined);

	/*
	 * The "unassigned" state is special because we don't want to retry
	 * failed save attempts.
	 */
	if (job.j_state == 'unassigned') {
		if (!job.j_save.dirty()) {
			this.jobAssign(job);
			return;
		}

		if (job.j_save_throttle.ongoing())
			return;

		if (job.j_save.dirty()) {
			job.j_log.warn('failed to assign job');
			this.jobRemove(job);
			return;
		}

		job.j_log.info('successfully assigned job');
		job.j_init_start = Date.now();
		this.jobTransition('unassigned', 'initializing');
		return;
	}

	/*
	 * Save the job periodically and any time it's dirty.
	 */
	if (!job.j_save_throttle.tooRecent() && !job.j_save.dirty())
		job.j_save.markDirty();

	if (!job.j_save_throttle.ongoing() && job.j_save.dirty())
		this.jobSave(job);

	if (job.j_state == 'initializing')
		/* We'll get kicked out of this state asynchronously. */
		return;

	if (job.j_state == 'finishing' && !job.j_save.dirty()) {
		this.jobRemove(job);
		return;
	}

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
mwWorker.prototype.jobAssign = function (job)
{
	mod_assert.equal(this.j_state, 'unassigned');

	if (job.j_job['worker']) {
		job.j_log.info('attempting to steal job from %s',
		    job.j_job['worker']);
	} else {
		job.j_log.info('attempting to take unassigned job');
	}

	job.j_job['state'] = 'running';
	job.j_job['worker'] = this.mw_uuid;
	job.j_save.markDirty();
	this.jobSave(job);
};

mwWorker.prototype.jobSave = function (job)
{
	var worker = this;

	mod_assert.ok(!job.j_save_throttle.ongoing());
	job.j_save_throttle.start();
	job.j_log.debug('saving job (%j)', job.j_save);
	job.j_save.saveStart();

	job.j_moray.putObject(this.mw_buckets['job'], job.j_id,
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

		if (job.j_state == 'unassigned' && !job.j_dropped)
			worker.jobTick(job);
	});
};

/*
 * Invoked when we've finished loading all existing state on this job.
 */
mwWorker.prototype.jobInit = function (job)
{
	mod_assert.ok(mod_jsprim.isEmpty(job.j_init_waiting));
	mod_assert.equal(job.j_state, 'initializing');

	/* XXX compute the current state based on what's here. */

	this.jobTransition(job, 'initializing', 'running');
};

/*
 * Returns true iff the job is complete.
 */
mwWorker.prototype.jobDone = function (job)
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

mwWorker.prototype.enqueue = function (name, key, value, options, callback)
{
	var bucket, c;

	bucket = this.mw_buckets[name];
	c = this.mw_records[bucket];

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

	this.mw_records_out.push({ 'q_bucket': bucket, 'q_key': key });
};

mwWorker.prototype.reenqueue = function (ent)
{
	ent.r_attempts++;
	ent.r_queued = Date.now();
	ent.r_issued = undefined;

	this.mw_records_out.push({
	    'q_bucket': ent.r_bucket,
	    'q_key': ent.r_key
	});
};

mwWorker.prototype.writePending = function (name, key)
{
	var bucket = this.mw_buckets[name];
	return (this.mw_records[bucket].hasOwnProperty(key));
};

/*
 * Most of the work done by this service goes through one of several queues
 * documented in mwWorker above.  This function is invoked periodically to
 * process work on each of these queues.
 */
mwWorker.prototype.processQueues = function ()
{
	var now, ent, key, loc, lreq, i, task, len, job;
	var changedtasks = {};
	var queries = [];

	now = mod_jsprim.iso8601(new Date());

	while (this.mw_jobinputs_in.length > 0) {
		ent = this.mw_jobinputs_in.shift();
		key = ent['value']['key'];
		this.keyLocate(key, ent);
	}

	while (this.mw_locates_in.length > 0) {
		loc = this.mw_locates_in.shift();
		lreq = this.mw_locates[loc.lr_key];
		mod_assert.ok(lreq);

		for (i = 0; i < lreq.l_origins.length; i++) {
			ent = lreq.l_origins[i];
			this.keyDispatch(ent, loc, now);
		}

		delete (this.mw_locates[loc.lr_key]);
	}

	while (this.mw_tasks_in.length > 0) {
		ent = this.mw_tasks_in.shift();
		task = this.mw_tasks[ent['value']['taskId']];

		if (!task) {
			this.mw_log.warn(
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
		task.t_mtime = ent['_mtime'];
		changedtasks[task.t_id] = true;
	}

	while (this.mw_taskoutputs_in.length > 0) {
		ent = this.mw_taskoutputs_in.shift();
		task = this.mw_tasks[ent['value']['taskId']];

		if (!task) {
			this.mw_log.warn(
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
		task = this.mw_tasks[key];
		job = this.mw_jobs[task.t_value['jobId']];
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

	if (this.mw_pending_locates < this.mw_max_pending_locates &&
	    this.mw_locates_out.length > 0) {
		queries = this.mw_locates_out.splice(0,
		    this.mw_max_pending_locates - this.mw_pending_locates);
		this.mw_pending_locates += queries.length;
		this.mw_locator.locate(queries,
		    this.onLocate.bind(this, queries));
	}

	while (this.mw_pending_puts < this.mw_max_pending_puts &&
	    this.mw_records_out.length > 0) {
		ent = this.mw_records_out.shift();
		this.recordSave(ent);
	}

	/*
	 * By this point, we should have processed all incoming records, and
	 * either the output queues should be empty or we've reached internal
	 * limits.
	 */
	mod_assert.equal(this.mw_jobinputs_in.length, 0);
	mod_assert.equal(this.mw_locates_in.length, 0);
	mod_assert.equal(this.mw_tasks_in.length, 0);
	mod_assert.equal(this.mw_taskoutputs_in.length, 0);
	mod_assert.ok(this.mw_locates_out.length === 0 ||
	    this.mw_pending_locates === this.mw_max_pending_locates);
	mod_assert.ok(this.mw_records_out.length === 0 ||
	    this.mw_pending_puts === this.mw_max_pending_puts);
};

/*
 * Enqueue a request to locate key "key", triggered by record "ent".
 */
mwWorker.prototype.keyLocate = function (key, ent)
{
	if (this.mw_locates.hasOwnProperty(key)) {
		this.mw_locates[key].l_origins.push(ent);
		return;
	}

	this.mw_locates[key] = {
	    'l_key': key,
	    'l_origins': [ ent ]
	};

	this.mw_locates_out.push(key);
};

/*
 * Create a new "task" record for phase "pi" of job "job".
 */
mwWorker.prototype.taskCreate = function (job, pi, now)
{
	var id, task;

	mod_assert.ok(pi >= 0 && pi < job.j_phases.length);

	id = mod_uuid.v4();

	task = {
	    't_id': id,
	    't_value': {
		'jobId': job.j_id,
		'taskId': id,
		'phaseNum': pi,
		'state': 'dispatched',
		'timeDispatched': now
	    },
	    't_xoutputs': [],
	    't_record': undefined,
	    't_mtime': undefined
	};

	job.j_phases[pi].p_npending++;
	job.j_tasks[id] = true;
	this.mw_tasks[id] = task;

	return (task);
};

mwWorker.prototype.taskAssignServer = function (task, server)
{
	mod_assert.equal(task.t_value['server'], server);

	if (!this.mw_agents.hasOwnProperty(server))
		this.mw_agents[server] = {
		    'a_last': 0,
		    'a_tasks': {}
		};

	if (!this.mw_agents[server].a_last)
		this.mw_agents[server].a_last = Date.now();

	this.mw_agents[server].a_tasks[task.t_id] = true;
};

mwWorker.prototype.taskTimedOut = function (task, now)
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
mwWorker.prototype.keyDispatch = function (source, loc, now)
{
	var job, pi, err;

	job = this.mw_jobs[source['value']['jobId']];
	mod_assert.ok(job);

	if (source['bucket'] == this.mw_bucket['jobinput']) {
		pi = 0;
	} else {
		mod_assert.equal(source['bucket'], this.mw_bucket['task']);
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
mwWorker.prototype.keyDispatchMap = function (job, pi, loc, now, err)
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
mwWorker.prototype.keyDispatchReduce = function (job, pi, loc, now, err)
{
	var task, record, phase;

	mod_assert.equal(job.j_phases[pi].p_type, 'reduce');

	if (job.j_phases[pi].p_task === null) {
		task = this.taskCreate(job, pi, now);
		job.j_phases[pi].p_task = task;
		record = task.t_value;
		this.enqueue('task', task.t_id, record);

		if (mod_jsprim.isEmpty(this.mw_agents)) {
			record['state'] = 'aborted';
			record['server'] = undefined;
			record['error'] = {
			    'code': 'EJ_NORESOURCES',
			    'message': 'no servers available to run task'
			};
		} else {
			record['server'] = mod_jsprim.randElt(
			    Object.keys(this.mw_agents));
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
mwWorker.prototype.taskCommit = function (task, now)
{
	var record, job, i;

	record = task.t_value;
	record['timeCommitted'] = now;

	this.enqueue('task', task.t_id, record);

	job = this.mw_jobs[record['jobId']];
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
mwWorker.prototype.recordSave = function (ent)
{
	var worker = this;

	var bucket = ent.q_bucket;
	var key = ent.q_key;
	var qent = this.mw_records[bucket][key];
	var value = qent.r_value;
	var options = qent.r_options;

	if (!this.mw_jobs.hasOwnProperty(value['jobId'])) {
		this.mw_log.warn('save %s/%s: job %s no longer exists',
		    bucket, key, value['jobId']);
		delete (this.mw_records[bucket][key]);
		return;
	}

	mod_assert.ok(qent.r_issued === undefined);
	qent.r_issued = Date.now();

	this.mw_pending_puts++;
	this.mw_moray.putObject(bucket, key, value, options, function (err) {
		worker.mw_pending_puts--;

		if (err) {
			/*
			 * XXX Distinguish between transient and intrinsic
			 * failures.
			 */
			worker.mw_log.warn(err, 'failed to save "%s" "%s"',
			    bucket, key, value, options);
			worker.reenqueue(qent);
			return;
		}

		delete (worker.mw_records[bucket][key]);
		worker.mw_log.debug('saved "%s" "%s"', bucket, key);

		if (qent['callback'])
			qent['callback']();
	});
};

/*
 * Handle an incoming "locate" response.
 */
mwWorker.prototype.onLocate = function (keys, err, locations)
{
	var key, i;
	var worker = this;

	if (err) {
		/*
		 * This should indicate a transient failure (like a failure to
		 * connect to Moray), not an intrinsic one (like ENOENT).  As a
		 * result, we should retry later.
		 */
		this.mw_log.warn(err,
		    'failed to locate "%s" (will retry)', keys);
		this.mw_locates_out = this.mw_locates_out.concat(keys);
		return;
	}

	for (i = 0; i < keys.length; i++) {
		key = keys[i];

		if (!locations.hasOwnProperty(key)) {
			this.mw_log.warn('locate result missing value for "%s"',
			    key);
			this.mw_locates_out.push(key);
			continue;
		}

		if (locations[key].length === 0) {
			this.mw_locates_in.push({
			    'lr_key': key,
			    'lr_objectid': undefined,
			    'lr_locations': []
			});
			continue;
		}

		locations[key].forEach(function (loc) {
			if (worker.mw_agents.hasOwnProperty(loc['host']))
				return;

			worker.mw_agents[loc['host']] = {
			    'a_last': 0,
			    'a_tasks': {}
			};
		});

		this.mw_locates_in.push({
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
