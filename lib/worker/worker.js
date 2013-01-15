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
 * (1) User submits a job via the manta front door (muskie).  Muskie creates a
 *     "job" record that includes:
 *
 *         o the specification of what to do in each phase,
 *
 *         o the effective credentials of the user (including uuid, login, and
 *           authentication token), and
 *
 *         o various metadata about the job (time created and the like)
 *
 * (2) Some time very shortly later, one of several job workers finds the job
 *     and does an etag-conditional PUT to set "worker" to the worker's uuid.
 *     Many workers may attempt this, but only one may succeed.  The others
 *     forget about the job.
 *
 * (3) The user submits inputs for the job via muskie.  For each input, muskie
 *     creates a "jobinput" record that includes the object's name.
 *
 * (4) For each input record:
 *
 *         (a) The worker does a simple access check: if the job's owner's uuid
 *             matches the uuid corresponding to the login in the object name,
 *             or the object's name refers to a public directory, access is
 *             granted (see step (b)).  Otherwise, the worker writes a
 *             transaction that (1) updates the jobinput record to set
 *             propagated = true, and (2) writes an "error" record for this
 *             object that references the jobid, the phase number, the object
 *             name, and the reason for the error.
 *
 *         (b) If access is granted, the worker queries the Moray ring to locate
 *             the object.
 *
 *                 (i)   If the object does not exist, the worker writes an
 *                       "error" object in a transaction with an update to the
 *                       "jobinput" object, exactly as in (4a) above.
 *
 *                 (ii)  If the object exists in at least one location, the
 *                       worker writes out a transaction that sets propagated =
 *                       true on the "jobinput" and writes a new "task" record
 *                       with state "dispatched" assigned to the agent
 *                       corresponding to one of the locations selected at
 *                       random.
 *
 *                 (iii) If the object is zero bytes in length, the worker
 *                       writes out a transaction that sets propagated = true on
 *                       the "jobinput" and writes a new "task" record with
 *                       state "dispatched" and assigned to any agent it knows
 *                       about, selected at random.
 *
 *         (c) If the "task" was written out with state "dispatched" and
 *             assigned to some agent, the corresponding agent picks up the task
 *             and immediately sets state = "accepted".
 *
 *         (d) Some time later, the agent actually starts running the task.  As
 *             output objects are emitted (as via "mpipe"), a "taskoutput"
 *             record is written out for each output object.
 *
 *         (e) When the task completes, the agent updates the task record with
 *             state = "done".  If the task completes successfully, the number
 *             of emitted output objects is recorded, along with result = "ok".
 *             Otherwise, a separate "error" object is included in the same
 *             transaction as the task update, and the task's result = "fail".
 *
 *         (f) The worker sees the task has been updated with state = "done".
 *             If the task failed, then the worker updates the task *and* all
 *             taskoutput records with "committed" and "propagated" to true, but
 *             "valid" = false.  If the task completed successfully, the worker
 *             writes a transaction that sets "committed" and "valid" to true on
 *             the task *and* all the taskoutput records.  Note that in both
 *             cases, the taskoutput updates are a server-side mass update,
 *             since there could be an arbitrarily large number of them.
 *
 * (5) For each "taskoutput" record committed but not marked propagated in (4f),
 *     all of step (4) is repeated, except that in (4b), the worker sets
 *     propagated = true on the "taskoutput" record (instead of a "jobinput"
 *     record).
 *
 * (6) Some time later, the user indicates to muskie that the job's input stream
 *     has ended.  When this happens, muskie updates the "job" record to
 *     indicate that inputDone = true.
 *
 * (7) When the job completes, the worker updates the job record to set state =
 *     "done".  The job is complete only when:
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
 * There are two subtle issues with this:
 *
 * (1) It's difficult to assess the "done" condition because each of these
 *     criteria would be separate Moray "count" queries, and records can move
 *     between states outside of the worker in between two queries.  We probably
 *     want to say that in step (2), after assigning the job to itself, the
 *     worker does a "limit 1" query for each of (c) and (d), neither of which
 *     can change without the worker changing it.  This means the worker will
 *     have (and be able to maintain) an exact count of each of these sets.  (a)
 *     is already known from the job record, and (b) just requires doing at
 *     least one Moray poll after the job's input stream has been discovered to
 *     have ended.
 *
 * (2) The above "done" condition assumes that all taskoutput records for a
 *     "done" task will be written (by the agent) by the time the "task" itself
 *     is written to indicate that state = "done".  Otherwise, all of (7a) -
 *     (7e) will be true, but there may still be a "taskoutput" record still
 *     buffered in the agent.  But this is somewhat tricky to guarantee.  It
 *     either requires writing them all at once in a transaction, or waiting
 *     until they've all been acked before writing the "task" record (which
 *     adds latency).  There's a similar issue with writing "taskinput" records.
 *
 *
 * CLIENT
 *
 * Clients list outputs and errors.
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
 *     XXX worker needs to write out "errors" for these cancelled tasks
 *
 * (3) The agents process task cancellation exactly as described below.
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
 *     "job done" condition described in (7) above, except it must only be true
 *     for phases 0 ... i - 1, where i = the reducer's phase.
 *
 *
 * SUMMARY OF WORKER POLLS (asterisk means the query is per-job)
 *
 * REASON               BUCKET          FILTER
 * find new jobs        jobs            (!(worker=*))
 * find abandoned jobs  jobs            (_mtime < $timeout_ms_ago)
 * find cancelled jobs  jobs            (&(cancelled=true)(state!=done))
 * find "input done"    jobs            (&(inputDone=true)
 *                                        (!(inputDoneAck=true)))
 * find jobs I own      jobs            (&(worker=*)(state!=done))
 *   (startup only)
 * find job inputs      *jobinputs      (&(jobId=$jobid)(!(propagated=true)))
 * find reduce tasks    *tasks          (&(jobId=$jobid)(!(key=*)))
 *    (startup only)
 * find done tasks      *tasks          (&(jobId=$jobid)(state=done)
 *                                        (!(committed=true)))
 * find timed-out tasks *tasks          (&(jobId=$jobid)(state!=done)
 *                                        (_mtime<$timeout_ago))
 * find taskoutputs     *taskoutputs    (&(jobId=$jobid)(committed=true)
 *     to be propagated                   (!(propagated=true))
 *
 *
 * SUMMARY OF AGENT POLLS
 *
 * REASON               BUCKET          FILTER
 * find new tasks       tasks           (&(server=$self)(state=dispatched))
 * find tasks cancelled tasks           (&(server=$self)(state!=done)
 *                                        (cancelled=true))
 * find task input done tasks           (&(server=$self)(inputDone=true)
 *                                        (!(inputDoneAck=true))
 * find task inputs     taskinputs      (&(server=$self)(!(read=true)))
 * find job details     *jobs           (jobId=$jobid)
 *
 * While the "jobs" query is per-job, it only needs to be done once per job per
 * agent (NOT per poll interval) since the relevant job state does not change.
 *
 * XXX MANTA-928 TODO:
 * o jobCreate needs to use barrier
 * o keyDispatch needs to update the source record and update the barrier for
 *   the origin record
 * o review taskCommit / onRecordTask / onRecordTaskOutput / onSearchDone
 *   (which used to call jobLoad)
 * o make sure we manage nuncommitted and nunpropagated correctly?
 * o make sure we call propagateEnd whereever we need, including after
 *   propagating taskoutputs (any time nuncommitted or nunpropagated is
 *   decremented?)
 * o remove references to:
 *     - w_tasks_in, w_taskoutputs_in, w_abandon_throttle
 *     - j_queued
 *     - maxRecentRequests tunable
 * o bail out after too many EtagConflict errors, and call jobRemove
 * o piped jobs: pipeJob{SourceUpdated,TaskUpdated,TaskOutputReceived}
 * o quiesce: update quiesce() implementation
 * o make sure that the agent actually executes zero-key reduce tasks
 * o re-add agent timeouts
 * o don't handle redis connection going away transiently
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_extsprintf = require('extsprintf');
var mod_fs = require('fs');
var mod_path = require('path');
var mod_util = require('util');

var mod_carrier = require('carrier');
var mod_jsprim = require('jsprim');
var mod_redis = require('redis');
var mod_manta = require('manta');
var mod_mkdirp = require('mkdirp');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_bus = require('../bus');
var mod_locator = require('./locator');
var mod_mamoray = require('../moray');
var mod_moray = require('./moray');
var mod_schema = require('../schema');
var mod_mautil = require('../util');

var sprintf = mod_extsprintf.sprintf;
var mwConfSchema = require('./schema');
var Throttler = mod_mautil.Throttler;

var wQueries = require('./queries');

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
function WorkerJobState(args)
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

	this.j_input_done = !j['input'] && j['timeInputDone'] !== undefined;

	/*
	 * If this job's input comes from another job's output, extract that
	 * other job now.  This should have been validated by muskie already,
	 * and the form may be either "/:account/jobs/:jobid", or just ":jobid".
	 * To pipe input from another job, we must poll on the source job's job
	 * record (to watch for state == "done", which signals inputDone for
	 * this job), and the source job's last-phase, committed "taskoutput"
	 * records (which include output references).
	 */
	if (j['input']) {
		this.j_source_job = undefined;		/* source job record */
		this.j_source = j['input'].substr(
		    j['input'].lastIndexOf('/') + 1);	/* source jobid */

		/* XXX-MANTA-928: what to do with this? */
		this.j_source_poll_job = new mod_mamoray.PollState(
		    tunables['timePoll'], tunables['maxRecentRequests']);
		this.j_source_tasks = {};
	} else {
		this.j_source = undefined;
	}

	this.j_locates = 0;			/* nr of pending locates */
	this.j_auths = 0;			/* nr of pending auths */
	this.j_queued = 0;			/* nr of queued records */

	this.j_tasks = {};			/* set of outstanding tasks */
	this.j_phases = j['phases'].map(
	    function (phase) { return (new WorkerJobPhase(phase)); });

	this.j_save_throttle = new Throttler(tunables['timeJobSave']);
	this.j_save = new mod_mautil.SaveGeneration();
	this.j_init_barrier = undefined;
	this.j_subscrips = {};

	/*
	 * XXX-MANTA-928: removed j_init_records, j_init_waiting, j_polls
	 */
}

WorkerJobState.prototype.debugState = function ()
{
	return ({
	    'record': this.j_job,
	    'state': this.j_state,
	    'state_time': this.j_state_time,
	    'dropped': this.j_dropped,
	    'input_done': this.j_input_done,
	    'input_fully_read': this.j_input_fully_read,
	    'cancelled': this.j_cancelled,
	    'phases': this.j_phases,
	    'save_throttle': this.j_save_throttle,
	    'save_gen': this.j_save,
	    'nlocates': this.j_locates,
	    'nauths': this.j_auths,
	    'nqueued': this.j_queued
	});
};


/*
 * Stores runtime state about each phase in a job.
 */
function WorkerJobPhase(phase)
{
	var count = phase.hasOwnProperty('count') ? phase['count'] : 1;

	this.p_type = phase['type'];	/* phase type (same as in job record) */
	/*
	 * XXX remove p_npending, replace with p_nuncommitted, p_nunpropagated
	 */
	this.p_npending = 0;		/* number of uncommitted tasks */
	this.p_nuncommitted = undefined;	/* nr of uncommitted tasks */
	this.p_nunpropagated = undefined;	/* nr of unpropagated outputs */

	if (this.p_type != 'reduce')
		return;

	this.p_reducers = new Array(count);	/* task records */

	for (var i = 0; i < count; i++) {
		this.p_reducers[i] = {
		    'r_task': undefined,	/* task record */
		    'r_ninput': 0,		/* total nr of input keys */
		    'r_anon_stream': undefined,	/* file storing list of keys */
		    'r_anon_filename': undefined,
		    'r_anon_bufs': []
		};
	}
}


/*
 * Stores information about a specific task within a job.
 */
function WorkerJobTask(jobid, pi, taskid)
{
	this.t_id = taskid;		/* unique task identifier */
	this.t_xoutputs = [];		/* output keys NOT inside task record */
	this.t_record = undefined;	/* last received record for this task */
	this.t_value = {		/* authoritative task record */
	    'jobId': jobid,
	    'taskId': taskid,
	    'phaseNum': pi
	};
}


/*
 * XXX MANTA-928: removed WorkerAgent because we'll do timeouts by looking for
 * task records that haven't been updated in too long, rather than relying on
 * not having heard from an agent in too long.
 */

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

	worker = this;
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
	this.w_max_pending_locates = conf['tunables']['maxPendingLocates'];
	this.w_max_pending_auths = conf['tunables']['maxPendingAuths'];
	this.w_max_pending_deletes = conf['tunables']['maxPendingDeletes'];
	this.w_time_tick = conf['tunables']['timeTick'];
	this.w_time_poll = conf['tunables']['timePoll'];
	this.w_time_job_abandon = conf['tunables']['timeJobAbandon'];
	this.w_time_job_save = conf['tunables']['timeJobSave'];
	this.w_time_task_abandon = conf['tunables']['timeTaskAbandon'];
	this.w_tmproot = mwTmpfileRoot + '-' + this.w_uuid;

	this.w_names = {};
	mod_jsprim.forEachKey(this.w_buckets, function (name, bucket) {
		worker.w_names[bucket] = name;
	});

	/*
	 * XXX MANTA-928: removed w_poll_options
	 */

	/* helper objects */
	this.w_log = args['log'].child(
	    { 'component': 'Worker-' + this.w_uuid });

	this.w_redis = undefined;

	this.w_bus = mod_bus.createBus(conf, {
	    'log': this.w_log.child({ 'component': 'MorayBus' })
	});

	this.w_bus_options = {
	    'limit': conf['tunables']['maxRecordsPerQuery'],
	    'timePoll': conf['tunables']['timePoll']
	};

	this.w_record_handlers = {
	    'job': this.onRecordJob.bind(this),
	    'jobinput': this.onRecordJob.bind(this),
	    'task': this.onRecordTask.bind(this),
	    'taskoutput': this.onRecordTaskOutput.bind(this)
	};

// XXX MANTA-928
//	polls.wPollsOnce.forEach(function (pollconfig) {
//		var bucket, query, options;
//
//		bucket = worker.w_buckets[pollconfig['bucket']];
//
//		query = function (now) {
//			return (pollconfig['query'](
//			    worker.w_conf['tunables'], now));
//		};
//
//		options = Object.create(worker.w_bus_options);
//		if (pollconfig['limit'] !== undefined)
//			options['limit'] = pollconfig['limit'];
//
//		worker.w_bus.subscribe(bucket, query, options,
//			function (record, barrier) {
//			});
//	});

	this.w_locator = mod_locator.createLocator(args['conf'], {
	    'log': this.w_log.child({ 'component': 'MantaLocator' })
	});

	this.w_moray = mod_moray.createMoray({
	    'conf': conf,
	    'log': this.w_log.child({ 'component': 'MorayClient' })
	});

	this.w_queue = new mod_mamoray.MorayWriteQueue({
	    'log': this.w_log.child({ 'component': 'MorayQueue' }),
	    'client': function () { return (worker.w_moray); },
	    'buckets': this.w_buckets,
	    'maxpending': conf['tunables']['maxPendingPuts']
	});

	this.w_manta = mod_manta.createClient({
	    'connectTimeout': conf['manta']['connectTimeout'],
	    'url': conf['manta']['url'],
	    'log': this.w_log.child({ 'component': 'MantaClient' }),
	    'sign': mod_mautil.mantaSignNull
	});

	this.w_init_barrier = undefined;

	this.w_ticker = this.tick.bind(this);

	this.w_subscrips = {};			/* moray bus subscriptions */

	/* global dynamic state */
	this.w_pending_locates = 0;		/* nr of pending locate ops */
	this.w_pending_auths = 0;		/* nr of pending auth ops */
	this.w_pending_deletes = 0;		/* nr of pending delete ops */
	this.w_worker_start = undefined;	/* time worker started */
	this.w_worker_stopped = undefined;	/* time worker stopped */
	this.w_tick_start = undefined;		/* time last tick started */
	this.w_tick_done = undefined;		/* time last tick finished */
	this.w_timeout = undefined;		/* JS timeout handle */
	this.w_jobs = {};			/* all jobs, by jobId */
	this.w_tasks = {};			/* pending tasks, by taskId */
	this.w_agents = {};			/* known agents */
	this.w_jobs_piped = {};			/* jobs being piped into */
	this.w_quiesced = undefined;		/* whether to get new jobs */

	this.w_stats = {			/* stat counters */
	    'asgn_failed': 0,			/* failed job assignments */
	    'asgn_restart': 0			/* jobs picked up on restart */
	};

	/* incoming message queues */
	this.w_tasks_in = [];		/* incoming "task" record updates */
	this.w_taskoutputs_in = [];	/* incoming "task output" records */

	/*
	 * Outgoing locate requests are specified simply by Manta key (as a
	 * string).  The responses are enqueued on w_locates_in as an object:
	 *
	 *	lr_key		Manta key located
	 *
	 *	lr_objectid	Manta object uuid
	 *
	 *	lr_error	Detailed error message
	 *
	 *	lr_locations	Array of locations, each an object:
	 *
	 *	    lrl_server		physical server uuid
	 *
	 *	    lrl_zonename	mako zone uuid
	 *
	 * The pending requests are stored as objects in w_locates with:
	 *
	 *	l_key		Manta key to be located (uses account uuid)
	 *
	 *      l_login		Display name for this key's user
	 *
	 *	l_origins	List of jobinput or taskoutput records that are
	 *			waiting on the result of this request, in the
	 *			same format as aq_origins below
	 */
	this.w_locates_out = [];	/* outgoing "locate" requests */
	this.w_locates_in = [];		/* incoming "locate" responses */
	this.w_locates = {};		/* pending "locate" requests */

	/*
	 * Similarly, we keep a set of pending auth requests, each with:
	 *
	 *    aq_type		'jobinput' | 'taskinput'
	 *
	 *    aq_origins	list of records waiting on this request, each
	 *    			with:
	 *
	 *			    o_record	actual Moray record object
	 *
	 *			    o_ridx	reducer index, if any
	 *
	 *    aq_issued		issue time (for debugging)
	 *
	 * And corresponding responses, each with:
	 *
	 *    ar_key		user-facing key name (uses login name)
	 *
	 *    ar_error		error, if any
	 *
	 *    ar_login		resolved login name
	 *
	 *    ar_account	resolved account name
	 */
	this.w_auths_out = [];		/* outgoing "auth" requests */
	this.w_auths_in = [];		/* incoming "auth" responses */
	this.w_auths = {};		/* pending "auth" requests */

	/*
	 * Finally, we also keep track of pending "delete" requests.
	 */
	this.w_deletes_out = [];	/* outgoing "delete" requests */
	this.w_deletes = {};		/* pending "delete" requests */
}

Worker.prototype.debugState = function ()
{
	return ({
	    'conf': this.w_conf,
	    'pending_locates': this.w_pending_locates,
	    'pending_auths': this.w_pending_auths,
	    'pending_deletes': this.w_pending_deletes,
	    'worker_start': this.w_worker_start,
	    'worker_stopped': this.w_worker_stopped,
	    'tick_start': this.w_tick_start,
	    'tick_done': this.w_tick_done,
	    'agents': Object.keys(this.w_agents),
	    'ntasks_in': this.w_tasks_in.length,
	    'ntaskoutputs_in': this.w_taskoutputs_in.length,
	    'nauths_in': this.w_auths_in.length,
	    'nauths_out': this.w_auths_out.length,
	    'nlocates_in': this.w_locates_in.length,
	    'nlocates_out': this.w_locates_out.length,
	    'quiesced': this.w_quiesced
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
 * Start the worker: connect to Moray and start looking for work to do.
 */
Worker.prototype.start = function ()
{
	var worker = this;

	mod_assert.ok(this.w_worker_start === undefined);
	this.w_worker_start = new Date();
	this.w_init_barrier = mod_vasync.barrier();

	this.initTmp();
	this.initMoray();
	this.initRedis();

	mod_assert.ok(this.w_init_barrier.pending > 0);
	this.w_init_barrier.on('drain', function () {
		worker.w_log.info('worker started');
		process.nextTick(worker.w_ticker);
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

	this.w_log.info('initializing moray');
	this.w_init_barrier.start('moray');
	this.w_moray.setup(function (err) {
		if (err) {
			worker.w_log.fatal(err, 'failed to initialize moray');
			throw (err);
		}

		worker.w_log.info('moray connected and set up');

		worker.initPoll();
		worker.w_init_barrier.done('moray');
	});
};

Worker.prototype.initRedis = function ()
{
	var worker = this;
	var redis_conf = this.w_conf['auth'];
	var redis;

	this.w_log.info('initializing redis');
	this.w_init_barrier.start('redis');

	this.w_redis = redis = mod_redis.createClient(redis_conf['port'],
	    redis_conf['host'], redis_conf['options'] || {});

	redis.once('error', function (suberr) {
		worker.w_log.fatal(suberr, 'failed to connect to redis');
		throw (suberr);
	});

	redis.once('end', function () {
		worker.w_log.fatal('redis connection closed');
		throw (new Error('redis connection closed'));
	});

	redis.once('ready', function () {
		worker.w_log.info('redis connected');
		worker.w_redis.removeAllListeners('error');
		worker.w_init_barrier.done('redis');
	});
};

Worker.prototype.initPoll = function ()
{
	var worker = this;
	var queryconf = wQueries.wqJobsOwned;
	var query = queryconf['query'].bind(worker.w_conf);
	var bucket = this.w_buckets[queryconf['bucket']];
	var count = 0;

	this.w_bus.oneshot(bucket, query, this.w_poll_options,
	    function (record, barrier) {
		worker.jobCreate(record, barrier);
		count++;
	    },
	    function () {
		worker.w_log.info('found %d existing jobs', count);

		/*
		 * XXX MANTA-928 This should really only happen on
		 * barrier.on('drain'), but haven't wired that up yet.
		 */
		worker.initPollPost();
	    });
};

Worker.prototype.initPollPost = function ()
{
	var worker = this;
	var queries = [
	    wQueries.wqJobsAbandoned,
	    wQueries.wqJobsCancelled,
	    wQueries.wqJobsCreated,
	    wQueries.wqJobsInputEnded
	];

	queries.forEach(function (queryconf) {
		worker.w_subscrips[queryconf['name']] = worker.w_bus.subscribe(
		    worker.w_buckets[queryconf['bucket']], queryconf['query'],
		    worker.w_poll_options, worker.onRecord.bind(worker));
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
 * Process incoming messages and send queued outgoing messages.
 */
Worker.prototype.flush = function ()
{
	this.processQueues();
	this.w_queue.flush();
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
	this.flush();

	/* Check whether each job needs to be saved. */
	for (jobid in this.w_jobs)
		this.jobTick(this.w_jobs[jobid]);

	this.w_tick_done = new Date();
	this.w_timeout = setTimeout(this.w_ticker, this.w_time_tick);
};

Worker.prototype.onRecord = function (record, barrier)
{
	var schema, error, handler;

	this.w_log.debug('record: "%s" "%s" etag %s',
	    record['bucket'], record['key'], record['_etag']);

	schema = mod_schema.sBktJsonSchemas[this.w_names[record['bucket']]];
	error = mod_jsprim.validateJsonObject(schema, record['value']);
	if (error) {
		/* TODO how do we avoid seeing this record every time? */
		this.w_log.warn(error, 'onRecord: validation error', record);
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
	var worker = this;
	var job;

	/*
	 * If we don't already know about this job, then this must be new,
	 * abandoned, or one one of ours and we've just crashed.  Attempt to
	 * assign it to ourselves.
	 */
	if (!this.w_jobs.hasOwnProperty(job['jobId'])) {
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

	if (record['value']['timeCancelled'] !== undefined &&
	    record['value']['state'] != 'done') {
		job.j_log.info('job cancelled');
		job.j_cancelled = record['value']['timeCancelled'];

		job.j_etag = record['_etag'];
		job.j_job['state'] = 'done';
		job.j_job['timeCancelled'] = job.j_cancelled;
		job.j_job['timeDone'] = mod_jsprim.iso8601(Date.now());
		/* XXX dirty */

		this.jobCleanup(job);
		this.jobRemove(job);
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
		job.j_log.warn('onRecord: unknown reason for record match',
		    record);
		return;
	}

	if (!job.j_subscrips.hasOwnProperty('job inputs')) {
		job.j_log.warn('onRecord: already ended input', record);
		return;
	}

	job.j_log.info('job input completed');
	job.j_input_done = true;

	/* XXX dirty */
	job.j_etag = record['_etag'];
	job.j_job['timeInputDone'] = record['value']['timeInputDone'];

	this.w_bus.convertOneshot(job.j_subscrips['job inputs'], function () {
		job.j_input_fully_read = true;
		delete (job.j_subscrips['job inputs']);
		worker.jobPropagateEnd(job, null);
		worker.jobTick(job);
	});
};

Worker.prototype.onRecordJobInput = function (record)
{
	var job;

	if ((job = this.jobForRecord(record)) === null)
		return;

	mod_assert.ok(record['value']['nextRecordType'] === undefined);
	mod_assert.ok(record['value']['nextRecordId'] === undefined);

	job.j_job['stats']['nInputsRead']++;
	this.keyResolveUser(record['value']['key'], record);
};

Worker.prototype.onRecordTask = function (record)
{
	var job;

	if ((job = this.jobForRecord(record)) === null)
		return;

};

Worker.prototype.onRecordTaskOutput = function (record)
{
	var job;

	if ((job = this.jobForRecord(record)) === null)
		return;

};

Worker.prototype.jobForRecord = function (record)
{
	if (!this.w_jobs.hasOwnProperty(record['value']['jobId'])) {
		this.w_log.warn('onRecord: dropping record for unknown job',
		    record);
		return (null);
	}

	var job = this.w_jobs[record['value']['jobId']];

	if (job.j_dropped !== undefined) {
		this.w_log.warn('onRecord: dropping record for dropped job',
		    record);
		return (null);
	}

	if (job.j_state != 'running') {
		job.j_log.warn('onRecord: dropping record in job state "%s"',
		    job.j_state, record);
		return (null);
	}

	mod_assert.equal(job.j_state, 'running');
	return (job);
};

/*
 * Invoked when a Moray search request completes.  This only affects jobs in the
 * "initializing" state, which need to know when they've loaded all existing
 * state for a job.
 */
Worker.prototype.onSearchDone = function (job, bucket, start, count)
{
	var name = this.w_names[bucket];
	var limit = this.w_poll_options['limit'];

	if (job.j_dropped)
		return;

	mod_assert.ok(name != 'job');

	if (count === limit)
		/*
		 * We don't know we're done reloading the bucket until
		 * we get some number of records less than the limit.
		 */
		return;

	if (name == 'jobinput') {
		job.j_input_read = start;

		if (job.j_input_done &&
		    job.j_input_read > job.j_input_done)
			delete (job.j_polls['jobinput']);
	}

	if (job.j_state !== 'initializing')
		return;

	job.j_log.info('read all "%s" records', name);
	delete (job.j_init_waiting[name]);

	if (name == 'taskinput')
		delete (job.j_polls['taskinput']);

	this.flush();
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
	job = this.w_jobs[jobid] = new WorkerJobState({
	    'conf': this.w_conf,
	    'log': this.w_log.child({ 'component': 'Job-' + jobid }),
	    'record': record
	});

	if (job.j_source)
		this.w_jobs_piped[job.j_id] = true;

	/*
	 * Attempt to move this job from "unassigned" state to "initializing" by
	 * updating the Moray job record to have job['worker'] == our uuid.
	 */
	mod_assert.equal(job.j_state, 'unassigned');

	if (!job.j_job['timeAssigned']) {
		job.j_job['timeAssigned'] = mod_jsprim.iso8601(Date.now());
		job.j_job['stats'] = {
		    'nAssigns': 1,
		    'nErrors': 0,
		    'nInputsRead': 0,
		    'nJobOutputs': 0,
		    'nTasksDispatched': 0,
		    'nTasksCommittedOk': 0,
		    'nTasksCommittedFail': 0
		};
	} else {
		job.j_job['stats']['nAssigns']++;
	}

	job.j_job['state'] = 'running';
	job.j_job['worker'] = this.w_uuid;
	job.j_save.markDirty();
	this.jobSave(job);
};

Worker.prototype.jobAssigned = function (job)
{
	var worker = this;
	var barrier, queryconf, query;

	this.jobTransition(job, 'unassigned', 'initializing');
	barrier = job.j_init_barrier = mod_vasync.barrier();

	/*
	 * In order to know when the job is finished, we need to know how many
	 * uncommitted tasks and committed, unpropagated taskoutputs there are.
	 * Since we're the only component that writes new tasks and commits
	 * taskoutputs, these numbers cannot go up except by our own action, and
	 * if both of these numbers are zero, then there cannot be any
	 * outstanding work.  We need this same information on a per-phase basis
	 * in order to know when individual reducers' inputs are done.
	 */
	job.j_phases.forEach(function (_, i) {
		queryconf = wQueries.wqCountJobTasksUncommitted;
		query = queryconf['query'].bind(null, i, job.j_id);
		barrier.start('phase ' + i + ' tasks');
		worker.w_bus.count(worker.w_buckets[queryconf['bucket']],
		    query, worker.w_poll_options, function (c) {
			barrier.done('phase ' + i + ' tasks');
			mod_assert.equal(typeof (c), 'number');
			job.j_phases[i].p_nuncommitted = c;
		    });

		queryconf = wQueries.wqCountJobTaskOutputsUnpropagated;
		query = queryconf['query'].bind(null, i, job.j_id);
		barrier.start('phase ' + i + ' taskoutputs');
		worker.w_bus.count(worker.w_buckets[queryconf['bucket']],
		    query, worker.w_poll_options, function (c) {
			barrier.done('phase ' + i + ' taskoutputs');
			mod_assert.equal(typeof (c), 'number');
			job.j_phases[i].p_nunpropagated = c;
		    });
	});

	/*
	 * We also need to know the full set of reduce tasks already dispatched.
	 */
	queryconf = wQueries.wqJobTasksReduce;
	query = queryconf['query'].bind(null, job.j_id);
	barrier.start('fetch reduce tasks');
	this.w_bus.oneshot(worker.w_buckets[queryconf['bucket']], query,
	    this.w_poll_options, function (record) {
		/* XXX range check */
		var value = record['value'];
		var phase = job.j_phases[value['phaseNum']];
		var reducer = phase.p_reducers[value['rIdx']];

		/* XXX MANTA-928: initialize reducers properly */
		/* XXX initialize task properly, including r_ninput */
		reducer.r_task = record;
	    },
	    function () { barrier.done('fetch reduce tasks'); });

	barrier.on('drain', function () {
		worker.jobTransition(job, 'initializing', 'running');
		worker.jobLoaded(job);
	});
};

Worker.prototype.jobError = function (job, pi, code, message)
{
	job.j_job['stats']['nErrors']++;
	this.w_queue.dirty('error', mod_uuid.v4(), {
	    'jobId': job.j_id,
	    'phaseNum': pi,
	    'errorCode': code,
	    'errorMessage': message
	});
};

Worker.prototype.jobLoaded = function (job)
{
	/*
	 * Now that we've reloaded the job state, add subscriptions for the
	 * Moray queries we need to keep track of.
	 */
	var worker = this;
	var queries = [
	    wQueries.wqJobTasksDone,
	    wQueries.wqJobTaskOutputsUnpropagated
	];
	var qconf;

	if (!job.j_input_done) {
		queries.push(wQueries.wqJobInputs);
	} else {
		qconf = wQueries.wqJobInputs;
		job.j_subscrips[qconf['name']] = worker.w_bus.oneshot(
		    worker.w_buckets[qconf['bucket']],
		    qconf['query'].bind(null, job.j_id),
		    worker.w_poll_options, worker.onRecord.bind(worker),
		    function () {
			job.j_input_fully_read = true;
			delete (job.j_subscrips[qconf['name']]);
			worker.jobPropagateEnd(job, null);
			worker.jobTick(job);
		    });
	}

	queries.forEach(function (queryconf) {
		job.j_subscrips[queryconf['name']] = worker.w_bus.subscribe(
		    worker.w_buckets[queryconf['bucket']],
		    queryconf['query'].bind(null, job.j_id),
		    worker.w_poll_options, worker.onRecord.bind(worker));
	});

	/*
	 * Additionally, dispatch any reduce tasks that haven't already been
	 * dispatched.
	 */
	var now, pi, ri;
	var phase, reducer, task, value;

	now = mod_jsprim.iso8601(Date.now());

	for (pi = 0; pi < job.j_phases.length; pi++) {
		phase = job.j_phases[pi];
		if (!phase.p_reducers)
			continue;

		for (ri = 0; ri < phase.p_reducers.length; ri++) {
			reducer = phase.p_reducers[ri];

			if (mod_jsprim.isEmpty(this.w_agents)) {
				this.jobError(job, pi, 'EJ_NORESOURCES',
				    sprintf('no servers available for ' +
				        'reducer %d in phase %d', ri, pi));
				continue;
			}

			task = reducer.r_task = this.taskCreate(job, pi, now);
			value = task.t_value;
			value['rIdx'] = ri;
			value['server'] = mod_jsprim.randElt(
			    Object.keys(this.w_agents));
			this.w_queue.dirty('task', task.t_id, value);

			if (pi > 0)
				this.reducerInitKeyFile(reducer, job);
		}
	}
};

Worker.prototype.jobCleanup = function (job)
{
	var pi, phase, ri, reducer;

	for (pi = 0; pi < job.j_phases.length; pi++) {
		phase = job.j_phases[pi];

		if (!phase.p_reducers)
			continue;

		for (ri = 0; ri < phase.p_reducers.length; ri++) {
			reducer = phase.p_reducers[ri];
			this.reducerFiniKeyFile(reducer, job);
		}
	}
};

/*
 * Invoked to completely remove a job from this worker.
 */
Worker.prototype.jobRemove = function (job)
{
	var worker = this;
	var npending, taskid;

	if (job.j_state == 'unassigned')
		this.w_stats['asgn_failed']++;

	job.j_dropped = new Date();
	delete (this.w_jobs[job.j_id]);

	if (job.j_source)
		delete (this.w_jobs_piped[job.j_id]);

	mod_jsprim.forEachKey(job.j_subscrips, function (name, id) {
		worker.unsubscribe(id);
		delete (job.j_subscrips[name]);
	});

	job.j_log.info('job removed');

	if (job.j_save_throttle.ongoing())
		job.j_log.info('job removed with pending save operation');

	if (job.j_auths !== 0)
		job.j_log.info('job removed with %d pending auth requests',
		    job.j_auths);

	if (job.j_locates !== 0)
		job.j_log.info('job removed with %d pending locate requests',
		    job.j_locates);

	if (job.j_queued !== 0)
		job.j_log.info('job removed with %d pending incoming records',
		    job.j_queued);

	npending = 0;
	for (taskid in job.j_tasks) {
		if (this.w_queue.pending('task', taskid))
			npending++;

		delete (this.w_tasks[taskid]);
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
		job.j_job['timeDone'] = mod_jsprim.iso8601(new Date());
		job.j_job['state'] = 'done';

		this.w_bus.unsubscribe(job.j_subscrips['taskoutputs']);
		delete (job.j_subscrips['taskoutputs']);

		this.w_bus.unsubscribe(job.j_subsrips['done tasks']);
		delete (job.j_subscrips['done tasks']);

		mod_assert.ok(mod_jsprim.isEmpty(job.j_subscrips));
		this.jobTransition(job, 'running', 'finishing');
	}
};

Worker.prototype.jobSave = function (job)
{
	var worker = this;

	mod_assert.ok(!job.j_save_throttle.ongoing());
	job.j_save_throttle.start();
	job.j_log.debug('saving job (%j)', job.j_save);
	job.j_save.saveStart();

	this.w_moray.putObject(this.w_buckets['job'], job.j_id,
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
			job.j_save.saveFailed();
			job.j_log.warn(err, 'failed to save job');
		} else {
			job.j_save.saveOk();
			job.j_log.debug('saved job (%j)', job.j_save);
		}

		job.j_save_throttle.done();

		if (job.j_state == 'unassigned' && !job.j_dropped) {
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

	if (job.j_locates > 0 || job.j_queued > 0 || job.j_auths > 0)
		return (false);

	for (pi = 0; pi < job.j_phases.length; pi++) {
		if (job.j_phases[pi].p_nuncommitted > 0 ||
		    job.j_phases[pi].p_nunpropagated > 0)
			return (false);
	}

	return (true);
};

/*
 * Load an updated source record for a piped job.
 */
Worker.prototype.pipeJobSourceUpdated = function (job, record)
{
};

/*
 * Load an updated source task record for a piped job.
 */
Worker.prototype.pipeJobTaskUpdated = function (job, record)
{
};

Worker.prototype.pipeJobTaskOutputReceived = function (job, state, record)
{
};

/*
 * Most of the work done by this service goes through one of several queues
 * documented in Worker above.  This function is invoked periodically to
 * process work on each of these queues.
 */
Worker.prototype.processQueues = function ()
{
	var now, ent, key, loc, lreq, auth, areq, i, task, len, job;
	var err, dispatch;
	var changedtasks = {};
	var queries = [];

	now = mod_jsprim.iso8601(new Date());

	while (this.w_auths_in.length > 0) {
		auth = this.w_auths_in.shift();
		areq = this.w_auths[auth.ar_key];
		mod_assert.ok(areq);

		for (i = 0; i < areq.aq_origins.length; i++) {
			ent = areq.aq_origins[i];
			job = this.w_jobs[ent.o_record['value']['jobId']];

			if (!job) {
				this.w_log.warn('record references unknown job',
				    ent.o_record);
				continue;
			}

			job.j_auths--;

			job.j_log.debug('resolve "%s"', auth.ar_key, auth);

			err = auth.ar_error;
			if (!err &&
			    !isAuthorized(job, auth.ar_account, auth.ar_key)) {
				err = {
				    'code': 'EJ_ACCES',
				    'message': 'permission denied: "' +
					auth.ar_key + '"'
				};
			}

			if (!err) {
				this.keyLocate(
				    pathSwapFirst(auth.ar_key, auth.ar_account),
				    auth.ar_login, ent);
				continue;
			}

			dispatch = {
			    'd_job': job,
			    'd_pi': undefined,
			    'd_p0key': undefined,
			    'd_origin': ent.o_record,
			    'd_ridx': ent.o_ridx,
			    'd_key': auth.ar_key,
			    'd_account': null,
			    'd_locate': null,
			    'd_time': now,
			    'd_error': err
			};

			this.keyDispatch(dispatch);
		}

		this.w_pending_auths--;
		delete (this.w_auths[auth.ar_key]);
	}

	while (this.w_locates_in.length > 0) {
		loc = this.w_locates_in.shift();
		lreq = this.w_locates[loc.lr_key];
		mod_assert.ok(lreq);

		for (i = 0; i < lreq.l_origins.length; i++) {
			ent = lreq.l_origins[i];
			job = this.w_jobs[ent.o_record['value']['jobId']];

			if (!job) {
				this.w_log.warn('record references unknown job',
				    ent.o_record);
				continue;
			}

			job.j_locates--;

			dispatch = {
			    'd_job': job,
			    'd_pi': undefined,
			    'd_p0key': undefined,
			    'd_origin': ent.o_record,
			    'd_ri': ent.o_ridx,
			    'd_key': pathSwapFirst(lreq.l_key, lreq.l_login),
			    'd_account': mod_mautil.pathExtractFirst(
				lreq.l_key),
			    'd_locate': loc,
			    'd_time': now,
			    'd_error': undefined
			};

			this.keyDispatch(dispatch);
		}

		this.w_pending_locates--;
		delete (this.w_locates[loc.lr_key]);
	}

	while (this.w_tasks_in.length > 0) {
		ent = this.w_tasks_in.shift();
		task = this.w_tasks[ent['value']['taskId']];
		job = this.w_jobs[ent['value']['jobId']];

		if (!job) {
			this.w_log.warn('record references unknown job', ent);
			continue;
		}

		job.j_queued--;

		if (!task) {
			this.w_log.warn('record references unknown task', ent);
			continue;
		}

		if (task.t_record && task.t_record['_etag'] == ent['_etag'])
			continue;

		if (task.t_value['timeCommitted'])
			continue;

		/*
		 * If we've got a pending write for this task already, then
		 * we've either timed it out, cancelled it, or we've got a
		 * pending update to timeInputDone.  In the first two cases, we
		 * can ignore the external update because we've already given up
		 * on this task.  In the last case, the only relevant external
		 * update is a state change to "aborted", in which case we
		 * want to clobber our own change.
		 */
		if (this.w_queue.pending('task', task.t_id) &&
		    task.t_value['state'] == 'cancelled') {
			job.j_log.warn('task "%s": ignoring update because ' +
			    'a clobbering write is already pending',
			    task.t_id, ent);
			continue;
		}

		if (!ent['value']['timeInputDone'] &&
		    task.t_value['timeInputDone'])
			ent['value']['timeInputDone'] =
			    task.t_value['timeInputDone'];
		if (!ent['value']['nInputs'] && task.t_value['nInputs'])
			ent['value']['nInputs'] = task.t_value['nInputs'];

		task.t_record = ent;
		task.t_value = ent['value'];
		changedtasks[task.t_id] = true;

		/*
		 * If there is a non-clobbering write pending, we must update
		 * the etag being used in the write.
		 */
		if (this.w_queue.pending('task', task.t_id)) {
			job.j_log.debug('task "%s": updating etag in cache',
			    task.t_id);
			this.w_queue.dirty('task', task.t_id, task.t_value,
			    { 'etag': ent['_etag'] });
		}
	}

	while (this.w_taskoutputs_in.length > 0) {
		ent = this.w_taskoutputs_in.shift();
		task = this.w_tasks[ent['value']['taskId']];
		job = this.w_jobs[ent['value']['jobId']];

		if (!job) {
			this.w_log.warn('record references unknown job', ent);
			continue;
		}

		job.j_queued--;

		if (!task) {
			this.w_log.warn(
			    'taskoutput record references unknown task', ent);
			continue;
		}

		/* See above. */
		if (this.w_queue.pending('task', task.t_id)) {
			job.j_log.warn('task "%s": ignoring new taskoutput ' +
			    'because a task write is already pending',
			    task.t_id, ent);
			continue;
		}

		if (ent['value']['error'])
			job.j_job['stats']['nErrors']++;

		task.t_xoutputs.push(ent['value']);
		changedtasks[task.t_id] = true;
	}

	for (key in changedtasks) {
		task = this.w_tasks[key];
		job = this.w_jobs[task.t_value['jobId']];
		len = task.t_xoutputs.length +
		    (task.t_value['firstOutputs'] ?
		    task.t_value['firstOutputs'].length : 0);

		/*
		 * Wait until the task is finished running and, if successful,
		 * we've collected all of its output.  Ignore cancelled tasks.
		 */
		if (task.t_value['state'] == 'running' ||
		    task.t_value['state'] == 'dispatched' ||
		    task.t_value['state'] == 'cancelled')
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
		    task.t_value['nOutputs'] <= len),
		    'task ' + task.t_id + ' is invalid');

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

	while (this.w_pending_auths < this.w_max_pending_auths &&
	    this.w_auths_out.length > 0) {
		key = this.w_auths_out.shift();
		this.w_pending_auths++;
		this.doResolve(key);
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
	mod_assert.equal(this.w_tasks_in.length, 0);
	mod_assert.equal(this.w_taskoutputs_in.length, 0);
	mod_assert.ok(this.w_locates_out.length === 0 ||
	    this.w_pending_locates === this.w_max_pending_locates);
};

/*
 * Create a new "task" record for phase "pi" of job "job".
 */
Worker.prototype.taskCreate = function (job, pi, now)
{
	mod_assert.ok(pi >= 0 && pi < job.j_phases.length);

	var task = new WorkerJobTask(job.j_id, pi, mod_uuid.v4());
	task.t_value['state'] = 'dispatched';
	task.t_value['timeDispatched'] = now;
	job.j_phases[pi].p_npending++;
	job.j_tasks[task.t_id] = true;
	this.w_tasks[task.t_id] = task;

	job.j_job['stats']['nTasksDispatched']++;

	return (task);
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

	/* XXX MANTA-928 need to figure out "write" story here */
	this.w_queue.dirty('task', task.t_id, record);

	job = this.w_jobs[record['jobId']];
	job.j_log.debug('committing task "%s"', task.t_id);
	job.j_phases[record['phaseNum']].p_npending--;

	if (record['result'] == 'ok') {
		job.j_job['stats']['nTasksCommittedOk']++;

		if (record['phaseNum'] == job.j_phases.length - 1)
			job.j_job['stats']['nJobOutputs'] += record['nOutputs'];
	} else {
		job.j_job['stats']['nTasksCommittedFail']++;

		if (record['error'])
			job.j_job['stats']['nErrors']++;
	}

	if (record['phaseNum'] > 0)
		this.taskCleanup(task, job);

	/*
	 * If there's no output to propagate, check whether we should mark a
	 * subsequent reduce phase as done.  We must only do this on the next
	 * tick in case there remain other keys to be processed first (which
	 * will become inputs for reduce tasks whose input we mark "done").
	 */
	if (record['nOutputs'] === 0 || record['result'] == 'fail') {
		var worker = this;

		process.nextTick(function () {
			worker.jobPropagateEnd(job, now);
		});
	}

	if (record['result'] != 'ok' ||
	    record['phaseNum'] == job.j_phases.length - 1)
		return;

	for (i = 0; i < record['firstOutputs'].length; i++)
		this.keyResolveUser(record['firstOutputs'][i]['key'],
		    task.t_record, record['firstOutputs'][i]['rIdx']);

	for (i = 0; i < task.t_xoutputs.length; i++)
		this.keyResolveUser(task.t_xoutputs[i]['key'], task.t_record,
		    task.t_xoutputs[i]['rIdx']);
};

Worker.prototype.taskCleanup = function (task, job)
{
	var key = task.t_value['key'];
	var phase, reducer, stream, i;
	var worker = this;

	/*
	 * Map tasks are easy because the key we have to delete is stored inside
	 * the record itself.
	 */
	if (key) {
		if (keyIsAnonymous(key, job.j_id))
			this.w_deletes_out.push({
			    'token': job.j_job['authToken'],
			    'key': key
			});

		return;
	}

	phase = job.j_phases[task.t_value['phaseNum']];
	if (!phase) {
		this.w_log.warn('taskCleanup: invalid phase number', task);
		return;
	}

	this.w_log.info('taskCleanup: deleting keys for reduce task "%s"',
	    task.t_id);

	for (i = 0; i < phase.p_reducers.length; i++) {
		reducer = phase.p_reducers[i];
		if (reducer.r_task && reducer.r_task.t_id == task.t_id)
			break;
	}

	mod_assert.ok(i < phase.p_reducers.length);

	stream = mod_fs.createReadStream(reducer.r_anon_filename);

	stream.on('open', function () {
		worker.reducerFiniKeyFile(reducer, job);
	});

	stream.on('error', function (err) {
		worker.w_log.error(err, 'failed to open "%s" for read',
		    reducer.r_anon_filename);
	});

	mod_carrier.carry(stream, function (filekey) {
		var keyname = filekey.toString('utf8');
		mod_assert.ok(keyIsAnonymous(keyname, job.j_id));
		/* TODO flow control */
		worker.w_deletes_out.push({
		    'token': job.j_job['authToken'],
		    'key': keyname
		});
	});
};

/*
 * Resolve the username associated with a Manta key.
 */
Worker.prototype.keyResolveUser = function (key, ent, ri)
{
	var job = this.w_jobs[ent['value']['jobId']];
	var type = ent['bucket'] == this.w_buckets['jobinput'] ?
	    'jobinput' : 'taskinput';
	var entry = {
	    'o_record': ent,
	    'o_ridx': ri
	};

	job.j_auths++;

	if (this.w_auths.hasOwnProperty(key)) {
		mod_assert.equal(this.w_auths[key].aq_type, type);
		this.w_auths[key].aq_origins.push(entry);
		job.j_log.debug('resolve "%s": piggy-backing onto existing ' +
		    'request', key);
		return;
	}

	this.w_auths[key] = {
	    'aq_type': type,
	    'aq_origins': [ entry ]
	};

	this.w_auths_out.push(key);
	job.j_log.debug('resolve "%s": new request', key);
};

Worker.prototype.doResolve = function (key)
{
	var areq, worker, component, redis_key;

	areq = this.w_auths[key];
	mod_assert.ok(areq);

	component = mod_mautil.pathExtractFirst(key);
	if (!component) {
		this.w_auths_in.push({
		    'ar_key': key,
		    'ar_error': {
			'code': 'EJ_NOENT',
			'message': 'malformed key: "' + key + '"'
		    }
		});

		return;
	}

	worker = this;
	areq.aq_issued = Date.now();
	redis_key = '/login/' + component;
	this.w_redis.get(redis_key, function (err, data) {
		var auth, record;

		auth = {};
		auth.ar_key = key;

		if (!err) {
			try {
				record = JSON.parse(data);
			} catch (ex) {
				err = ex;
			}
		}

		if (err) {
			auth.ar_error = {
			    'code': 'EJ_INTERNAL',
			    'message': err.message
			};
		} else if (record === null) {
			auth.ar_error = {
			    'code': 'EJ_NOENT',
			    'message': 'no such object: "' + key + '"'
			};
		} else {
			auth.ar_account = record['uuid'];
			auth.ar_login = component;
		}

		worker.w_auths_in.push(auth);
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

	this.w_manta.unlink(key, options, function (err) {
		worker.w_pending_deletes--;
		delete (worker.w_deletes[key]);

		if (err) {
			worker.w_log.error(err, 'delete "%s": failed', key);
			return;
		}

		worker.w_log.debug('delete "%s": okay', key);
	});
};

/*
 * Enqueue a request to locate key "key", triggered by record "ent".
 */
Worker.prototype.keyLocate = function (key, login, ent)
{
	var job = this.w_jobs[ent.o_record['value']['jobId']];
	job.j_locates++;

	if (this.w_locates.hasOwnProperty(key)) {
		job.j_log.debug('locate "%s": piggy-backing onto existing ' +
		    'request', key);
		this.w_locates[key].l_origins.push(ent);
		return;
	}

	this.w_locates[key] = {
	    'l_key': key,
	    'l_login': login,
	    'l_origins': [ ent ]
	};

	this.w_locates_out.push(key);
	job.j_log.debug('locate "%s": enqueued new request', key);
};

/*
 * Dispatch a key to its map or reduce phase.  The "dispatch" argument has the
 * following fields:
 *
 *    d_job		Job object.
 *
 *    d_origin		The record that generated this key: either a job input
 *    			record or a task record from a previous phase.
 *
 *    d_ri		The reducer index, if any, for this key.
 *
 *    d_key		The key to be dispatched, as the user sees it (with an
 *    			unresolved account login name).
 *
 *    d_error		If set, an error has already occurred and the task
 *    			should be dispatched just to indicate the error.
 *
 *    d_time		Current time, to become the dispatch time for the task.
 *
 * If d_error is not set, then the following additional fields should be set:
 *
 *    d_account		The account uuid for the key's owner.  The agent needs
 *    			this to locate the key.
 *
 *    d_locate		Location result (see documentation above).
 *
 * This function fills in the following fields before delegating to the map- or
 * reduce-specific dispatch function:
 *
 *    d_pi		Phase number for the task to be dispatched.
 *
 *    d_error		If the key doesn't exist.
 */
Worker.prototype.keyDispatch = function (dispatch)
{
	var job, source;

	job = dispatch.d_job;
	mod_assert.ok(job);

	source = dispatch.d_origin;

	if (source['bucket'] == this.w_buckets['jobinput']) {
		dispatch.d_pi = 0;
	} else {
		mod_assert.equal(source['bucket'], this.w_buckets['task']);
		dispatch.d_pi = source['value']['phaseNum'] + 1;
		mod_assert.ok(dispatch.d_pi < job.j_phases.length);
	}

	if (!dispatch.d_error && dispatch.d_locate.lr_error) {
		var error = dispatch.d_locate.lr_error;

		dispatch.d_error = {
		    'code': error.code,
		    'message': sprintf('%s: "%s"', error.message,
			dispatch.d_key)
		};
	}

	if (dispatch.d_error !== undefined)
		this.keyDispatchError(dispatch);
	else if (job.j_phases[dispatch.d_pi].p_type == 'reduce')
		this.keyDispatchReduce(dispatch);
	else
		this.keyDispatchMap(dispatch);
};

Worker.prototype.keyDispatchError = function (dispatch)
{
	var job = dispatch.d_job;

	var value = {
	    'jobId': job.j_id,
	    'phaseNum': dispatch.d_pi,
	    'errorCode': dispatch.d_error['code'],
	    'errorMessage': dispatch.d_error['message'],
	    'input': dispatch.d_key,
	    'p0input': dispatch.d_pi === 0 ? dispatch.d_key :
	        dispatch.d_origin['value']['p0key']
	};

	if (dispatch.d_error['messageInternal'] !== undefined)
		value['errorMessageInternal'] =
		    dispatch.d_error['messageInternal'];

	this.w_queue.dirty('error', mod_uuid.v4(), value);

	job.j_job['stats']['nErrors']++;
};

/*
 * Implementation of keyDispatch() for map phases.  This is simple because we
 * always dispatch a new task.
 */
Worker.prototype.keyDispatchMap = function (dispatch)
{
	var loc, which, server, zonename, task, value;

	/*
	 * Figure out where to run this task.  For non-zero-byte objects, run
	 * the task on one of the systems where the object is stored.
	 * Otherwise, pick a server at random that we know about.
	 */
	loc = dispatch.d_locate;
	if (loc.lr_objectid !== null) {
		which = mod_jsprim.randElt(loc.lr_locations);
		server = which.lrl_server;
		zonename = which.lrl_zonename;
	} else if (!mod_jsprim.isEmpty(this.w_agents)) {
		server = mod_jsprim.randElt(Object.keys(this.w_agents));
		zonename = '';
	} else {
		dispatch.d_error = {
		    'code': 'EJ_NORESOURCES',
		    'message': 'no servers available to run task'
		};
		this.keyDispatchError(dispatch);
		return;
	}

	task = this.taskCreate(dispatch.d_job, dispatch.d_pi, dispatch.d_time);
	value = task.t_value;
	value['key'] = dispatch.d_key;
	value['p0key'] = dispatch.d_pi === 0 ? dispatch.d_key :
	    dispatch.d_origin['value']['p0key'];
	value['account'] = dispatch.d_account;
	value['objectid'] = loc.lr_objectid || '/dev/null';
	value['server'] = server;
	value['zonename'] = zonename;
	this.w_queue.dirty('task', task.t_id, value);
};

/*
 * Implementation of keyDispatch() for reduce phases.  Write a taskinput record
 * for this key.
 */
Worker.prototype.keyDispatchReduce = function (dispatch)
{
	var job, pi, ri, uuid;
	var task, reducer, phase;
	var worker = this;

	job = dispatch.d_job;
	pi = dispatch.d_pi;
	mod_assert.equal(job.j_phases[pi].p_type, 'reduce');

	phase = job.j_phases[pi];
	ri = dispatch.d_ri !== undefined ? dispatch.d_ri :
	    Math.floor(Math.random() * phase.p_reducers.length);

	if (ri >= phase.p_reducers.length) {
		dispatch.d_error = {
		    'code': 'EJ_INVAL',
		    'message': 'specified reducer out of range: "' +
			ri + '"'
		};

		this.keyDispatchError(dispatch);
		return;
	}

	reducer = phase.p_reducers[ri];
	reducer.r_ninput++;

	task = reducer.r_task;
	uuid = mod_uuid.v4();
	this.w_queue.dirty('taskinput', uuid, {
	    'taskInputId': uuid,
	    'jobId': job.j_id,
	    'taskId': task.t_id,
	    'server': task.t_value['server'],
	    'key': dispatch.d_key,
	    'p0key': dispatch.d_pi === 0 ? dispatch.d_key :
	        dispatch.d_origin['value']['p0key'],
	    'account': dispatch.d_account,
	    'objectid': dispatch.d_locate.lr_objectid || '/dev/null',
	    'servers': dispatch.d_locate.lr_locations.map(function (l) {
		return ({
		    'server': l.lrl_server,
		    'zonename': l.lrl_zonename
		});
	    }),
	    'timeDispatched': dispatch.d_time
	});

	if (pi > 0 && keyIsAnonymous(dispatch.d_key, job.j_id)) {
		if (reducer.r_anon_bufs)
			reducer.r_anon_bufs.push(dispatch.d_key);
		else
			reducer.r_anon_stream.write(dispatch.d_key + '\n');
	}

	/*
	 * We want to do the rest of this only after any remaining keys for this
	 * phase are dispatched.  Those keys must be dispatched on this tick.
	 * XXX MANTA-928: review me
	 */
	process.nextTick(function () {
		worker.jobPropagateEnd(job, dispatch.d_time);
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

		if (phase.p_nuncommitted > 0 || phase.p_nunpropagated)
			/* Tasks still running. */
			return;
	}

	if (pi < job.j_phases.length) {
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

		if (reducer.r_anon_stream &&
		    reducer.r_anon_stream.writable &&
		    !reducer.r_anon_bufs)
			reducer.r_anon_stream.end();

		job.j_log.info('marking input done for ' +
		    'reduce phase %d (task %s)', i, task.t_id);
		task.t_value['timeInputDone'] = now;
		task.t_value['nInputs'] = reducer.r_ninput;

		var options = {};
		if (task.t_record)
			options['etag'] = task.t_record['_etag'];

		worker.w_queue.dirty('task', task.t_id, task.t_value, options);
	});
};

Worker.prototype.reducerInitKeyFile = function (reducer, job)
{
	var task = reducer.r_task;

	reducer.r_anon_filename = sprintf('%s/%s', this.w_tmproot, task.t_id);
	reducer.r_anon_stream = mod_fs.createWriteStream(
	    reducer.r_anon_filename);

	var onErr = function (err) {
		job.j_log.error(err, 'failed to open "%s"',
		    reducer.r_anon_filename);
	};

	reducer.r_anon_stream.on('error', onErr);

	reducer.r_anon_stream.on('open', function () {
		reducer.r_anon_stream.removeListener('error', onErr);
		reducer.r_anon_stream.on('error', function (err) {
			job.j_log.error(err, 'error on "%s"',
			    reducer.r_anon_filename);
		});

		reducer.r_anon_bufs.forEach(function (key) {
			reducer.r_anon_stream.write(key + '\n');
		});

		if (reducer.r_task.t_value['timeInputDone'])
			reducer.r_anon_stream.end();

		reducer.r_anon_bufs = null;
	});
};

Worker.prototype.reducerFiniKeyFile = function (reducer, job)
{
	if (!reducer.r_anon_filename)
		return;

	job.j_log.info('deleting "%s"', reducer.r_anon_filename);

	mod_fs.unlink(reducer.r_anon_filename, function (err) {
		if (err) {
			job.j_log.warn(err, 'failed to delete "%s"',
			    reducer.r_anon_filename);
		}
	});
};

/*
 * Handle an incoming "locate" response.
 */
Worker.prototype.onLocate = function (keys, err, locations)
{
	var key, i;
	var worker = this;

	if (err)
		this.w_log.warn(err, 'failed to locate some keys');

	for (i = 0; i < keys.length; i++) {
		key = keys[i];

		this.w_log.debug('locate response for "%s"', key);

		if (locations[key]['error']) {
			this.w_log.warn(locations[key]['error'],
			    'error locating key "%s"', key);

			this.w_locates_in.push({
			    'lr_key': key,
			    'lr_objectid': undefined,
			    'lr_error': locations[key]['error'],
			    'lr_locations': []
			});

			continue;
		}

		locations[key].forEach(function (loc) {
			if (worker.w_agents.hasOwnProperty(loc['host']))
				return;

			worker.w_agents[loc['host']] = true;
		});

		var objectid = locations[key].length > 0 ?
		    locations[key][0]['objectid'] : null;

		this.w_locates_in.push({
			'lr_key': key,
			'lr_objectid': objectid,
			'lr_error': undefined,
			'lr_locations': locations[key].map(function (l) {
			    return ({
				'lrl_server': l['host'],
				'lrl_zonename': l['zonename']
			    });
			})
		});
	}

	this.flush();
};


/*
 * Quiesce the worker, as in preparation for an update.  Stop picking up new
 * jobs, but keep processing existing ones.
 */
Worker.prototype.quiesce = function (doquiesce)
{
	if (doquiesce && this.w_quiesced === undefined) {
		this.w_quiesced = Date.now();
		this.w_log.info('quiesced');
	} else if (!doquiesce && this.w_quiesced !== undefined) {
		this.w_quiesced = undefined;
		this.w_log.info('resumed');
	}
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
	if (account == job.j_job['owner'])
		return (true);

	/*
	 * Operators are allowed to access anyone's objects.
	 */
	if (job.j_job['auth'] !== undefined &&
	    job.j_job['auth']['groups'].indexOf('operators') != -1)
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
