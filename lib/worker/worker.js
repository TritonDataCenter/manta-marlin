/*
 * lib/worker/worker.js: job worker implementation
 */

/*
 * A Marlin deployment includes a fleet of job workers that are responsible for
 * managing the distributed execution of Marlin jobs.  The core of each worker
 * is a loop that looks for new and abandoned jobs, divides each job into chunks
 * called task groups, assigns these task groups to individual compute nodes,
 * monitors each node's progress, and collects the results.  While individual
 * workers are not resource-intensive, a fleet is used to support very large
 * numbers of jobs concurrently and to provide increased availability in the
 * face of failures and partitions.
 *
 * Jobs are represented as records within Moray instances, which are themselves
 * highly available.  At any given time, a job is assigned to at most one
 * worker, and this assignment is stored in the job's record in Moray.  Workers
 * do not maintain any state which cannot be reconstructed from the state stored
 * in Moray, which makes it possible for workers to pick up jobs abandoned by
 * other workers which have failed or become partitioned.  In order to detect
 * such failures, workers must update job records on a regular basis (even if
 * there's no substantial state change).  Job records which have not been
 * updated for too long will be grabbed up by idle workers.
 *
 * All communication among the workers, compute nodes, and the web tier (through
 * which jobs are submitted and monitored) goes through Moray.  The Moray
 * interface is abstracted out so that it can be replaced with an alternative
 * mechanism for testing.
 */

/*
 * Implementation TODO:
 * o Make sure all operations using js_pending_start have timeouts and also
 *   record that they're happening somewhere for debugging.
 */

var mod_assert = require('assert');

var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_verror = require('verror');

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
	'findInterval': mod_schema.sIntervalRequired,
	'jobAbandonTime': mod_schema.sIntervalRequired,
	'taskGroupInterval': mod_schema.sIntervalRequired,
	'jobsBucket': mod_schema.sStringRequiredNonEmpty,
	'taskGroupsBucket': mod_schema.sStringRequiredNonEmpty
    }
};


/* Public interface */
exports.mwConfSchema = mwConfSchema;
exports.mwWorker = mwWorker;


/*
 * Manages jobs owned by a single Marlin worker.  Most job management actually
 * happens in the mwJobState class below.  Arguments for this class include:
 *
 *    instanceUuid	Unique identifier for this worker instance
 *
 *    moray		Moray interface
 *
 *    locator		Locator interface
 *
 *    log		Bunyan-style logger instance
 *
 *    jobAbandonTime	idle time after which a job is declared abandoned
 *
 *    saveInterval	how frequently to save each job, even when idle
 *
 *    tickInterval	how frequently to check for various timeouts
 *
 *    taskGroupIdleTime	how long before a task group is declared dead
 *
 * The worker itself doesn't do much except accept incoming jobs, which are
 * managed by separate mwJobState objects.  The worker also manages a global
 * timeout that fires every 'tickInterval' milliseconds, which causes each job's
 * state to be reevaluated.
 */
function mwWorker(args)
{
	mod_assert.equal(typeof (args['instanceUuid']), 'string');
	mod_assert.equal(typeof (args['moray']), 'object');
	mod_assert.equal(typeof (args['locator']), 'object');
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.equal(typeof (args['jobAbandonTime']), 'number');
	mod_assert.equal(typeof (args['tickInterval']), 'number');
	mod_assert.equal(typeof (args['saveInterval']), 'number');
	mod_assert.equal(typeof (args['taskGroupIdleTime']), 'number');

	/* immutable state */
	this.mw_uuid = args['instanceUuid'];
	this.mw_interval = args['tickInterval'];
	this.mw_job_abandon_time = args['jobAbandonTime'];
	this.mw_save_interval = args['saveInterval'];
	this.mw_tg_idle_time = args['taskGroupIdleTime'];

	/* helper objects */
	this.mw_log = args['log'].child({ 'worker': this.mw_uuid });
	this.mw_moray = args['moray'];
	this.mw_locator = args['locator'];

	/* dynamic state */
	this.mw_jobs_initial = true;		/* initial jobs query */
	this.mw_jobs = {};			/* all jobs, by jobId */
	this.mw_timeout = undefined;		/* JS timeout handle */
	this.mw_worker_start = undefined;	/* time worker started */
	this.mw_worker_stopped = undefined;	/* time worker stopped */
	this.mw_tick_start = undefined;		/* time last tick started */
	this.mw_tick_done = undefined;		/* time last tick finished */
	this.mw_stats = {			/* stat counters */
	    'asgn_failed': 0,		/* failed job assignments */
	    'asgn_restart': 0		/* jobs picked up on restart */
	};
}

/*
 * Start the worker by scheduling an immediate tick.
 */
mwWorker.prototype.start = function ()
{
	var worker = this;
	this.mw_worker_start = new Date();
	this.mw_log.info('starting worker');
	process.nextTick(function workerTickStart() { worker.tick(); });
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
 * Schedule the next tick at the appropriate interval in the future.
 */
mwWorker.prototype.setTimer = function ()
{
	var worker = this;

	mod_assert.ok(this.mw_timeout === undefined);
	this.mw_timeout = setTimeout(
	    function workerTickTick() { worker.tick(); }, this.mw_interval);
};

/*
 * The heart of the job worker, this function tells our Moray interface to scan
 * for new unassigned jobs and then invokes "tick" on each of our existing jobs
 * to reevaluate their states.
 */
mwWorker.prototype.tick = function ()
{
	this.mw_tick_start = new Date();
	this.mw_timeout = undefined;

	this.mw_moray.findUnassignedJobs(this.mw_jobs_initial ? this.mw_uuid :
	    undefined, this.mw_job_abandon_time, this.onJob.bind(this));

	mod_jsprim.forEachKey(this.mw_jobs, function (_, job) { job.tick(); });

	this.mw_tick_done = new Date();
	this.setTimer();
};

/*
 * Invoked when our Moray interface finds an unassigned job for us to pick up.
 */
mwWorker.prototype.onJob = function (metadata, job)
{
	var log = this.mw_log;
	var error, jobid;

	this.mw_jobs_initial = false;

	/*
	 * We can get some common cases out of the way before validation.
	 * Ignore jobs we already know about.  We'll see records for all jobs
	 * that we own for their duration, at least until we have a more
	 * efficient way to query Moray.
	 */
	jobid = job['jobId'];
	if (jobid && this.mw_jobs.hasOwnProperty(jobid))
		return;

	/*
	 * Ignore jobs we've already completed before.
	 */
	if (job['doneTime'])
		return;

	error = mod_jsprim.validateJsonObject(mod_schema.sMorayJob, job);
	if (error) {
		log.warn(error, 'ignoring invalid job record', job);
		return;
	}

	if (job['worker'] == this.mw_uuid) {
		log.info('picking up job previously owned by us: %s', jobid);
		this.mw_stats['asgn_restart']++;
	}

	var newjob = new mwJobState({
	    'job': job,
	    'initial_etag': metadata['etag'],
	    'log': this.mw_log,
	    'locator': this.mw_locator,
	    'moray': this.mw_moray,
	    'worker_uuid': this.mw_uuid,
	    'worker': this,
	    'save_interval': this.mw_save_interval,
	    'taskgroup_idle_time': this.mw_tg_idle_time
	});

	this.mw_jobs[jobid] = newjob;
	newjob.tick();
};

mwWorker.prototype.dropJob = function (jobid)
{
	var job = this.mw_jobs[jobid];

	if (job.js_state == 'unassigned')
		this.mw_stats['asgn_failed']++;

	job.js_dropped = new Date();
	delete (this.mw_jobs[jobid]);

	/*
	 * A job may be dropped at any time, even if there are asynchronous
	 * operations pending.  This shouldn't be a problem, since the object
	 * won't be GC'd while those operations are ongoing, and we won't keep
	 * operating on the job because of the check for js_dropped in tick().
	 * The only problem is that there's async activity going on that we
	 * can't see with our debugging tools.  Ideally, we wouldn't remove the
	 * job from global state until these operations complete, but until we
	 * implement that, we at least log a note when we remove a job with
	 * asynchronous operations going.
	 */
	if (job.js_pending_start)
		job.js_log.info('job dropped with pending operation');

	if (job.js_save_throttle.ongoing())
		job.js_log.info('job dropped with pending save operation');
};

/*
 * Returns stats.  This asynchronous call is used for testing only, and could be
 * replaced in the future with a call to a remote worker.
 */
mwWorker.prototype.stats = function (callback)
{
	var worker = this;

	process.nextTick(function () {
		callback(null, mod_jsprim.deepCopy(worker.mw_stats));
	});
};


/*
 * Manages a single job.  Jobs run through the following state machine:
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
 *                  |           | Finish retrieving task groups
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
 *                  +-------> DONE
 */
function mwJobState(args)
{
	mod_assert.equal(typeof (args['job']), 'object');
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.equal(typeof (args['locator']), 'object');
	mod_assert.equal(typeof (args['moray']), 'object');
	mod_assert.equal(typeof (args['worker']), 'object');
	mod_assert.equal(typeof (args['worker_uuid']), 'string');
	mod_assert.equal(typeof (args['initial_etag']), 'string');
	mod_assert.equal(typeof (args['save_interval']), 'number');
	mod_assert.equal(typeof (args['taskgroup_idle_time']), 'number');

	/* job definition and static state */
	this.js_job = args['job'];
	this.js_etag = args['initial_etag'];
	this.js_jobid = this.js_job['jobId'];
	this.js_worker_uuid = args['worker_uuid'];

	/* configuration / tunables */
	this.js_save_interval = args['save_interval'];
	this.js_tg_idle_time = args['taskgroup_idle_time'];

	/* helper objects */
	this.js_locator = args['locator'];
	this.js_moray = args['moray'];
	this.js_worker = args['worker'];
	this.js_log = args['log'].child({ 'job': this.js_jobid });
	this.js_ontaskgroup = this.onTaskGroup.bind(this);

	/* dynamic state */
	this.js_state = 'unassigned';		/* current state */
	this.js_state_time = new Date();	/* time of last state change */
	this.js_pending_start = undefined;	/* time last async op started */
	this.js_dropped = undefined;		/* time the job was dropped */

	/* save state */
	this.js_save_throttle = new mod_mautil.Throttler(
	    this.js_save_interval);
	this.js_dirtied = 0;			/* gen. # for in-memory state */
	this.js_saved = 0;			/* gen. # last saved */
	this.js_save_pending = undefined;	/* gen. # of pending save op */

	/*
	 * Phase state: the keys for each phase are always in one of three
	 * states: unassigned, assigned but unprocessed, or processed.  The keys
	 * for the first phase are the input keys for the overall job, while
	 * keys for subsequent phases are streamed in as previous phases
	 * execute.  Thus, a phase is not complete until all phases before it
	 * are complete AND there are no unassigned or unprocessed keys.
	 * XXX remove p_results as part of input/output streamification.
	 */
	this.js_phases = this.js_job['phases'].map(function (jobphase) {
		return ({
		    p_keys_unasgn: {},	/* set of unassigned input keys */
		    p_keys_left: {},	/* set of assigned, unprocessed keys */
		    p_keys_done: {},	/* set of processed keys */
		    p_results: []	/* list of phase results */
		});
	});

	/*
	 * Task group objects have the following fields:
	 *
	 *    tg_record		Last record received or saved.  This contains
	 *    			the authoritative assignment of input keys and
	 *    			non-authoritative result objects for each input
	 *    			key processed.  The format is described in the
	 *    			schema.
	 *
	 *    tg_etag		Etag for last received record.  When this
	 *    			changes, we know the contents of the record has
	 *    			changed and we must fully reprocess it.
	 *
	 *    tg_mtime		Remote mtime for last received record.  When
	 *    			this changes, even if the etag hasn't, we know
	 *    			the remote agent is still alive and heartbeating
	 *    			to us.  In this case, we update tg_updated but
	 *    			don't reprocess the actual record.
	 *
	 *    tg_updated	Local time of last record update.  When this
	 *    			gets too old, we presume the remote agent is
	 *    			dead and mark all unfinished input keys as
	 *    			failed.  We use this instead of tg_mtime to
	 *    			avoid having to deal with clock synchronization
	 *    			issues across the network.
	 *
	 *    tg_dead		Boolean indicating whether the given task group
	 *    			has been declared "dead".
	 *
	 * Task groups for all phases are stored in js_groups indexed by task
	 * group id.
	 */
	this.js_groups = {};		/* all task groups, by tgid */

	/*
	 * The heart of the job worker is tick(), a loop that looks for
	 * unassigned keys, locates them in Manta, assigns them to task groups
	 * for execution on remote hosts, propagates outputs to the next phase
	 * of the job (as inputs), and checks whether the job has finished.  For
	 * simplicity, we only allow one of (locate objects, save task groups)
	 * at a time.  For both clarity and debugging, all state is stored in
	 * this job object, using the following fields:
	 *
	 *    js_location_queries	Object mapping (input key) to (phase).
	 *				If this object is defined, there is an
	 *				outstanding "locate" operation for the
	 *				given input keys.  This mapping is used
	 *				to dispatch task groups for each key
	 *				that we find.
	 *
	 *    js_location_results	Object mapping (input key) to (Manta
	 *   				location record).  See the Locator
	 *   				interface for details on this object.
	 *   				If this object is defined, then we have
	 *   				successfully completed the "locate"
	 *   				request described by
	 *   				js_location_queries, but have not yet
	 *   				computed task group assignments for the
	 *   				results.
	 *
	 *    js_groups_tosave		Set of new task groups indexed by task
	 *   				group id.  Normally, this object
	 *   				contains new task group assignments for
	 *   				a previous "locate" request that have
	 *   				not yet been saved into Moray, but it
	 *   				may also contain groups that have been
	 *   				cancelled.
	 *
	 *    js_groups_saving		Set of task groups (indexed by task
	 *    				group id) which are currently being
	 *    				saved to Moray.
	 *
	 *    js_changed		List of task groups with updated state
	 *   				that we must process.
	 *
	 * The basic flow is as follows:
	 *
	 *    1. Unassigned keys are thrown into js_location_queries, and we
	 *       dispatch a request to locate these keys in Manta via
	 *       this.js_locator.
	 *
	 *    2. When the locate request completes, the results are temporarily
	 *       saved into js_location_results.  We immediately compute a set
	 *       of task groups based on the locations of these keys, store them
	 *       into js_groups_tosave.  We shortly move these to
	 *       js_groups_saving and issue a bunch of Moray requests to save
	 *       these task groups.
	 *
	 *    3. After the first such save completes, remote agents will process
	 *       these task groups and update the Moray records.  We
	 *       periodically look for task group updates, enqueue them onto
	 *       js_changed, and process them eventually.
	 *
	 * Of course, all of this is actually asynchronous, so any given call to
	 * tick() examines these fields to figure out what work needs to be
	 * done.
	 */
	this.js_changed = [];
	this.js_groups_tosave = {};
	this.js_groups_saving = {};
	this.js_location_queries = undefined;
	this.js_location_results = undefined;
}

/*
 * "tick" is the heart of each job: it's invoked once per tickInterval and
 * examines the current state of the job to figure out what to do next.  The
 * rest of this class consists of functions that are invoked from a given state
 * in order to move to another state.
 */
mwJobState.prototype.tick = function ()
{
	if (this.js_worker.mw_worker_stopped) {
		this.js_log.info('ignoring job tick because worker is stopped');
		return;
	}

	if (this.js_dropped) {
		this.js_log.info('ignoring job tick because job dropped');
		return;
	}

	/*
	 * The "unassigned" state is special because we don't want to retry
	 * failed save attempts.
	 */
	if (this.js_state == 'unassigned') {
		if (this.js_dirtied === 0) {
			this.jobAssign();
			return;
		}

		if (this.js_save_throttle.ongoing())
			return;

		if (this.js_saved != this.js_dirtied) {
			this.js_log.warn('failed to assign job; dropping');
			this.js_worker.dropJob(this.js_jobid);
			return;
		}

		this.js_log.info('successfully assigned job');
		this.transition('unassigned', 'initializing');
		return;
	}

	/*
	 * In all states, be sure to save the job record periodically, lest
	 * another worker think that we're neglecting this job.
	 */
	if (!this.js_save_throttle.ongoing() &&
	    (!this.js_save_throttle.tooRecent() ||
	    this.js_dirtied > this.js_saved))
		this.save();

	/*
	 * If there's already an asynchronous operation pending, we don't have
	 * any more work to do.  The operations that set this must use timeouts
	 * to ensure we don't get stuck here.
	 */
	if (this.js_pending_start !== undefined)
		return;

	if (this.js_state == 'initializing') {
		this.initInputKeys();
		this.initTaskGroups();
		return;
	}

	if (this.js_state == 'finishing') {
		this.finish();
		return;
	}

	/*
	 * See the block comment in the mwJobState function for details
	 * on how this all works.
	 */
	mod_assert.equal(this.js_state, 'running');
	this.runIncomingGroupUpdates();
	this.runLocateUnassignedKeys();
	this.runAssignKeys();
	this.runSaveGroups();
	this.runCheckTaskGroups();
	this.runCheckDone();

	this.js_moray.watchTaskGroups(this.js_jobid, this.js_ontaskgroup);
};

mwJobState.prototype.save = function ()
{
	var job = this;
	var opts = {};

	mod_assert.ok(!this.js_save_throttle.ongoing());

	this.js_save_throttle.start();
	this.js_save_pending = this.js_dirtied;

	this.js_log.debug('saving job (dirty = %s, saved = %s)',
	    this.js_dirtied, this.js_saved);

	/*
	 * Propagate committed phase results into the job record.
	 */
	this.js_job['phaseResults'] = this.js_phases.map(function (p) {
		return (p.p_results);
	});

	/*
	 * Use the "etag" option on initial save to avoid clobbering a job that
	 * we don't own yet.
	 */
	if (this.js_saved === 0)
		opts = { 'etag': this.js_etag };

	this.js_moray.saveJob(this.js_job, opts, function (err) {
		if (err) {
			job.js_log.warn(err, 'failed to save job');
		} else {
			job.js_saved = job.js_save_pending;
			job.js_log.info('saved job up to %s', job.js_saved);
		}

		job.js_save_throttle.done();
	});
};


/*
 * Common function to transition from state S1 to state S2.
 */
mwJobState.prototype.transition = function (s1, s2)
{
	if (s1 !== undefined)
		mod_assert.equal(this.js_state, s1);

	this.js_log.info('transitioning to state %s', s2);
	this.js_state = s2;
	this.js_state_time = new Date();
	this.tick();
};

/*
 * Attempt to move this job from "unassigned" state to "initializing" by
 * updating the Moray job record to have job['worker'] == our uuid.
 */
mwJobState.prototype.jobAssign = function ()
{
	var jobid = this.js_jobid;
	var log = this.js_log;

	mod_assert.equal(this.js_state, 'unassigned');

	if (this.js_job['worker']) {
		log.info('attempting to steal job %s from %s',
		    jobid, this.js_job['worker']);
	} else {
		log.info('attempting to take unassigned job %s', jobid);
	}

	this.js_job['state'] = 'running';
	this.js_job['worker'] = this.js_worker_uuid;
	this.js_dirtied++;
	this.save();
};

/*
 * Loads the input keys for the job.  For now, we still use the soon-to-be-
 * deprecated job['inputKeys'] mechanism.  At some point in the future, this
 * will be replaced with a streaming mechanism.
 */
mwJobState.prototype.initInputKeys = function ()
{
	var phase = this.js_phases[0];

	/*
	 * For now, we mark all of the job's input keys as "unassigned" in the
	 * first phase.  As soon as we transition into "running", we'll go
	 * through the task groups we found and update each key's state
	 * accordingly.
	 */
	this.js_job['inputKeys'].forEach(function (key) {
		phase.p_keys_unasgn[key] = true;
	});
};

/*
 * Move this job from "initializing" to "running" by reading all existing task
 * group records for this job.  We cannot start processing the job until we've
 * read all of the taskgroup records that have been written out because we don't
 * know which keys in which phases have been successfully processed.
 */
mwJobState.prototype.initTaskGroups = function ()
{
	var job = this;
	var jobid = this.js_jobid;
	var log = this.js_log;

	mod_assert.equal(this.js_state, 'initializing');
	mod_assert.ok(this.js_pending_start === undefined);

	/*
	 * XXX There may be an arbitrarily large number of taskgroups returned
	 * here.  This should use the new Moray streaming interface for
	 * retrieving this data, which will still allow us to know when the
	 * stream is done so that we can transition to "running".
	 */
	this.js_pending_start = new Date();
	this.js_moray.listTaskGroups(jobid, function (err, groups) {
		job.js_pending_start = undefined;

		if (err) {
			log.error(err, 'failed to list task groups');
			return;
		}

		var now = Date.now();
		groups.forEach(function (ent) { job.loadTaskGroup(ent, now); });
		job.transition('initializing', 'running');
	});
};

/*
 * Given a Moray object describing a particular task group (including "etag",
 * "mtime", and "value" properties), validate the record and determine whether
 * it's actually changed since the last copy we saw.  This function updates
 * js_groups, which keeps track of the latest copy of a taskgroup, but it does
 * NOT examine the new record to figure out how it affects the rest of the job.
 */
mwJobState.prototype.loadTaskGroup = function (groupent, now)
{
	var group, tgid, grouprec, err;

	group = groupent['value'];
	tgid = group['taskGroupId'];

	if (this.js_groups.hasOwnProperty(tgid)) {
		grouprec = this.js_groups[tgid];

		if (grouprec.tg_dead)
			return;

		if (grouprec.tg_etag == groupent['etag']) {
			/*
			 * This record hasn't substantively changed.  If the
			 * mtime has changed, update tg_mtime and tg_updated to
			 * indicate that the agent is still alive.
			 */
			if (grouprec.tg_mtime != groupent['mtime']) {
				grouprec.tg_mtime = groupent['mtime'];
				grouprec.tg_updated = now;
			}

			return;
		}
	} else {
		grouprec = this.js_groups[tgid] = {};
	}

	grouprec.tg_record = group;
	grouprec.tg_etag = groupent['etag'];
	grouprec.tg_mtime = groupent['mtime'];
	grouprec.tg_updated = now;
	grouprec.tg_dead = false;

	err = mod_jsprim.validateJsonObject(mod_schema.sMorayTaskGroup, group);
	if (err) {
		this.js_log.warn(err, 'invalid task group record', groupent);
		return;
	}

	if (group['phaseNum'] >= this.js_phases.length) {
		this.js_log.warn('invalid task group %s (phase %d, but job ' +
		    'only has %d phases', tgid, group['phaseNum'],
		    this.js_phases.length, groupent);
		return;
	}

	this.js_changed.push(tgid);
};

/*
 * Process incoming task group updates, identified by tgid in js_changed.
 */
mwJobState.prototype.runIncomingGroupUpdates = function ()
{
	var tgid, group, groupkeys, res, key;
	var phase, nextphase, jobchanged, error;
	var gi, oi, pi, i;

	/*
	 * Recall that all of the keys for a given phase are in one of three
	 * states: unassigned (p_keys_unasgn), assigned-but-not-completed
	 * (p_left), and completed (p_done).  Since we're only looking at
	 * updated task groups, we don't consider unassigned keys here, and each
	 * key is only one task group, so we can consider them independently.
	 * This function iterates the updated task groups and updates the state
	 * of each key assigned to that task group.
	 */
	for (gi = 0; gi < this.js_changed.length; gi++) {
		tgid = this.js_changed[gi];

		this.js_log.debug('processing updated taskgroup record', tgid);

		group = this.js_groups[tgid].tg_record;
		pi = group['phaseNum'];
		phase = this.js_phases[pi];
		nextphase = (pi == this.js_phases.length - 1 ?
		    undefined : this.js_phases[pi + 1]);
		groupkeys = {};
		jobchanged = false;

		/*
		 * First, build a set of this group's assigned keys and make
		 * sure none of them are marked "unassigned".  We may have done
		 * this already when we wrote out the task group record, but
		 * it's also possible that this job was previously processed by
		 * a different worker, in which case we can't rely on our
		 * in-memory state.
		 */
		for (i = 0; i < group['inputKeys'].length; i++) {
			key = group['inputKeys'][i]['key'];
			groupkeys[key] = true;
			delete (phase.p_keys_unasgn[key]);
		}

		/*
		 * Iterate the completed keys and update each one's in-memory
		 * state.
		 */
		for (i = 0; i < group['results'].length; i++) {
			res = group['results'][i];
			key = res['input'];

			delete (groupkeys[key]);

			if (phase.p_keys_done[key])
				/*
				 * Keys in "p_keys_done" have already had their
				 * results processed, so we're done with this
				 * key.
				 */
				continue;

			/* Mark this key done. */
			delete (phase.p_keys_left[key]);
			phase.p_keys_done[key] = true;
			phase.p_results.push(res);

			if (res['result'] != 'ok')
				continue;

			/*
			 * Propagate output keys for successful tasks into the
			 * next phase (or the job's output keys, if this is the
			 * last phase).
			 */
			for (oi = 0; oi < res['outputs'].length; oi++) {
				key = res['outputs'][oi];

				if (!nextphase) {
					this.js_job['outputKeys'].push(key);
					jobchanged = true;
					continue;
				}

				/*
				 * If we have already processed a task group
				 * with this key for the next phase, ignore this
				 * change.
				 */
				if (nextphase.p_keys_left.hasOwnProperty(key) ||
				    nextphase.p_keys_done.hasOwnProperty(key))
					continue;

				nextphase.p_keys_unasgn[key] = true;
			}
		}

		/*
		 * Any keys for which we don't have results either are being
		 * processed or have failed, depending on the taskgroup state.
		 */
		for (key in groupkeys) {
			if (this.js_groups[tgid].tg_dead ||
			    group['state'] == 'done' ||
			    group['state'] == 'cancelled') {

				if (group['state'] == 'done') {
					error = {
					    'code': 'EJ_UNKNOWN',
					    'message': 'internal error'
					};
				} else {
					error = {
					    'code': 'EJ_TIMEOUT',
					    'message': 'internal timeout'
					};
				}

				delete (phase.p_keys_left[key]);
				phase.p_keys_done[key] = true;
				phase.p_results.push({
				    'input': key,
				    'outputs': [],
				    'result': 'fail',
				    'error': error
				});

				continue;
			}

			mod_assert.ok(!phase.p_keys_done.hasOwnProperty(key));
			phase.p_keys_left[key] = true;
		}

		if (jobchanged)
			this.js_dirtied++;
	}

	this.js_changed = [];
};

/*
 * Runs through all unassigned keys in all phases and attempts to locate each
 * one in Manta.
 */
mwJobState.prototype.runLocateUnassignedKeys = function ()
{
	var pi, phase, key;
	var keyphases, queries, job;

	/*
	 * For simplicity, we only allow one outstanding "locate" query at a
	 * time, and we make sure the results have been processed before
	 * starting the next one.
	 */
	if (this.js_location_queries !== undefined ||
	    this.js_location_results !== undefined)
		return;

	/*
	 * Collect all unassigned keys, and mark the first phase in which
	 * each one is used. XXX throttle the number of these in each go.
	 */
	keyphases = {};

	for (pi = 0; pi < this.js_phases.length; pi++) {
		phase = this.js_phases[pi];

		for (key in phase.p_keys_unasgn) {
			if (keyphases.hasOwnProperty[key])
				continue;

			keyphases[key] = pi;
		}
	}

	if (mod_jsprim.isEmpty(keyphases))
		return;

	this.js_location_queries = keyphases;
	queries = Object.keys(keyphases);

	this.js_log.debug('locating %d keys', queries.length);
	mod_assert.ok(this.js_pending_start === undefined);
	this.js_pending_start = new Date();
	job = this;

	this.js_locator.locate(queries, function (err, locations) {
		job.js_pending_start = undefined;

		if (err) {
			job.js_log.warn(err, 'failed to locate keys');
			job.js_location_queries = undefined;
		} else {
			job.js_location_results = locations;
			job.tick();
		}
	});
};

/*
 * Runs through the results of a previous "locate" operation and uses the
 * results to assign keys to new task groups.  This logically happens after a
 * locate operation, but it happens before the next locate operation within a
 * given tick().
 */
mwJobState.prototype.runAssignKeys = function ()
{
	if (this.js_location_results === undefined)
		/* Nothing to do. */
		return;

	var newgroups, groupforzone, key, loc, pi, phase, copy, uuid, group;
	var keyphases = this.js_location_queries;

	newgroups = this.js_groups_tosave;
	groupforzone = {};

	for (key in this.js_location_results) {
		loc = this.js_location_results[key];

		if (!keyphases.hasOwnProperty(key)) {
			this.js_log.warn('spurious location result for key',
			    key, loc);
			continue;
		}

		pi = keyphases[key];
		phase = this.js_phases[pi];

		if (loc.length === 0) {
			mod_assert.ok(phase.p_keys_unasgn.hasOwnProperty(key));
			delete (phase.p_keys_unasgn[key]);
			phase.p_keys_done[key] = true;
			phase.p_results.push({
			    'input': key,
			    'outputs': [],
			    'result': 'fail',
			    'error': {
				'code': 'EJ_NOENT',
				'message': 'object does not exist'
			    }
			});
			continue;
		}

		/*
		 * We don't update the state for each key until the task group
		 * save successfully completes.  This works because we also
		 * won't try to assign keys until we've finished with the
		 * previous batch.
		 */
		copy = loc[ Math.floor(Math.random() * loc.length) ];
		if (!groupforzone.hasOwnProperty(copy['zonename'])) {
			uuid = mod_uuid.v4();

			newgroups[uuid] = {
			    'jobId': this.js_jobid,
			    'taskGroupId': uuid,
			    'host': copy['host'],
			    'inputKeys': [],
			    'phase': this.js_job['phases'][pi],
			    'phaseNum': pi,
			    'state': 'dispatched',
			    'results': []
			};

			groupforzone[copy['zonename']] = newgroups[uuid];
		}

		group = groupforzone[copy['zonename']];
		group['inputKeys'].push({
		    'key': key,
		    'objectid': copy['objectid'],
		    'zonename': copy['zonename']
		});

		if (group['inputKeys'].length >= 25) /* XXX constant */
			delete (groupforzone[copy['zonename']]);
	}

	this.js_location_queries = undefined;
	this.js_location_results = undefined;
};

/*
 * Saves updated task group records back to Moray.
 */
mwJobState.prototype.runSaveGroups = function ()
{
	if (mod_jsprim.isEmpty(this.js_groups_tosave))
		/* Nothing to do. */
		return;

	if (!mod_jsprim.isEmpty(this.js_groups_saving))
		/* Previous save in progress. */
		return;

	var job = this;

	this.js_groups_saving = this.js_groups_tosave;
	this.js_groups_tosave = {};

	this.js_log.debug('saving task groups',
	    Object.keys(this.js_groups_saving));
	this.js_pending_start = new Date();
	this.js_moray.saveTaskGroups(this.js_groups_saving, function (err) {
		var now, newgroups, tgid;

		job.js_pending_start = undefined;

		if (err) {
			/*
			 * Put the unsaved groups back into js_groups_tosave for
			 * the next time around.
			 * XXX if the error is a 40x, we should mark all of
			 * these groups both cancelled and dead.
			 */
			job.js_log.warn(err, 'failed to save task groups');

			for (tgid in job.js_groups_saving)
				job.js_groups_tosave[tgid] =
				    job.js_groups_saving[tgid];
			job.js_groups_saving = {};
			return;
		}

		job.js_log.debug('successfully saved task groups');

		now = Date.now();
		newgroups = job.js_groups_saving;
		job.js_groups_saving = {};

		for (tgid in newgroups)
			job.loadTaskGroup({ 'value': newgroups[tgid] }, now);

		job.tick();
	});
};

/*
 * Examines all task groups for this job, looking for ones which have timed out.
 */
mwJobState.prototype.runCheckTaskGroups = function ()
{
	var tgid, group, now, diff;

	now = Date.now();

	for (tgid in this.js_groups) {
		group = this.js_groups[tgid];

		if (group.tg_dead || group.tg_record['state'] == 'done')
			continue;

		diff = now - group.tg_updated;
		if (diff < this.js_tg_idle_time)
			continue;

		/*
		 * Mark the taskgroup dead and push it onto js_changed as
		 * though it had been updated externally.  This does two things:
		 * first, we'll ignore all future updates for this taskgroup,
		 * and second, we'll see it when we next process incoming
		 * taskgroup updates.  At that point, we'll treat it the same as
		 * an explicit failure of the group, which means to fail all of
		 * the keys.
		 */
		group.tg_dead = true;
		this.js_changed.push(tgid);

		/*
		 * We also want to write out an explicit "cancelled" record so
		 * that the remote agent knows to stop working on this.
		 */
		group.tg_record['state'] = 'cancelled';
		this.js_groups_tosave[tgid] = group.tg_record;

		this.js_log.error(
		    'task group %s has timed out (not updated in %dms)',
		    tgid, diff);
	}
};

/*
 * Check whether the given job has completed.  A job is finished when every
 * phase has zero keys in the unassigned or uncompleted states.
 */
mwJobState.prototype.runCheckDone = function ()
{
	var i, phase;

	for (i = 0; i < this.js_phases.length; i++) {
		phase = this.js_phases[i];

		if (!mod_jsprim.isEmpty(phase.p_keys_unasgn) ||
		    !mod_jsprim.isEmpty(phase.p_keys_left))
			break;
	}

	if (i < this.js_phases.length)
		return;

	this.js_log.info('job is complete');
	this.js_job['state'] = 'done';
	this.js_job['doneTime'] = mod_jsprim.iso8601(new Date());
	this.js_dirtied++;
	this.transition('running', 'finishing');
};

mwJobState.prototype.onTaskGroup = function (taskgroups)
{
	var job = this;
	var now = Date.now();

	if (this.js_state == 'finishing')
		return;

	/*
	 * Until Moray supports querying by sequence number, we examine all
	 * taskgroup records all the time.
	 */
	taskgroups.forEach(function (group) { job.loadTaskGroup(group, now); });
};

/*
 * After we decide that a job has completed, we must write the last update to
 * the job's Moray record.  Additionally, if there are any task groups that we
 * declared dead during the course of the job execution, we must cancel them now
 * so that an agent doesn't try to pick one up not realizing that the job has
 * finished.  This function is invoked periodically by the "tick" handler when
 * the job is in the "finishing" stage to carry out these operations.
 */
mwJobState.prototype.finish = function ()
{
	mod_assert.equal('finishing', this.js_state);

	if (!mod_jsprim.isEmpty(this.js_groups_tosave)) {
		this.runSaveGroups();
		return;
	}

	if (this.js_dirtied == this.js_saved) {
		/* We're all done -- completely. */
		this.js_log.info('removing job');
		this.js_worker.dropJob(this.js_jobid);
	}
};
