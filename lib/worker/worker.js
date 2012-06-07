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
 *    o Need work on dropJob.  It shouldn't remove all trace of the job unless
 *      there are no ongoing async activities for it.  If there are, it should
 *      move it to a "dropped" list and abort all such activities.
 *    o Assigning jobs to the worker should use test-and-set, rather than a
 *      simple objectPut.
 *    o We should be validating records read from Moray (job records and task
 *      groups).
 *    o We shouldn't try to assign *all* found jobs to ourselves -- just up to
 *      some limit.
 *    o Add timeouts for retries that happen as a result of calling job tick().
 *    o All async events should record that they're happening somewhere for
 *      debugging.
 */

var mod_assert = require('assert');

var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_verror = require('verror');

/*
 * These parameters must remain consistent with consumers of mwConf, including
 * the test suite.
 */
var mwConf = {
    'jobsBucket': 'marlinJobs',
    'taskGroupsBucket': 'marlinTaskGroups',

    /* polling intervals */
    'findInterval': 5 * 1000,		/* ping Moray for new jobs (ms) */
    'saveInterval': 30 * 1000,		/* how often to save unchanged jobs */
    'taskGroupInterval': 5 * 1000,	/* poll task group state */
    'tickInterval': 1 * 1000		/* re-evalute job state */
};

/*
 * Public interface
 */
exports.mwConf = mwConf;
exports.mwWorker = mwWorker;


/*
 * Manages jobs owned by a single Marlin worker.  Most job management actually
 * happens in the mwJobState class below.  Arguments for this class include:
 *
 *    uuid	Unique identifier for this worker instance
 *
 *    moray	Moray interface
 *
 *    log	Bunyan-style logger instance
 *
 * The worker itself doesn't do much except accept incoming jobs, which are
 * managed by separate mwJobState objects.  The worker also manages a global
 * timeout that fires every mwConf['tickInterval'] milliseconds, which causes
 * each job's state to be reevaluated.
 */
function mwWorker(args)
{
	mod_assert.equal(typeof (args['uuid']), 'string');
	mod_assert.equal(typeof (args['moray']), 'object');
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.ok(!args['saveInterval'] ||
	    typeof (args['saveInterval']) == 'number');

	/* immutable state */
	this.mw_uuid = args['uuid'];
	this.mw_interval = mwConf['tickInterval'];
	this.mw_save_interval = args['saveInterval'] || mwConf['saveInterval'];

	/* helper objects */
	this.mw_log = args['log'].child({ 'worker': this.mw_uuid });
	this.mw_moray = args['moray'];
	this.mw_moray.on('job', this.onJob.bind(this));

	/* dynamic state */
	this.mw_jobs = {};			/* all jobs, by jobId */
	this.mw_timeout = undefined;		/* JS timeout handle */
	this.mw_worker_start = undefined;	/* time worker started */
	this.mw_worker_stopped = undefined;	/* time worker stopped */
	this.mw_tick_start = undefined;		/* time last tick started */
	this.mw_tick_done = undefined;		/* time last tick finished */
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

mwWorker.prototype.stop = function ()
{
	this.mw_log.info('shutting down worker');
	this.mw_worker_stopped = new Date();

	if (this.mw_timeout) {
		clearTimeout(this.mw_timeout);
		this.mw_timeout = undefined;
	}
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

	this.mw_moray.findUnassignedJobs();

	mod_jsprim.forEachKey(this.mw_jobs, function (_, job) { job.tick(); });

	this.mw_tick_done = new Date();
	this.setTimer();
};

/*
 * Invoked when our Moray interface finds an unassigned job for us to pick up.
 */
mwWorker.prototype.onJob = function (job)
{
	var jobid = job['jobId'];
	var log = this.mw_log;

	if (typeof (job) != 'object') {
		log.warn('ignoring invalid job', job);
		return;
	}

	if (this.mw_jobs.hasOwnProperty(jobid)) {
		if (this.mw_jobs[jobid].js_state == 'unassigned')
			/* We're already trying to take this job. */
			return;

		/*
		 * It shouldn't be possible to find a job here that we already
		 * thought we owned because we'll prematurely drop any jobs for
		 * which our lock has expired.  But if this happens, drop the
		 * job now and try to take it again.
		 */
		log.warn('found unassigned job "%s" that we thought we owned',
		    jobid);
		this.dropJob(jobid);
		mod_assert.ok(!this.mw_jobs.hasOwnProperty(jobid));
	}

	var newjob = new mwJobState({
	    'job': job,
	    'log': this.mw_log,
	    'moray': this.mw_moray,
	    'worker_uuid': this.mw_uuid,
	    'worker': this,
	    'save_interval': this.mw_save_interval
	});

	this.mw_jobs[jobid] = newjob;
	newjob.tick();
};

mwWorker.prototype.dropJob = function (jobid)
{
	delete (this.mw_jobs[jobid]);
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
 *                        UNINITIALIZED
 *                              |
 *                              | Finish retrieving task groups
 *                              v
 *                  +---->  PLANNING
 *  Compute node    |           |
 *  times out or    |           | Compute and write task group assignments
 *  phase completes |           v
 *                  +-----   RUNNING
 *                          /   |
 *                  +------+    | Last phase completes or
 *                  |           | job encounters fatal failure
 *    Job dropped   |           v
 *    because lock  |       FINISHING
 *    was lost      |           |
 *                  |           | Final save completes
 *                  |           v
 *                  +-------- DONE
 */
function mwJobState(args)
{
	mod_assert.equal(typeof (args['job']), 'object');
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.equal(typeof (args['moray']), 'object');
	mod_assert.equal(typeof (args['worker']), 'object');
	mod_assert.equal(typeof (args['worker_uuid']), 'string');
	mod_assert.equal(typeof (args['save_interval']), 'number');

	this.js_job = args['job'];
	this.js_log = args['log'].child({ 'job': this.js_jobid });
	this.js_moray = args['moray'];
	this.js_worker = args['worker'];
	this.js_worker_uuid = args['worker_uuid'];
	this.js_save_interval = args['save_interval'];

	this.js_jobid = this.js_job['jobId'];
	this.js_state = 'unassigned';
	this.js_state_time = new Date();
	this.js_pending_start = undefined;
	this.js_ontaskgroup = this.onTaskGroup.bind(this);

	this.js_save_last = undefined;	/* start of last successful save */
	this.js_save_start = undefined;	/* start of pending save */
	this.js_dirtied = 0;		/* generation # of current data */
	this.js_saved = 0;		/* generation # last saved */
	this.js_save_pending = undefined;

	this.js_phasei = undefined;
	this.js_phases = new Array(this.js_job['phases'].length);

	for (var i = 0; i < this.js_job['phases'].length; i++)
		this.js_phases[i] = {
		    p_input: [],
		    p_groups: {},
		    p_keys_unassigned: {},
		    p_keys_left: {}
		};
}

/*
 * "tick" is the heart of each job: it's invoked once per tickInterval and
 * examines the current state of the job to figure out what to do next.  The
 * rest of this class consists of functions that are invoked from a given state
 * in order to move to another state.  Most of these are asynchronous and set
 * js_pending_start while the asynchronous operation is ongoing.
 */
mwJobState.prototype.tick = function ()
{
	/*
	 * The "unassigned" state is special because we don't want to retry
	 * failed save attempts.
	 */
	if (this.js_state == 'unassigned') {
		if (this.js_dirtied === 0) {
			this.jobAssign();
			return;
		}

		if (this.js_save_start !== undefined)
			/* Initial assignment save is still pending. */
			return;

		if (this.js_saved != this.js_dirtied) {
			this.js_log.warn('failed to assign job; dropping');
			this.js_worker.dropJob(this.js_jobid);
			return;
		}

		this.js_log.info('successfully assigned job');
		this.transition('unassigned', 'uninitialized');
		return;
	}

	if (this.js_save_start === undefined &&
	    (this.js_dirtied > this.js_saved ||
	    Date.now() - this.js_save_last.getTime() > this.js_save_interval))
		this.save();

	/*
	 * If there's already an asynchronous operation pending, we don't have
	 * any more work to do.  The operations that set this must use timeouts
	 * to ensure we don't get stuck here.
	 */
	if (this.js_pending_start !== undefined)
		return;

	switch (this.js_state) {
	case 'uninitialized':
		this.jobRestore();
		break;

	case 'planning':
		this.taskGroupAssign();
		break;

	case 'running':
		this.js_moray.watchTaskGroups(this.js_jobid,
		    this.js_phasei, this.js_ontaskgroup);
		break;

	case 'finishing':
		if (this.js_dirtied == this.js_saved)
			this.js_worker.dropJob(this.js_jobid);

		break;

	default:
		var err = new mod_verror.VError(
		    'found job in invalid state: %j', this.js_state);
		this.js_log.fatal(err);
		throw (err);
	}
};

mwJobState.prototype.save = function ()
{
	var job = this;

	mod_assert.ok(this.js_save_start === undefined);
	mod_assert.ok(this.js_save_pending === undefined);

	this.js_save_pending = this.js_dirtied;
	this.js_save_start = new Date();

	this.js_log.info('saving job (dirty = %s, saved = %s)',
	    this.js_dirtied, this.js_saved);

	this.js_moray.saveJob(this.js_job, function (err) {
		if (err) {
			job.js_log.warn(err, 'failed to save job');
		} else {
			job.js_saved = job.js_save_pending;
			job.js_log.info('saved job up to %s', job.js_saved);
		}

		job.js_save_last = job.js_save_start;
		job.js_save_start = undefined;
		job.js_save_pending = undefined;
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
 * From the "unassigned" state, attempt to bring the job to "uninitialized" by
 * updating the Moray job record with worker == our uuid.
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

	this.js_job['worker'] = this.js_worker_uuid;
	this.js_dirtied++;
	this.save();
};

/*
 * From the "uninitialized" state, attempt to bring the job to "planning" by
 * reading all existing task group records for this job.
 */
mwJobState.prototype.jobRestore = function ()
{
	var job = this;
	var jobid = this.js_jobid;
	var log = this.js_log;

	mod_assert.equal(this.js_state, 'uninitialized');
	mod_assert.ok(this.js_pending_start === undefined);

	this.js_log.info('searching for task groups');

	this.js_pending_start = new Date();
	this.js_moray.listTaskGroups(jobid, function (err, groups) {
		job.js_pending_start = undefined;

		if (err) {
			log.error(err, 'failed to list task groups');
			return;
		}

		job.loadTaskGroups(groups);
		job.transition('uninitialized', 'planning');
	});
};

/*
 * This function is responsible for loading new task group records from
 * "groups".  If "phasenum" is specified, then all task groups may be expected
 * to describe the given phase.  Otherwise, this function determines what phase
 * the job is operating on by looking at the last phase for which we have task
 * group records.  When this function has completed, the list of input keys,
 * unassigned keys, and completed keys for the current phase will be populated
 * based on all previous and current task group records.
 *
 * This function is called in three main contexts:
 *
 *	o during initialization of a newly-created job, in which case there will
 *	  be zero task groups to load and all of the job's input keys will be
 *	  unassigned at the end
 *
 *      o during recovery of an existing job that has been partially processed
 *        by some other worker instance, in which case there will be one or more
 *        task groups to load and there may be some unassigned keys left over
 *
 *      o during normal execution of a phase when task group records have been
 *        updated, in which case there will be some task groups to load but no
 *        new unassigned keys
 */
mwJobState.prototype.loadTaskGroups = function (groups, phasenum)
{
	var job = this;
	var log = this.js_log;
	var curphase = phasenum === undefined ? 0 : phasenum;
	var phase, prevphase, phasegroup, tgid;

	/*
	 * Load all task group records into their respective phases first so
	 * that we can process the groups in phase order.
	 */
	groups.forEach(function (group) {
		tgid = group['taskGroupId'];

		/*
		 * On initial load, "phase" is undefined and any valid phase is
		 * acceptable.  Later on, we expect only a particular phase.
		 */
		if (phasenum === undefined &&
		    group['phaseNum'] >= job.js_phases.length) {
			log.warn('ignoring task group %s: ' +
			    'phase %s is out of range', tgid,
			    group['phaseNum']);
			return;
		} else if (phasenum !== undefined &&
		    group['phaseNum'] != phasenum) {
			log.warn('found unexpected task group %s with phase '+
			    '%s (current phase is %s)', tgid, group['phaseNum'],
			    phasenum);
			return;
		}

		phase = job.js_phases[group['phaseNum']];

		if (phase.p_groups.hasOwnProperty(tgid) &&
		    phase === undefined) {
			/* This is only surprising during init. */
			log.warn('ignoring duplicate task group %s', tgid);
			return;
		}

		phase.p_groups[tgid] = group;

		if (group['phaseNum'] > curphase)
			curphase = group['phaseNum'];
	});

	/*
	 * If we find task group records for phase i > 0, we know we completed
	 * all phases up through i - 1.  We won't bother populating the
	 * in-memory structures for those phases.
	 */
	log.info('loading task groups for phase %s', curphase);
	this.js_phasei = curphase;
	phase = this.js_phases[curphase];

	/*
	 * If we haven't already done so, compute the full set of input keys for
	 * the current phase based on the results of the previous phase's task
	 * groups.  The input keys for the first phase are taken directly from
	 * the job definition.
	 */
	if (phase.p_input.length === 0) {
		log.info('computing input keys for phase %d', curphase);
		phase.p_keys_left = {};
		phase.p_keys_unassigned = {};

		if (curphase === 0) {
			this.js_job['inputKeys'].forEach(function (key) {
				phase.p_input.push(key);
				phase.p_keys_left[key] = true;
				phase.p_keys_unassigned[key] = true;
			});
		} else {
			prevphase = this.js_phases[curphase - 1];
			prevphase.p_groups.forEach(function (group) {
				group['results'].forEach(function (result) {
					if (result['result'] != 'ok')
						return;

					result['outputs'].forEach(function (k) {
						phase.p_input.push(k);
						phase.p_keys_left[k] = true;
						phase.p_keys_unassigned[k] =
						    true;
					});
				});
			});
		}

		if (phase.p_input.length === 0) {
			this.advancePhase();
			return;
		}
	}

	for (tgid in phase.p_groups) {
		phasegroup = phase.p_groups[tgid];

		phasegroup['inputKeys'].forEach(function (key) {
			delete (phase.p_keys_unassigned[key]);
		});

		phasegroup['results'].forEach(function (result) {
			if (!result.hasOwnProperty('result'))
				return;

			if (!phase.p_keys_left.hasOwnProperty(result['input']))
				return;

			delete (phase.p_keys_left[result['input']]);
			job.js_job['results'].push(result);
			job.js_dirtied++;
		});
	}
};

/*
 * From the "planning" state, attempt to bring the job to the "running" state by
 * making sure that all input keys for the current phase have been assigned to
 * task groups.
 */
mwJobState.prototype.taskGroupAssign = function ()
{
	var job = this;
	var jobid = this.js_jobid;
	var log = this.js_log;
	var curphase = this.js_phasei;
	var phase = this.js_phases[this.js_phasei];
	var unassigned;

	/*
	 * For unassigned keys, locate them within Manta and partition them into
	 * new task groups.
	 */
	unassigned = Object.keys(phase.p_keys_unassigned);
	log.trace('phase %d: %d unassigned keys (%d total)', curphase,
	    unassigned.length, phase.p_input.length);

	mod_assert.ok(this.js_pending_start === undefined);
	this.js_pending_start = new Date();
	job.js_moray.mantaLocate(unassigned, function (err, locs) {
		if (err) {
			job.js_pending_start = undefined;
			log.warn(err, 'failed to locate Manta keys');
			return;
		}

		var groups = {};
		var key;

		for (key in locs) {
			if (!phase.p_keys_unassigned.hasOwnProperty(key)) {
				log.warn('locate returned extra key: %s', key);
				continue;
			}

			if (locs[key].length < 1) {
				/* XXX append key result to job */
				continue;
			}

			if (!groups.hasOwnProperty(locs[key][0])) {
				groups[locs[key][0]] = {
				    'jobId': jobid,
				    'taskGroupId': mod_uuid.v4(),
				    'host': locs[key][0],
				    'inputKeys': [],
				    'phase': job.js_job['phases'][curphase],
				    'phaseNum': curphase,
				    'state': 'dispatched',
				    'results': []
				};
			}

			groups[locs[key][0]]['inputKeys'].push(key);
			delete (phase.p_keys_unassigned[key]);
		}

		job.js_moray.saveTaskGroups(groups, function (suberr) {
			job.js_pending_start = undefined;

			if (suberr) {
				log.warn(suberr,
				    'failed to save new task groups');
				return;
			}

			mod_jsprim.forEachKey(groups, function (_, group) {
				mod_assert.ok(!phase.p_groups.hasOwnProperty(
				    group['taskGroupId']));
				phase.p_groups[group['taskGroupId']] = group;
			});

			/*
			 * There may still be unassigned keys, in which case
			 * we'll have to take another lap through the "planning"
			 * state.
			 */
			if (mod_jsprim.isEmpty(phase.p_keys_unassigned)) {
				job.transition('planning', 'running');
			} else {
				mod_assert.equal(job.js_state, 'planning');
				job.tick();
			}
		});
	});
};

mwJobState.prototype.onTaskGroup = function (taskgroups)
{
	/*
	 * Until Moray supports querying by sequence number, we examine all
	 * taskgroup records all the time.
	 */
	this.loadTaskGroups(taskgroups, this.js_phasei);

	/*
	 * Check whether there remain any keys which are not yet processed.
	 */
	var phase = this.js_phases[this.js_phasei];
	mod_assert.ok(mod_jsprim.isEmpty(phase.p_keys_unassigned));

	if (!mod_jsprim.isEmpty(phase.p_keys_left))
		return;

	this.advancePhase();
};

/*
 * Advance to the next phase in the job.
 */
mwJobState.prototype.advancePhase = function ()
{
	var phase = this.js_phases[this.js_phasei];

	mod_assert.ok(mod_jsprim.isEmpty(phase.p_keys_left));
	mod_assert.ok(this.js_phasei < this.js_phases.length);

	this.js_log.info('phase %d is complete', this.js_phasei);

	if (++this.js_phasei < this.js_phases.length) {
		this.transition('running', 'planning');
		return;
	}

	this.js_log.info('job is complete');
	this.js_dirtied++;
	this.transition('running', 'finishing');
};
