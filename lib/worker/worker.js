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
 *    o Add timeouts for retries that happen as a result of calling job tick().
 *    o All async events should record that they're happening somewhere for
 *      debugging.
 */

var mod_assert = require('assert');

var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_verror = require('verror');

var mod_schema = require('../schema');

var mwConfSchema = {
    'type': 'object',
    'properties': {
	'instanceUuid': {
	    'required': true,
	    'type': 'string',
	    'minLength': 1
	},
	'port': {
	    'required': true,
	    'type': 'integer',
	    'minimum': 1,
	    'maximum': 65535
	},
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
			    'items': { 'type': 'string', 'minLength': 1 },
			    'minItems': 1
			}
		    }
		},
		'storage': {
		    'required': true,
		    'type': 'object',
		    'properties': {
			'url': { 'type': 'string', 'minLength': 1 }
		    }
		}
	    }
	},
	'findInterval': {
	    'required': true,
	    'type': 'integer',
	    'minimum': 0
	},
	'taskGroupInterval': {
	    'required': true,
	    'type': 'integer',
	    'minimum': 0
	},
	'jobsBucket': {
	    'required': true,
	    'type': 'string',
	    'minLength': 1
	},
	'taskGroupsBucket': {
	    'required': true,
	    'type': 'string',
	    'minLength': 1
	}
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
 *    log		Bunyan-style logger instance
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
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.equal(typeof (args['tickInterval']), 'number');
	mod_assert.equal(typeof (args['saveInterval']), 'number');
	mod_assert.equal(typeof (args['taskGroupIdleTime']), 'number');

	/* immutable state */
	this.mw_uuid = args['instanceUuid'];
	this.mw_interval = args['tickInterval'];
	this.mw_save_interval = args['saveInterval'];
	this.mw_tg_idle_time = args['taskGroupIdleTime'];

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
	this.mw_stats = {			/* stat counters */
	    'asgn_failed': 0		/* failed job assignments */
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
mwWorker.prototype.onJob = function (metadata, job)
{
	var log = this.mw_log;
	var error, jobid;

	error = mod_jsprim.validateJsonObject(mod_schema.sMorayJob, job);
	if (error) {
		log.warn(error, 'ignoring invalid job record', job);
		return;
	}

	jobid = job['jobId'];

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
	    'initial_etag': metadata['etag'],
	    'log': this.mw_log,
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
	if (this.mw_jobs[jobid].js_state == 'unassigned')
		this.mw_stats['asgn_failed']++;

	delete (this.mw_jobs[jobid]);
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
	mod_assert.equal(typeof (args['initial_etag']), 'string');
	mod_assert.equal(typeof (args['save_interval']), 'number');
	mod_assert.equal(typeof (args['taskgroup_idle_time']), 'number');

	this.js_job = args['job'];
	this.js_etag = args['initial_etag'];
	this.js_log = args['log'].child({ 'job': this.js_jobid });
	this.js_moray = args['moray'];
	this.js_worker = args['worker'];
	this.js_worker_uuid = args['worker_uuid'];
	this.js_save_interval = args['save_interval'];
	this.js_tg_idle_time = args['taskgroup_idle_time'];

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
	this.js_deadgroups = undefined;

	this.js_phasei = undefined;
	this.js_phases = new Array(this.js_job['phases'].length);

	for (var i = 0; i < this.js_job['phases'].length; i++)
		this.js_phases[i] = {
		    p_input: [],		/* input keys */
		    p_groups: {},		/* task group records */
		    p_updated: {},		/* task group update time */
		    p_etag: {},			/* task group last etag */
		    p_dead: {},			/* set of dead task groups */
		    p_keys_unassigned: {},	/* set of unassigned keys */
		    p_keys_left: {},		/* set of unfinished keys */
		    p_results: []		/* committed phase results */
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
		this.checkTaskGroups();
		break;

	case 'finishing':
		this.finish();
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
	var opts = {};

	mod_assert.ok(this.js_save_start === undefined);
	mod_assert.ok(this.js_save_pending === undefined);

	this.js_save_pending = this.js_dirtied;
	this.js_save_start = new Date();

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

	this.js_job['state'] = 'running';
	this.js_job['worker'] = this.js_worker_uuid;
	this.js_job['outputKeys'] = [];
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

	this.js_log.debug('searching for task groups');

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
 * Compute the full set of input keys for the current phase based on the results
 * of the previous phase's task groups.  The input keys for the first phase are
 * taken directly from the job definition.
 */
mwJobState.prototype.preparePhase = function ()
{
	var curphase = this.js_phasei;
	var phase = this.js_phases[curphase];
	var log = this.js_log;
	var prevphase;

	if (phase.p_input.length === 0) {
		log.debug('computing input keys for phase %d', curphase);
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
			mod_jsprim.forEachKey(prevphase.p_groups,
			    function (_, group) {
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

		mod_jsprim.forEachKey(phase.p_groups, function (_, group) {
			group['inputKeys'].forEach(function (key) {
				delete (phase.p_keys_unassigned[key['key']]);
			});
		});
	}
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
	var now, phase;

	/*
	 * Load all task group records into their respective phases first so
	 * that we can process the groups in phase order.
	 */
	now = new Date();
	groups.forEach(function (groupent) {
		var group = groupent['value'];
		var tgid = group['taskGroupId'];

		var error = mod_jsprim.validateJsonObject(
		    mod_schema.sMorayTaskGroup, group);

		if (error) {
			log.warn(error, 'ignoring invalid task group record',
			    group);
			return;
		}

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

		if (groupent['etag'] == phase.p_etag[tgid])
			return;

		phase.p_groups[tgid] = group;
		phase.p_etag[tgid] = groupent['etag'];
		phase.p_updated[tgid] = now;

		if (group['phaseNum'] > curphase)
			curphase = group['phaseNum'];
	});

	/*
	 * If we find task group records for phase i > 0, we know we completed
	 * all phases up through i - 1.  We won't bother populating the
	 * in-memory structures for those phases.
	 */
	this.js_phasei = curphase;
	phase = this.js_phases[curphase];
	this.preparePhase();

	groups.forEach(function (groupent) {
		groupent['value']['results'].forEach(function (result) {
			if (!result.hasOwnProperty('result'))
				return;

			if (!phase.p_keys_left.hasOwnProperty(result['input']))
				return;

			delete (phase.p_keys_left[result['input']]);
			phase.p_results.push(result);
			job.js_dirtied++;

			if (curphase == job.js_phases.length - 1) {
				job.js_job['outputKeys'] =
				    job.js_job['outputKeys'].concat(
				    result['outputs']);
			}
		});
	});

	mod_jsprim.forEachKey(phase.p_dead, function (tgid) {
		var group = phase.p_groups[tgid];
		group['inputKeys'].forEach(function (key) {
			if (!phase.p_keys_left.hasOwnProperty(key['key']))
				return;

			/*
			 * At this point, we have an unfinished input key in a
			 * task group that we've declared the task group dead,
			 * so mark it failed.
			 */
			phase.p_results.push({
			    'input': key['key'],
			    'outputs': [],
			    'result': 'fail',
			    'error': {
				'code': 'EJ_TIMEOUT',
				'message': 'system timed out'
			    }
			});

			job.js_dirtied++;
			delete (phase.p_keys_left[key['key']]);
		});
	});

	if (mod_jsprim.isEmpty(phase.p_keys_left))
		this.advancePhase();
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
	log.info('phase %d: %d unassigned keys (%d total)', curphase,
	    unassigned.length, phase.p_input.length);

	if (unassigned.length === 0) {
		job.transition('planning', 'running');
		return;
	}

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
				delete (phase.p_keys_left[key]);
				job.js_dirtied++;

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

			if (!groups.hasOwnProperty(locs[key][0]['host'])) {
				groups[locs[key][0]['host']] = {
				    'jobId': jobid,
				    'taskGroupId': mod_uuid.v4(),
				    'host': locs[key][0]['host'],
				    'inputKeys': [],
				    'phase': job.js_job['phases'][curphase],
				    'phaseNum': curphase,
				    'state': 'dispatched',
				    'results': []
				};
			}

			groups[locs[key][0]['host']]['inputKeys'].push({
			    'key': key,
			    'objectid': locs[key][0]['objectid'],
			    'zonename': locs[key][0]['zonename']
			});

			delete (phase.p_keys_unassigned[key]);
		}

		job.js_moray.saveTaskGroups(groups, function (suberr) {
			job.js_pending_start = undefined;

			if (suberr) {
				log.warn(suberr,
				    'failed to save new task groups');
				return;
			}

			var now = new Date();

			mod_jsprim.forEachKey(groups, function (_, group) {
				mod_assert.ok(!phase.p_groups.hasOwnProperty(
				    group['taskGroupId']));
				phase.p_groups[group['taskGroupId']] = group;
				phase.p_updated[group['taskGroupId']] = now;
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

/*
 * Examines all task groups for this job, looking for ones which have timed out.
 */
mwJobState.prototype.checkTaskGroups = function ()
{
	var job = this;
	var phase = this.js_phases[this.js_phasei];
	var now = Date.now();

	mod_jsprim.forEachKey(phase.p_groups, function (tgid, group) {
		if (group['state'] == 'done')
			return;

		if (phase.p_dead.hasOwnProperty(tgid))
			return;

		var diff = now - phase.p_updated[tgid].getTime();
		if (diff < job.js_tg_idle_time)
			return;

		job.js_log.error('task group %s has timed out (not updated ' +
		    'in %dms)', tgid, diff);
		phase.p_dead[tgid] = true;
	});

	this.loadTaskGroups([], this.js_phasei);
};

mwJobState.prototype.onTaskGroup = function (taskgroups)
{
	if (this.js_state == 'finishing')
		return;

	/*
	 * Until Moray supports querying by sequence number, we examine all
	 * taskgroup records all the time.
	 */
	this.js_log.debug('updating task groups for phase %s', this.js_phasei);
	this.loadTaskGroups(taskgroups, this.js_phasei);
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
		this.preparePhase();
		this.transition('running', 'planning');
		return;
	}

	this.js_log.info('job is complete');
	this.js_job['state'] = 'done';
	this.js_job['doneTime'] = mod_jsprim.iso8601(new Date());
	this.js_dirtied++;

	/*
	 * Look for any dead task groups in any phase of this job.  Since we
	 * currently don't retry these, we know that these caused the job to
	 * fail, but if we don't explicitly cancel them, an agent might pick
	 * them up and run them anyway without realizing the job is gone.
	 */
	var job = this;
	mod_assert.ok(this.js_deadgroups === undefined);
	this.js_deadgroups = [];
	this.js_phases.forEach(function (phasei) {
		mod_jsprim.forEachKey(phasei.p_groups, function (tgid, group) {
			group['state'] = 'cancelled';
			job.js_deadgroups.push(group);
		});
	});

	this.transition('running', 'finishing');
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
	var job = this;

	mod_assert.equal('finishing', this.js_state);

	if (this.js_deadgroups.length === 0) {
		if (this.js_dirtied == this.js_saved) {
			/* We're all done -- completely. */
			this.js_log.info('removing job');
			this.js_worker.dropJob(this.js_jobid);
		}

		return;
	}

	/*
	 * The tick handler is a no-op while an asynchronous operation is
	 * pending, so by the time we get here there should be nothing going on.
	 */
	this.js_log.debug('cancelling groups', this.js_deadgroups.map(
	    function (group) { return (group['taskGroupId']); }));

	mod_assert.ok(this.js_pending_start === undefined);
	this.js_pending_start = new Date();

	this.js_moray.saveTaskGroups(this.js_deadgroups, function (err) {
		job.js_pending_start = undefined;

		if (err) {
			job.js_log.warn(err, 'failed to cancel task groups');
		} else {
			job.js_deadgroups = [];
			job.tick();
		}
	});
};
