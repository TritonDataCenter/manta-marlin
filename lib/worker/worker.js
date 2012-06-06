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
 *    o For testing, we need a way to add keys and jobs to the mock moray.
 *    o Assignment should happen in the context of a JobState object, not the
 *      worker itself.  That also allows us to avoid trying to assign the same
 *      job to ourselves twice.
 *    o After we locate keys from the Manta indexing tier, we need to check that
 *      we've successfully assigned all keys and try again if not.
 *    o Need work on dropJob.  It shouldn't remove all trace of the job unless
 *      there are no ongoing async activities for it.  If there are, it should
 *      move it to a "dropped" list and abort all such activities.
 *    o Assigning jobs to the worker should use test-and-set, rather than a
 *      simple objectPut.
 *    o We should be validating records read from Moray (job records and task
 *      groups).
 *    o We shouldn't try to assign *all* found jobs to ourselves -- just up to
 *      some limit.
 *    o If we fail to initialize the job, we should retry again later.
 *    o All async events should record that they're happening.
 */

var mod_assert = require('assert');
var mod_os = require('os');

var mod_bunyan = require('bunyan');
var mod_jsprim = require('jsprim');
var mod_kang = require('kang');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_moray = require('./moray');

var mwConf = {
    'jobsBucket': 'marlinJobs',
    'taskGroupsBucket': 'marlinTaskGroups',
    'findInterval': 5 * 1000,	/* how often to ping Moray for new jobs (ms) */
    'tickInterval': 1 * 1000	/* how often to re-evalute state */
};


/*
 * Manages jobs owned by a single Marlin worker.  Most job management actually
 * happens in the mwJobState class below.  Arguments for this class include:
 *
 *    uuid	Unique identifier for this worker instance
 *
 *    moray	Moray interface
 *
 *    log	Bunyan-style logger instance
 */
function mwWorker(args)
{
	mod_assert.equal(typeof (args['uuid']), 'string');
	mod_assert.equal(typeof (args['moray']), 'object');
	mod_assert.equal(typeof (args['log']), 'object');

	/* immutable state */
	this.mw_uuid = args['uuid'];
	this.mw_moray = args['moray'];
	this.mw_log = args['log'].child({ 'worker': this.mw_uuid });
	this.mw_interval = mwConf['tickInterval'];

	this.mw_jobs = {};			/* assigned jobs, by jobId */
	this.mw_timeout = undefined;		/* JS timeout handle */
	this.mw_worker_start = undefined;	/* time worker started */
	this.mw_tick_start = undefined;		/* time last tick started */
	this.mw_tick_done = undefined;		/* time last tick finished */

	this.mw_moray.on('job', this.onJob.bind(this));
}

mwWorker.prototype.start = function ()
{
	var worker = this;

	this.mw_worker_start = new Date();
	this.mw_log.info('starting worker');

	process.nextTick(function workerTickStart() { worker.tick(); });
};

mwWorker.prototype.setTimer = function ()
{
	var worker = this;

	mod_assert.ok(this.mw_timeout === undefined);
	this.mw_timeout = setTimeout(
	    function workerTickTick() { worker.tick(); }, this.mw_interval);
};

mwWorker.prototype.tick = function ()
{
	this.mw_timeout = undefined;
	this.mw_tick_start = new Date();
	this.mw_moray.findUnassignedJobs();
	this.setTimer();
	this.mw_tick_done = new Date();
};

/*
 * Invoked when our Moray interface finds an unassigned job that we can pick up.
 */
mwWorker.prototype.onJob = function (job)
{
	var jobid = job['jobId'];
	var log = this.mw_log;
	var worker = this;
	var newjob;

	if (this.mw_jobs.hasOwnProperty(jobid)) {
		/*
		 * It shouldn't be possible to find a job that we already
		 * thought we owned because we'll prematurely drop any jobs for
		 * which our lock has expired.  But if this happens, drop the
		 * job now, log a message, and drive on.
		 */
		log.warn('found unassigned job "%s" that we thought we owned',
		    jobid);
		this.dropJob(jobid);
	}

	if (job['worker']) {
		log.info('attempting to steal job %s from %s', jobid,
		    job['worker']);
	} else {
		log.info('attempting to take unassigned job %s', jobid);
	}

	newjob = Object.create(job);
	newjob['worker'] = this.mw_uuid;

	this.mw_moray.assignJob(newjob, function (err) {
		if (err) {
			log.warn(err, 'failed to assign job %s', jobid);
			return;
		}

		log.info('assigned job %s', jobid);
		mod_assert.ok(!worker.mw_jobs.hasOwnProperty(jobid));

		var jobstate = new mwJobState({
		    'job': job,
		    'log': worker.mw_log,
		    'moray': worker.mw_moray
		});

		worker.mw_jobs[jobid] = jobstate;
		jobstate.start();
	});
};

mwWorker.prototype.dropJob = function (jobid)
{
	var jobstate = this.mw_jobs[jobid];
	delete (this.mw_jobs[jobstate]);
};


/*
 * Manages a single job.
 * XXX state machine
 */
function mwJobState(args)
{
	mod_assert.equal(typeof (args['job']), 'object');
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.equal(typeof (args['moray']), 'object');

	this.js_job = args['job'];
	this.js_moray = args['moray'];
	this.js_log = args['log'].child({ 'job': args['job']['jobId'] });

	this.js_init = undefined;		/* init pipeline and state */
	this.js_init_groups = undefined;
	this.js_init_locs = undefined;

	this.js_phasei = undefined;		/* current phase */
	this.js_phases = new Array(this.js_job['phases'].length);

	for (var i = 0; i < this.js_job['phases'].length; i++)
		this.js_phases[i] = {
		    p_input: [],
		    p_groups: {},
		    p_keys_unassigned: {}
		};
}

mwJobState.prototype.start = function ()
{
	var job = this;
	var log = this.js_log;

	this.js_log.info('restoring job %s', job.js_job['jobId']);

	this.js_init = mod_vasync.pipeline({
	    'arg': this,
	    'funcs': mwJobStagesInit
	}, function (err, results) {
		if (err) {
			log.error(err, 'failed to restore job "%s"',
			    job.js_job['jobId']);
			return;
		}

		log.info('successfully restored job %s', job.js_job['jobId']);
	});
};


var mwJobStagesInit = [
    mwJobInitFindTaskGroups,
    mwJobInitLoadTaskGroups,
    mwJobInitWriteAssignments
];

function mwJobInitFindTaskGroups(job, callback)
{
	var jobid = job.js_job['jobId'];

	job.js_moray.listTaskGroups(jobid, function (err, entries) {
		if (err) {
			callback(new mod_verror.VError(err,
			    'failed to find existing task groups'));
			return;
		}

		job.js_init_groups = entries;
		callback();
	});
}

function mwJobInitLoadTaskGroups(job, callback)
{
	mod_assert.ok(job.js_init_groups !== undefined);

	var jobid = job.js_job['jobId'];
	var log = job.js_log;
	var curphase = 0;
	var phase, prevphase;
	var tgid;

	/*
	 * Just load all task groups into their respective phases first because
	 * we have to process the groups in phase order and we don't know in
	 * what order they've been returned.
	 */
	job.js_init_groups.forEach(function (group) {
		var groups;

		tgid = group['taskGroupId'];

		if (group['phaseNum'] >= job.js_phases.length) {
			log.warn('restoring job %s: ignoring task group %s: ' +
			    'phase %s is out of range', jobid, tgid,
			    group['phaseNum']);
			return;
		}

		phase = job.js_phases[group['phaseNum']];
		groups = phase.p_groups;

		if (groups.hasOwnProperty(tgid)) {
			log.warn('restoring job %s: ignoring ' +
			    'duplicate task group %s', jobid, tgid);
			return;
		}

		groups[tgid] = group;

		if (group['phaseNum'] > curphase)
			curphase = group['phaseNum'];
	});

	/*
	 * If we find task group records for phase i > 0, we know we completed
	 * all phases up through i - 1.  We don't bother populating the
	 * in-memory structures for those phases.
	 */
	log.info('restoring job %s: resuming at phase %s', jobid, curphase);
	job.js_phasei = curphase;

	phase = job.js_phases[curphase];

	/*
	 * Compute the full set of input keys for job phase based on the
	 * results of the previous phase's task groups.  The input keys for the
	 * first phase are taken directly from the job definition.
	 */
	if (curphase === 0) {
		phase.p_input = job.js_job['inputKeys'].slice();
	} else {
		prevphase = job.mjr_phases[curphase - 1];
		prevphase.p_groups.forEach(function (group) {
			group['results'].forEach(function (result) {
				if (result['result'] != 'ok')
					return;

				phase.p_input = phase.p_input.concat(
				    result['outputs']);
			});
		});
	}

	/*
	 * Determine which of these keys have yet to be assigned.  For those
	 * which have not yet been assigned to task groups, locate them within
	 * Manta and compute a set of task groups.
	 */
	log.trace('restoring job %s: phase %s has %d input keys',
	    jobid, curphase, phase.p_input.length);

	phase.p_input.forEach(function (key) {
		phase.p_keys_unassigned[key] = true;
	});

	for (tgid in phase.p_groups) {
		var pgroup = phase.p_groups[tgid];

		pgroup['inputKeys'].forEach(function (key) {
			delete (phase.p_keys_unassigned[key]);
		});
	}

	var unassigned = Object.keys(phase.p_keys_unassigned);

	job.js_moray.mantaLocate(unassigned, function (err, locs) {
		if (err) {
			callback(new mod_verror.VError(err,
			    'failed to locate remaining keys'));
			return;
		}

		job.js_init_locs = locs;
		callback();
	});
}

function mwJobInitWriteAssignments(job, callback)
{
	mod_assert.ok(job.js_init_locs !== undefined);

	/* Invert the assignment, creating one new group per host. */
	var jobid = job.js_job['jobId'];
	var curphase = job.js_phasei;
	var phase = job.js_phases[curphase];
	var log = job.js_log;
	var locs = job.js_init_locs;
	var newgroups = {};

	for (var key in locs) {
		if (!phase.p_keys_unassigned.hasOwnProperty(key)) {
			log.warn('locate returned extra key: %s', key);
			continue;
		}

		if (locs[key].length < 1) {
			log.warn('locate returned zero hosts for key: %s', key);
			continue;
		}

		if (!newgroups.hasOwnProperty(locs[key][0])) {
			newgroups[locs[key][0]] = {
			    'jobId': jobid,
			    'taskGroupId': mod_uuid.v4(),
			    'host': locs[key][0],
			    'inputKeys': [],
			    'phase': job.js_job['phases'][curphase],
			    'state': 'dispatched',
			    'results': []
			};
		}

		newgroups[locs[key][0]]['inputKeys'].push(key);
		delete (phase.p_keys_unassigned[key]);
	}

	job.js_moray.saveTaskGroups(newgroups, function (err) {
		if (err) {
			callback(new mod_verror.VError(err,
			    'failed to save new task groups'));
			return;
		}

		mod_jsprim.forEachKey(newgroups, function (tgid, group) {
			mod_assert.ok(!phase.p_groups.hasOwnProperty(tgid));
			phase.p_groups[tgid] = group;
		});

		callback();
	});
}


/*
 * Kang (introspection) entry points
 */
function mwKangListTypes()
{
	return ([ 'worker', 'jobs' ]);
}

function mwKangListObjects(type)
{
	if (type == 'worker')
		return ([ 0 ]);

	return (Object.keys(mwWorkerInstance.mw_jobs));
}

function mwKangGetObject(type, ident)
{
	if (type == 'worker')
		return ({
		    'uuid': mwWorkerInstance.mw_uuid,
		    'interval': mwWorkerInstance.mw_interval,
		    'moray': mwWorkerInstance.mw_moray.mwm_buckets,
		    'timeout': mwWorkerInstance.mw_timeout ? 'yes' : 'no',
		    'worker_start': mwWorkerInstance.mw_worker_start,
		    'tick_start': mwWorkerInstance.mw_tick_start,
		    'tick_done': mwWorkerInstance.mw_tick_done
		});

	var obj = mwWorkerInstance.mw_jobs[ident];
	return ({
	    'job': obj.js_job,
	    'init': obj.js_init,
	    'init_groups': obj.js_init_groups,
	    'init_locs': obj.js_init_locs,
	    'phasei': obj.js_phasei,
	    'phases': obj.js_phases
	});
}

var mwWorkerInstance;

/*
 * Currently, running this file directly just exercises some of the
 * functionality defined here.
 */
function main()
{
	var log, moray, worker, jobid;

	log = new mod_bunyan({ 'name': 'worker-demo' });

	moray = new mod_moray.MockMoray({
	    'log': log,
	    'findInterval': mwConf['findInterval'],
	    'jobsBucket': mwConf['jobsBucket'],
	    'taskGroupsBucket': mwConf['taskGroupsBucket']
	});

	worker = mwWorkerInstance = new mwWorker({
	    'uuid': 'worker-001',
	    'moray': moray,
	    'log': log
	});

	jobid = 'job-001';

	moray.put(mwConf['jobsBucket'], jobid, {
	    'jobId': jobid,
	    'phases': [ { } ],
	    'inputKeys': [ 'key1', 'key2', 'key3', 'key4', 'key5', 'key6' ],
	    'results': []
	});

	process.nextTick(function () { worker.start(); });

	mod_kang.knStartServer({
	    'uri_base': '/kang',
	    'port': 8083,
	    'service_name': 'worker demo',
	    'version': '0.0.1',
	    'ident': mod_os.hostname(),
	    'list_types': mwKangListTypes,
	    'list_objects': mwKangListObjects,
	    'get': mwKangGetObject
	}, function (err, server) {
		if (err)
			throw (err);

		var addr = server.address();
		log.info('kang server listening at http://%s:%d',
		    addr['address'], addr['port']);
	});
}

main();
