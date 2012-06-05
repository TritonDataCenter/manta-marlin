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
 *    o Requests to save task groups to Moray should be batched rather than sent
 *      separately.
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

var mwConf = {
    'bucketJobs': 'marlinJobs',	/* Moray bucket for job records */
    'bucketTaskGroups': 'marlinTaskGroups',
    'queryInterval': 10 * 1000	/* how often to ping Moray for new jobs (ms) */
};


/*
 * Mock interface to Moray that stores data in memory.
 */
function mwMockMoray(args)
{
	mod_assert.equal(typeof (args['log']), 'object');

	this.mwm_log = args['log'].child({ 'component': 'mock-moray' });
	this.mwm_buckets = {};
}

mwMockMoray.prototype.objectSearch = function (bucket, filter, callback)
{
	if (!this.mwm_buckets.hasOwnProperty(bucket)) {
		process.nextTick(function () { callback(null, []); });
		return;
	}

	/*
	 * Since this is only used by the worker for testing, we assume the
	 * filter is the only one that we know the worker uses.  We don't bother
	 * using an index, since this is just for testing.  XXX assert filter.
	 */
	var rv, key, obj;
	rv = [];
	for (key in this.mwm_buckets[bucket]) {
		obj = this.mwm_buckets[bucket][key];
		/* XXX */
		rv.push(obj);
	}

	process.nextTick(function () { callback(null, rv); });
};

mwMockMoray.prototype.objectPut = function (bucket, key, obj, callback)
{
	var moray = this;

	process.nextTick(function () {
		if (!moray.mwm_buckets.hasOwnProperty(bucket))
			moray.mwm_buckets[bucket] = {};

		moray.mwm_buckets[bucket][key] = mod_jsprim.deepCopy(obj);
		moray.mwm_log.info('bucket %s, key %s: %j', bucket, key,
		    moray.mwm_buckets[bucket][key]);
		callback(null);
	});
};

mwMockMoray.prototype.objectGet = function (bucket, key, callback)
{
	var moray = this;

	process.nextTick(function () {
		var rv = moray.mwm_buckets.hasOwnProperty(bucket) ?
		    moray.mwm_buckets[bucket][key] : undefined;
		callback(null, mod_jsprim.deepCopy(rv));
	});
};

mwMockMoray.prototype.mantaLocate = function (keys, callback)
{
	var rv = {};
	var i = 0;

	keys.forEach(function (key) {
		rv[key] = [ 'node' + (i++ % 3) ];
	});

	process.nextTick(function () { callback(null, rv); });
};


/*
 * Manages all the state for a single Marlin worker.  Arguments include:
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
	this.mw_interval = mwConf['queryInterval'];

	this.mw_jobs = {};			/* assigned jobs, by jobId */
	this.mw_timeout = undefined;		/* JS timeout handle */
	this.mw_assign_op = undefined;		/* pending job assignment */
	this.mw_worker_start = undefined;	/* time worker started */
	this.mw_tick_start = undefined;		/* time last tick started */
	this.mw_tick_done = undefined;		/* time last tick finished */
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
	var worker = this;
	var filter = ''; /* XXX */

	this.mw_tick_start = new Date();

	this.mw_moray.objectSearch(mwConf['bucketJobs'], filter,
	    function (err, jobs) {
		if (err) {
			worker.mw_log.warn(err, 'failed to query for new jobs');
			worker.setTimer();
			worker.mw_tick_done = new Date();
			return;
		}

		worker.tryAssignJobs(jobs, function () {
			worker.setTimer();
			worker.mw_tick_done = new Date();
		});
	    });
};

/*
 * Given a list of jobs that we currently believe are unassigned, try to assign
 * them to ourselves.
 */
mwWorker.prototype.tryAssignJobs = function (jobs, callback)
{
	var ops, i, job;

	ops = [];

	for (i = 0; i < jobs.length; i++) {
		job = jobs[i];

		if (this.mw_jobs.hasOwnProperty(job['jobId'])) {
			/*
			 * It shouldn't be possible to find a job that we
			 * already thought we owned because we'll prematurely
			 * drop any jobs for which our lock has expired.  But
			 * if this happens, drop the job now, log a message, and
			 * drive on.
			 */
			this.mw_log.warn('found unassigned job "%s" that we ' +
			    'thought we owned', job['jobId']);
			this.dropJob(job['jobId']);
		}

		if (job['worker']) {
			this.mw_log.info('attempting to steal job %s from %s',
			    job['jobId'], job['worker']);
		} else {
			this.mw_log.info('attempting to take unassigned job %s',
			    job['jobId']);
		}

		ops.push(job);
	}

	if (ops.length === 0) {
		callback();
		return;
	}

	var worker = this;

	mod_assert.ok(this.mw_assign_op === undefined);
	this.mw_assign_op = mod_vasync.forEachParallel({
	    'inputs': ops,
	    'func': this.tryAssign.bind(this)
	}, function (err, results) {
		var res, asnjob;

		worker.mw_assign_op = undefined;

		for (i = 0; i < results['operations'].length; i++) {
			res = results['operations'][i];
			asnjob = ops[i];

			if (res['status'] != 'ok') {
				worker.mw_log.warn(res['err'],
				    'failed to assign job %s', asnjob['jobId']);
				continue;
			}

			worker.mw_log.info('assigned job %s', asnjob['jobId']);
			worker.addJob(asnjob);
		}

		callback();
	});
};

/*
 * Attempt to assign the given job to this worker.
 */
mwWorker.prototype.tryAssign = function (origjob, callback)
{
	var bucket = mwConf['bucketJobs'];
	var job = Object.create(origjob);
	job['worker'] = this.mw_uuid;
	this.mw_moray.objectPut(bucket, job['jobId'], job, callback);
};

mwWorker.prototype.addJob = function (job)
{
	var jobstate = new mwJobState({
	    'job': job,
	    'log': this.mw_log,
	    'moray': this.mw_moray
	});

	mod_assert.ok(!this.mw_jobs.hasOwnProperty(job['jobId']));
	this.mw_jobs[job['jobId']] = jobstate;
	jobstate.start();
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

	this.mjs_job = args['job'];
	this.mjs_moray = args['moray'];
	this.mjs_log = args['log'].child({ 'job': args['job']['jobId'] });

	this.mjs_init = undefined;		/* init pipeline and state */
	this.mjs_init_groups = undefined;
	this.mjs_init_locs = undefined;
	this.mjs_init_assign = undefined;

	this.mjs_phasei = undefined;		/* current phase */
	this.mjs_phases = new Array(this.mjs_job['phases'].length);

	for (var i = 0; i < this.mjs_job['phases'].length; i++)
		this.mjs_phases[i] = {
		    p_input: [],
		    p_groups: {},
		    p_keys_unassigned: {}
		};
}

mwJobState.prototype.start = function ()
{
	var job = this;
	var log = this.mjs_log;

	this.mjs_log.info('restoring job %s', job.mjs_job['jobId']);

	this.mjs_init = mod_vasync.pipeline({
	    'arg': this,
	    'funcs': mwJobStagesInit
	}, function (err, results) {
		if (err) {
			log.error(err, 'failed to restore job "%s"',
			    job.mjs_job['jobId']);
			return;
		}

		log.info('successfully restored job %s', job.mjs_job['jobId']);
	});
};


var mwJobStagesInit = [
    mwJobInitFindTaskGroups,
    mwJobInitLoadTaskGroups,
    mwJobInitWriteAssignments
];

function mwJobInitFindTaskGroups(job, callback)
{
	var bucket = mwConf['bucketTaskGroups'];
	var filter = ''; /* XXX */

	job.mjs_moray.objectSearch(bucket, filter, function (err, entries) {
		if (err) {
			callback(new mod_verror.VError(err,
			    'failed to find existing task groups'));
			return;
		}

		job.mjs_init_groups = entries;
		callback();
	});
}

function mwJobInitLoadTaskGroups(job, callback)
{
	mod_assert.ok(job.mjs_init_groups !== undefined);

	var jobid = job.mjs_job['jobId'];
	var log = job.mjs_log;
	var curphase = 0;
	var phase, prevphase;
	var tgid;

	/*
	 * Just load all task groups into their respective phases first because
	 * we have to process the groups in phase order and we don't know in
	 * what order they've been returned.
	 */
	job.mjs_init_groups.forEach(function (group) {
		var groups;

		tgid = group['taskGroupId'];

		if (group['phaseNum'] >= job.mjs_phases.length) {
			log.warn('restoring job %s: ignoring task group %s: ' +
			    'phase %s is out of range', jobid, tgid,
			    group['phaseNum']);
			return;
		}

		phase = job.mjs_phases[group['phaseNum']];
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
	job.mjs_phasei = curphase;

	phase = job.mjs_phases[curphase];

	/*
	 * Compute the full set of input keys for job phase based on the
	 * results of the previous phase's task groups.  The input keys for the
	 * first phase are taken directly from the job definition.
	 */
	if (curphase === 0) {
		phase.p_input = job.mjs_job['inputKeys'].slice();
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

	job.mjs_moray.mantaLocate(unassigned, function (err, locs) {
		if (err) {
			callback(new mod_verror.VError(err,
			    'failed to locate remaining keys'));
			return;
		}

		job.mjs_init_locs = locs;
		callback();
	});
}

function mwJobInitWriteAssignments(job, callback)
{
	mod_assert.ok(job.mjs_init_locs !== undefined);

	/* Invert the assignment, creating one new group per host. */
	var jobid = job.mjs_job['jobId'];
	var curphase = job.mjs_phasei;
	var phase = job.mjs_phases[curphase];
	var log = job.mjs_log;
	var locs = job.mjs_init_locs;
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
			    'phase': job.mjs_job['phases'][curphase],
			    'state': 'dispatched',
			    'results': []
			};
		}

		newgroups[locs[key][0]]['inputKeys'].push(key);
		delete (phase.p_keys_unassigned[key]);
	}

	job.mjs_init_assign = mod_vasync.forEachParallel({
	    'inputs': Object.keys(newgroups),
	    'func': function (host, subcallback) {
		var group = newgroups[host];
		job.mjs_moray.objectPut(mwConf['bucketTaskGroups'],
		    group['taskGroupId'], group, subcallback);
	    }
	}, function (err, results) {
		if (err) {
			callback(new mod_verror.VError(err,
			'failed to save new task groups'));
			return;
		}

		for (var tgid in newgroups) {
			mod_assert.ok(!phase.p_groups.hasOwnProperty(tgid));
			phase.p_groups[tgid] = newgroups[tgid];
		}

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
		    'assign_op': mwWorkerInstance.mw_assign_op,
		    'worker_start': mwWorkerInstance.mw_worker_start,
		    'tick_start': mwWorkerInstance.mw_tick_start,
		    'tick_done': mwWorkerInstance.mw_tick_done
		});

	var obj = mwWorkerInstance.mw_jobs[ident];
	return ({
	    'job': obj.mjs_job,
	    'init': obj.mjs_init,
	    'init_groups': obj.mjs_init_groups,
	    'init_locs': obj.mjs_init_locs,
	    'init_assign': obj.mjs_init_assign,
	    'phasei': obj.mjs_phasei,
	    'phases': obj.mjs_phases
	});
}

var mwWorkerInstance;

/*
 * Currently, running this file directly just exercises some of the
 * functionality defined here.
 */
function main()
{
	var log, moray, worker, noop, jobid;

	log = new mod_bunyan({ 'name': 'worker-demo' });

	moray = new mwMockMoray({ 'log': log });

	worker = mwWorkerInstance = new mwWorker({
	    'uuid': mod_uuid.v4(),
	    'moray': moray,
	    'log': log
	});

	noop = function () {};

	jobid = mod_uuid.v4();

	moray.objectPut(mwConf['bucketJobs'], jobid, {
		'jobId': jobid,
		'phases': [ {
		} ],
		'inputKeys': [ 'key1', 'key2', 'key3', 'key4', 'key5', 'key6' ],
		'results': []
	}, noop);

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
