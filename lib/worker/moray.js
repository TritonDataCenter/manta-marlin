/*
 * lib/worker/moray.js: worker interface to Moray, abstracted out for easy
 *     replacement for automated testing.
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_util = require('util');

var mod_jsprim = require('jsprim');
var mod_vasync = require('vasync');

var mod_moray_client = require('moray-client');

/* Public interface */
exports.MockMoray = MockMoray;
exports.RemoteMoray = RemoteMoray;


/*
 * Simple interface for making sure an asynchronous operation doesn't occur more
 * frequently than the given interval.
 */
function Throttler(interval)
{
	this.p_interval = interval;
	this.p_start = undefined;
	this.p_done = undefined;
}

Throttler.prototype.start = function ()
{
	this.p_start = new Date();
};

Throttler.prototype.done = function ()
{
	mod_assert.ok(this.p_start !== undefined);
	this.p_done = new Date();
};

Throttler.prototype.tooRecent = function ()
{
	if (this.p_start &&
	    (!this.p_done || this.p_start.getTime() > this.p_done.getTime()))
		/* Request ongoing */
		return (true);

	if (this.p_done && Date.now() - this.p_done.getTime() < this.p_interval)
		/* Last request was too recent to try again. */
		return (true);

	return (false);
};


/*
 * Mock interface that just stores data in memory.  Arguments include:
 *
 *	log			Bunyan-style logger
 *
 *	findInterval		Minimum time between "find" requests
 *				(milliseconds)
 *
 *	taskGroupInterval 	Minimum time between requests for task group
 *				updates (milliseconds)
 *
 *	jobsBucket		Moray bucket for jobs
 *
 *      taskGroupsBucket	Moray bucket for task groups
 *
 *	[requestDelay]		Simulated delay time for requests (milliseconds)
 *				(Default: 100ms)
 */
function MockMoray(args)
{
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.equal(typeof (args['findInterval']), 'number');
	mod_assert.equal(typeof (args['taskGroupInterval']), 'number');
	mod_assert.equal(typeof (args['jobsBucket']), 'string');
	mod_assert.equal(typeof (args['taskGroupsBucket']), 'string');
	mod_assert.ok(args['requestDelay'] === undefined ||
	    typeof (args['requestDelay']) == 'number' &&
	    Math.floor(args['requestDelay']) == args['requestDelay'] &&
	    args['requestDelay'] >= 0);

	mod_events.EventEmitter();

	var delay = args['requestDelay'] === undefined ? 100 :
	    args['requestDelay'];

	this.mm_log = args['log'].child({ 'component': 'mock-moray' });
	this.mm_delay = delay;
	this.mm_buckets = {};

	/*
	 * We keep track of the last time we initiated a request to find jobs
	 * as well as the last time we completed one.  We use these to avoid
	 * making concurrent requests as well as to limit the frequency of such
	 * requests.  Ditto for task groups.
	 */
	this.mm_find = new Throttler(Math.floor(args['findInterval']));
	this.mm_tg = new Throttler(Math.floor(args['taskGroupInterval']));

	this.mm_bkt_jobs = args['jobsBucket'];
	this.mm_bkt_taskgroups = args['taskGroupsBucket'];

	/*
	 * We track outstanding job operations here for debuggability.
	 */
	this.mm_jobops = {};
}

mod_util.inherits(MockMoray, mod_events.EventEmitter);


MockMoray.prototype.setup = function (callback)
{
	if (callback)
		setTimeout(callback, this.mm_delay);
};

/*
 * Suggests that a request be made to query Moray for new and abandoned jobs.
 * The results are returned via the 'job' event rather than through a callback
 * so that the caller need not worry about managing the request context.  No
 * request will be made if it has not been long enough since the last one
 * completed (see the "findInterval" constructor parameter).
 */
MockMoray.prototype.findUnassignedJobs = function ()
{
	if (this.mm_find.tooRecent())
		return;

	var moray = this;
	var jobs = this.mm_buckets[this.mm_bkt_jobs];

	this.mm_find.start();
	setTimeout(function () {
		moray.mm_find.done();

		mod_jsprim.forEachKey(jobs, function (_, job) {
			if (!job['worker'])
				moray.emit('job', mod_jsprim.deepCopy(job));
		});
	}, this.mm_delay);
};

/*
 * Saves the current job record back to Moray, overwriting whatever's there.
 * This should only be used when the worker knows that it owns it.  Only one
 * outstanding operation may be ongoing for a given job, including saves,
 * assignments, and listing task groups.
 */
MockMoray.prototype.saveJob = function (job, callback)
{
	var jobid = job['jobId'];
	var jobs = this.mm_buckets[this.mm_bkt_jobs];
	var jobops = this.mm_jobops;
	var log = this.mm_log;

	mod_assert.equal(typeof (jobid), 'string');
	mod_assert.ok(!jobops.hasOwnProperty(jobid),
	    'operation for job ' + jobid + ' is already outstanding');

	jobops[jobid] = { 'op': 'save', 'job': job, 'start': new Date() };

	setTimeout(function () {
		jobs[jobid] = mod_jsprim.deepCopy(job);
		delete (jobops[jobid]);
		log.info('saved job', jobs[jobid]);
		callback();
	}, this.mm_delay);
};

/*
 * Just like saveJob, but should use an atomic test-and-set operation.
 */
MockMoray.prototype.assignJob = function (job, callback)
{
	this.saveJob(job, callback);
};

/*
 * Returns the task groups associated with the given job.
 */
MockMoray.prototype.listTaskGroups = function (jobid, callback)
{
	var taskgroups = this.mm_buckets[this.mm_bkt_taskgroups];
	var jobops = this.mm_jobops;

	mod_assert.equal(typeof (jobid), 'string');
	mod_assert.ok(!jobops.hasOwnProperty(jobid),
	    'operation for job ' + jobid + ' is already outstanding');

	jobops[jobid] = { 'op': 'list', 'job': jobid, 'start': new Date() };

	setTimeout(function () {
		var rv = [];

		delete (jobops[jobid]);

		mod_jsprim.forEachKey(taskgroups, function (_, group) {
			if (group['jobId'] != jobid)
				return;

			rv.push(group);
		});

		callback(null, rv);
	}, this.mm_delay);
};

/*
 * Batch-saves a bunch of task group records (stored as an object).
 */
MockMoray.prototype.saveTaskGroups = function (newgroups, callback)
{
	if (!this.mm_buckets.hasOwnProperty(this.mm_bkt_taskgroups))
		this.mm_buckets[this.mm_bkt_taskgroups] = {};

	var allgroups = this.mm_buckets[this.mm_bkt_taskgroups];
	var log = this.mm_log;

	setTimeout(function () {
		mod_jsprim.forEachKey(newgroups, function (_, group) {
			log.info('saving task group', group);
			allgroups[group['taskGroupId']] = group;
		});

		callback();
	}, this.mm_delay);
};

/*
 * Suggests that a request be made to query Moray for updated task group records
 * for the given job.  The results are returned by invoking the given callback
 * if any task groups are found.  The callback will not be invoked on error.
 * These semantics free the caller from having to manage the request context;
 * they simply provide what is essentially an event handler for a "taskgroup"
 * event.  No request will be made if it has not been long enough since the last
 * one completed.
 */
MockMoray.prototype.watchTaskGroups = function (jobid, phase, ontaskgroups)
{
	if (this.mm_tg.tooRecent())
		return;

	var moray = this;
	var groups = this.mm_buckets[this.mm_bkt_taskgroups];

	this.mm_tg.start();

	setTimeout(function () {
		var rv = [];

		moray.mm_tg.done();

		mod_jsprim.forEachKey(groups, function (_, group) {
			if (group['jobId'] != jobid ||
			    group['phaseNum'] != phase)
				return;

			rv.push(mod_jsprim.deepCopy(group));
		});

		ontaskgroups(rv);
	}, this.mm_delay);
};

/*
 * Given a set of Manta keys, (asynchronously) return what physical nodes
 * they're stored on.
 */
MockMoray.prototype.mantaLocate = function (keys, callback)
{
	var rv = {};
	var i = 0;

	keys.forEach(function (key) {
		rv[key] = [ 'node' + (i++ % 3) ];
	});

	setTimeout(function () { callback(null, rv); }, this.mm_delay);
};

/*
 * Private interface for loading data.
 */
MockMoray.prototype.put = function (bucket, key, obj, callback)
{
	if (!this.mm_buckets.hasOwnProperty(bucket))
		this.mm_buckets[bucket] = {};

	this.mm_buckets[bucket][key] = obj;
	this.mm_log.info('saving %s/%s', bucket, key, obj);

	if (callback)
		setTimeout(callback, 0);
};

/*
 * Private interface for retrieving data.
 */
MockMoray.prototype.get = function (bucket, key, callback)
{
	var moray = this;

	setTimeout(function () {
		if (!moray.mm_buckets.hasOwnProperty(bucket))
			callback(null, undefined);
		else
			callback(null, moray.mm_buckets[bucket][key]);
	}, 0);
};

MockMoray.prototype.restify = function (server)
{
	var moray = this;

	server.get('/m/:bucket/:key', function (request, response, next) {
		var rv = moray.get(
		    request.params['bucket'], request.params['key']);

		if (rv === undefined)
			response.send(404);
		else
			response.send(200, rv);

		next();
	});

	server.get('/m/:bucket', function (request, response, next) {
		var bucket, rv;

		bucket = request.params['bucket'];

		if (!moray.mm_buckets.hasOwnProperty(bucket)) {
			response.send(404);
			next();
			return;
		}

		rv = Object.keys(moray.mm_buckets[bucket]);
		response.send(200, rv);
		next();
	});

	server.get('/m/', function (request, response, next) {
		response.send(200, Object.keys(moray.mm_buckets));
		next();
	});

	server.put('/m/:bucket/:key', function (request, response, next) {
		moray.put(request.params['bucket'], request.params['key'],
		    request.body);
		response.send(201);
		next();
	});
};

/* Private interface (for testing only) */
MockMoray.prototype.stop = function () {};

MockMoray.prototype.wipe = function (callback)
{
	var moray = this;
	setTimeout(function () {
		moray.mm_log.info('wiping mock moray buckets');
		delete (moray.mm_buckets[moray.mm_bkt_jobs]);
		delete (moray.mm_buckets[moray.mm_bkt_taskgroups]);
		callback();
	}, 0);
};


/*
 * Implements the same interface as MockMoray, but backed by an actual remote
 * Moray instance.  Arguments include:
 *
 *	url			URL for remote Moray instance
 *
 *	log			Bunyan-style logger
 *
 *	findInterval		Minimum time between "find" requests
 *				(milliseconds)
 *
 *	taskGroupInterval 	Minimum time between requests for task group
 *				updates (milliseconds)
 *
 *	jobsBucket		Moray bucket for jobs
 *
 *      taskGroupsBucket	Moray bucket for task groups
 */
function RemoteMoray(args)
{
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.equal(typeof (args['findInterval']), 'number');
	mod_assert.equal(typeof (args['taskGroupInterval']), 'number');
	mod_assert.equal(typeof (args['jobsBucket']), 'string');
	mod_assert.equal(typeof (args['taskGroupsBucket']), 'string');

	mod_events.EventEmitter();

	this.rm_log = args['log'].child({ 'component': 'remote-moray' });

	this.rm_client = mod_moray_client.createClient({
	    'url': args['url'],
	    'log': this.rm_log
	});

	this.rm_find = new Throttler(Math.floor(args['findInterval']));
	this.rm_tg = new Throttler(Math.floor(args['taskGroupInterval']));
	this.rm_bkt_jobs = args['jobsBucket'];
	this.rm_bkt_taskgroups = args['taskGroupsBucket'];

	this.rm_schemas = {};

	this.rm_schemas[this.rm_bkt_jobs] = {
		'jobId': {
			'type': 'string',
			'unique': true
		}
	};

	this.rm_schemas[this.rm_bkt_taskgroups] = {
		'taskGroupId': {
			'type': 'string',
			'unique': true
		},
		'jobId': {
			'type': 'string',
			'unique': false
		},
		'phaseNum': {
			'type': 'number',
			'unique': false
		}
	};
}

mod_util.inherits(RemoteMoray, mod_events.EventEmitter);


RemoteMoray.prototype.setup = function (callback)
{
	var moray = this;

	mod_vasync.forEachParallel({
	    'inputs': [ this.rm_bkt_jobs, this.rm_bkt_taskgroups ],
	    'func': function (bucket, subcallback) {
		moray.rm_client.putBucket(bucket,
		    { 'schema': moray.rm_schemas[bucket] }, subcallback);
	    }
	}, function (err) {
		if (callback) {
			callback(err);
			return;
		}

		if (err) {
			moray.rm_log.fatal(err, 'moray setup failed');
			throw (err);
		}
	});
};

RemoteMoray.prototype.findUnassignedJobs = function ()
{
	if (this.rm_find.tooRecent())
		return;

	var moray = this;
	var bucket = this.rm_bkt_jobs;
	var filter = 'jobId=*';

	this.rm_find.start();
	this.rm_client.search(bucket, filter, function (err, objects) {
		moray.rm_find.done();

		if (err) {
			moray.rm_log.warn(err, 'failed to search jobs bucket');
			return;
		}

		mod_jsprim.forEachKey(objects, function (_, job) {
			if (!job['worker'])
				moray.emit('job', job);
		});
	});
};

RemoteMoray.prototype.saveJob = function (job, callback)
{
	this.doSaveJob(job, {}, callback);
};

RemoteMoray.prototype.assignJob = function (job, callback)
{
	/* XXX save and use etags */
	this.doSaveJob(job, { 'etag': '' }, callback);
};

RemoteMoray.prototype.doSaveJob = function (job, options, callback)
{
	var bucket = this.rm_bkt_jobs;
	this.rm_client.put(bucket, job['jobId'], job, options, callback);
};

RemoteMoray.prototype.listTaskGroups = function (jobid, callback)
{
	var moray = this;
	var bucket = this.rm_bkt_taskgroups;
	var filter = 'jobId=' + jobid;

	this.rm_tg.start();
	this.rm_client.search(bucket, filter, function (err, groups) {
		moray.rm_tg.done();

		if (err) {
			moray.rm_log.error(err, 'failed to search tgs bucket');
			callback(err);
			return;
		}

		var rv = [];

		mod_jsprim.forEachKey(groups, function (_, group) {
			mod_assert.equal(group['jobId'], jobid);
			rv.push(group);
		});

		callback(null, rv);
	});
};

RemoteMoray.prototype.saveTaskGroups = function (newgroups, callback)
{
	/*
	 * XXX Until Moray supports batch updates, we write these all in
	 * parallel and fail if any of them fail.  This will just cause our
	 * caller to try to write them again.  If we decide this is a fine
	 * solution, we should at least cap the number of these we write at
	 * once.
	 */
	var bucket = this.rm_bkt_taskgroups;
	var client = this.rm_client;
	var log = this.rm_log;

	mod_vasync.forEachParallel({
	    'inputs': Object.keys(newgroups),
	    'func': function (tgid, subcallback) {
		log.info('saving task group', newgroups[tgid]);
		client.put(bucket, newgroups[tgid]['taskGroupId'],
		    newgroups[tgid], subcallback);
	    }
	}, callback);
};

RemoteMoray.prototype.watchTaskGroups = function (jobid, phase, ontaskgroups)
{
	if (this.rm_tg.tooRecent())
		return;

	var moray = this;
	var bucket = this.rm_bkt_taskgroups;
	var filter = '(&(jobId=' + jobid + ')(phaseNum=' + phase + '))';

	this.rm_tg.start();
	this.rm_client.search(bucket, filter, function (err, groups) {
		moray.rm_tg.done();

		if (err) {
			moray.rm_log.warn(err, 'failed to search tgs bucket');
			return;
		}

		var rv = [];

		mod_jsprim.forEachKey(groups, function (_, group) {
			mod_assert.equal(group['jobId'], jobid);
			mod_assert.equal(group['phaseNum'], phase);
			rv.push(group);
		});

		ontaskgroups(rv);
	});
};

/* XXX This is still mocked up. */
RemoteMoray.prototype.mantaLocate = function (keys, callback)
{
	var rv = {};
	var i = 0;

	keys.forEach(function (key) {
		rv[key] = [ 'node' + (i++ % 3) ];
	});

	setTimeout(function () { callback(null, rv); }, 100);
};

/* Private interface (for testing only) */
RemoteMoray.prototype.restify = function () {
	this.rm_log.warn('restify interface not supported on remote moray');
};

/* Private interface (for testing only) */
RemoteMoray.prototype.put = function (bucket, key, obj, callback)
{
	var log = this.rm_log;

	this.rm_client.put(bucket, key, obj, function (err) {
		if (callback) {
			callback(err);
			return;
		}

		if (err) {
			log.fatal(err, 'failed to "put" %s/%s',
			    bucket, key, obj);
			throw (err);
		}
	});
};

/* Private interface (for testing only) */
RemoteMoray.prototype.get = function (bucket, key, callback)
{
	var log = this.rm_log;

	this.rm_client.get(bucket, key, function (err, meta, obj) {
		if (err)
			log.fatal(err, 'failed to "get" %s/%s', bucket, key);

		callback(err, obj);
	});
};

/* Private interface (for testing only) */
RemoteMoray.prototype.stop = function ()
{
	var log = this.rm_log;

	this.rm_client.quit(function () {
		log.info('remote Moray client stopped');
	});
};

/* Private interface (for testing only) */
RemoteMoray.prototype.wipe = function (callback)
{
	var moray = this;

	mod_vasync.forEachParallel({
	    'inputs': [ this.rm_bkt_jobs, this.rm_bkt_taskgroups ],
	    'func': function (bucket, subcallback) {
		moray.rm_log.info('wiping bucket %s', bucket);
		moray.rm_client.delBucket(bucket, function (err) {
			if (err && err['code'] == 'ResourceNotFound')
				err = null;
			subcallback(err);
		});
	    }
	}, callback);
};
