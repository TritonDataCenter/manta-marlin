/*
 * lib/worker/moray.js: worker interface to Moray, abstracted out for easy
 *     replacement for automated testing.
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_util = require('util');

var mod_jsprim = require('jsprim');

/* Public interface */
exports.MockMoray = MockMoray;

/*
 * Mock interface that just stores data in memory.  Arguments include:
 *
 *	log		Bunyan-style logger
 *
 *	findInterval	Minimum time between "find" requests (milliseconds)
 *
 *	jobsBucket	Moray bucket for jobs
 *
 *      taskGroupsBucket Moray bucket for task groups
 *
 *	[requestDelay]	Simulated delay time for requests (milliseconds)
 *			(Default: 100ms)
 */
function MockMoray(args)
{
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.equal(typeof (args['findInterval']), 'number');
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
	this.mm_find_interval = Math.floor(args['findInterval']);
	this.mm_delay = delay;
	this.mm_buckets = {};

	/*
	 * We keep track of the last time we initiated a request to find jobs
	 * as well as the last time we completed one.  We use these to avoid
	 * making concurrent requests as well as to limit the frequency of such
	 * requests.
	 */
	this.mm_find_start = undefined;
	this.mm_find_done = undefined;

	this.mm_bkt_jobs = args['jobsBucket'];
	this.mm_bkt_taskgroups = args['taskGroupsBucket'];

	/*
	 * We track outstanding job operations here for debuggability.
	 */
	this.mm_jobops = {};
}

mod_util.inherits(MockMoray, mod_events.EventEmitter);


/*
 * Suggests that a request be made to query Moray for new and abandoned jobs.
 * The results are returned via the 'job' event rather than through a callback
 * so that the caller need not worry about managing the request context.  No
 * request will be made if it has not been long enough since the last one
 * completed (see the "findInterval" constructor parameter).
 */
MockMoray.prototype.findUnassignedJobs = function ()
{
	if (this.mm_find_start &&
	    this.mm_find_start.getTime() > this.mm_find_done.getTime())
		/* There's a request ongoing. */
		return;

	if (this.mm_find_done &&
	    (Date.now() - this.mm_find_done.getTime()) < this.mm_find_interval)
		/* Last request was too recent to make another. */
		return;

	var moray = this;
	var jobs = this.mm_buckets[this.mm_bkt_jobs];

	this.mm_find_start = new Date();

	setTimeout(function () {
		moray.mm_find_done = new Date();

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
MockMoray.prototype.put = function (bucket, key, obj)
{
	if (!this.mm_buckets.hasOwnProperty(bucket))
		this.mm_buckets[bucket] = {};

	this.mm_buckets[bucket][key] = obj;
};

/*
 * Private interface for retrieving data.
 */
MockMoray.prototype.get = function (bucket, key)
{
	if (!this.mm_buckets.hasOwnProperty(bucket))
		return (undefined);

	return (this.mm_buckets[bucket][key]);
};
