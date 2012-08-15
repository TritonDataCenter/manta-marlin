/*
 * lib/worker/moray.js: worker interface to Moray, abstracted out for easy
 *     replacement for automated testing.
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_util = require('util');

var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_moray = require('moray');

var mod_schema = require('../schema');
var mod_mrutil = require('../util');

/* Public interface */
exports.MockMoray = MockMoray;
exports.RemoteMoray = RemoteMoray;
exports.RemoteMoraySetup = RemoteMoraySetup;


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
	 * requests.
	 */
	this.mm_find = new mod_mrutil.Throttler(
	    Math.floor(args['findInterval']));

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

MockMoray.prototype.drain = function (callback)
{
	/*
	 * Kludgy, but we know that no outstanding operations can take longer
	 * than this.mm_delay.
	 */
	setTimeout(callback, this.mm_delay);
};

/*
 * Suggests that a request be made to query Moray for new and abandoned jobs.
 * The results are returned via the 'job' event rather than through a callback
 * so that the caller need not worry about managing the request context.  No
 * request will be made if it has not been long enough since the last one
 * completed (see the "findInterval" constructor parameter).
 */
MockMoray.prototype.findUnassignedJobs = function (self_uuid, timeout, emit)
{
	if (this.mm_find.tooRecent())
		return;

	var moray = this;
	var jobs = this.mm_buckets[this.mm_bkt_jobs];

	this.mm_find.start();
	setTimeout(function () {
		moray.mm_find.done();

		mod_jsprim.forEachKey(jobs, function (_, jobent) {
			var job = jobent['value'];
			if (!job['worker'] ||
			    (self_uuid && job['worker'] == self_uuid) ||
			    Date.now() - Date.parse(jobent['mtime']) >
			    timeout) {
				/*
				 * The "job" event is currently only used by
				 * the test suite.
				 */
				moray.emit('job', { 'etag': jobent['etag'] },
				    mod_jsprim.deepCopy(job));
				emit({ 'etag': jobent['etag'] },
				    mod_jsprim.deepCopy(job));
			}
		});
	}, this.mm_delay);
};

/*
 * Saves the current job record back to Moray.  Only one outstanding operation
 * may be ongoing for a given job, including saves, assignments, and listing
 * task groups.
 */
MockMoray.prototype.saveJob = function (job, options, callback)
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
		delete (jobops[jobid]);

		if (options['etag'] && jobs[jobid] &&
		    jobs[jobid]['etag'] !== options['etag']) {
			callback(new Error('etag mismatch'));
			return;
		}

		jobs[jobid] = {
		    'etag': mod_mrutil.makeEtag(job),
		    'mtime': mod_jsprim.iso8601(new Date()),
		    'value': mod_jsprim.deepCopy(job)
		};

		log.info('saved job', jobs[jobid]);
		callback();
	}, this.mm_delay);
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

		mod_jsprim.forEachKey(taskgroups, function (_, groupent) {
			var group = groupent['value'];

			if (group['jobId'] != jobid)
				return;

			rv.push({ 'etag': groupent['etag'], 'value': group });
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

			allgroups[group['taskGroupId']] = {
			    'etag': mod_mrutil.makeEtag(group),
			    'mtime': mod_jsprim.iso8601(new Date()),
			    'value': group
			};
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
MockMoray.prototype.watchTaskGroups = function (jobid, ontaskgroups)
{
	var groups = this.mm_buckets[this.mm_bkt_taskgroups];

	setTimeout(function () {
		var rv = [];

		mod_jsprim.forEachKey(groups, function (_, groupent) {
			var group = groupent['value'];

			if (group['jobId'] != jobid)
				return;

			rv.push({
			    'etag': groupent['etag'],
			    'value': mod_jsprim.deepCopy(group)
			});
		});

		ontaskgroups(rv);
	}, this.mm_delay);
};

/*
 * Private interface for loading data.
 */
MockMoray.prototype.put = function (bucket, key, obj, callback)
{
	if (!this.mm_buckets.hasOwnProperty(bucket))
		this.mm_buckets[bucket] = {};

	this.mm_buckets[bucket][key] = {
	    'etag': mod_mrutil.makeEtag(obj),
	    'mtime': mod_jsprim.iso8601(new Date()),
	    'value': obj
	};

	this.mm_log.info('saving %s/%s', bucket, key,
	    this.mm_buckets[bucket][key]);

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
		if (!moray.mm_buckets.hasOwnProperty(bucket) ||
		    !moray.mm_buckets[bucket].hasOwnProperty(key))
			callback(null, undefined);
		else
			callback(null, moray.mm_buckets[bucket][key].value);
	}, 0);
};

MockMoray.prototype.restify = function (server)
{
	var moray = this;

	server.get('/m/:bucket/:key', function (request, response, next) {
		moray.get(request.params['bucket'], request.params['key'],
		    function (err, rv) {
			if (err) {
				next(err);
				return;
			}

			if (rv === undefined)
				response.send(404);
			else
				response.send(200, rv);

			next();
		    });
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
		    request.body, function (err) {
			if (err) {
				next(err);
				return;
			}

			response.send(201);
			next();
		    });
	});
};

/* Private interface (for testing only) */
MockMoray.prototype.stop = function () {};

MockMoray.prototype.wipe = function (callback)
{
	var moray = this;
	setTimeout(function () {
		moray.mm_log.info('wiping mock moray buckets');
		delete (moray.mm_buckets[moray.mm_bkt_taskgroups]);
		callback();
	}, 0);
};

MockMoray.prototype.debugState = function ()
{
	return ({
	    'find_state': this.mm_find,
	    'jobops': this.mm_jobops
	});
};

/*
 * Implements the same interface as MockMoray, but backed by an actual remote
 * Moray instance.  Arguments include:
 *
 *	host			DNS hostname for remote Moray instance
 *
 *	port			TCP port for remote Moray instance
 *
 *	log			Bunyan-style logger
 *
 *	findInterval		Minimum time between "find" requests
 *				(milliseconds)
 *
 *	jobsBucket		Moray bucket for jobs
 *
 *      taskGroupsBucket	Moray bucket for task groups
 */
function RemoteMoray(args)
{
	mod_assert.equal(typeof (args['host']), 'string');
	mod_assert.equal(typeof (args['port']), 'number');
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.equal(typeof (args['findInterval']), 'number');
	mod_assert.equal(typeof (args['jobsBucket']), 'string');
	mod_assert.equal(typeof (args['taskGroupsBucket']), 'string');

	mod_events.EventEmitter();

	this.rm_host = args['host'];
	this.rm_port = args['port'];
	this.rm_log = args['log'].child({ 'component': 'remote-moray' });

	this.rm_find = new mod_mrutil.Throttler(
	    Math.floor(args['findInterval']));
	this.rm_bkt_jobs = args['jobsBucket'];
	this.rm_bkt_taskgroups = args['taskGroupsBucket'];

	/*
	 * We track outstanding job operations here for debuggability.
	 */
	this.rm_jobops = {};
	this.rm_tgops = {};
	this.rm_setup = undefined;
	this.rm_ondrain = undefined;
}

mod_util.inherits(RemoteMoray, mod_events.EventEmitter);


RemoteMoray.prototype.initClient = function (callback)
{
	var moray = this;

	if (this.rm_client) {
		if (callback)
			callback();

		return;
	}

	this.rm_client = mod_moray.createClient({
	    'host': this.rm_host,
	    'port': this.rm_port,
	    'log': this.rm_log
	});

	this.rm_client.once('error', function (err) {
		moray.rm_log.error(err, 'client error');

		if (callback)
			callback(err);
	});

	this.rm_client.once('close', function (had_err) {
		moray.rm_log.error('disconnected; attempting reconnect');
		moray.rm_client = undefined;
		moray.initClient();
	});

	this.rm_client.once('connect', function () {
		moray.rm_log.info('connected');
		moray.rm_client.removeAllListeners('error');

		if (callback)
			callback();
	});
};

RemoteMoray.prototype.setup = function (callback)
{
	var moray = this;

	this.initClient(function (err) {
		if (err) {
			callback(err);
			return;
		}

		mod_assert.ok(moray.rm_setup === undefined);

		moray.rm_setup = RemoteMoraySetup({
		    'moray': moray.rm_client,
		    'jobsBucket': moray.rm_bkt_jobs,
		    'taskGroupsBucket': moray.rm_bkt_taskgroups
		}, function (suberr) {
			moray.rm_setup = undefined;

			if (callback) {
				callback(suberr);
				return;
			}

			if (suberr) {
				moray.rm_log.fatal(suberr, 'moray setup');
				throw (suberr);
			}
		});
	});
};

/*
 * "drain" just pauses until outstanding requests have completed.  More requests
 * can continue to be dispatched.
 */
RemoteMoray.prototype.drain = function (callback)
{
	if (this.rm_setup === undefined &&
	    !this.rm_find.ongoing() &&
	    mod_jsprim.isEmpty(this.rm_jobops) &&
	    mod_jsprim.isEmpty(this.rm_tgops)) {
		callback();
		this.rm_ondrain = undefined;
		return;
	}

	this.rm_ondrain = callback;
};

RemoteMoray.prototype.checkDrain = function ()
{
	if (this.rm_ondrain)
		this.drain(this.rm_ondrain);
};

RemoteMoray.prototype.findUnassignedJobs = function (self_uuid, timeout, emit)
{
	if (this.rm_find.tooRecent())
		return;

	var moray = this;
	var bucket = this.rm_bkt_jobs;
	/*
	 * XXX when Moray supports it, this filter should use jobId == null ||
	 * jobId == me || NOW() - mtime > timeout.
	 */
	var filter = 'jobId=*';

	this.rm_find.start();
	var req = this.rm_client.findObjects(bucket, filter,
	    { 'noCache': true });

	req.once('error', function (err) {
		moray.rm_find.done();
		moray.checkDrain();
		moray.rm_log.warn(err, 'failed to search jobs bucket');
	});

	req.on('end', function () {
		moray.rm_find.done();
		moray.checkDrain();
	});

	req.on('record', function (job) {
		if (!job['value']['worker'] ||
		    (self_uuid && job['value']['worker'] == self_uuid) ||
		    Date.now() - job['_mtime'] > timeout) {
			/*
			 * See implementation in MockMoray.
			 */
			var meta = {
			    'etag': job['_etag']
			};
			moray.emit('job', meta, job['value']);
			emit(meta, job['value']);
		}
	});
};

RemoteMoray.prototype.saveJob = function (job, options, callback)
{
	var moray = this;
	var bucket = this.rm_bkt_jobs;
	var jobops = this.rm_jobops;

	mod_assert.ok(!jobops.hasOwnProperty(job['jobId']));
	jobops[job['jobId']] = {
	    'jobId': job['jobId'],
	    'op': 'saveJob',
	    'opts': options,
	    'start': new Date()
	};

	this.rm_client.putObject(bucket, job['jobId'], job, options,
	    function (err) {
		delete (jobops[job['jobId']]);
		moray.checkDrain();
		callback(err);
	});
};

RemoteMoray.prototype.listTaskGroups = function (jobid, callback)
{
	var moray = this;
	var bucket = this.rm_bkt_taskgroups;
	var filter = 'jobId=' + jobid;
	var rv = [];

	var req = this.rm_client.findObjects(bucket, filter,
	    { 'noCache': true });

	req.once('error', function (err) {
		moray.checkDrain();
		moray.rm_log.error(err, 'failed to search taskgroups bucket');
		callback(err);
	});

	req.on('end', function () {
		moray.checkDrain();
		callback(null, rv);
	});

	req.on('record', function (record) {
		mod_assert.equal(record['value']['jobId'], jobid);
		rv.push({ 'etag': record['_etag'], 'value': record['value'] });
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
	var moray = this;
	var bucket = this.rm_bkt_taskgroups;
	var client = this.rm_client;
	var log = this.rm_log;
	var tgops = this.rm_tgops;
	var uuid = mod_uuid.v4();
	var inputs = Object.keys(newgroups);

	this.rm_tgops[uuid] = {
	    'op': 'saveTgs',
	    'start': new Date(),
	    'jobId': newgroups[inputs[0]]['jobId'],
	    'taskGroupIds': inputs.map(function (tgid) {
		return (newgroups[tgid]['taskGroupId']);
	    })
	};

	mod_vasync.forEachParallel({
	    'inputs': inputs,
	    'func': function (tgid, subcallback) {
		log.info('saving task group', newgroups[tgid]);
		client.putObject(bucket, newgroups[tgid]['taskGroupId'],
		    newgroups[tgid], subcallback);
	    }
	}, function (err) {
		delete (tgops[uuid]);
		moray.checkDrain();
		callback(err);
	});
};

RemoteMoray.prototype.watchTaskGroups = function (jobid, ontaskgroups)
{
	var moray = this;
	var bucket = this.rm_bkt_taskgroups;
	var filter = 'jobId=' + jobid;
	var rv = [];

	this.rm_log.debug('querying taskgroups for job %s', jobid);

	var req = this.rm_client.findObjects(bucket, filter,
	    { 'noCache': true });

	req.once('error', function (err) {
		moray.checkDrain();
		moray.rm_log.warn(err, 'failed to search taskgroups bucket');
	});

	req.on('end', function () {
		moray.checkDrain();
		moray.rm_log.debug('job %s: found %d taskgroups',
		    jobid, rv.length);
		ontaskgroups(rv);
	});

	req.on('record', function (record) {
		mod_assert.equal(record['value']['jobId'], jobid);

		rv.push({
		    'etag': record['_etag'],
		    'mtime': record['_mtime'],
		    'value': record['value']
		});
	});
};

/* Private interface (for testing only) */
RemoteMoray.prototype.restify = function () {
	this.rm_log.warn('restify interface not supported on remote moray');
};

/* Private interface (for testing only) */
RemoteMoray.prototype.put = function (bucket, key, obj, callback)
{
	var log = this.rm_log;

	this.rm_client.putObject(bucket, key, obj, function (err) {
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

	this.rm_client.getObject(bucket, key, { 'noCache': true },
	    function (err, record) {
		if (err)
			log.fatal(err, 'failed to "get" %s/%s', bucket, key);

		callback(err, record['value']);
	});
};

/* Private interface (for testing only) */
RemoteMoray.prototype.stop = function ()
{
	this.rm_log.info('stopping RemoteMoray client');
	this.rm_client.close();
};

/* Private interface (for testing only) */
RemoteMoray.prototype.wipe = function (callback)
{
	var moray = this;

	this.initClient(function (err) {
		if (err) {
			callback(new mod_verror.VError(err, 'wipe failed'));
			return;
		}

		mod_vasync.forEachParallel({
		    'inputs': [ moray.rm_bkt_jobs, moray.rm_bkt_taskgroups ],
		    'func': function (bucket, subcallback) {
			moray.rm_log.info('wiping bucket %s', bucket);
			moray.rm_client.delBucket(bucket, function (suberr) {
				if (suberr &&
				    /does not exist/.test(suberr['message']))
					suberr = null;
				subcallback(suberr);
			});
		    }
		}, callback);
	});
};

RemoteMoray.prototype.debugState = function ()
{
	return ({
	    'find_state': this.rm_find,
	    'jobops': this.rm_jobops,
	    'tgops': this.rm_tgops,
	    'setup': this.rm_setup
	});
};


function RemoteMoraySetup(args, callback)
{
	mod_assert.equal(typeof (args['moray']), 'object');
	mod_assert.equal(typeof (args['jobsBucket']), 'string');
	mod_assert.equal(typeof (args['taskGroupsBucket']), 'string');

	var moray = args['moray'];
	var bktJobs = args['jobsBucket'];
	var bktTgs = args['taskGroupsBucket'];
	var schemas = {};

	schemas[bktJobs] = mod_schema.sBktSchemas['job'];
	schemas[bktTgs] = mod_schema.sBktSchemas['taskgroup'];

	return (mod_vasync.forEachParallel({
	    'inputs': [ bktJobs, bktTgs ],
	    'func': function putBucket(bucket, subcallback) {
		moray.putBucket(bucket, { 'index': schemas[bucket] },
		    subcallback);
	    }
	}, callback));
}
