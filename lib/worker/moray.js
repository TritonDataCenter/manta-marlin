/*
 * lib/worker/moray.js: worker interface to Moray, abstracted out for easy
 *     replacement for automated testing.
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_url = require('url');
var mod_util = require('util');

var mod_extsprintf = require('extsprintf');
var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_moray = require('moray');

var mod_schema = require('../schema');
var mod_mrutil = require('../util');

/* Public interface */
exports.createMoray = createMoray;
exports.MockMoray = MockMoray;
exports.RemoteMoray = RemoteMoray;
exports.RemoteMoraySetup = RemoteMoraySetup;

/*
 * Create an appropriate Moray backend for the given worker configuration, which
 * should have been validated previously.  We use the URL to determine whether
 * to create a local "mock" instance or a remote one.
 *
 * The returned object has the following methods:
 *
 *    setup(callback)		Asynchronously creates all of the worker's Moray
 *    				buckets.
 *
 *    putObject()		Same as moray_client.putObject().
 *
 *    getObject()		Same as moray_client.getObject().
 *
 *    findObjects()		Same as moray_client.findObjects(), except that
 *    				the filter should be one of those returned by
 *    				the filter* functions below.
 *
 *    drain(callback)		Stops accepting new requests and invokes
 *    				"callback" when all existing requests have
 *    				completed.
 *
 * The following functions return filters for use by findObjects:
 *
 *    filterJobs(worker_uuid, abandon_time):
 *
 *	  Finds uncompleted jobs that are either owned by the given worker,
 *	  haven't been updated in "abandon_time" milliseconds, or are unowned.
 *
 *    filterJobInputs(jobids):
 *    filterTasks(jobids):
 *    filterTaskInputs(jobids):
 *    filterTaskOutputs(jobids):
 *
 *	  Finds all job input, task, task input, or task output records
 *	  (respectively) for the given jobs.
 */
function createMoray(args)
{
	var conf, cons;

	/*
	 * This argument should generally have been validated at a higher level,
	 * but we do a quick validation just in case it wasn't.
	 */
	mod_assert.equal('object', typeof (args));
	mod_assert.equal('object', typeof (args['log']));
	mod_assert.equal('object', typeof (args['conf']));
	mod_assert.equal('object', typeof (args['conf']['moray']));
	mod_assert.equal('object', typeof (args['conf']['moray']['storage']));

	conf = args['conf']['moray']['storage'];
	mod_assert.equal('string', typeof (conf['url']));
	cons = conf['url'] == 'local' ? MockMoray : RemoteMoray;
	return (new cons({ 'conf': args['conf'], 'log': args['log'] }));
}

/*
 * Mock interface that just stores data in memory.  Filters are implemented as
 * actual JavaScript functions.
 */
function MockMoray(args)
{
	mod_assert.equal(typeof (args['log']), 'object');

	this.mm_log = args['log'];
	this.mm_delay = 100;
	this.mm_buckets = {};
}

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

MockMoray.prototype.filterJobs = function (worker_uuid, timeout)
{
	return (function (record) {
		return (!record['value']['timeDone'] &&
		    (!record['value']['worker'] ||
		    record['value']['worker'] == worker_uuid ||
		    Date.now() - record['_mtime'] > timeout));
	});
};

MockMoray.prototype.filterJobIds = function (jobids)
{
	if (jobids.length === 0)
		return (null);

	return (function (record) {
		return (jobids.indexOf(record['value']['jobId']) != -1);
	});
};

MockMoray.prototype.filterJobInputs = MockMoray.prototype.filterJobIds;
MockMoray.prototype.filterTasks = MockMoray.prototype.filterJobIds;
MockMoray.prototype.filterTaskInputs = MockMoray.prototype.filterJobIds;
MockMoray.prototype.filterTaskOutputs = MockMoray.prototype.filterJobIds;

MockMoray.prototype.findObjects = function (bucket, filter, options)
{
	var moray = this;
	var req = new mod_events.EventEmitter();

	setTimeout(function () {
		mod_jsprim.forEachKey(moray.mm_buckets[bucket] || {},
		    function (key, value) {
			if (filter(value))
				req.emit('record', value);
		    });

		req.emit('end');
	}, this.mm_delay);
};

MockMoray.prototype.putObject = function (bucket, key, value, options, callback)
{
	var moray = this;

	setTimeout(function () {
		if (!moray.mm_buckets[bucket])
			moray.mm_buckets[bucket] = {};

		if (options && options.etag &&
		    (!moray.mm_buckets[bucket][key] ||
		    moray.mm_buckets[bucket][key]['_etag'] != options.etag)) {
			callback(new Error('conflict error'));
			return;
		}

		moray.mm_buckets[bucket][key] = {
		    '_etag': mod_mrutil.makeEtag(value),
		    '_mtime': Date.now(),
		    'key': key,
		    'value': value
		};

		callback();
	}, this.mm_delay);
};

MockMoray.prototype.getObject = function (bucket, key, value, options, callback)
{
	var moray = this;

	setTimeout(function () {
		if (!moray.mm_buckets[bucket] ||
		    !moray.mm_buckets[bucket][key]) {
			callback(new Error('key not found'));
			return;
		}

		callback(null, moray.mm_buckets[bucket][key]);
	}, this.mm_delay);
};

/* Private interface (for testing only) */
MockMoray.prototype.stop = function () {};

MockMoray.prototype.wipe = function (callback)
{
	var moray = this;
	setTimeout(function () {
		moray.mm_log.info('wiping mock moray buckets');
		moray.mm_buckets = {};
		callback();
	}, 0);
};


/*
 * Moray interface backed by an actual remote Moray instance.
 */
function RemoteMoray(args)
{
	var urlconf, reconnect;

	this.rm_conf = mod_jsprim.deepCopy(args['conf']);
	this.rm_log = args['log'];
	this.rm_setup = undefined;
	this.rm_ondrain = undefined;
	this.rm_npending = 0;

	urlconf = mod_url.parse(args['conf']['moray']['storage']['url']);
	mod_assert.ok(urlconf['hostname'],
	    'moray storage url must have a hostname');
	mod_assert.ok(urlconf['port'], 'moray storage url must have a port');
	this.rm_host = urlconf['hostname'];
	this.rm_port = parseInt(urlconf['port'], 10);
}

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
	    'log': this.rm_log,
	    'reconnect': true,
	    'retry': this.rm_conf['moray']['storage']['reconnect'],
	});

	this.rm_client.once('error', function (err) {
		moray.rm_log.error(err, 'client error');

		if (callback)
			callback(err);
	});

	this.rm_client.once('close', function (had_err) {
		moray.rm_log.warn('disconnected (internal reconnect)');
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
		    'client': moray.rm_client,
		    'conf': moray.rm_conf,
		    'log': moray.rm_log
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
 * "drain" just waits for outstanding requests to complete.  More requests can
 * continue to be dispatched.
 */
RemoteMoray.prototype.drain = function (callback)
{
	if (this.rm_setup === undefined && this.rm_npending === 0) {
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

RemoteMoray.prototype.putObject = function (bucket, key, value, options,
    callback)
{
	var moray = this;

	this.initClient(function (err) {
		if (err) {
			callback(err);
			return;
		}

		moray.rm_npending++;
		moray.rm_client.putObject(bucket, key, value, options,
		    function (suberr) {
			moray.rm_npending--;
			moray.checkDrain();
			callback(suberr);
		    });
	});
};

RemoteMoray.prototype.getObject = function (bucket, key, options, callback)
{
	var moray = this;

	this.initClient(function (err) {
		if (err) {
			callback(err);
			return;
		}

		moray.rm_npending++;
		moray.rm_client.getObject(bucket, key, options,
		    function (suberr, value) {
			moray.rm_npending--;
			moray.checkDrain();
			callback(suberr, value);
		    });
	});
};

RemoteMoray.prototype.findObjects = function (bucket, filter, options)
{
	var rv = new mod_events.EventEmitter();
	var moray = this;
	var query = filter.query;
	var extrafilter = filter.extra;
	this.rm_job_filter = undefined;

	this.initClient(function () {
		moray.rm_npending++;
		var req = moray.rm_client.findObjects(bucket, query, options);

		req.on('record', function (record) {
			if (!extrafilter || extrafilter(record)) {
				rv.emit('record', record);
				return;
			}
		});

		req.on('end', function () {
			moray.rm_npending--;
			moray.checkDrain();
			rv.emit('end');
		});

		req.on('error', function (err) {
			moray.rm_npending--;
			moray.checkDrain();
			rv.emit('error', err);
		});
	});

	return (rv);
};

RemoteMoray.prototype.filterJobs = function (worker_uuid, abandon_time)
{
	/*
	 * XXX We eventually want to include all of our criteria in the Moray
	 * filter, but until that point we use an extra filter function.
	 */
	var func = function (record) {
		return (record['value']['worker'] == worker_uuid ||
		    !record['value']['worker'] ||
		    Date.now() - record['_mtime'] > abandon_time);
	};

	// var query = mod_extsprintf.sprintf(
	//     '(&(!(timeDone=*))(|(!(worker=*))(worker=%s)))', worker_uuid);
	var query = '!(timeDone=*)';

	return ({ 'query': query, 'extra': func });
};

RemoteMoray.prototype.filterJobIds = function (jobids)
{
	if (jobids.length === 0)
		return (null);

	return ({
	    'query': '|' + jobids.map(
		function (j) { return ('(jobId=' + j + ')'); })
	});
};

RemoteMoray.prototype.filterJobInputs = RemoteMoray.prototype.filterJobIds;
RemoteMoray.prototype.filterTasks = RemoteMoray.prototype.filterJobIds;
RemoteMoray.prototype.filterTaskInputs = RemoteMoray.prototype.filterJobIds;
RemoteMoray.prototype.filterTaskOutputs = RemoteMoray.prototype.filterJobIds;

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
	var buckets = [];

	mod_jsprim.forEachKey(this.rm_conf['buckets'], function (_, value) {
		buckets.push(value);
	});

	this.initClient(function (err) {
		if (err) {
			callback(new mod_verror.VError(err, 'wipe failed'));
			return;
		}

		mod_vasync.forEachParallel({
		    'inputs': buckets,
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


function RemoteMoraySetup(args, callback)
{
	mod_assert.equal(typeof (args['client']), 'object');
	mod_assert.equal(typeof (args['conf']), 'object');
	mod_assert.equal(typeof (args['conf']['buckets']), 'object');

	var client = args['client'];
	var log = args['log'];
	var schemas = {};
	var buckets = [];

	mod_jsprim.forEachKey(args['conf']['buckets'], function (k, bkt) {
		schemas[bkt] = mod_schema.sBktSchemas[k];
		buckets.push(bkt);
	});

	return (mod_vasync.forEachParallel({
	    'inputs': buckets,
	    'func': function putBucket(bucket, subcallback) {
		log.info('putBucket "%s"', bucket);
		client.putBucket(bucket, { 'index': schemas[bucket] },
		    subcallback);
	    }
	}, callback));
}
