/*
 * lib/moray.js: common Moray abstractions
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_util = require('util');

var mod_extsprintf = require('extsprintf');
var mod_jsprim = require('jsprim');
var mod_mautil = require('./util');
var mod_moray = require('moray');

var sprintf = mod_extsprintf.sprintf;


/* Public interface. */
exports.MorayWriteQueue = MorayWriteQueue;
exports.PollState = PollState;
exports.poll = poll;

/*
 * Batches and throttles Moray writes.  Consumers enqueue objects to be saved
 * out.  These saves will be throttled.  Upon failure, saves can be retried.
 * Consumers may also mark objects dirty while a write is pending, in which case
 * the queue either updates the state to be written out or enqueues another
 * write (if the previous one is already in flight).
 *
 * Arguments include:
 *
 *    client		Moray client reference
 *
 *    log		Bunyan-style logger
 *
 *    buckets		Mapping of program bucket names to actual bucket names.
 *
 *    maxpending	Maximum concurrent "put" operations.
 */
function MorayWriteQueue(args)
{
	mod_assert.equal(typeof (args), 'object');
	mod_assert.equal(typeof (args['client']), 'function');
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.equal(typeof (args['buckets']), 'object');
	mod_assert.equal(typeof (args['maxpending']), 'number');

	this.q_buckets = args['buckets'];
	this.q_client = args['client'];
	this.q_log = args['log'];
	this.q_max_puts = args['maxpending'];
	this.q_pending_puts = 0;
	this.q_nsaved = 0;
	this.q_nfailed = 0;

	/*
	 * We keep track of queued and pending save operations in q_records,
	 * indexed by bucket and then key.  The contents is an object with:
	 *
	 *    r_queued		time when the record was queued for write
	 *
	 *    r_issued		time when the write request was issued
	 *
	 *    r_attempts	number of previous failed attempts
	 *
	 *    r_key		key being saved
	 *
	 *    r_value		value to save into Moray
	 *
	 *    r_options		options to pass through to putObject
	 *
	 *    r_pending		added when an object with an in-flight record is
	 *    			dirtied.  This object has r_queued, r_value, and
	 *    			r_options.
	 */
	this.q_records = {};

	/*
	 * Queued requests are stored on q_outq as objects with:
	 *
	 *    q_bucket, q_key	identifies a record in q_records.
	 *
	 * These are separated from the records because throttling fairly
	 * requires a queue, but consumer requests to dirty existing records
	 * requires being able to look them up by key too.
	 */
	this.q_outq = [];
}

/*
 * Enqueue a write to the specified object identified by friendly bucket name
 * and key.  If a write is already pending but has not yet been dispatched, the
 * new value will clobber the previous one.  If a write is already in flight,
 * another write will be issued later after the previous one completes.
 * "options" are passed through to moray.putObject, so it can contain properties
 * like "etag" and the like.
 */
MorayWriteQueue.prototype.dirty = function (name, key, value, options)
{
	var bucket = this.q_buckets[name];
	var now = Date.now();
	var group, record;

	if (!this.q_records[bucket])
		this.q_records[bucket] = {};
	group = this.q_records[bucket];

	if (group[key]) {
		record = group[key];

		if (record.r_issued) {
			this.q_log.debug('dirty "%s"/%s" (write in flight)',
			    bucket, key);
			record.r_pending.r_queued = now;
			record.r_pending.r_value = value;

			if (options)
				record.r_pending.r_options = options;
		} else {
			this.q_log.debug('dirty "%s"/%s" (write queued)',
			    bucket, key);
			record.r_value = value;

			if (options)
				record.r_options = options;
		}

		return;
	}

	this.q_log.debug('dirty "%s"/%s" (new record)', bucket, key);

	group[key] = {
	    'r_queued': now,
	    'r_issued': undefined,
	    'r_attempts': 0,
	    'r_bucket': bucket,
	    'r_key': key,
	    'r_value': value,
	    'r_options': options || {},
	    'r_pending': {
	        'r_queued': undefined,
		'r_value': undefined,
		'r_options': undefined
	    }
	};

	this.q_outq.push({
	    'q_bucket': bucket,
	    'q_key': key
	});
};

/*
 * Returns true if the given Moray object has a write pending in this queue.
 */
MorayWriteQueue.prototype.pending = function (name, key)
{
	var bucket = this.q_buckets[name];
	return (this.q_records[bucket] &&
	    this.q_records[bucket].hasOwnProperty(key));
};

/*
 * [private] Write a single record.
 */
MorayWriteQueue.prototype.recordSave = function (client, qent)
{
	var queue = this;

	var bucket = qent.q_bucket;
	var key = qent.q_key;
	var record = this.q_records[bucket][key];
	var value = record.r_value;
	var options = record.r_options;

	mod_assert.ok(record.r_issued === undefined);
	record.r_issued = Date.now();

	this.q_pending_puts++;
	client.putObject(bucket, key, value, options, function (err) {
		queue.q_pending_puts--;

		var retry = false;

		if (err) {
			/*
			 * XXX Distinguish between transient and intrinsic
			 * failures.  Maybe have a max # of attempts?
			 */
			retry = true;
			queue.q_nfailed++;
			queue.q_log.warn(err, 'recordSave "%s"/"%s": failed',
			    bucket, key, value, options);
			record.r_attempts++;
			record.r_queued = Date.now();
		} else {
			queue.q_nsaved++;
		}

		if (record.r_pending.r_queued) {
			retry = true;

			queue.q_log.debug('recordSave "%s"/"%s": updating ' +
			    'value dirtied while write was in flight',
			    bucket, key);

			record.r_queued = record.r_pending.r_queued;
			record.r_value = record.r_pending.r_value;

			/*
			 * The appropriate value of "options" is tricky.  If the
			 * second write had an etag, we always want to use that,
			 * regardless of whether the previous write succeeded
			 * or whether it was conditional on some etag, since the
			 * point is to make sure we're not clobbering a record,
			 * and the subsequent write's etag is newer.  Otherwise,
			 * if the first write failed, we must use its etag to
			 * avoid clobbering.  Otherwise, we must use no etag,
			 * since there's only one write and it's intended to
			 * clobber.
			 */
			if (!mod_jsprim.isEmpty(record.r_pending.r_options))
				record.r_options = record.r_pending.r_options;
			else if (!err)
				record.r_options = {};

			record.r_pending.r_queued = undefined;
			record.r_pending.r_value = undefined;
			record.r_pending.r_options = undefined;
		}

		if (retry) {
			queue.q_log.debug('recordSave "%s"/"%s": enqueued ' +
			    'for later retry', bucket, key);

			record.r_issued = undefined;

			queue.q_outq.push({
			    'q_bucket': record.r_bucket,
			    'q_key': record.r_key
			});

			return;
		}

		delete (queue.q_records[bucket][key]);
		queue.q_log.debug('recordSave "%s"/"%s": done', bucket, key);
	});
};

/*
 * Save as many records as possible, up to the current max.
 */
MorayWriteQueue.prototype.flush = function ()
{
	var nflushed = 0;
	var client = this.q_client();

	if (!client) {
		this.q_log.warn('flushing queue: failed (no client)');
		return (0);
	}

	this.q_log.trace('flushing queue');

	while (this.q_pending_puts < this.q_max_puts &&
	    this.q_outq.length > 0) {
		var qent = this.q_outq.shift();
		this.recordSave(client, qent);
		nflushed++;
	}

	return (nflushed);
};

MorayWriteQueue.prototype.stats = function ()
{
	return ({
	    'nfailed': this.q_nfailed,
	    'nsaved': this.q_nsaved
	});
};


/*
 * A PollState encapsulates the state and logic required to manage a sliding
 * window of records in a single Moray bucket.  The consumer's goal is to
 * process record changes exactly once, but this is surprisingly difficult
 * to do efficiently because there's no per-record field which defines a total
 * order on the changes that we can use to "ack" changes up to a given point.
 *
 * We're guaranteed that if we see a record with txn_snap T, then all records
 * with txn_snap T0 < T must also be visible.  But it's possible that there are
 * other record with txn_snap T that are not yet visible.  So in order to
 * implement exactly-once, we must keep track of both the latest txn_snap we've
 * seen as well as the set of records seen with that txn_snap.
 */
function PollState(interval, ringbufsize)
{
	/*
	 * As described above, we have to keep track of the latest transaction
	 * snapshot we've seen (so that our next request retrieves only the
	 * records that can possibly have changed) as well as the set of objects
	 * we've seen at that snapshot (so that we can dedup subsequent results
	 * against that set).
	 */
	this.p_txn_snap = null;
	this.p_cache = null;
	this.p_throttle = new mod_mautil.Throttler(interval);

	/* debugging state */
	this.p_req = undefined;		/* most recent request */
	this.p_recent = [];		/* recent request filters */
	this.p_maxrecent = ringbufsize;	/* nr of requests to keep */
}

/*
 * Check whether the given record should be processed.  Additionally, update the
 * poll state to reflect this record.  Records must be processed in increasing
 * order by txn_snap.
 */
PollState.prototype.record = function (record)
{
	var rsnap = record['_txn_snap'];

	mod_assert.equal('number', typeof (rsnap));

	if (this.p_txn_snap === rsnap &&
	    this.p_cache.hasOwnProperty(record['_id']))
		/* We saw this record last time. */
		return (false);

	/* XXX handle txn_snap wraparound */
	mod_assert.ok(this.p_txn_snap === null ||
	    rsnap >= this.p_txn_snap,
	    'records processed out of _txn_snap order');

	if (this.p_txn_snap === null || rsnap > this.p_txn_snap) {
		this.p_txn_snap = rsnap;
		this.p_cache = {};
	}

	this.p_cache[record['_id']] = true;

	return (true);
};

PollState.prototype.request = function (filter, req)
{
	this.p_req = req;
	this.p_recent.push(filter);

	if (this.p_recent.length > this.p_maxrecent)
		this.p_recent.shift();
};

/*
 * Returns a Moray filter to search for new records, given the current poll
 * state.
 */
PollState.prototype.filter = function ()
{
	if (this.p_txn_snap === null)
		return ('');

	return ('(_txn_snap>=' + this.p_txn_snap + ')');
};


/*
 * Polls Moray for new records:
 *
 *    client	Moray client object
 *
 *    options	Moray findObjects options
 *
 *    now	timestamp of the request
 *
 *    log	bunyan logger
 *
 *    throttle	throttler to determine whether to skip this poll
 *
 *    bucket	bucket to poll
 *
 *    filter	Moray filter string
 *
 *    onrecord	callback to invoke on each found record, as callback(record)
 *
 *    [ondone]	optional callback to invoke upon successful completion, as
 *
 *			callback(bucket, now, count)
 */
function poll(args)
{
	mod_assert.equal(typeof (args['client']), 'object');
	mod_assert.equal(typeof (args['options']), 'object');
	mod_assert.equal(typeof (args['now']), 'number');
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.equal(typeof (args['throttle']), 'object');
	mod_assert.equal(typeof (args['bucket']), 'string');
	mod_assert.equal(typeof (args['filter']), 'string');
	mod_assert.equal(typeof (args['onrecord']), 'function');

	var client = args['client'];
	var options = args['options'];
	var now = args['now'];
	var log = args['log'];
	var throttle = args['throttle'];
	var bucket = args['bucket'];
	var filter = args['filter'];
	var onrecord = args['onrecord'];
	var ondone = args['ondone'];

	var count = 0;
	var req;

	if (throttle.tooRecent())
		return (null);

	throttle.start();

	log.debug('poll start: %s', bucket, filter);

	req = client.findObjects(bucket, filter, options);

	req.on('error', function (err) {
		log.warn(err, 'poll failed: %s', bucket);
		throttle.done();
	});

	req.on('record', function (record) {
		count++;
		onrecord(record);
	});

	req.on('end', function () {
		log.debug('poll completed: %s', bucket);
		throttle.done();

		if (ondone)
			ondone(bucket, now, count);
	});

	return (req);
}

/*
 * XXX Do we need an abstraction for fetch-or-create a Moray client for a given
 * DNS hostname?  For both the worker and the agent, any existing Moray clients
 * should stop being used when they're no longer a valid client for the given
 * hostname (i.e., when the "master" fails over within a Moray shard).  If the
 * non-authoritative Moray instances explicitly fail when we make requests to
 * them, then we don't need to do anything special to make sure we stop using
 * stale clients.  We just need to make sure that we re-resolve the IP when we
 * go create a new client.  However, even if that's the case, the agent still
 * needs to resolve the IP *before* calling moray.createClient, since the
 * default DNS resolution won't work.
 */
