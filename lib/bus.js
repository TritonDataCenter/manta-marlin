/*
 * lib/bus.js: Marlin service's interface to the outside world
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_url = require('url');
var mod_util = require('util');

var mod_jsprim = require('jsprim');
var mod_vasync = require('vasync');

var mod_moray = require('moray');

var mod_mamoray = require('./moray');
var mod_schema = require('./schema');
var mod_mautil = require('./util');

var EventEmitter = mod_events.EventEmitter;
var Throttler = mod_mautil.Throttler;
var VError = mod_vasync.VError;

var mReportInterval = 5000;	/* min time between transient error log msgs */
var mMinRetryTime = 100;	/* min time between retries */
var mMaxRetryTime = 10000;	/* max time between retries */
var mMaxConflictRetries = 5;	/* max "etag conflict" retries */

/* Public interface */
exports.createBus = createBus;

function createBus(conf, options)
{
	mod_assert.equal(typeof (conf), 'object');
	mod_assert.ok(conf.hasOwnProperty('moray'));

	mod_assert.equal(typeof (options), 'object');
	mod_assert.equal(typeof (options['log']), 'object');

	return (new MorayBus(conf, options));
}

function MorayBus(conf, options)
{
	var url;

	url = mod_url.parse(conf['moray']['storage']['url']);
	this.mb_host = url['hostname'];
	this.mb_port = parseInt(url['port'], 10);
	this.mb_reconnect = conf['moray']['storage']['reconnect'];

	this.mb_log = options['log'];
	this.mb_client = undefined;	/* current Moray client */
	this.mb_connecting = false;	/* currently connecting */
	this.mb_reported = {};		/* last report time, by error name */
	this.mb_onconnect = [];

	/* read-side */
	this.mb_subscriptions = {};	/* current subscriptions */

	/* write-side */
	this.mb_nmaxputs = conf['tunables']['maxPendingPuts'];
	this.mb_npendingputs = 0;	/* currently pending PUTs */
	this.mb_txns = {};		/* all pending requests */
	this.mb_putq = [];		/* outgoing queue of requests */
	this.mb_txns_byrecord = {};	/* maps records to pending txn */

	/* other operations */
	this.mb_init_buckets = undefined;
}

mod_util.inherits(MorayBus, EventEmitter);

/*
 * "Subscribes" to the given Moray query.  The bucket "bucket" is polled
 * periodically, with "query" invoked for each request to return the query
 * string to use for the request.  This allows slight changes to the query based
 * on, e.g., the current time, which is passed as an argument to "query".
 *
 * "options" should contain:
 *
 *    timePoll		minimum time between requests (but see "onrecord" below)
 *
 *    limit		maximum number of records to return in one query
 *
 * "onrecord" is invoked for each record found, as onrecord(record, barrier).
 * Subsequent polls will not begin until the configured timeout has elapsed AND
 * the barrier has zero pending operations.  This allows callers to delay
 * subsequent polls until they have finished processing the records found by the
 * current poll.
 */
MorayBus.prototype.subscribe = function (bucket, query, options, onrecord)
{
	var subscrip;

	subscrip = new MorayBusSubscription(bucket, query, options, onrecord);
	this.mb_subscriptions[subscrip.mbs_id] = subscrip;

	return (subscrip.mbs_id);
};

/*
 * Like "subscribe", except that the subscription will be removed and "ondone"
 * will be invoked once the query has been executed successfully.  However, if a
 * request fails, it will be retried, and it's still possible to emit the same
 * matching record more than once.
 */
MorayBus.prototype.oneshot = function (bucket, query, options, onrecord, ondone)
{
	var id = this.subscribe(bucket, query, options, onrecord);
	this.convertOneshot(id, ondone);
};

/*
 * Given an id for a subscription (as returned from subscribe()), remove the
 * subscription after the next successful poll request.  If a poll request is
 * outstanding, the subscription will *not* be removed after that request
 * completes, even if successful, but rather after the subsequent one completes
 * successfully.  That's usually the desired behavior, since this is typically
 * used when the caller knows that the *current* database state is complete, but
 * that doesn't mean the currently *pending* request will find the complete
 * state.
 */
MorayBus.prototype.convertOneshot = function (id, ondone)
{
	var worker = this;
	var subscrip;

	subscrip = this.mb_subscriptions[id];
	subscrip.mbs_onsuccess = function () {
		delete (worker.mb_subscriptions[id]);
		ondone();
	};
};

/*
 * Remove the given subscription.  "onrecord" and "ondone" for pending
 * operations may still be invoked.  (XXX should we ignore those here?)
 */
MorayBus.prototype.unsubscribe = function (id)
{
	mod_assert.ok(this.mb_subscriptions.hasOwnProperty(id));
	delete (this.mb_subscriptions[id]);
};

/*
 * Return the server-side count of the number of records in "bucket" matching
 * "query".  "callback" is invoked as callback(count).  Errors aren't possible
 * because the operation will be retried until it completes.
 */
MorayBus.prototype.count = function (bucket, query, uoptions, callback)
{
	var options, done;

	/*
	 * We implement "count" by doing a "limit 1" query and returning the
	 * _count of the one result we get back, or 0 if we got no results.
	 */
	options = Object.create(uoptions);
	options['limit'] = 1;

	done = false;
	this.oneshot(bucket, query, options, function (record) {
		if (done)
			return;

		done = true;
		callback(record['_count']);
	}, function () {
		if (done)
			return;

		done = true;
		callback(0);
	});
};

MorayBus.prototype.connect = function ()
{
	var bus = this;
	var client;

	if (this.mb_client !== undefined || this.mb_connecting)
		return;

	this.mb_connecting = true;

	client = mod_moray.createClient({
	    'host': this.mb_host,
	    'port': this.mb_port,
	    'log': this.mb_log.child({ 'component': 'MorayClient' }),
	    'reconnect': true,
	    'retry': this.mb_reconnect
	});

	client.on('error', function (err) {
		bus.mb_connecting = false;
		bus.mb_log.error(err, 'moray client error');
	});

	client.on('close', function () {
		bus.mb_log.error('moray client closed');
	});

	client.on('connect', function () {
		mod_assert.ok(bus.mb_client === undefined ||
		    bus.mb_client == client);
		bus.mb_client = client;
		bus.mb_connecting = false;

		var wakeup = bus.mb_onconnect;
		bus.mb_onconnect = [];
		wakeup.forEach(function (callback) { callback(); });
	});
};

MorayBus.prototype.poll = function (now)
{
	mod_assert.equal(typeof (now), 'number');
	var nowdate = new Date(now);

	if (this.mb_client === undefined) {
		this.mb_log.debug('skipping poll (still connecting)');
		this.connect();
		return;
	}

	for (var id in this.mb_subscriptions)
		this.pollOne(this.mb_subscriptions[id], nowdate);
};

MorayBus.prototype.pollOne = function (subscrip, nowdate)
{
	if (!mod_jsprim.isEmpty(subscrip.mbs_barrier.pending))
		return;

	if (subscrip.mbs_throttle.tooRecent())
		return;

	/*
	 * It's a little subtle, but the fact that we save mbs_onsuccess here at
	 * the beginning of the poll is critical to satisfy the convertOneshot
	 * contract that "ondone" is invoked after the next successful request
	 * that starts *after* convertOneshot() is invoked.  If we used a
	 * closure here that resolved subscrip.mbs_onsuccess only after the poll
	 * completed, this would do the wrong thing (and the result would be a
	 * very subtle race that might rarely be hit).
	 */
	var query = subscrip.mbs_query(nowdate.getTime());
	var onsuccess = subscrip.mbs_onsuccess;

	subscrip.mbs_nreqs++;
	subscrip.mbs_cur_start = nowdate;
	subscrip.mbs_cur_query = query;
	subscrip.mbs_cur_nrecs = 0;

	mod_mamoray.poll({
	    'client': this.mb_client,
	    'options': {
		'limit': subscrip.mbs_limit,
		'noCache': true
	    },
	    'now': nowdate.getTime(),
	    'log': this.mb_log,
	    'throttle': subscrip.mbs_throttle,
	    'bucket': subscrip.mbs_bucket,
	    'filter': query,
	    'onrecord': subscrip.mbs_onrecord,
	    'ondone': function () {
		subscrip.mbs_last_start = subscrip.mbs_cur_start;
		subscrip.mbs_last_query = subscrip.mbs_cur_query;

		if (subscrip.mbs_cur_nrecs > 0) {
			subscrip.mbs_last_nonempty_start =
			    subscrip.mbs_cur_start;
			subscrip.mbs_last_nonempty_nrecs =
			    subscrip.mbs_cur_nrecs;
		} else {
			subscrip.mbs_nreqs_empty++;
		}

		subscrip.mbs_cur_start = undefined;
		subscrip.mbs_cur_nrecs = undefined;
		subscrip.mbs_cur_query = undefined;

		if (onsuccess)
			onsuccess();
	    }
	});
};

/*
 * Enqueue an atomic "put" for the given records.  The update will be executed
 * as soon as possible and the callback will be invoked when the update
 * completes successfully or is abandoned.
 *
 * "records" is specified as an array of arrays of the form:
 *
 *     [ bucket, key, value, [options] ]
 *
 * The per-record "options" object may contain an etag on which to predicate the
 * write.
 *
 * The separate "options" argument may contain "retryConflict", which may refer
 * to a function "merge":
 *
 *     merge(old, new)		on EtagConflict, fetch the current value,
 *     (function)		invoke "merge" to merge the result, and
 *     				retry predicated on the new etag
 *
 * If options.retryConflict is not specified, EtagConflict errors will not be
 * retried.
 *
 * The callback is invoked as callback(error, etags), where "etags" is an array
 * of the etags resulting from the update operations.
 *
 * Important note: it is illegal to issue concurrent updates for the same
 * record unless subsequent updates only change exactly one record, in which
 * case the update will be merged (if the first update hasn't been issued yet or
 * that update subsequently fails) or serialized (if it has been issued and that
 * request completes successfully).
 * XXX if first request completes, need to update etag of subsequent request to
 * use the result of the first one
 */
MorayBus.prototype.putBatch = function (records, options, callback)
{
	var bus = this;
	var txn = new MorayBusTransaction(records, options, callback);

	txn.tx_records.forEach(function (rec) {
		/* XXX should be allowed */
		mod_assert.ok(
		    !bus.mb_txns_byrecord.hasOwnProperty(rec['ident']),
		    'attempted concurrent writes on the same record');

		bus.mb_txns_byrecord[rec['ident']] = txn;
	});

	bus.mb_txns[txn.tx_ident] = txn;
	bus.mb_putq.push(txn.tx_ident);
	bus.flush();
};

MorayBus.prototype.flush = function (unow)
{
	var client, now, txn;

	if ((client = this.mb_client) === undefined) {
		this.mb_log.warn('flush: no client');
		return;
	}

	now = unow ? unow : mod_jsprim.iso8601(Date.now());
	this.mb_log.trace('flush');

	while (this.mb_npendingputs < this.mb_nmaxputs &&
	    this.mb_putq.length > 0) {
		txn = this.mb_txns[this.mb_putq.pop()];
		mod_assert.ok(txn !== undefined);
		this.txnPut(client, txn, now);
	}
};

MorayBus.prototype.txnPut = function (client, txn, now)
{
	var bus = this;
	var objects = txn.tx_records.slice(0);

	txn.tx_issued = now;
	this.mb_npendingputs++;
	client.batchPutObject(objects, {}, function (err, meta) {
		--bus.mb_npendingputs;
		txn.tx_issued = undefined;

		if (err)
			bus.txnHandleError(txn, err);
		else
			bus.txnFini(txn, null, meta);
	});
};

MorayBus.prototype.txnHandleError = function (txn, err)
{
	switch (err.name) {
	/*
	 * It would be preferable if node-moray told us whether this was a
	 * transient failure or not,  but for now we hardcode the known
	 * retryable errors.
	 */
	case 'ConnectionClosedError':	/* client-side */
	case 'ConnectionTimeoutError':
	case 'DNSError':
	case 'NoConnectionError':
	case 'UnsolicitedMessageError':
	case 'ConnectTimeoutError':	/* server-side */
	case 'NoDatabasePeersError':
	case 'QueryTimeoutError':
		this.txnReportTransientError(txn, err);
		this.txnRetry(txn);
		return;
	}

	if (err.name != 'EtagConflictError' || !txn.tx_retry_conflict) {
		this.txnFini(txn, err);
		return;
	}

	/*
	 * To deal with the EtagConflictError, we need to know which record
	 * conflicted.  MANTA-966 will include this information with the error,
	 * but in the meantime, we try to handle the cases where it's obvious
	 * (txn.tx_records.length == 1) here.  This code should be cleaned up
	 * once MANTA-966 is generally available.
	 */
	if ((!err.bucket || !err.key) && txn.tx_records.length > 1) {
		this.txnFini(txn, err);
		this.mb_log.error(err, 'got retryable EtagConflict error, ' +
		    'but server didn\'t specify the conflicting record');
		return;
	}

	var bus = this;
	var i, rec;

	if (err.bucket && err.key) {
		for (i = 0; i < txn.tx_records[i]; i++) {
			if (txn.tx_records[i]['bucket'] == err.bucket &&
			    txn.tx_records[i]['key'] == err.key)
				break;
		}

		if (i == txn.tx_records.length) {
			this.txnFini(txn, err);
			this.mb_log.error(err,
			    'got retryable EtagConflict error for ' +
			    'non-existent object');
			return;
		}

		rec = txn.tx_records[i];
	} else {
		rec = txn.tx_records[0];
	}

	this.mb_client.getObject(rec['bucket'], rec['key'], { 'noCache': true },
	    function (err2, record) {
		if (err2) {
			bus.txnFini(txn, new VError(err2,
			    'failed to fetch object after retryable ' +
			    'EtagConflict error'));
			return;
		}

		var newval = txn.tx_retry_conflict(record, rec);

		if (newval instanceof Error) {
			bus.txnFini(txn, new VError(err,
			    'merge failed after retryable EtagConflict error'));
			return;
		}

		rec['value'] = newval;
		rec['options']['etag'] = record['_etag'];
		bus.txnRetry(txn);
	    });
};

MorayBus.prototype.txnReportTransientError = function (txn, err)
{
	var throttle;

	if (!this.mb_reported.hasOwnProperty(err.name))
		this.mb_reported[err.name] = new Throttler(mReportInterval);
	throttle = this.mb_reported[err.name];

	if (throttle.tooRecent()) {
		this.mb_log.debug(err, 'batchPut transient failure (will ' +
		    'retry)', txn.tx_records);
	} else {
		this.mb_log.warn(err, 'batchPut transient failure ' +
		    '(will retry, messages throttled)', txn.tx_records);
	}
};

MorayBus.prototype.txnRetry = function (txn)
{
	var bus = this;

	/*
	 * We assume that transient failures may represent server-side capacity
	 * issues, so we backoff accordingly but retry indefinitely.
	 */
	txn.tx_nfails++;
	txn.tx_wait_start = Date.now();
	txn.tx_wait_delay = txn.tx_nfails > 30 ?
	    mMaxRetryTime :
	    Math.min(mMinRetryTime << (txn.tx_nfails - 1), mMaxRetryTime);
	txn.tx_wait_timer = setTimeout(function () {
		txn.tx_wait_timer = undefined;
		txn.tx_wait_start = undefined;
		txn.tx_wait_delay = undefined;

		bus.mb_log.debug('retrying txn %s', txn.tx_ident);
		bus.mb_putq.push(txn.tx_ident);
		bus.flush(Date.now());
	}, txn.tx_wait_delay);
};

MorayBus.prototype.txnFini = function (txn, err, meta)
{
	var bus = this;

	if (err)
		this.mb_log.error(err, 'batchPut failed', txn.tx_records);

	mod_assert.ok(this.mb_txns[txn.tx_ident] == txn);
	delete (this.mb_txns[txn.tx_ident]);

	txn.tx_records.forEach(function (rec) {
		mod_assert.ok(bus.mb_txns_byrecord[rec['ident']] == txn);
		delete (bus.mb_txns_byrecord[rec['ident']]);
	});

	if (txn.tx_callback !== undefined)
		txn.tx_callback(err, meta);
};

MorayBus.prototype.initBuckets = function (buckets, callback)
{
	mod_assert.equal(typeof (buckets), 'object');

	var bus = this;
	var next = function () { bus.initBucketsConnected(buckets, callback); };

	if (this.mb_client === undefined) {
		this.connect();
		this.mb_onconnect.push(next);
	} else {
		next();
	}
};

MorayBus.prototype.initBucketsConnected = function (buckets, callback)
{
	mod_assert.ok(this.mb_client !== undefined);
	mod_assert.ok(this.mb_init_buckets == undefined);

	var bus = this;
	var client = this.mb_client;
	var configs = {};
	var bucketargs = [];

	mod_jsprim.forEachKey(buckets, function (k, bkt) {
		configs[bkt] = mod_schema.sBktConfigs[k];
		bucketargs.push(bkt);
	});

	this.mb_init_buckets = mod_vasync.forEachParallel({
	    'inputs': bucketargs,
	    'func': function putBucket(bucket, subcallback) {
		bus.mb_log.info('putBucket "%s"', bucket);
		client.putBucket(bucket, configs[bucket], subcallback);
	    }
	}, function (err) {
		bus.mb_init_buckets = undefined;
		callback(err);
	});
};

/*
 * Kang (introspection) entry points.
 */
MorayBus.prototype.kangListTypes = function ()
{
	return (['subscriptions', 'transactions']);
};

MorayBus.prototype.kangListObjects = function (type)
{
	if (type == 'subscriptions')
		return (Object.keys(this.mb_subscriptions));

	return (Object.keys(this.mb_txns));
};

MorayBus.prototype.kangGetObject = function (type, id)
{
	if (type == 'subscriptions')
		return (this.mb_subscriptions[id].kangState());

	return (this.mb_txns[id].kangState());
};


function MorayBusSubscription(bucket, query, options, onrecord)
{
	var subscrip = this;

	mod_assert.equal(typeof (options), 'object');
	mod_assert.equal(typeof (options['timePoll']), 'number');
	mod_assert.equal(typeof (options['limit']), 'number');

	this.mbs_id = bucket + '-' + MorayBusSubscription.uniqueId++;
	this.mbs_bucket = bucket;
	this.mbs_query = query;
	this.mbs_limit = options['limit'];
	this.mbs_throttle = new mod_mautil.Throttler(options['timePoll']);
	this.mbs_barrier = mod_vasync.barrier();
	this.mbs_onrecord = function (record) {
		subscrip.mbs_nrecs++;
		subscrip.mbs_cur_nrecs++;

		onrecord(record, subscrip.mbs_barrier);
	};
	this.mbs_onsuccess = undefined;

	/*
	 * We maintain several additional pieces of state for kang-based
	 * debugging, including the start time of the current request, the start
	 * time of the last request that finished, the start time of the last
	 * request that finished with at least one result, and the number of
	 * records in the last non-empty batch of results.
	 */
	this.mbs_cur_start = undefined;
	this.mbs_cur_query = undefined;
	this.mbs_cur_nrecs = undefined;

	this.mbs_last_start = undefined;
	this.mbs_last_query = undefined;

	this.mbs_last_nonempty_start = undefined;
	this.mbs_last_nonempty_nrecs = undefined;

	this.mbs_nrecs = 0;
	this.mbs_nreqs = 0;
	this.mbs_nreqs_empty = 0;
}

MorayBusSubscription.uniqueId = 0;

MorayBusSubscription.prototype.kangState = function ()
{
	return ({
	    'bucket': this.mbs_bucket,
	    'lastQuery': this.mbs_last_query,
	    'lastStart': this.mbs_last_start,
	    'lastNonEmptyStart': this.mbs_last_nonempty_start,
	    'lastNonEmptyNRecords': this.mbs_last_nonempty_nrecs,
	    'running': this.mbs_cur_start !== undefined,
	    'nBarrierPending': Object.keys(this.mbs_barrier.pending).length,
	    'nRequests': this.mbs_nreqs,
	    'nRecords': this.mbs_nrecs,
	    'oneShot': this.mbs_onsuccess !== undefined
	});
};


function MorayBusTransaction(records, options, callback)
{
	var txn = this;

	mod_assert.ok(Array.isArray(records),
	    '"records" must be an array');
	mod_assert.ok(records.length > 0,
	    'at least one record is required');

	this.tx_ident = MorayBusTransaction.uniqueId++;
	this.tx_records = new Array(records.length);	/* records to write */
	this.tx_issued = undefined;			/* request start time */
	this.tx_callback = callback;			/* "done" callback */

	/* retry options */
	this.tx_retry_conflict = options ? options['retryConflict'] : undefined;
	this.tx_nfails = 0;				/* failed attempts */
	this.tx_wait_timer = undefined;			/* timeout for retry */
	this.tx_wait_start = undefined;			/* backoff start time */
	this.tx_wait_delay = undefined;			/* backoff delay */

	records.forEach(function (rec, i) {
		mod_assert.ok(Array.isArray(rec),
		    'each record for writing must be an array');
		mod_assert.ok(rec.length == 3 || rec.length == 4);
		mod_assert.equal(typeof (rec[0]), 'string',
		    'bucket name must be a string');
		mod_assert.equal(typeof (rec[1]), 'string',
		    'record key must be a string');

		txn.tx_records[i] = {
		    'bucket': rec[0],
		    'key': rec[1],
		    'value': rec[2],
		    'options': rec[3] || {},
		    'ident': rec[0] + '/' + rec[1]
		};
	});
}

MorayBusTransaction.uniqueId = 0;

MorayBusTransaction.prototype.kangState = function ()
{
	return ({
	    'records': this.tx_records.map(
	        function (rec) { return (rec['ident']); }),
	    'running': this.tx_issued !== undefined,
	    'nFails': this.tx_nfails,
	    'waitStart': this.tx_wait_start,
	    'waitDelay': this.tx_wait_delay
	});
};
