/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * lib/bus.js: Marlin service's interface to the outside world
 */

var mod_assert = require('assert');
var mod_assertplus = require('assert-plus');
var mod_events = require('events');
var mod_url = require('url');
var mod_util = require('util');

var mod_jsprim = require('jsprim');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_moray = require('moray');

var mod_mamoray = require('./moray');
var mod_schema = require('./schema');
var mod_mautil = require('./util');

var EventEmitter = mod_events.EventEmitter;
var Throttler = mod_mautil.Throttler;
var VError = mod_verror.VError;

var mReportInterval = 5000;	/* min time between transient error log msgs */
var mMinRetryTime = 100;	/* min time between retries */
var mMaxRetryTime = 10000;	/* max time between retries */
var mMaxConflictRetries = 5;	/* max "etag conflict" retries */

/* Public interface */
exports.createBus = createBus;
exports.mergeRecords = mergeRecords;

/*
 * The hardcoding of errors here is deeply regrettable.  See txnHandleError()
 * for details.  The only error messages that should generally be included here
 * are errors that (a) have no programmatic error code that we can use instead
 * of the message to identify them, (b) correspond to transient errors (i.e.,
 * errors for which the client could reasonably retry and expect success), and
 * (c) indicate that the request in question *definitely* did not succeed.
 * The goal is that Moray itself should allow us to identify cases (a) and (b)
 * programmatically without resorting to looking at each error's message, but
 * this table exists because that's not always the case.
 *
 * The third constraint is the trickiest.  Since some of the operations may not
 * be idempotent (e.g., PUT with a specific etag), it's better to avoid assuming
 * failure when we actually don't know whether the request succeeded or failed.
 */
var mGenericTransientErrors = {
	/* Node errors (possibly remote), usually having name "Error" */
	'connect ECONNREFUSED': true,
	'connect ECONNRESET': true,

	/*
	 * See above.  This is one of those cases where we don't actually know
	 * that the request didn't complete successfully.  However, we see this
	 * case often enough in production that it's worth attempting to handle.
	 * In the worst case, we'll retry a non-idempotent operation that
	 * already succeeded, which should generally fail with an EtagConflict
	 * error that (if we don't have conflict resolution logic already) may
	 * cause Marlin to erroneously abandon the task.  If this happens, we'll
	 * be able to diagnose it from the logs and have a better sense of how
	 * to deal with that failure mode.
	 */
	'read ECONNRESET': true,

	/* Moray errors. */
	'no active connections': true,

	/* Postgres errors, usually having name "error" */
	'cannot execute SELECT FOR UPDATE in a read-only transaction': true,
	'cannot execute UPDATE in a read-only transaction': true,
	'deadlock detected': true,
	/* JSSTYLED */
	'remaining connection slots are reserved for non-replication superuser connections': true,
	'sorry, too many clients already': true,
	'the database system is starting up': true
};

function createBus(conf, options)
{
	mod_assert.equal(typeof (conf), 'object');
	mod_assert.ok(conf.hasOwnProperty('moray'));

	mod_assert.equal(typeof (options), 'object');
	mod_assert.equal(typeof (options['log']), 'object');

	var morayconf, dnsconf, tunconf;
	dnsconf = conf['dns'];
	morayconf = conf['moray'];
	if (morayconf['storage'])
		morayconf = morayconf['storage'];
	tunconf = conf['tunables'];

	return (new MorayBus(morayconf, dnsconf, tunconf, options));
}

function MorayBus(morayconf, dnsconf, tunconf, options)
{
	var url;

	url = mod_url.parse(morayconf['url']);
	this.mb_host = url['hostname'];
	this.mb_port = parseInt(url['port'], 10);
	this.mb_reconnect = morayconf['reconnect'];
	this.mb_dns = dnsconf ?
	    { 'resolvers': dnsconf['nameservers'].slice(0) } : undefined;

	this.mb_log = options['log'];
	this.mb_client = undefined;	/* current Moray client */
	this.mb_connecting = false;	/* currently connecting */
	this.mb_reported = {};		/* last report time, by error name */
	this.mb_onconnect = [];

	/* read-side */
	this.mb_subscriptions = {};	/* current subscriptions */

	/* write-side */
	this.mb_nmaxputs = tunconf['maxPendingPuts'];
	this.mb_npendingputs = 0;	/* currently pending PUTs */
	this.mb_txns = {};		/* all pending requests */
	this.mb_putq = [];		/* outgoing queue of requests */
	this.mb_txns_byrecord = {};	/* maps records to pending txn */

	/* other operations */
	this.mb_init_buckets = undefined;
	this.mb_init_reindex = undefined;
	this.mb_reindex = undefined;
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
 * "options" may also contain:
 *
 *    dbgDelayStart	If non-null, a delay will be inserted of "dbgDelayStart"
 *    			milliseconds between when each request is scheduled to
 *    			be made and when it's actually made.  Internally, this
 *    			behaves as though the request were sent at the scheduled
 *    			time (e.g., a fence() during this period would make
 *    			another request).
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
	this.fence(id, function () {
		delete (worker.mb_subscriptions[id]);

		if (ondone !== undefined)
			ondone();
	});
};

/*
 * Remove the given subscription.  "onrecord" and "ondone" for pending
 * operations may still be invoked.
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

/*
 * Invoke "callback" when all records that currently match the query have been
 * returned.  This is similar to convertOneshot(), except without actually
 * removing the subscription.
 */
MorayBus.prototype.fence = function (id, callback)
{
	var subscrip = this.mb_subscriptions[id];
	subscrip.mbs_onsuccesses.push(callback);
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
	    'log': this.mb_log.child({ 'component': 'moray' }),
	    'retry': this.mb_reconnect,
	    'dns': this.mb_dns,
	    'unwrapErrors': true
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
		bus.emit('ready');

		var wakeup = bus.mb_onconnect;
		bus.mb_onconnect = [];
		wakeup.forEach(function (callback) { callback(); });
		bus.flush();
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

	if (subscrip.mbs_delaying_start !== null)
		return;

	/*
	 * It's a little subtle, but the fact that we save mbs_onsuccesses here
	 * at the beginning of the poll is critical to satisfy the
	 * fence() contract that "callback" is invoked after the next successful
	 * request that starts *after* fence() is invoked.  If we used a closure
	 * here that resolved subscrip.mbs_onsuccesses only after the poll
	 * completed, this would do the wrong thing (and the result would be a
	 * very subtle race that might rarely be hit).
	 */
	var bus = this;
	var query = subscrip.mbs_query(nowdate.getTime());
	var onsuccesses = subscrip.mbs_onsuccesses;

	subscrip.mbs_onsuccesses = [];
	subscrip.mbs_nreqs++;
	subscrip.mbs_cur_start = nowdate;
	subscrip.mbs_cur_query = query;
	subscrip.mbs_cur_nrecs = 0;

	var doPoll = function () {
		mod_mamoray.poll({
		    'client': bus.mb_client,
		    'options': {
			'limit': subscrip.mbs_limit,
			'noCache': true
		    },
		    'now': nowdate.getTime(),
		    'log': bus.mb_log,
		    'throttle': subscrip.mbs_throttle,
		    'bucket': subscrip.mbs_bucket,
		    'filter': query,
		    'onrecord': subscrip.mbs_onrecord,
		    'onerr': function () {
			subscrip.mbs_onsuccesses = subscrip.mbs_onsuccesses.
			    concat(onsuccesses);
		    },
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

			var nrecs = subscrip.mbs_cur_nrecs;

			subscrip.mbs_cur_start = undefined;
			subscrip.mbs_cur_nrecs = undefined;
			subscrip.mbs_cur_query = undefined;

			if (subscrip.mbs_limit > 1 &&
			    nrecs >= subscrip.mbs_limit) {
				bus.mb_log.warn('moray query overflow (%s)',
				    subscrip.mbs_id);
				subscrip.mbs_onsuccesses =
				    subscrip.mbs_onsuccesses.concat(
				    onsuccesses);
				return;
			}

			onsuccesses.forEach(
			    function (callback) { callback(); });
		    }
		});
	};

	if (subscrip.mbs_delay_start !== null) {
		mod_assert.ok(subscrip.mbs_delaying_start === null);
		subscrip.mbs_delaying_start = Date.now();
		bus.mb_log.info('dbgDelayStart query delay', query);
		setTimeout(function () {
			bus.mb_log.info('dbgDelayStart query run', query);
			mod_assert.ok(subscrip.mbs_delaying_start !== null);
			subscrip.mbs_delaying_start = null;
			doPoll();
		}, subscrip.mbs_delay_start);
	} else {
		doPoll();
	}
};

/*
 * Enqueue an atomic "put" or "update" for the given records.  In both cases the
 * update will be executed as soon as possible and the callback will be invoked
 * when the update completes successfully or is abandoned.
 *
 * "records" is specified as an array of arrays of one of these forms:
 *
 *     [ 'put', bucket, key, value, [options] ]
 *     [ 'update', bucket, filter, fields, [options] ]
 *
 * For 'put', the per-record "options" object may contain an etag on which to
 * predicate the write.
 *
 * The separate "options" argument (as opposed to the per-record one) may
 * contain any of the following properties:
 *
 *    dbgDelayStart		If specified, the outgoing request will be
 *    				delayed by the specified number of milliseconds
 *    				using setTimeout.  Internally, the request is
 *    				processed as normal, but the outgoing message is
 *    				not issued until the delay has elapsed.  This is
 *    				intended for testing only to help blow open race
 *    				condition windows.
 *
 *    dbgDelayDone		If specified, response processing will be
 *    				delayed by the specified number of milliseconds
 *    				using setTimeout.  Internally, the request is
 *    				processed as normal until the response is
 *    				received, at which point all response processing
 *    				is delayed as specified.  This is intended for
 *    				testing only to blow open race condition
 *    				windows.
 *
 *    retryConflict		If specified, and if an EtagConflict error is
 *    				encountered in processing this request, the
 *    				current value of the conflicting record will be
 *    				fetched and retryConflict will be invoked as
 *    				retryConflict(old, new) to merge the result.
 *    				The request will then be retried conditional on
 *    				the new etag.
 *
 *    				If an EtagConflict error is encountered and
 *    				retryConflict is not specified, the error will
 *    				not be retried.
 *
 *    urgent			If true, the request will be moved to the head
 *    				of the pending queue instead of the back.  This
 *    				should only be used for a very small number of
 *    				time-critical writes (like heartbeats).
 *
 * The callback is invoked as callback(error, etags), where "etags" is an array
 * of the etags resulting from the update operations.
 *
 * Important note: concurrent writes to the same record will be serialized (and
 * will therefore fail if conditional and retryConflict is not set).  It is
 * illegal for any write to be dependent on more than one previous write.  For
 * examples:
 *
 *    Okay:	write R1, write R1
 *    Okay:	write R1, conditional write R1
 *    Okay:	conditional write R1 + write R2, conditional write R1
 *    Okay:	conditional write R1, conditional write R1 + write R2
 *    NOT okay:	write R1, write R2, write R1 + write R2
 *    		(second operation has two dependencies)
 *    NOT okay: write R1, conditional write R1 without retryConflict
 *    		(this is illegal because the second write would always result in
 *    		an etag conflict error with the first, and such errors are not
 *    		handled)
 *
 * This is obviously not general-purpose, but solving this in general requires
 * policy input from the consumer, and this approach satisfies the constraints
 * of today's consumers.
 */
MorayBus.prototype.batch = function (records, options, callback)
{
	var bus = this;
	var txn = new MorayBusTransaction(records, options, callback);
	var deptxn;

	txn.tx_records.forEach(function (rec) {
		if (rec['ident'] === undefined)
			return;

		if (!bus.mb_txns_byrecord.hasOwnProperty(rec['ident']))
			return;

		if (deptxn !== undefined) {
			throw (new VError('attempted putBatch with multiple ' +
			     'dependencies: %j', records));
		}

		if (rec['options']['etag'] !== undefined &&
		    txn.tx_issued === undefined &&
		    txn.tx_retry_conflict === undefined) {
			throw (new VError('attempted conditional putBatch ' +
			    'with concurrent write without retryConflict ' +
			    '(would necessarily result in error)'));
		}

		deptxn = bus.mb_txns_byrecord[rec['ident']];
	});

	/*
	 * We always link this transaction into mb_txns, even if there's a
	 * dependent transaction.  We also link this transaction into
	 * mb_txns_byrecord even if there's a dependent record, because
	 * mb_txns_byrecord[ident] represents the last txn in the dependency
	 * chain for record "ident".
	 */
	this.mb_txns[txn.tx_ident] = txn;
	txn.tx_records.forEach(function (rec) {
		if (rec['ident'] === undefined)
			return;

		bus.mb_txns_byrecord[rec['ident']] = txn;
	});

	if (deptxn !== undefined) {
		this.mb_log.debug('batch: enter dep', txn,
		    'blocked on dependent',
		    mod_util.inspect(deptxn, false, 5));
		deptxn.tx_dependents.push(txn);
	} else {
		this.mb_log.debug('batch: enter immediate',
		    options && options['urgent'] ? 'urgent' : 'normal',
		    mod_util.inspect(txn, false, 5));
		if (options && options['urgent'])
			this.mb_putq.unshift(txn.tx_ident);
		else
			this.mb_putq.push(txn.tx_ident);
		this.flush();
	}
};

MorayBus.prototype.putBatch = function (records, options, callback)
{
	var objects = records.map(
	    function (rec) { return ([ 'put' ].concat(rec)); });
	return (this.batch(objects, options, callback));
};

MorayBus.prototype.flush = function (unow)
{
	var client, now, txn;

	if ((client = this.mb_client) === undefined) {
		this.connect();
		return;
	}

	now = unow ? unow : mod_jsprim.iso8601(Date.now());
	this.mb_log.trace('flush');

	while (this.mb_putq.length > 0 &&
	    (this.mb_npendingputs < this.mb_nmaxputs ||
	    this.mb_putq[0].tx_urgent)) {
		txn = this.mb_txns[this.mb_putq.shift()];
		mod_assert.ok(txn !== undefined);
		this.txnPut(client, txn, now);
	}
};

MorayBus.prototype.txnPut = function (client, txn, now)
{
	var bus = this;
	var objects = txn.tx_records.slice(0);
	var doput = function () {
		if (txn.tx_delay_start !== null) {
			mod_assert.ok(txn.tx_delaying_start);
			txn.tx_delaying_start = null;
		}

		client.batch(objects, {}, function (err, meta) {
			if (txn.tx_delay_done !== null) {
				txn.tx_delaying_done = Date.now();
				setTimeout(done, txn.tx_delay_done, err, meta);
			} else {
				done(err, meta);
			}
		});
	};
	var done = function (err, meta) {
		if (txn.tx_delay_done !== null) {
			mod_assert.ok(txn.tx_delaying_done);
			txn.tx_delaying_done = false;
		}

		--bus.mb_npendingputs;
		txn.tx_issued = undefined;

		if (err)
			bus.txnHandleError(txn, err);
		else
			bus.txnFini(txn, null, meta);
	};

	txn.tx_issued = now;
	this.mb_npendingputs++;
	mod_assert.ok(txn.tx_delaying_start === null);
	if (txn.tx_delay_start !== null) {
		txn.tx_delaying_start = Date.now();
		setTimeout(doput, txn.tx_delay_start);
	} else {
		doput();
	}
};

MorayBus.prototype.txnHandleError = function (txn, err)
{
	var err_transient = false;

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
	case 'PoolShuttingDownError':
	case 'QueryTimeoutError':
		err_transient = true;
		break;

	/*
	 * This is even more regrettable because these don't even have proper
	 * error codes.  See MORAY-182 and the comment near the top of this
	 * file.
	 */
	case 'error':
	case 'Error':
		err_transient = mGenericTransientErrors.hasOwnProperty(
		    err.message);
		break;
	}

	if (err_transient) {
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
	 * conflicted.  This should be on "err.context", but if not, we deduce
	 * what it must have been.
	 */
	if (!err.context || !err.context.bucket || !err.context.key) {
		this.mb_log.warn('got EtagConflict without context details',
		    err);

		var conditional = txn.tx_records.filter(function (record) {
			return (record['operation'] == 'put' &&
			    record['options']['etag'] !== undefined);
		});

		if (conditional.length != 1) {
			this.txnFini(txn, err);
			this.mb_log.error(err, 'got retryable EtagConflict ' +
			    'error, but server didn\'t specify the ' +
			    'conflicting record');
			return;
		}

		if (!err.context)
			err.context = {};
		err.context.bucket = conditional[0]['bucket'];
		err.context.key = conditional[0]['key'];
	}

	var bus = this;
	var i, rec;

	for (i = 0; i < txn.tx_records.length; i++) {
		if (txn.tx_records[i]['bucket'] == err.context.bucket &&
		    txn.tx_records[i]['key'] == err.context.key)
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

	this.mb_client.getObject(rec['bucket'], rec['key'], { 'noCache': true },
	    function (err2, record) {
		if (err2) {
			bus.mb_log.warn(err2, 'error fetching object after ' +
			    'retryable EtagConflict error');
			bus.txnHandleError(txn, err2);
			return;
		}

		var newval = txn.tx_retry_conflict(record, rec);

		if (newval instanceof Error) {
			bus.mb_log.warn('merge failed (old, new)', record, rec);
			bus.txnFini(txn, new VError(newval,
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
		this.mb_log.warn(err, 'batchPut failed', txn.tx_records);
	else
		this.mb_log.debug('batchPut: done',
		    mod_util.inspect(txn, false, 5), meta);

	mod_assert.ok(this.mb_txns[txn.tx_ident] == txn);
	delete (this.mb_txns[txn.tx_ident]);

	txn.tx_records.forEach(function (rec) {
		if (rec['ident'] === undefined)
			return;

		if (bus.mb_txns_byrecord[rec['ident']] != txn) {
			mod_assert.ok(txn.tx_dependents.length > 0);
			return;
		}

		delete (bus.mb_txns_byrecord[rec['ident']]);
	});

	if (txn.tx_callback !== undefined)
		txn.tx_callback(err, meta);

	txn.tx_dependents.forEach(function (dependenttxn) {
		bus.mb_log.debug('enqueueing dependent txn', dependenttxn);
		bus.mb_putq.push(dependenttxn.tx_ident);
	});

	bus.flush();
};

/*
 * initBuckets() is called by the consumer (in this case, the only consumer is
 * the jobsupervisor) to update Moray with the latest version of the bucket
 * schema.  This does not kick off any reindexing operations.  The caller should
 * do that separately by calling initReindex().
 */
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
		if (configs[bkt]['nocreate'] === undefined ||
		    configs[bkt]['nocreate'] === false) {
			bucketargs.push(bkt);
		}
	});

	this.mb_init_buckets = mod_vasync.forEachParallel({
	    'inputs': bucketargs,
	    'func': function putBucket(bucket, subcallback) {
		bus.mb_log.info('putBucket "%s"', bucket);
		client.putBucket(bucket, configs[bucket], function (err) {
			if (err && err['name'] == 'BucketVersionError') {
				bus.mb_log.warn(err,
				    'bucket schema out of date');
				err = null;
			}

			subcallback(err);
		});
	    }
	}, function (err) {
		bus.mb_init_buckets = undefined;
		callback(err);
	});
};

/*
 * initReindex() is called by the consumer (in this case, the jobsupervisor) to
 * kick off any reindexing operations that are required for any of the buckets
 * that we would have updated during initBuckets().
 *
 * For background: Moray keeps indexes for fields specified in the bucket
 * schema.  Our caller normally has already called initBuckets(), which may have
 * updated Moray with a new schema that specifies new fields (and therefore new
 * indexes to be created).  But Moray does not proactively create these indexes.
 * We have to do that by calling reindexObjects() until no more objects are
 * found.  Until that completes, Moray will not use the field's index.
 *
 * This operation can in principle take a long time, depending on the number of
 * rows in each bucket.  In a sense, the caller probably doesn't need to care
 * when this operation completes, since Moray serves queries using the new
 * fields without the index.  However, that path is potentially very expensive
 * and can also impact correctness when limits are used (i.e., always).  As a
 * result, the caller is advised to avoid making queries using the new fields
 * until this operation has completed.  To avoid having to keep track of which
 * fields are new, the caller just blocks startup on this completing.
 */
MorayBus.prototype.initReindex = function (buckets, callback)
{
	var bus = this;

	mod_assert.ok(this.mb_client !== undefined);
	mod_assert.ok(this.mb_init_reindex == undefined);
	mod_assert.ok(this.mb_reindex == undefined);

	this.mb_reindex = {};

	mod_jsprim.forEachKey(buckets, function (k, bkt) {
		var cfg = mod_schema.sBktConfigs[k];
		if (cfg['nocreate'] === undefined ||
		    cfg['nocreate'] === false) {
			bus.mb_reindex[bkt] = {
			    'bucket': bkt,
			    'records_per_rq': 500,

			    'nrqs': 0,
			    'nerrs': 0,
			    'nupdated': 0,
			    'pending': false,
			    'last_error': null,
			    'last_count': 0,

			    'last_start_time': null,
			    'last_done_time': null,
			    'last_done_ok_time': null,
			    'last_error_time': null
			};
		}
	});

	this.mb_init_reindex = mod_vasync.forEachParallel({
	    'inputs': Object.keys(this.mb_reindex),
	    'func': this.reindexBucket.bind(this)
	}, function (err) {
		/* reindexBucket() continues until it succeeds. */
		mod_assert.ok(!err);
		callback();
	});
};

MorayBus.prototype.reindexBucket = function (bucket, callback)
{
	var bus, client, info;

	mod_assert.ok(this.mb_client !== undefined);

	bus = this;
	client = this.mb_client;
	info = this.mb_reindex[bucket];
	mod_assertplus.string(info.bucket);
	mod_assertplus.equal(bucket, info.bucket);
	mod_assertplus.bool(info.pending);
	mod_assertplus.ok(!info.pending);

	info.nrqs++;
	info.pending = true;
	info.last_start_time = new Date();
	bus.mb_log.info(info, 'reindex suboperation: start');
	client.reindexObjects(bucket, info.records_per_rq,
	    function onReindexResponse(err, res) {
		info.last_done_time = new Date();
		info.pending = false;

		if (!err && (res === null || res === undefined ||
		    typeof (res.processed) != 'number')) {
			err = new VError('moray server unexpectedly ' +
			    'returned bad value: "%s"',
			    mod_util.inspect(res));
		}

		if (err) {
			bus.mb_log.warn(err, info,
			    'reindex suboperation: error');
			info.last_error = err;
			info.last_error_time = info.last_done_time;
			info.nerrs++;

			setTimeout(function retryReindex() {
				bus.reindexBucket(bucket, callback);
			}, 3000);
		} else {
			bus.mb_log.info(res, 'reindex suboperation: done');
			info.last_done_ok_time = info.last_done_time;
			info.last_count = res.processed;
			info.nupdated += res.processed;

			if (res.processed === 0) {
				bus.mb_log.info(info, 'reindex: all done');
				callback();
			} else {
				bus.reindexBucket(bucket, callback);
			}
		}
	    });
};

/*
 * Kang (introspection) entry points.
 */
MorayBus.prototype.kangListTypes = function ()
{
	return (['subscription', 'transaction']);
};

MorayBus.prototype.kangSchema = function (type)
{
	if (type == 'subscription') {
		return ({
		    'summaryFields': [
			'bucket',
			'lastNonEmptyStart',
			'lastNonEmptyNRecords',
			'running',
			'nRecords',
			'nBarrierPending',
			'lastQuery'
		    ]
		});
	}

	return ({
	    'summaryFields': [
		'running',
		'nFails',
		'waitStart',
		'waitDelay'
	    ]
	});
};

MorayBus.prototype.kangListObjects = function (type)
{
	if (type == 'subscription')
		return (Object.keys(this.mb_subscriptions));

	return (Object.keys(this.mb_txns));
};

MorayBus.prototype.kangGetObject = function (type, id)
{
	if (type == 'subscription')
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

		onrecord(record, subscrip.mbs_barrier, subscrip.mbs_id);
	};
	this.mbs_onsuccesses = [];
	this.mbs_delay_start = null;
	this.mbs_delaying_start = null;
	if (options.hasOwnProperty('dbgDelayStart')) {
		mod_assert.equal(typeof (options['dbgDelayStart']), 'number');
		this.mbs_delay_start = options['dbgDelayStart'];
	}

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
	    'nRecords': this.mbs_nrecs
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
	this.tx_dependents = [];			/* dependent txns */

	if (options && options.hasOwnProperty('dbgDelayStart')) {
		mod_assert.equal(typeof (options['dbgDelayStart']), 'number');
		this.tx_delay_start = options['dbgDelayStart'];
		this.tx_delaying_start = null;
	} else {
		this.tx_delay_start = null;
		this.tx_delaying_start = null;
	}

	if (options && options.hasOwnProperty('dbgDelayDone')) {
		mod_assert.equal(typeof (options['dbgDelayDone']), 'number');
		this.tx_delay_done = options['dbgDelayDone'];
		this.tx_delaying_done = null;
	} else {
		this.tx_delay_done = null;
		this.tx_delaying_done = null;
	}

	/* retry options */
	this.tx_retry_conflict = options ? options['retryConflict'] : undefined;
	this.tx_urgent = options && options['urgent'];
	this.tx_nfails = 0;				/* failed attempts */
	this.tx_wait_timer = undefined;			/* timeout for retry */
	this.tx_wait_start = undefined;			/* backoff start time */
	this.tx_wait_delay = undefined;			/* backoff delay */

	records.forEach(function (rec, i) {
		mod_assert.ok(Array.isArray(rec),
		    'each record for writing must be an array');

		if (rec[0] == 'put') {
			mod_assert.ok(rec.length == 4 || rec.length == 5);
			mod_assert.equal(typeof (rec[1]), 'string',
			    'bucket name must be a string');
			mod_assert.equal(typeof (rec[2]), 'string',
			    'record key must be a string');

			txn.tx_records[i] = {
			    'operation': 'put',
			    'bucket': rec[1],
			    'key': rec[2],
			    'value': mod_jsprim.deepCopy(rec[3]),
			    'options': rec[4] || {},
			    'ident': rec[1] + '/' + rec[2]
			};
		} else {
			mod_assert.ok(rec.length == 4 || rec.length == 5);
			mod_assert.equal(rec[0], 'update',
			    'only "put" or "update" are supported in batch');
			mod_assert.equal(typeof (rec[1]), 'string',
			    'bucket name must be a string');
			mod_assert.equal(typeof (rec[2]), 'string',
			    'record filter must be a string');
			mod_assert.equal(typeof (rec[3]), 'object',
			    'record itself must be an object');
			if (rec[0] == 'update')
				mod_assert.ok(rec.length == 5 &&
				    typeof (rec[4]) == 'object' &&
				    rec[4].hasOwnProperty('limit'),
				    '"limit" must be specified for updates');
			txn.tx_records[i] = {
			    'operation': 'update',
			    'bucket': rec[1],
			    'filter': rec[2],
			    'fields': rec[3],
			    'options': rec[4] || {}
			};
		}
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

/*
 * Implements a simple algorithm to merge two Moray records in terms of:
 *
 *     extfields	List of top-level fields that may legally be changed by
 *     			another component.  These fields may differ between
 *     			"oldval" and "newval" and will be copied from the "old"
 *     			value into the new one.
 *
 *     changedfields	List of top-level fields that may have been changed
 *     			locally.  These fields may differ between "oldval" and
 *     			"newval" and the "newval" ones will be unchanged.
 *
 *     oldval		"old" value, which is the current value in Moray.  This
 *     			is the value in Moray that conflicted with an attempt to
 *     			PUT "newval".
 *
 *     newval		"new" value, which is the value we attempted to PUT to
 *     			Moray.
 *
 * This function is intended to be invoked from the "retryConflict" function
 * supported by putBatch above.  This function is invoked when we attempt to
 * write an object to Moray predicated on a particular etag, but that etag
 * conflicts.  putBatch will fetch the current record and invoke "retryConflict"
 * to merge it.  This function merges the records by copying fields that are
 * explicitly allowed to be changed externally, ignoring differences in fields
 * that we change locally, and failing if there are any other differences (to
 * avoid silently corrupting data by dropping or blindly merging remote
 * changes).  A field may be in both "extfields" and "changedfields", in which
 * case the merge will fail unless the field is undefined remotely.
 */
function mergeRecords(extfields, changedfields, oldval, newval)
{
	for (var k in oldval) {
		if (extfields.indexOf(k) != -1 ||
		    changedfields.indexOf(k) != -1)
			continue;

		if (!mod_jsprim.deepEqual(oldval[k], newval[k]))
			return (new VError('field "%s" cannot be merged', k));
	}

	/*
	 * Marlin components never delete fields, but we check just to be sure.
	 */
	for (k in newval) {
		if (extfields.indexOf(k) != -1 ||
		    changedfields.indexOf(k) != -1)
			continue;

		if (!oldval.hasOwnProperty(k))
			return (new VError('field "%s" cannot be merged', k));
	}

	var common = extfields.filter(function (l) {
		return (changedfields.indexOf(l) != -1);
	});

	/*
	 * This check actually does assume that Marlin never deletes fields, but
	 * it's necessary for the desired behavior with respect to
	 * job.timeInputDone.
	 */
	if (common.length > 0 &&
	    oldval[common[0]] !== undefined &&
	    newval[common[0]] !== undefined &&
	    oldval[common[0]] != newval[common[0]])
		return (new VError('field "%s" cannot be merged', common[0]));

	extfields.forEach(function (f) {
		if (common.indexOf(f) == -1 || newval[f] === undefined)
			newval[f] = oldval[f];
	});

	return (newval);
}
