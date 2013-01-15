/*
 * lib/bus.js: Marlin service's interface to the outside world
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_url = require('url');
var mod_util = require('util');

var mod_vasync = require('vasync');

var mod_moray = require('moray');

var mod_mamoray = require('../moray');
var mod_mautil = require('../util');

var EventEmitter = mod_events.EventEmitter;

/* Public interface */
exports.createBus = createBus;

function createBus(conf, options)
{
	mod_assert.equal(typeof (conf), 'object');
	mod_assert.ok(conf.hasOwnProperty('moray'));

	mod_assert.equal(typeof (options), 'object');
	mod_assert.equal(typeof (options['log']), 'object');

	return (new MorayBus(conf['moray'], options));
}

function MorayBus(conf, options)
{
	var url;

	url = mod_url.parse(conf['url']);
	this.mb_host = url['hostname'];
	this.mb_port = parseInt(url['port'], 10);
	this.mb_reconnect = conf['reconnect'];

	this.mb_log = options['log'];
	this.mb_dns = options['dns'];
	this.mb_client = undefined;
	this.mb_connecting = false;
	this.mb_subscriptions = {};
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
	var host, client;

	if (this.mb_client !== undefined || this.mb_connecting)
		return;

	if (this.mb_dns) {
		host = this.mb_dns.lookupv4(this.mb_host);

		if (!host) {
			this.mb_log.warn('no IP available for "%s"',
			    this.mb_host);
			return;
		}
	} else {
		host = this.mb_host;
	}

	this.mb_connecting = true;

	client = mod_moray.createClient({
	    'host': host,
	    'port': this.mb_port,
	    'log': this.mb_log.child({ 'component': 'MorayClient' }),
	    'reconnect': false,
	    'retry': this.ma_conf['moray']['reconnect']
	});

	client.on('error', function (err) {
		bus.mb_connecting = false;
		bus.mb_log.error(err, 'moray client error');
	});

	client.on('close', function () {
		bus.mb_log.error('moray client closed');

		if (bus.mb_client == client) {
			bus.mb_client = undefined;
			bus.connect();
		}
	});

	client.on('connect', function () {
		mod_assert.ok(bus.mb_client === undefined ||
		    bus.mb_client == client);
		bus.mb_client = client;
		bus.mb_connecting = false;
	});
};

MorayBus.prototype.poll = function (now)
{
	mod_assert.equal(typeof (now), 'number');

	if (this.mb_client === undefined) {
		this.mb_log.debug('skipping poll (still connecting)');
		this.connect();
		return;
	}

	for (var id in this.mb_subscriptions)
		this.pollOne(this.mb_subscriptions[id], now);
};

MorayBus.prototype.pollOne = function (subscrip, now)
{
	if (subscrip.mbs_barrier.pending > 0)
		return;

	if (subscrip.mbs_throttle.tooRecent())
		return;

	/*
	 * It's a little subtle, but the fact that we pass subscrip.mbs_ondone
	 * here at the beginning of the poll is critical to satisfy the
	 * convertOneshot contract that "ondone" is invoked after the next
	 * successful request that starts *after* convertOneshot() is invoked.
	 * If we used a closure here that resolved subscrip.mbs_ondone only
	 * after the poll completed, this would do the wrong thing (and the
	 * result would be a very subtle race that might rarely be hit).
	 */
	mod_mamoray.poll({
	    'client': this.mb_client,
	    'options': {
		'limit': subscrip.mbs_limit,
		'noCache': true
	    },
	    'now': now,
	    'log': this.mb_log,
	    'throttle': subscrip.mbs_throttle,
	    'bucket': subscrip.mbs_bucket,
	    'filter': subscrip.mbs_query(now),
	    'onrecord': subscrip.mbs_onrecord,
	    'ondone': subscrip.mbs_ondone
	});
};

function MorayBusSubscription(bucket, query, options, onrecord)
{
	var subscrip = this;

	mod_assert.equal(typeof (options), 'object');
	mod_assert.equal(typeof (options['timePoll']), 'number');
	mod_assert.equal(typeof (options['limit']), 'number');

	this.mbs_id = bucket + '-' + query + '-' +
	    MorayBusSubscription.uniqueId++;
	this.mbs_bucket = bucket;
	this.mbs_query = query;
	this.mbs_limit = options['limit'];
	this.mbs_throttle = new mod_mautil.Throttler(options['timePoll']);
	this.mbs_barrier = mod_vasync.barrier();
	this.mbs_onrecord = function (record) {
		onrecord(record, subscrip.mbs_barrier);
	};
	this.mbs_onsuccess = undefined;
}

MorayBusSubscription.uniqueId = 0;
