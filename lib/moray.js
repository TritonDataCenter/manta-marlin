/*
 * lib/moray.js: common Moray abstractions
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_util = require('util');

var mod_extsprintf = require('extsprintf');
var mod_mautil = require('./util');
var mod_moray = require('moray');

var sprintf = mod_extsprintf.sprintf;


/* Public interface. */
exports.MorayPoller = MorayPoller;


/*
 * Interface for polling periodically on a number of different Moray buckets.
 * This object is constructed with:
 *
 *	client		function returning a Moray client interface
 *
 *	log		bunyan logger
 *
 * Each bucket to poll is configured via addPoll with the following fields:
 *
 *	name		convenient programmatic name for the bucket
 *
 *	bucket		actual Moray bucket name
 *
 *	mkfilter	function returning a Moray filter for the object
 *
 *	interval	minimum time between poll requests (used as a throttle)
 *
 * The caller periodically calls poll(), which triggers a new findObjects
 * request for each bucket whose polling interval has elapsed since the last
 * request.  The following events are emitted:
 *
 *	on('record' function (record, haschanged) { ... })
 *
 *	    Emitted for each "new" record.  For buckets using a cache, a
 *	    "record" object is emitted either when a new object is discovered or
 *	    when an existing one's mtime or etag has changed.  For buckets not
 *	    using a cache, all records matching the filter are emitted.
 *	    "record" is the raw Moray record, and "haschanged" indicates whether
 *	    the etag has changed.
 *
 *	on('search-done', function (name, when) { ... })
 *
 *	    Emitted when a poll request completes for a bucket.  "name" is the
 *	    friendly name for the bucket's poll.  "when" is the time when the
 *	    poll began (in msec since epoch).  This event is useful for
 *	    applications that need to know when all objects existing at a given
 *	    time have been found.
 */
function MorayPoller(args)
{
	mod_assert.equal(typeof (args), 'object');
	mod_assert.equal(typeof (args['client']), 'function');
	mod_assert.equal(typeof (args['log']), 'object');
	mod_events.EventEmitter();

	this.mp_client = args['client'];
	this.mp_log = args['log'];
	this.mp_polls = {};
	this.mp_cache = {};
}

mod_util.inherits(MorayPoller, mod_events.EventEmitter);

MorayPoller.prototype.addPoll = function (conf)
{
	mod_assert.equal(typeof (conf), 'object');
	mod_assert.equal(typeof (conf['name']), 'string');
	mod_assert.equal(typeof (conf['bucket']), 'string');
	mod_assert.equal(typeof (conf['mkfilter']), 'function');
	mod_assert.equal(typeof (conf['interval']), 'number');

	var bucket = conf['bucket'];

	this.mp_polls[conf['name']] = {
	    'bucket': bucket,
	    'mkfilter': conf['mkfilter'],
	    'throttle': new mod_mautil.Throttler(conf['interval']),
	    'last_mtime': 0,
	    'last_id': 0
	};

	this.mp_cache[bucket] = {};
};

MorayPoller.prototype.poll = function ()
{
	var client = this.mp_client();

	if (!client) {
		this.mp_log.debug('poll skipped (no client)');
		return;
	}

	var now = Date.now();
	for (var name in this.mp_polls)
		this.pollBucket(client, name, now);
};

MorayPoller.prototype.pollBucket = function (client, name, now)
{
	var conf = this.mp_polls[name];
	var bucket = conf['bucket'];
	var cache = this.mp_cache[bucket];
	var throttle = conf['throttle'];
	var poller = this;
	var filter, req;

	if (throttle.tooRecent())
		return;

	filter = conf['mkfilter'](conf['last_mtime'], conf['last_id']);
	if (!filter)
		return;

	this.mp_log.debug('polling "%s"', name, filter);
	throttle.start();
	req = client.findObjects(bucket, filter, { 'noCache': true });

	req.on('error', function (err) {
		throttle.done();
		poller.mp_log.warn(err, 'poll "%s" failed', name);
		poller.emit('warn', err);
	});

	req.on('record', function (record) {
		if (record['_id'] > conf['last_id'])
			conf['last_id'] = record['_id'];
		if (record['_mtime'] > conf['last_mtime'])
			conf['last_mtime'] = record['_mtime'];

		poller.cacheUpdate(cache, record);
	});

	req.on('end', function () {
		throttle.done();
		poller.cacheFlush(cache, conf['last_mtime']);
		poller.emit('search-done', name, now);
	});
};

MorayPoller.prototype.cacheUpdate = function (cache, record)
{
	if (cache[record['key']] &&
	    cache[record['key']]['_mtime'] == record['_mtime'] &&
	    cache[record['key']]['_etag'] == record['_etag'])
		return;

	cache[record['key']] = {
	    '_etag': record['_etag'],
	    '_mtime': record['_mtime']
	};

	this.emit('record', record);
};

/*
 * Remove entries from the given cache with mtimes strictly older than "time".
 * This operation is linear in the number of entries in the cache, but in
 * practice that's bounded above by the number of entries returned by the most
 * recent poll.
 */
MorayPoller.prototype.cacheFlush = function (cache, time)
{
	for (var key in cache) {
		if (cache[key]['_mtime'] < time)
			delete (cache[key]);
	}
};

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
