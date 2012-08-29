/*
 * lib/moray.js: common Moray abstractions
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_util = require('util');

var mod_mautil = require('./util');
var mod_moray = require('moray');


/* Public interface. */
exports.MorayEtagCache = MorayEtagCache;
exports.MorayPoller = MorayPoller;


/*
 * This cache receives records (as via a Moray client "findObjects" request),
 * keeps track of the last-seen version of each object, and emits:
 *
 *	.emit('record', record, haschanged)
 *
 * only for those objects with new mtimes or etags.  "haschanged" indicates
 * whether the record's etag has changed.
 */
function MorayEtagCache()
{
	this.c_objects = {};
	mod_events.EventEmitter();
}

mod_util.inherits(MorayEtagCache, mod_events.EventEmitter);

MorayEtagCache.prototype.remove = function (bucket, key)
{
	if (this.c_objects.hasOwnProperty(bucket))
		delete (this.c_objects[bucket][key]);
};

MorayEtagCache.prototype.record = function (record)
{
	var bucket = record['bucket'];
	var key = record['key'];
	var saw;

	if (!this.c_objects[bucket])
		this.c_objects[bucket] = {};

	saw = this.c_objects[bucket][key];

	this.c_objects[bucket][key] = {
	    'mtime': record['_mtime'],
	    'etag': record['_etag']
	};

	if (!saw || saw['mtime'] !== record['_mtime'] ||
	    saw['etag'] !== record['_etag'])
		this.emit('record', record,
		    !saw || saw['etag'] !== record['_etag']);
};


/*
 * Interface for polling periodically on a number of different Moray buckets.
 * This object is constructed with:
 *
 *	client		function returning a Moray client interface
 *
 * Each bucket to poll is configured via addPoll with the following fields:
 *
 *	name		convenient programmatic name for the bucket
 *
 *	bucket		actual Moray bucket name
 *
 *	cache		whether to filter new objects through an EtagCache
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
	this.mp_cache = new MorayEtagCache();
	this.mp_cache.on('record', this.emit.bind(this, 'record'));
}

mod_util.inherits(MorayPoller, mod_events.EventEmitter);

MorayPoller.prototype.addPoll = function (conf)
{
	mod_assert.equal(typeof (conf), 'object');
	mod_assert.equal(typeof (conf['name']), 'string');
	mod_assert.equal(typeof (conf['bucket']), 'string');
	mod_assert.equal(typeof (conf['cache']), 'boolean');
	mod_assert.equal(typeof (conf['mkfilter']), 'function');
	mod_assert.equal(typeof (conf['interval']), 'number');

	this.mp_polls[conf['name']] = {
	    'bucket': conf['bucket'],
	    'cache': conf['cache'] ? this.mp_cache : null,
	    'mkfilter': conf['mkfilter'],
	    'throttle': new mod_mautil.Throttler(conf['interval'])
	};
};

MorayPoller.prototype.remove = function (bucket, obj)
{
	this.mp_cache.remove(bucket, obj);
};

MorayPoller.prototype.poll = function ()
{
	this.mp_log.trace('fetching client for poll');
	var client = this.mp_client();

	if (!client) {
		this.mp_log.trace('poll failed (no client)');
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
	var cache = conf['cache'];
	var throttle = conf['throttle'];
	var poller = this;
	var filter, req;

	if (throttle.tooRecent())
		return;

	filter = conf['mkfilter']();
	if (!filter) {
		this.mp_log.trace('skipping "%s" (no filter)', name);
		return;
	}

	this.mp_log.trace('polling "%s"', name);
	throttle.start();
	req = client.findObjects(bucket, filter, { 'noCache': true });

	req.on('error', function (err) {
		throttle.done();
		poller.emit('warn', err);
	});

	req.on('record', function (record) {
		if (cache)
			cache.record(record);
		else
			poller.emit('record', true);
	});

	req.on('end', function () {
		throttle.done();
		poller.emit('search-done', name, now);
	});
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
