/*
 * lib/moray.js: common Moray abstractions
 */

var mod_events = require('events');
var mod_util = require('util');

var mod_mautil = require('./util');
var mod_moray = require('moray');


/* Public interface. */
exports.MorayEtagCache = MorayEtagCache;


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
