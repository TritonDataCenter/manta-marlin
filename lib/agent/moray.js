/*
 * lib/agent/moray.js: agent interface to Moray, abstracted out for easy
 *     replacement for automated testing.
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_util = require('util');

var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');

var mod_moray_client = require('moray-client');

var mod_mrutil = require('../util');

/* Public interface */
exports.RemoteMoray = RemoteMoray;


/*
 * Implements the agent Moray interface backed by an actual remote Moray
 * instance.  Arguments include:
 *
 *	url			URL for remote Moray instance
 *
 *	log			Bunyan-style logger
 *
 *	taskGroupInterval 	Minimum time between requests for task group
 *				updates (milliseconds)
 *
 *      taskGroupsBucket	Moray bucket for task groups
 */
function RemoteMoray(args)
{
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.equal(typeof (args['taskGroupInterval']), 'number');
	mod_assert.equal(typeof (args['taskGroupsBucket']), 'string');

	mod_events.EventEmitter();

	this.rm_log = args['log'].child({ 'component': 'remote-moray' });

	this.rm_client = mod_moray_client.createClient({
	    'url': args['url'],
	    'log': this.rm_log
	});

	this.rm_tg = new mod_mrutil.Throttler(
	    Math.floor(args['taskGroupInterval']));
	this.rm_tgops = {};
	this.rm_bkt_jobs = args['jobsBucket'];
	this.rm_bkt_taskgroups = args['taskGroupsBucket'];
}

mod_util.inherits(RemoteMoray, mod_events.EventEmitter);


RemoteMoray.prototype.saveTaskGroups = function (newgroups, callback)
{
	/*
	 * XXX Until Moray supports batch updates, we write these all in
	 * parallel and fail if any of them fail.  This will just cause our
	 * caller to try to write them again.  If we decide this is a fine
	 * solution, we should at least cap the number of these we write at
	 * once.
	 */
	var bucket = this.rm_bkt_taskgroups;
	var client = this.rm_client;
	var log = this.rm_log;
	var tgops = this.rm_tgops;
	var uuid = mod_uuid.v4();
	var inputs = Object.keys(newgroups);

	this.rm_tgops[uuid] = {
	    'op': 'saveTgs',
	    'start': new Date(),
	    'taskGroupIds': inputs.map(function (tgid) {
		return (newgroups[tgid]['taskGroupId']);
	    })
	};

	mod_vasync.forEachParallel({
	    'inputs': inputs,
	    'func': function (tgid, subcallback) {
		log.debug('saving task group', tgid, newgroups[tgid]);
		client.put(bucket, newgroups[tgid]['taskGroupId'],
		    newgroups[tgid], subcallback);
	    }
	}, function (err) {
		delete (tgops[uuid]);
		callback(err);
	});
};

RemoteMoray.prototype.watchTaskGroups = function (host, ontaskgroups)
{
	if (this.rm_tg.tooRecent())
		return;

	var moray = this;
	var bucket = this.rm_bkt_taskgroups;
	var filter = 'host=' + host;

	this.rm_tg.start();
	this.rm_client.search(bucket, filter, function (err, groups, meta) {
		moray.rm_tg.done();

		if (err) {
			moray.rm_log.warn(err, 'failed to search tgs bucket');
			return;
		}

		var rv = [];

		mod_jsprim.forEachKey(groups, function (key, group) {
			rv.push({
			    'etag': meta[key]['etag'],
			    'value': group
			});
		});

		ontaskgroups(rv);
	});
};

/* Private interface (for testing only) */
RemoteMoray.prototype.put = function (bucket, key, obj, callback)
{
	var log = this.rm_log;

	this.rm_client.put(bucket, key, obj, function (err) {
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

	this.rm_client.get(bucket, key, function (err, meta, obj) {
		if (err)
			log.fatal(err, 'failed to "get" %s/%s', bucket, key);

		callback(err, obj);
	});
};

/* Private interface (for testing only) */
RemoteMoray.prototype.stop = function ()
{
	var log = this.rm_log;

	this.rm_client.quit(function () {
		log.info('remote Moray client stopped');
	});
};

/* Private interface (for testing only) */
RemoteMoray.prototype.wipe = function (callback)
{
	var moray = this;

	mod_vasync.forEachParallel({
	    'inputs': [ this.rm_bkt_taskgroups ],
	    'func': function (bucket, subcallback) {
		moray.rm_log.info('wiping bucket %s', bucket);
		moray.rm_client.delBucket(bucket, function (err) {
			if (err && err['code'] == 'ResourceNotFound')
				err = null;
			subcallback(err);
		});
	    }
	}, callback);
};

RemoteMoray.prototype.debugState = function ()
{
	return ({ 'watch_tg_state': this.rm_tg });
};
