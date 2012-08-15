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
var mod_verror = require('verror');
var VError = mod_verror.VError;

var mod_moray = require('moray');

var mod_mrutil = require('../util');

/* Public interface */
exports.RemoteMoray = RemoteMoray;


/*
 * Implements the agent Moray interface backed by an actual remote Moray
 * instance.  Arguments include:
 *
 *	host			DNS hostname for remote Moray instance
 *
 *	port			TCP port for remote Moray instance
 *
 *	dns			DNS cache for resolving Moray URL
 *
 *	log			Bunyan-style logger
 *
 *	taskGroupInterval 	Minimum time between requests for task group
 *				updates (milliseconds)
 *
 *	taskGroupsBucket	Moray bucket for task groups
 */
function RemoteMoray(args)
{
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.equal(typeof (args['dns']), 'object');
	mod_assert.equal(typeof (args['host']), 'string');
	mod_assert.equal(typeof (args['port']), 'number');
	mod_assert.equal(typeof (args['taskGroupInterval']), 'number');
	mod_assert.equal(typeof (args['taskGroupsBucket']), 'string');

	mod_events.EventEmitter();

	this.rm_log = args['log'];
	this.rm_dns = args['dns'];
	this.rm_host = args['host'];
	this.rm_port = args['port'];

	this.rm_clients = {};
	this.rm_dns.add(this.rm_host);
	this.rm_tg = new mod_mrutil.Throttler(
	    Math.floor(args['taskGroupInterval']));
	this.rm_tgops = {};
	this.rm_bkt_jobs = args['jobsBucket'];
	this.rm_bkt_taskgroups = args['taskGroupsBucket'];
}

mod_util.inherits(RemoteMoray, mod_events.EventEmitter);


RemoteMoray.prototype.client = function ()
{
	var ip;

	ip = this.rm_dns.lookupv4(this.rm_host);
	if (ip === null)
		return (null);

	if (!this.rm_clients.hasOwnProperty(ip)) {
		this.rm_clients[ip] = mod_moray.createClient({
		    'host': ip,
		    'port': this.rm_port,
		    'log': this.rm_log.child(
			{ 'component': 'remote-moray-' + ip })
		});

		/* XXX no way to wait for "connect" here */

		this.rm_clients[ip].on('error', function (err) {
			this.rm_log.error(err, 'fatal error on moray client');
		});

		this.rm_clients[ip].on('close', function () {
			delete (this.rm_clients[ip]);
		});
	}

	return (this.rm_clients[ip]);
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
	var bucket = this.rm_bkt_taskgroups;
	var client = this.client();
	var log = this.rm_log;
	var tgops = this.rm_tgops;
	var uuid = mod_uuid.v4();
	var inputs = Object.keys(newgroups);

	if (client === null) {
		process.nextTick(function () {
			callback(new VError('no moray client available'));
		});

		return;
	}

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
		client.putObject(bucket, newgroups[tgid]['taskGroupId'],
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
	var client = this.client();

	if (client === null) {
		this.rm_log.warn('failed to watch task groups: ' +
		    'no Moray client available');
		return;
	}

	this.rm_tg.start();
	var req = client.findObjects(bucket, filter, { 'noCache': true });
	var rv = [];

	req.once('error', function () {
		moray.rm_tg.done();
		moray.rm_log.warn('failed to search taskgroups bucket');
	});

	req.on('record', function (record) {
		rv.push({
		    'etag': record['_etag'],
		    'value': record['value']
		});
	});

	req.on('end', function () {
		moray.rm_tg.done();
		ontaskgroups(rv);
	});
};

/* Private interface (for testing only) */
RemoteMoray.prototype.put = function (bucket, key, obj, callback)
{
	var log = this.rm_log;
	var client = this.client();

	if (client === null) {
		process.nextTick(function () {
			callback(new VError('no moray client available'));
		});

		return;
	}

	client.putObject(bucket, key, obj, function (err) {
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
	var client = this.client();

	if (client === null) {
		process.nextTick(function () {
			callback(new VError('no moray client available'));
		});

		return;
	}

	client.getObject(bucket, key, { 'noCache': true },
	    function (err, meta, obj) {
		if (err)
			log.fatal(err, 'failed to "get" %s/%s', bucket, key);

		callback(err, obj['value']);
	});
};

/* Private interface (for testing only) */
RemoteMoray.prototype.stop = function ()
{
	var log = this.rm_log;

	mod_jsprim.forEachKey(this.rm_clients, function (ip, client) {
		log.info('stopping client for %s', ip);
		client.close();
	});
};

/* Private interface (for testing only) */
RemoteMoray.prototype.wipe = function (callback)
{
	var moray = this;
	var client = this.client();

	if (client === null) {
		process.nextTick(function () {
			callback(new VError('no moray client available'));
		});

		return;
	}

	mod_vasync.forEachParallel({
	    'inputs': [ this.rm_bkt_taskgroups ],
	    'func': function (bucket, subcallback) {
		moray.rm_log.info('wiping bucket %s', bucket);
		client.delBucket(bucket, function (err) {
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
