/*
 * lib/agent/moray.js: agent interface to Moray, abstracted out for easy
 *     replacement for automated testing.
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_util = require('util');

var mod_jsprim = require('jsprim');
var mod_url = require('url');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var mod_verror = require('verror');
var VError = mod_verror.VError;

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
	mod_assert.equal(typeof (args['url']), 'string');
	mod_assert.equal(typeof (args['taskGroupInterval']), 'number');
	mod_assert.equal(typeof (args['taskGroupsBucket']), 'string');

	mod_events.EventEmitter();

	this.rm_log = args['log'];
	this.rm_dns = args['dns'];

	/*
	 * The caller provides us with a URL in string form which may contain a
	 * DNS hostname that we want to resolve each time we use it.  So rather
	 * than maintain a single client object, we maintain a cache of them
	 * indexed by remote IP.  To accomplish this, we use "url.parse" to
	 * create a URL template, which includes other parameters like the port
	 * number.  Each time we resolve the hostname to a new IP, we want to
	 * copy the template and replace the DNS name with the new IP.  But
	 * there's a trick: the url module's "parse" method returns an object
	 * with both "hostname" (which is the actual host part of the URL) and
	 * "host" (which is the same, but also includes the port number -- if it
	 * was specified).  Modifying "host" properly requires the same ugly
	 * checks that "parse" and "format" already have to do, depending on
	 * whether the port is specified.  But modifying "hostname" alone
	 * doesn't work, because "url.format" ignores that field if "host" is
	 * set.  That's why we must delete the "host" field from our template
	 * entirely in order for this to work.
	 */
	this.rm_url_template = mod_url.parse(args['url']);
	delete (this.rm_url_template['host']);
	this.rm_clients = {};
	this.rm_dns.add(this.rm_url_template['hostname']);

	this.rm_tg = new mod_mrutil.Throttler(
	    Math.floor(args['taskGroupInterval']));
	this.rm_tgops = {};
	this.rm_bkt_jobs = args['jobsBucket'];
	this.rm_bkt_taskgroups = args['taskGroupsBucket'];
}

mod_util.inherits(RemoteMoray, mod_events.EventEmitter);


RemoteMoray.prototype.client = function ()
{
	var ip, url;

	ip = this.rm_dns.lookupv4(this.rm_url_template['hostname']);
	if (ip === null)
		return (null);

	if (!this.rm_clients.hasOwnProperty(ip)) {
		url = Object.create(this.rm_url_template);
		url['hostname'] = ip;

		this.rm_clients[ip] = mod_moray_client.createClient({
		    'url': mod_url.format(url),
		    'log': this.rm_log.child(
			{ 'component': 'remote-moray-' + ip })
		});

		this.rm_clients[ip].on('error', function (err) {
			this.rm_log.error(err, 'fatal error on moray client');
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
	var client = this.client();

	if (client === null) {
		this.rm_log.warn('failed to watch task groups: ' +
		    'no Moray client available');
		return;
	}

	this.rm_tg.start();
	client.search(bucket, filter, function (err, groups, meta) {
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
	var client = this.client();

	if (client === null) {
		process.nextTick(function () {
			callback(new VError('no moray client available'));
		});

		return;
	}

	client.put(bucket, key, obj, function (err) {
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

	client.get(bucket, key, function (err, meta, obj) {
		if (err)
			log.fatal(err, 'failed to "get" %s/%s', bucket, key);

		callback(err, obj);
	});
};

/* Private interface (for testing only) */
RemoteMoray.prototype.stop = function ()
{
	var log = this.rm_log;

	mod_jsprim.forEachKey(this.rm_clients, function (ip, client) {
		log.info('stopping client for %s', ip);
		client.quit(function () {
			log.info('remote client for %s stopped', ip);
		});
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
