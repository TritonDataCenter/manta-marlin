/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * adnscache.js: asynchronously-updating DNS cache
 */

var mod_assert = require('assert');

var mod_jsprim = require('jsprim');
var mod_nativedns = require('native-dns');
var mod_verror = require('verror');

var VError = mod_verror.VError;

/* Public interface. */
exports.AsyncDnsCache = AsyncDnsCache;

/*
 * Implements a DNS cache mapping the given hostname to its A (IPv4) record.
 * Unlike a traditional DNS cache, which is populated when a hostname is first
 * used and falls back on a traditional DNS lookup on cache miss, the requested
 * DNS names are specified up front and the records for these names are
 * automatically, asynchronously kept up to date.  Requests for records that are
 * unavailable or expired simply fail; they never result in another DNS request
 * (since the presumption is that a recent asynchronous request failed, so it
 * likely won't resolve now anyway).
 *
 * This is intended for applications which continually use a small number of
 * hostnames.
 *
 * This implementation doesn't kick off any requests on its own; it leaves that
 * under the control of the consumer.  You must call update() periodically, as
 * from an interval timer, to keep the cache up to date.  You can do this as
 * frequently as you like.  Requests will not be made unless cache entries are
 * missing or will soon become stale (as determined by triggerInterval).
 *
 * The following arguments are required:
 *
 *    log		Bunyan-style logger
 *
 *    nameServers	Array of IP addresses of nameservers to use.  If more
 *    			than one nameserver is specified, one will be selected
 *    			uniformly at random for each request.
 *
 *    triggerInterval	how long (in milliseconds) before a DNS record expires
 *			before we start attempting to refresh it
 *
 *    graceInterval	how long (in milliseconds) after a record has
 *    			technically expired before we'll stop serving it
 *
 * To be completely consistent with the DNS TTL (that is, to avoid ever using a
 * record after its TTL has expired), you might set triggerInterval = 10000 and
 * graceInterval = 0, which says to never use an expired record, and attempt to
 * update a record no sooner than 10 seconds before it will expire.
 *
 * If such strictness is not required, you could just as well set
 * triggerInterval = 0 and graceInterval = 10000, which says to update a record
 * only after the existing one has expired, but continue serving the expired
 * record for up to 10 seconds.
 *
 * These two configurations have the same properties with respect to DNS server
 * failure (i.e., will tolerate failures of up to 10 seconds in the worst case),
 * but the former will make more DNS requests since it's updating more
 * frequently.
 */
function AsyncDnsCache(args)
{
	mod_assert.ok(args.hasOwnProperty('triggerInterval') &&
	    typeof (args['triggerInterval']) == 'number',
	    'must specify a numeric "triggerInterval"');
	mod_assert.ok(args.hasOwnProperty('graceInterval') &&
	    typeof (args['graceInterval']) == 'number',
	    'must specify a numeric "graceInterval"');
	mod_assert.ok(args.hasOwnProperty('nameServers') &&
	    Array.isArray(args['nameServers']) &&
	    args['nameServers'].length > 0,
	    'must specify non-empty array of "nameServers"');

	args['nameServers'].forEach(function (n, i) {
	    mod_assert.equal(typeof (n), 'string',
		'nameserver ' + i + ' must be a string');
	});

	this.dns_nameservers = args['nameServers'].map(function (s) {
		return ({
			/* ip address of DNS server */
			'dnss_ip': s,

			/*
			 * Old nameservers (still in use) only support DNS over
			 * UDP with small packets.  More recent versions support
			 * EDNS and TCP-based DNS, and in large deployments,
			 * those modes are necessary in order to receive valid
			 * responses (because there are too many matching DNS
			 * entries).  To figure out what mode to use, we keep
			 * track of what we think is supported by this DNS
			 * server.  "null" indicates we don't know, "true"
			 * indicates support, and "false" indicates that we
			 * don't know.
			 */
			'dnss_tcpok': null,
			'dnss_ednsok': null,

			/*
			 * The following values are only provided for debugging.
			 * These values reflect the last time a successful DNS
			 * response was received using each of these modes.
			 */
			'dnss_last_udp_ok': null,
			'dnss_last_tcp_ok': null,
			'dnss_last_edns_ok': null,
			'dnss_last_mode': null,
			'dnss_last_time': null
		});
	});

	this.dns_interval = args['triggerInterval'];
	this.dns_grace = args['graceInterval'];
	this.dns_hosts = {};
	this.dns_log = args['log'];

	/* for testing only */
	if (args['ttlOverride'])
		this.dns_ttl_override = args['ttlOverride'];

	if (args['onResolve'])
		this.dns_onresolve = args['onResolve'];
}

/*
 * Add the given host to the cache.
 */
AsyncDnsCache.prototype.add = function (hostname)
{
	/*
	 * Like most DNS resolution facilities, we resolve an IPv4 address to
	 * itself, even though that's not technically what DNS does.  We can
	 * check for this case reliably because per RFC1123 S2.1, an IPv4
	 * address in dot-delimited form is not a valid hostname, since the last
	 * component of a DNS name must have letters.
	 */
	if (/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(hostname)) {
		this.dns_hosts[hostname] = {
			'records': [ {
			    'addr': hostname,
			    'expires': Infinity
			} ],
			'expires': Infinity
		};

		return;
	}

	this.dns_hosts[hostname] = {
	    'records': [],
	    'expires': undefined,
	    'pending': undefined
	};

	this.dns_log.trace('adding hostname "%s"', hostname);
};

/*
 * Kick off an asynchronous update for each hostname whose entry may become
 * stale soon.
 */
AsyncDnsCache.prototype.update = function ()
{
	var now, hostname, entry, count;

	now = Date.now();
	count = 0;

	for (hostname in this.dns_hosts) {
		entry = this.dns_hosts[hostname];

		if (entry['pending'])
			continue;

		if (entry['records'].length > 0 &&
		    entry['expires'] !== undefined &&
		    entry['expires'] > now &&
		    entry['expires'] - now > this.dns_interval)
			continue;

		this.kickEntry(hostname);
		count++;
	}

	return (count);
};

/*
 * Given information about a DNS server (namely, one of the elements of
 * this.dns_nameservers), return arguments used to construct a native-dns
 * Request object.  Here, we decide whether to use normal UDP, EDNS, or TCP.
 */
AsyncDnsCache.prototype.createRequest = function (servinfo, question)
{
	var rqargs;

	rqargs = {
	    'question': question,
	    'timeout': 5000,
	    'cache': false,
	    'server': {
		'address': servinfo.dnss_ip,
		'port': 53
	    }
	};

	/*
	 * We try TCP first, since modern deployments support TCP-based DNS and
	 * TCP is necessary to return the large responses used in large
	 * deployments.  If we've tried TCP and it failed, then we'll try EDNS,
	 * which at least supports returning more results than the normal UDP
	 * mode.  If we've tried that and it failed, then we'll use plain, small
	 * UDP packets (and this will not work on large deployments).
	 *
	 * We could do this the other way: try UDP first, then EDNS if that
	 * fails, then TCP if that fails.  That's harder because it's possible
	 * for UDP or EDNS to work for some hostnames, but not other hostnames
	 * that have a lot more records.  As a result, a successful UDP result
	 * does not imply that UDP works in general.
	 */
	if (servinfo.dnss_tcpok === null || servinfo.dnss_tcpok) {
		rqargs['server']['type'] = 'tcp';
	} else if (servinfo.dnss_ednsok === null || servinfo.dnss_ednsok) {
		rqargs['server']['type'] = 'udp';
		rqargs['try_edns'] = true;
	} else {
		rqargs['server']['type'] = 'udp';
	}

	return (rqargs);
};

AsyncDnsCache.prototype.kickEntry = function (hostname)
{
	var cache, now, entry, which, servinfo;
	var question, rqargs, request, onerr, summary;
	var mode_unsupported = false;

	cache = this;
	now = Date.now();
	entry = this.dns_hosts[hostname];
	which = Math.floor(Math.random() * this.dns_nameservers.length);
	servinfo = this.dns_nameservers[which];

	question = mod_nativedns.Question({ 'name': hostname });
	rqargs = this.createRequest(servinfo, question);
	request = mod_nativedns.Request(rqargs);

	summary = {
	    'hostname': hostname,
	    'server': rqargs['server'],
	    'mode': rqargs['server']['type'] == 'tcp' ? 'tcp' :
	        rqargs['try_edns'] ? 'edns' : 'udp'
	};

	onerr = function (err) {
		cache.dns_log.warn(err, summary, 'failed to resolve hostname');
	};

	request.on('timeout', function () {
		mode_unsupported = true;
		onerr(new VError('timed out'));
	});

	request.on('message', function (err, answer) {
		if (err) {
			mode_unsupported = true;
		} else if (answer.answer.length === 0) {
			err = new VError('request returned 0 results');
		}

		if (err) {
			onerr(err);
			return;
		}

		summary['answers'] = answer.answer;
		cache.dns_log.trace(summary, 'found answers');

		var msgtime = Date.now();
		var i, a, e;

		entry['expires'] = Number.MIN_VALUE;
		entry['records'] = new Array(answer.answer.length);

		for (i = 0; i < answer.answer.length; i++) {
			a = answer.answer[i];

			if (cache.dns_ttl_override)
				e = msgtime + cache.dns_ttl_override;
			else
				e = msgtime + a.ttl * 1000;

			entry['records'][i] = {
			    'addr': a.address,
			    'expires': e
			};

			if (e > entry['expires'])
				entry['expires'] = e;
		}
	});

	request.on('end', function () {
		mod_assert.equal(entry['pending'], now);
		entry['pending'] = undefined;

		/*
		 * If the failure mode is consistent with this mode being
		 * unsupported, and we didn't previously know whether the mode
		 * was supported, then indicate this here.
		 */
		if (mode_unsupported) {
			if (summary['mode'] == 'tcp') {
				cache.dns_log.warn({
				    'mode': 'tcp'
				}, 'mode appears to be unsupported');
				servinfo.dnss_tcpok = false;
			} else if (summary['mode'] == 'edns') {
				cache.dns_log.warn({
				    'mode': 'edns'
				}, 'mode appears to be unsupported');
				servinfo.dnss_ednsok = false;
			}
		}

		if (cache.dns_onresolve)
			cache.dns_onresolve();
	});

	this.dns_log.trace(summary, 'resolving');
	mod_assert.ok(entry['pending'] === undefined);
	entry['pending'] = now;
	request.send();
};

/*
 * Returns a known, valid IPv4 address for the given hostname, or null if no
 * such address exists.
 */
AsyncDnsCache.prototype.lookupv4 = function (hostname)
{
	mod_assert.ok(hostname in this.dns_hosts,
	    'hostname "' + hostname + '" is unknown to this cache');

	var now, entry, grace, valid;

	now = Date.now();
	entry = this.dns_hosts[hostname];
	grace = this.dns_grace;
	valid = entry['records'].filter(function (p) {
		return (p['expires'] + grace >= now);
	});

	if (valid.length === 0)
		return (null);

	return (valid[Math.floor(Math.random() * valid.length)]['addr']);
};

/*
 * This is an unfortunate consequence of native-dns's global "platform" object.
 */
if (mod_nativedns.platform.ready)
	initNativeDns();
else
	mod_nativedns.platform.once('ready', initNativeDns);

function initNativeDns()
{
	/* Clear global nameservers so that we always use ours. */
	mod_nativedns.platform.name_servers = [];
	mod_nativedns.platform.attempts = 1;
	mod_nativedns.platform.cache = false;
}
