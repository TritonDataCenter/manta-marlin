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
 * update a record no sooner than10 seconds before it will expire.
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

	this.dns_nameservers = args['nameServers'].slice(0);
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

AsyncDnsCache.prototype.kickEntry = function (hostname)
{
	var cache, now, entry, which, s, q, r, onerr;

	cache = this;
	now = Date.now();
	entry = this.dns_hosts[hostname];
	which = Math.floor(Math.random() * this.dns_nameservers.length);
	s = this.dns_nameservers[which];

	q = mod_nativedns.Question({ 'name': hostname });
	r = mod_nativedns.Request({
	    'question': q,
	    'server': { 'address': s, 'port': 53, 'type': 'udp' },
	    'timeout': 1000
	});

	onerr = function (err) {
		cache.dns_log.warn(err,
		    'failed to resolve "%s" from nameserver "%s"', hostname, s);
	};

	r.on('timeout', function () { onerr(new VError('timed out')); });

	r.on('message', function (err, answer) {
		if (!err && answer.answer.length === 0)
			err = new VError('request returned 0 results');

		if (err) {
			onerr(err);
			return;
		}

		cache.dns_log.trace('answers for hostname "%s" from "%s"',
		    hostname, s, answer.answer);

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

	r.on('end', function () {
		mod_assert.equal(entry['pending'], now);
		entry['pending'] = undefined;
		if (cache.dns_onresolve)
			cache.dns_onresolve();
	});

	this.dns_log.trace('attempting to resolve "%s" from "%s"', hostname, s);
	mod_assert.ok(entry['pending'] === undefined);
	entry['pending'] = now;
	r.send();
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
