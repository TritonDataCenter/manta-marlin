/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * tst.adnscache.js: tests the asynchronous DNS cache
 */

var mod_assert = require('assert');

var mod_bunyan = require('bunyan');
var mod_vasync = require('vasync');

var mod_adnscache = require('../../lib/agent/adnscache');

var log, cache, ip, advanced;

function looksLikeIp4(inip)
{
	if (!inip || typeof (inip) != 'string')
		return (false);

	return (/\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/.test(inip));
}

mod_vasync.pipeline({ 'funcs': [
    setup,
    checkResolved,
    forceUpdate,
    checkResolvedAgain,
    checkGrace,
    checkGraceExpired
] }, function (err) {
	if (err) {
		log.fatal(err, 'test failed');
		throw (err);
	}

	log.info('test passed');
});

function setup(_, next)
{
	log = new mod_bunyan({
	    'name': 'tst.adnscache.js',
	    'level': process.env['LOG_LEVEL'] || 'debug'
	});

	log.info('setup');

	cache = new mod_adnscache.AsyncDnsCache({
	    'log': log,
	    'nameServers': [ '8.8.8.8' ],
	    'triggerInterval': 1000,
	    'graceInterval': 2000,
	    'ttlOverride': 3000,
	    'onResolve': function () {
		if (advanced)
			return;

		advanced = true;
		next();
	    }
	});

	mod_assert.throws(function () {
		cache.lookupv4('us-east.manta.joyent.com');
	}, '"us-east.manta.joyent.com" is not known to this cache');

	cache.add('us-east.manta.joyent.com');
	mod_assert.ok(cache.lookupv4('us-east.manta.joyent.com') === null);
	cache.update();
	mod_assert.ok(cache.lookupv4('us-east.manta.joyent.com') === null);

	cache.add('127.0.0.1');
	mod_assert.equal(cache.lookupv4('127.0.0.1'), '127.0.0.1');
}

function checkResolved(_, next)
{
	log.info('checkResolved');

	/*
	 * Check that we've resolved the IP, and an immediate "update" doesn't
	 * bother making a new request.
	 */
	ip = cache.lookupv4('us-east.manta.joyent.com');
	mod_assert.ok(looksLikeIp4(ip));
	mod_assert.equal(0, cache.update());
	setTimeout(function () { next(); }, 2500);
}

function forceUpdate(_, next)
{
	/*
	 * Check that we properly update inside the triggerInterval, but we
	 * still get a valid address and we only make one request.
	 */
	log.info('forceUpdate');
	advanced = false;
	mod_assert.equal(1, cache.update());
	mod_assert.equal(0, cache.update());
	ip = cache.lookupv4('us-east.manta.joyent.com');
	mod_assert.ok(looksLikeIp4(ip));
}

function checkResolvedAgain(_, next)
{
	/*
	 * Check that we got an updated request, then check that we serve inside
	 * the grace period.
	 */
	log.info('checkResolved');
	ip = cache.lookupv4('us-east.manta.joyent.com');
	mod_assert.ok(looksLikeIp4(ip));
	mod_assert.equal(0, cache.update());
	setTimeout(function () { next(); }, 4000);
}

function checkGrace(_, next)
{
	log.info('checkGrace');
	ip = cache.lookupv4('us-east.manta.joyent.com');
	mod_assert.ok(looksLikeIp4(ip));
	setTimeout(function () { next(); }, 2000);
}

function checkGraceExpired(_, next)
{
	log.info('checkGraceExpired');
	ip = cache.lookupv4('us-east.manta.joyent.com');
	mod_assert.ok(ip === null);
	next();
}
