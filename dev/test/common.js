/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * test/common.js: Marlin integration functional test suite
 *
 * The tests contained under test/live are designed to test functionality
 * against a live, running Marlin instance (as opposed to the unit tests that
 * spin up instances of various components to test them individually).  To the
 * extent possible, the tests themselves should be table-driven so that the same
 * tests can be run in a stress-test configuration.
 */

var mod_assert = require('assert');
var mod_path = require('path');
var mod_util = require('util');
var mod_stream = require('stream');

var mod_bunyan = require('bunyan');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_mahi = require('mahi');
var mod_manta = require('manta');
var mod_marlin = require('../lib/marlin');
var UFDS = require('ufds');
var mod_maufds = require('../lib/ufds');

var VError = mod_verror.VError;

var testname = mod_path.basename(process.argv[1]);

var log = new mod_bunyan({
    'name': testname,
    'level': process.env['LOG_LEVEL'] || 'info'
});

/* Public interface */
exports.setup = setup;
exports.teardown = teardown;
exports.mahiClient = mahiClient;
exports.ufdsClient = ufdsClient;
exports.ensureAccount = ensureAccount;

exports.pipeline = pipeline;
exports.timedCheck = timedCheck;
exports.exnAsync = exnAsync;

exports.testname = testname;
exports.log = log;

exports.StringInputStream = StringInputStream;

var mahi_client;
var ufds_client;
var marlin_client;

function setup(callback)
{
	var manta_key_id = process.env['MANTA_KEY_ID'];
	var manta_url = process.env['MANTA_URL'];
	var manta_user = process.env['MANTA_USER'];
	var ufds_url = process.env['UFDS_URL'];
	var barrier = mod_vasync.barrier();

	if (!manta_key_id)
		throw (new VError('MANTA_KEY_ID not specified'));

	if (!manta_url)
		throw (new VError('MANTA_URL not specified'));

	if (!manta_user)
		throw (new VError('MANTA_USER not specified'));

	if (!ufds_url)
		throw (new VError('UFDS_URL not specified'));

	exports.manta = mod_manta.createClient({
	    'agent': false,
	    'rejectUnauthorized': false,
	    'log': log, /* manta client creates a child logger */
	    'url': manta_url,
	    'user': manta_user,
	    'sign': mod_manta.cliSigner({
		'keyId': manta_key_id,
		'log': log,
		'user': manta_user
	    })
	});

	barrier.start('ufds');
	log.info({ 'url': ufds_url }, 'connecting to ufds');
	ufds_client = new UFDS({
	    'url': ufds_url,
	    'bindDN': 'cn=root',
	    'bindPassword': 'secret'
	});
	ufds_client.once('ready', function () {
		log.info('ufds ready');
		barrier.done('ufds');
	});

	barrier.start('marlin client');
	mod_marlin.createClient({
	    'moray': { 'url': process.env['MORAY_URL'] },
	    'log': log.child({ 'component': 'marlin-client' })
	}, function (err, api) {
		if (err) {
			log.fatal(err, 'failed to setup test');
			throw (err);
		}

		api.manta = exports.manta;
		marlin_client = api;
		barrier.done('marlin client');
	});

	barrier.on('drain', function () {
		callback(marlin_client);
	});
}

/*
 * Ensure that the given test account exists with login "login".  If it doesn't,
 * it will be created based on information from the "poseidon" account.
 */
var ensureAccountQueue = mod_vasync.queue(function (f, cb) { f(cb); }, 1);
function ensureAccount(login, callback)
{
	var config = {};
	config[login] = {
	    'template': 'poseidon',
	    'keyid': process.env['MANTA_KEY_ID'],
	    'subusers': [ 'testuser' ],
	    'roles': {
		'testrole': {
		    'user': 'testuser',
		    'policy': 'readall'
		}
	    },
	    'policies': {
		'readall': 'can getobject'
	    }
	};

	/*
	 * ufdsMakeAccounts() is not atomic or concurrent-safe (but it is
	 * idempotent), so we run all calls through a queue.
	 */
	ensureAccountQueue.push(function (queuecb) {
		mod_maufds.ufdsMakeAccounts({
		    'log': log,
		    'manta': marlin_client.manta,
		    'ufds': ufdsClient(),
		    'config': config
		}, function (err) {
			queuecb();
			callback(err);
		});
	});
}

function teardown(api, callback)
{
	if (mahi_client)
		mahi_client.close();
	api.close();
	ufds_client.close(callback);
}

function mahiClient()
{
	if (!mahi_client) {
		if (!process.env['MAHI_URL']) {
			throw (new VError('MAHI_URL must be ' +
			    'specified in the environment'));
		}

		log.info('connecting to mahi', process.env['MAHI_URL']);
		mahi_client = mod_mahi.createClient({
		    'log': log.child({ 'component': 'MahiClient' }),
		    'url': process.env['MAHI_URL'],
		    'typeTable': {}
		});
	}

	return (mahi_client);
}

function ufdsClient()
{
	return (ufds_client);
}

function pipeline(args)
{
	mod_vasync.pipeline(args, function (err) {
		if (err) {
			log.fatal(err, 'TEST FAILED');
			process.exit(1);
		}

		log.info('TEST PASSED');
	});
}

/*
 * Invokes "test", an asynchronous function, as "test(callback)" up to "ntries"
 * times until it succeeds (doesn't throw an exception *and* invokes the
 * callback argument with no error), waiting "waittime" in between tries.  If
 * "test" ever succeeds, "onsuccess" is invoked.  Otherwise, the process is
 * killed.
 */
function timedCheck(ntries, waittime, test, onsuccess)
{
	mod_assert.equal(typeof (test), 'function');
	mod_assert.equal(typeof (onsuccess), 'function');

	var callback = function (err, result) {
		if (!err) {
			/*
			 * We invoke "success" on the next tick because we may
			 * actually still be in the context of the below
			 * try/catch block (two frames up) and we don't want
			 * to confuse failure of the success() function with
			 * failure of cb() itself.
			 */
			setTimeout(function () { onsuccess(result); }, 0);
			return;
		}

		if (ntries == 1 || err.noRetry)
			throw (err);

		log.info('timedCheck: retrying');
		setTimeout(timedCheck, waittime, ntries - 1, waittime,
		    test, onsuccess);
	};

	try {
		test(callback);
	} catch (ex) {
		/* Treat thrown exception exactly like a returned error. */
		callback(ex);
	}
}

/*
 * Given a function and callback, call "func".  If it throws, pass the exception
 * to "callback".
 */
function exnAsync(func, callback)
{
	mod_assert.ok(typeof (func) == 'function');
	mod_assert.ok(typeof (callback) == 'function');

	return (function () {
		try {
			func.apply(null, Array.prototype.slice.call(arguments));
		} catch (ex) {
			callback(ex);
		}
	});
}

function StringInputStream(contents)
{
	mod_stream.Stream();

	this.s_data = contents;
	this.s_paused = false;
	this.s_done = false;
	this.readable = true;

	this.scheduleEmit();
}

mod_util.inherits(StringInputStream, mod_stream.Stream);

StringInputStream.prototype.pause = function ()
{
	this.s_paused = true;
};

StringInputStream.prototype.resume = function ()
{
	this.s_paused = false;
	this.scheduleEmit();
};

StringInputStream.prototype.scheduleEmit = function ()
{
	var stream = this;

	process.nextTick(function () {
		if (stream.s_paused || stream.s_done)
			return;

		stream.emit('data', stream.s_data);
		stream.emit('end');

		stream.s_data = null;
		stream.s_done = true;
	});
};
