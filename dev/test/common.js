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
var mod_fs = require('fs');
var mod_path = require('path');
var mod_url = require('url');

var mod_bunyan = require('bunyan');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_mahi = require('mahi');
var mod_manta = require('manta');
var mod_marlin = require('../lib/marlin');
var mod_sdc = require('sdc-clients');

var VError = mod_verror.VError;

var testname = mod_path.basename(process.argv[1]);

var log = new mod_bunyan({
    'name': testname,
    'level': process.env['LOG_LEVEL'] || 'info'
});

/* Public interface */
exports.setup = setup;
exports.teardown = teardown;
exports.loginLookup = loginLookup;
exports.ensureUser = ensureUser;

exports.pipeline = pipeline;
exports.timedCheck = timedCheck;
exports.exnAsync = exnAsync;

exports.testname = testname;
exports.log = log;

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
	ufds_client = new mod_sdc.UFDS({
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
 * Ensure that the given test user exists with login "login".  If it doesn't, it
 * will be created based on information from the user "poseidon".
 */
function ensureUser(login, callback)
{
	var existing = null;
	var haskey = false;
	var raced = false;
	var templateuser = 'poseidon';
	var keyid, template, sshkey;
	var ulog = log.child({ 'login': login });

	keyid = process.env['MANTA_KEY_ID'];

	mod_vasync.pipeline({
	    'funcs': [
		function checkUserExists(_, subcb) {
			ulog.info('checking for user');
			ufds_client.getUser(login, function (err, ret) {
				if (!err) {
					ulog.debug('user exists');
					existing = ret;
					subcb();
				} else if (err.statusCode == 404) {
					ulog.debug('user doesn\'t exist');
					subcb();
				} else {
					ulog.error(err, 'user check failed');
					subcb(err);
				}
			});
		},
		function checkUserHasKey(_, subcb) {
			if (existing === null) {
				subcb();
				return;
			}

			ulog.info({ 'key_id': keyid },
			    'checking for user\'s key');
			existing.getKey(keyid, function (err, key) {
				if (!err) {
					haskey = true;
					ulog.info({ 'key_id': keyid },
					    'user exists with expected key');
					/* Abort the pipeline. */
					subcb(new VError('no error'));
				} else if (err.statusCode == 404) {
					ulog.info('user exists, but no key');
					subcb();
				} else {
					ulog.error(err,
					    'user key fetch failed');
					subcb(err);
				}
			});
		},
		function fetchTemplate(_, subcb) {
			ulog.info('fetching user "%s" as template',
			    templateuser);
			ufds_client.getUser(templateuser, function (err, ret) {
				if (err) {
					ulog.error(err, 'fetch template user');
					subcb(new VError(err,
					    'fetch user "%s"', templateuser));
				} else {
					template = ret;
					ulog.debug(template, 'user template');
					subcb();
				}
			});
		},
		function fetchKey(_, subcb) {
			ulog.info('fetching key "%s" from template user',
			    keyid);
			template.getKey(keyid, function (err, key) {
				if (err) {
					ulog.error(err, 'fetch template key');
					subcb(new VError(err,
					    'fetch template key'));
				} else {
					sshkey = key;
					ulog.debug(sshkey, 'user key');
					subcb();
				}
			});
		},
		function addUser(_, subcb) {
			if (existing !== null) {
				subcb();
				return;
			}

			var newemail = template['email'].replace(
			    '@', '+' + mod_uuid.v4().substr(0, 8) + '@');
			var user = {
			    'login': login,
			    'userpassword': 'secret123',
			    'email': newemail,
			    'approved_for_provisioning': true
			};
			ulog.debug(user, 'creating user');
			ufds_client.addUser(user, function (err, newuser) {
				if (err && err.statusCode == 409) {
					/*
					 * It's possible (even likely) that this
					 * is being run concurrently for the
					 * same user for a different test case,
					 * so we'll jump down to waiting for
					 * replication in this case.
					 */
					ulog.warn(err, 'error creating user ' +
					    '(concurent update?)', err);
					raced = true;
					subcb();
					return;
				}

				if (err) {
					ulog.error(err, 'creating user', err);
					subcb(new VError(err, 'creating user'));
				} else {
					ulog.info('created user');
					existing = newuser;
					subcb();
				}
			});
		},
		function addKey(_, subcb) {
			if (raced) {
				subcb();
				return;
			}

			ulog.debug('adding key');
			existing.addKey(sshkey['openssh'], function (err) {
				if (err) {
					ulog.error(err, 'adding key');
					subcb(new VError('adding key', err));
				} else {
					ulog.info('added key');
					subcb();
				}
			});
		},
		/*
		 * Wait for the key to be replicated to mahi.  There may be
		 * multiple mahi servers, and we have to wait for all of them to
		 * get the key.  To attempt to check this from the outside, we
		 * wait until at least 20 requests complete.  Yes, this is
		 * cheesy, and you might think fewer than 20 would reliably
		 * work, but it doesn't.
		 */
		function waitForReplication(_, subcb) {
			ulog.info('waiting for UFDS replication');

			var count = 0;
			var iter = function () {
				timedCheck(30, 1000, function (testcb) {
					marlin_client.manta.info(
					    '/' + login + '/stor',
					    function (err, rv) {
						testcb(err);
					    });
				}, function () {
					if (++count == 20) {
						ulog.info('completed %d ' +
						    'requests', count);
						subcb();
					} else {
						ulog.debug('waiting (%d)',
						    count);
						iter();
					}
				});
			};

			iter();
		}
	    ]
	}, function (err) {
		if (!err || haskey)
			callback();
		else
			callback(err);
	});
}

function teardown(api, callback)
{
	if (mahi_client)
		mahi_client.close();
	api.close();
	ufds_client.close(callback);
}

function loginLookup(login, callback)
{
	if (!mahi_client) {
		if (!process.env['MAHI_URL']) {
			process.nextTick(function () {
				callback(new VError('MAHI_URL must be ' +
				    'specified in the environment'));
			});

			return;
		}

		log.info('connecting to mahi', process.env['MAHI_URL']);
		mahi_client = mod_mahi.createClient({
		    'log': log.child({ 'component': 'MahiClient' }),
		    'url': process.env['MAHI_URL'],
		    'maxAuthCacheSize': 1000,
		    'maxAuthCacheAgeMs': 300,
		    'maxTranslationCacheSize': 1000,
		    'maxTranslationCacheAgeMs': 300
		});
	}

	mahi_client.getUuid({ 'account': login }, function (err, record) {
		if (err) {
			callback(err);
			return;
		}

		log.info('user "%s" has uuid', login, record['account']);
		callback(null, record['account']);
	});
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
