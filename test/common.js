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
var mod_url = require('url');

var mod_bunyan = require('bunyan');
var mod_redis = require('redis');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_manta = require('manta');
var mod_marlin = require('../lib/marlin');

var VError = mod_verror.VError;

var testname = mod_path.basename(process.argv[1]);

var log = new mod_bunyan({
    'name': testname,
    'level': process.env['LOG_LEVEL'] || 'info'
});

/* Public interface */
exports.setup = setup;
exports.teardown = teardown;
exports.resetBucket = resetBucket;
exports.loginLookup = loginLookup;

exports.pipeline = pipeline;
exports.timedCheck = timedCheck;
exports.exnAsync = exnAsync;

exports.testname = testname;
exports.log = log;

var login_cache = {};
var redis_client;

function setup(callback)
{
	var manta_key_id = process.env['MANTA_KEY_ID'];
	var manta_url = process.env['MANTA_URL'];
	var manta_user = process.env['MANTA_USER'];

	if (!manta_key_id)
		throw (new VError('MANTA_KEY_ID not specified'));

	if (!manta_url)
		throw (new VError('MANTA_URL not specified'));

	if (!manta_user)
		throw (new VError('MANTA_USER not specified'));

	exports.manta = mod_manta.createClient({
	    'log': log, /* manta client creates a child logger */
	    'url': manta_url,
	    'user': manta_user,
	    'sign': mod_manta.cliSigner({
		'keyId': manta_key_id,
		'log': log,
		'user': manta_user
	    })
	});

	mod_marlin.createClient({
	    'moray': { 'url': process.env['MORAY_URL'] },
	    'log': log.child({ 'component': 'marlin-client' })
	}, function (err, api) {
		if (err) {
			log.fatal(err, 'failed to setup test');
			throw (err);
		}

		api.manta = exports.manta;
		callback(api);
	});
}

function teardown(api, callback)
{
	if (redis_client)
		redis_client.quit();
	api.close();
	callback();
}

function resetBucket(client, bucket, options, callback)
{
	client.delBucket(bucket, function (err) {
		if (err && !/does not exist/.test(err.message)) {
			callback(err);
			return;
		}

		client.putBucket(bucket, options, callback);
	});
}

function loginLookup(login, callback)
{
	if (login_cache.hasOwnProperty(login)) {
		process.nextTick(function () {
			callback(null, login_cache[login]);
		});

		return;
	}

	if (!redis_client) {
		if (!process.env['MAHI_URL']) {
			process.nextTick(function () {
				callback(new VError('MAHI_URL must be ' +
				    'specified in the environment'));
			});

			return;
		}

		var conf = mod_url.parse(process.env['MAHI_URL']);
		log.info('connecting to redis', conf);
		redis_client = mod_redis.createClient(
		    parseInt(conf['port'], 10), conf['hostname'], {});

		redis_client.once('error', function (err) {
			var verr = new VError(err, 'failed to lookup login ' +
			    '"%s"', login);
			redis_client = undefined;
			log.warn(verr);
			callback(verr);
		});

		redis_client.once('end', function () {
			log.warn('redis connection closed');
			redis_client = undefined;
		});

		redis_client.once('ready', function () {
			log.info('redis connected');
			redis_client.removeAllListeners('error');
		});

		/*
		 * We fall-through since the client will queue our requests
		 * until it's ready.
		 */
	}

	redis_client.get('/login/' + login, function (err, result) {
		if (err) {
			callback(err);
			return;
		}

		var parsed = JSON.parse(result);

		if (!parsed) {
			callback(new VError('no such login: "%s"', login));
			return;
		}

		log.info('user "%s" has uuid', login, parsed['uuid']);
		login_cache[login] = parsed['uuid'];
		callback(null, parsed['uuid']);
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

		if (ntries == 1)
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
