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

var mod_vasync = require('vasync');
var mod_bunyan = require('bunyan');

var mod_marlin = require('../lib/marlin');

var testname = mod_path.basename(process.argv[1]);

var log = new mod_bunyan({
    'name': testname,
    'level': process.env['LOG_LEVEL'] || 'info'
});

/* Public interface */
exports.setup = setup;
exports.teardown = teardown;

exports.pipeline = pipeline;
exports.timedCheck = timedCheck;
exports.exnAsync = exnAsync;

exports.testname = testname;
exports.log = log;

function setup(callback)
{
	mod_marlin.createClient({
	    'moray': { 'url': process.env['MORAY_URL'] },
	    'log': log.child({ 'component': 'marlin-client' })
	}, function (err, api) {
		if (err) {
			log.fatal(err, 'failed to setup test');
			throw (err);
		}

		callback(api);
	});
}

function teardown(api, callback)
{
	api.close();
	callback();
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
