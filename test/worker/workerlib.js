/*
 * workerlib.js: utility functions for testing the worker
 */

var mod_path = require('path');

var mod_bunyan = require('bunyan');
var mod_extsprintf = require('extsprintf');

var mod_moray = require('../../lib/worker/moray.js');
var mod_worker = require('../../lib/worker/worker.js');

var testname = mod_path.basename(process.argv[0]);
var log = new mod_bunyan({ 'name': testname });
var idx = 0;

/*
 * Public interface
 */
exports.testname = testname;
exports.log = log;
exports.createMoray = createMoray;
exports.createWorker = createWorker;
exports.timedCheck = timedCheck;
exports.jobsBucket = mod_worker.mwConf['jobsBucket'];
exports.taskGroupsBucket = mod_worker.mwConf['taskGroupsBucket'];

function createMoray()
{
	var props = Object.create(mod_worker.mwConf);
	props['log'] = log;
	props['findInterval'] = 10;
	props['taskGroupInterval'] = 10;
	return (new mod_moray.MockMoray(props));
}

function createWorker(args)
{
	var worker_args = Object.create(args);

	worker_args['log'] = log;
	worker_args['uuid'] = mod_extsprintf.sprintf('worker-%03d', idx++);

	return (new mod_worker.mwWorker(worker_args));
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
