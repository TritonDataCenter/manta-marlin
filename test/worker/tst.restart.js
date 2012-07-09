/*
 * tst.restart.js: tests that if a worker restarts, it picks up jobs it
 *     previously owned.
 */

var mod_assert = require('assert');

var mod_jsprim = require('jsprim');
var mod_vasync = require('vasync');
var mod_worklib = require('./workerlib');

var log = mod_worklib.log;
var bktJobs = mod_worklib.jobsBucket;
var tcWrap = mod_worklib.tcWrap;
var moray, worker, worker2, jobdef;

mod_worklib.pipeline({
    'funcs': [
	setup,
	setupMoray,
	checkJob,
	swtch,
	teardown
    ]
});


function setup(_, next)
{
	log.info('setup');

	jobdef = mod_worklib.jobSpec1Phase;
	moray = mod_worklib.createMoray();
	worker = mod_worklib.createWorker({
	    'moray': moray,
	    'saveInterval': 1 * 1000,
	    'jobAbandonTime': 5 * 1000
	});
	worker2 = mod_worklib.createWorker({
	    'moray': moray,
	    'saveInterval': 1 * 1000,
	    'jobAbandonTime': 5 * 1000
	});
	moray.wipe(next);
}

function setupMoray(_, next)
{
	moray.setup(function (err) {
		if (err)
			throw (err);

		moray.put(bktJobs, jobdef['jobId'], jobdef, next);
		worker.start();
	});
}

function checkJob(_, next)
{
	log.info('checkJob');
	mod_worklib.timedCheck(10, 1000, function (callback) {
		moray.get(bktJobs, jobdef['jobId'], tcWrap(function (err, job) {
			if (err)
				throw (err);

			mod_assert.equal('worker-000', job['worker']);
			callback();
		}, callback));
	}, next);
}

function swtch(_, next)
{
	log.info('swtch');
	worker.stop();
	worker2.start();
	mod_worklib.timedCheck(10, 1000, function (callback) {
		worker2.stats(tcWrap(function (err, stats) {
			if (err) {
				next(err);
				return;
			}

			mod_assert.equal(1, stats['asgn_restart'], 1,
			    'worker did not report a resumed job');

			moray.get(bktJobs, jobdef['jobId'],
			    tcWrap(function (suberr, job) {
				if (suberr)
					throw (suberr);

				mod_assert.equal('worker-000', job['worker']);
				callback();
			    }, callback));
		}, callback));
	}, next);
}

function teardown(_, next)
{
	log.info('teardown');
	worker2.stop();
	moray.stop();
	next();
}
