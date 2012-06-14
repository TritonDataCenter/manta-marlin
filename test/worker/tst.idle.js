/*
 * tst.idle.js: tests that workers update Moray periodically, even when the job
 *     state hasn't changed
 */

var mod_assert = require('assert');

var mod_jsprim = require('jsprim');
var mod_vasync = require('vasync');
var mod_worklib = require('./workerlib');

var log = mod_worklib.log;
var bktJobs = mod_worklib.jobsBucket;
var tcWrap = mod_worklib.tcWrap;
var moray, worker, jobdef;

mod_worklib.pipeline({
    'funcs': [
	setup,
	setupMoray,
	checkJob,
	checkJob2,
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
	    'saveInterval': 8 * 1000
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
	}, function () {
		moray.put(bktJobs, jobdef['jobId'],
		    { 'jobId': jobdef['jobId'] }, next);
	});
}

function checkJob2(_, next)
{
	log.info('checkJob2');
	mod_worklib.timedCheck(10, 1000, function (callback) {
		moray.get(bktJobs, jobdef['jobId'], tcWrap(function (err, job) {
			if (err)
				throw (err);

			mod_assert.equal('worker-000', job['worker']);
			callback();
		}, callback));
	}, next);
}

function teardown(_, next)
{
	log.info('teardown');
	worker.stop();
	moray.stop();
	next();
}
