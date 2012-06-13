/*
 * tst.tgidle.js: tests that idle task groups get timed out.
 */

var mod_assert = require('assert');

var mod_jsprim = require('jsprim');
var mod_vasync = require('vasync');
var mod_worklib = require('./workerlib');

var log = mod_worklib.log;
var bktJobs = mod_worklib.jobsBucket;
var bktTgs = mod_worklib.taskGroupsBucket;
var tcWrap = mod_worklib.tcWrap;
var taskgroups;

mod_vasync.pipeline({
    'funcs': [
	setup,
	setupMoray,
	checkJob,
	wait1,
	checkJob2,
	checkJob3,
	teardown
    ]
}, function (err) {
	if (err) {
		log.fatal(err, 'test failed');
		return;
	}

	log.info('test passed');
});


var moray, worker, jobdef;

function setup(_, next)
{
	log.info('setup');

	taskgroups = {};
	jobdef = mod_worklib.jobSpec1Phase;
	moray = mod_worklib.createMoray();
	worker = mod_worklib.createWorker({
	    'moray': moray,
	    'taskGroupIdleTime': 8 * 1000
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
			if (err) {
				callback(err);
				return;
			}

			mod_assert.equal('worker-000', job['worker']);
			mod_assert.equal('running', job['state']);
			callback();
		}, callback));
	}, next);
}

function wait1(_, next)
{
	log.info('wait');

	setTimeout(next, 7 * 1000);
}

function checkJob2(_, next)
{
	moray.get(bktJobs, jobdef['jobId'], function (err, job) {
		if (err) {
			next(err);
			return;
		}

		mod_assert.equal('running', job['state']);
		setTimeout(next, 2 * 1000);
	});
}

function checkJob3(_, next)
{
	mod_worklib.timedCheck(5, 1000, function (callback) {
		moray.get(bktJobs, jobdef['jobId'], tcWrap(function (err, job) {
			if (err)
				throw (err);

			mod_assert.equal('done', job['state']);
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
