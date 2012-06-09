/*
 * tst.multiphase.js: runs a multi-phase job through the worker
 */

var mod_assert = require('assert');

var mod_jsprim = require('jsprim');
var mod_vasync = require('vasync');
var mod_worklib = require('./workerlib');

var log = mod_worklib.log;
var bktJobs = mod_worklib.jobsBucket;
var bktTgs = mod_worklib.taskGroupsBucket;
var taskgroups;

mod_vasync.pipeline({
    'funcs': [
	setup,
	checkJob,
	checkPhase1,
	checkPhase2,
	checkDone,
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

	jobdef = mod_worklib.jobSpec2Phase;
	moray = mod_worklib.createMoray();
	worker = mod_worklib.createWorker({ 'moray': moray });
	worker.start();
	moray.put(bktJobs, jobdef['jobId'], jobdef);
	next();
}

function checkJob(_, next)
{
	log.info('checkJob');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		mod_assert.equal('worker-000',
		    moray.get(bktJobs, jobdef['jobId'])['worker']);
		callback();
	}, next);
}

function checkPhase1(_, next)
{
	log.info('checkPhase1');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		var groups = mod_worklib.listTaskGroups(moray,
		    jobdef['jobId'], 0);
		var count = 0;

		mod_assert.equal(moray.get(bktJobs, jobdef['jobId'])['state'],
		    'running');

		taskgroups = {};

		groups.forEach(function (group) {
			mod_assert.equal(jobdef['jobId'], group['jobId']);
			mod_assert.equal('dispatched', group['state']);
			mod_assert.ok(0 === group['phaseNum']);
			mod_assert.deepEqual([], group['results']);

			mod_assert.ok(!taskgroups.hasOwnProperty(
			    group['taskGroupId']));
			taskgroups[group['taskGroupId']] = group;
			count++;
		});

		mod_assert.equal(count, 3);

		mod_worklib.finishPhase(moray, jobdef['jobId'], 0);
		callback();
	}, next);
}

function checkPhase2(_, next)
{
	log.info('checkPhase2');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		var groups = mod_worklib.listTaskGroups(moray,
		    jobdef['jobId'], 1);
		var count = 0;

		mod_assert.equal(moray.get(bktJobs, jobdef['jobId'])['state'],
		    'running');

		taskgroups = {};

		groups.forEach(function (group) {
			if (group['phaseNum'] === 0)
				return;

			mod_assert.equal(jobdef['jobId'], group['jobId']);
			mod_assert.equal('dispatched', group['state']);
			mod_assert.ok(1 === group['phaseNum']);
			mod_assert.deepEqual([], group['results']);

			mod_assert.ok(!taskgroups.hasOwnProperty(
			    group['taskGroupId']));
			taskgroups[group['taskGroupId']] = group;
			count++;
		});

		mod_assert.equal(count, 3);

		mod_worklib.finishPhase(moray, jobdef['jobId'], 1);
		callback();
	}, next);
}

function checkDone(_, next)
{
	mod_worklib.timedCheck(10, 1000, function (callback) {
		var job = moray.get(bktJobs, jobdef['jobId']);

		mod_assert.equal(job['jobId'], jobdef['jobId']);
		mod_assert.equal(job['state'], 'done');
		mod_assert.deepEqual(job['phases'], jobdef['phases']);
		mod_assert.deepEqual(job['inputKeys'], jobdef['inputKeys']);

		mod_assert.deepEqual(job['outputKeys'].sort(), [
		    'key101', 'key201', 'key301', 'key401' ]);

		mod_assert.ok(!worker.mw_jobs.hasOwnProperty(jobdef['jobId']));

		callback();
	}, next);
}

function teardown(_, next)
{
	log.info('teardown');
	worker.stop();
	next();
}