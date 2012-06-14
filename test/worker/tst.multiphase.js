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
var tcWrap = mod_worklib.tcWrap;
var moray, worker, jobdef, taskgroups;

mod_worklib.pipeline({
    'funcs': [
	setup,
	setupMoray,
	checkJob,
	checkPhase1,
	checkPhase2,
	checkDone,
	teardown
    ]
});


function setup(_, next)
{
	log.info('setup');

	jobdef = mod_worklib.jobSpec2Phase;
	moray = mod_worklib.createMoray();
	worker = mod_worklib.createWorker({ 'moray': moray });
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
			mod_assert.equal('running', job['state']);
			callback();
		}, callback));
	}, next);
}

function checkPhase1(_, next)
{
	log.info('checkPhase1');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		mod_worklib.listTaskGroups(moray, jobdef['jobId'], 0,
		    tcWrap(function (err, groups) {
			if (err)
				throw (err);

			var count = 0;

			taskgroups = {};

			groups.forEach(function (group) {
				mod_assert.equal(
				    jobdef['jobId'], group['jobId']);
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
		}, callback));
	}, next);
}

function checkPhase2(_, next)
{
	log.info('checkPhase2');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		mod_worklib.listTaskGroups(moray, jobdef['jobId'], 1,
		    tcWrap(function (err, groups) {
			if (err)
				throw (err);

			var count = 0;

			taskgroups = {};

			groups.forEach(function (group) {
				if (group['phaseNum'] === 0)
					return;

				mod_assert.equal(
				    jobdef['jobId'], group['jobId']);
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
		    }, callback));
	}, next);
}

function checkDone(_, next)
{
	mod_worklib.timedCheck(10, 1000, function (callback) {
		moray.get(bktJobs, jobdef['jobId'], tcWrap(function (err, job) {
			if (err)
				throw (err);

			mod_assert.equal(job['jobId'], jobdef['jobId']);
			mod_assert.equal(job['state'], 'done');
			mod_assert.deepEqual(job['phases'], jobdef['phases']);
			mod_assert.deepEqual(job['inputKeys'],
			    jobdef['inputKeys']);

			mod_assert.deepEqual(job['outputKeys'].sort(), [
			    'key101', 'key201', 'key301', 'key401' ]);

			mod_assert.ok(!worker.mw_jobs.hasOwnProperty(
			    jobdef['jobId']));

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
