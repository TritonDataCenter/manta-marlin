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
	submitJob,
	checkJob,
	checkPhase1,
	finishPhase1,
	checkPhase2,
	finishPhase2,
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


var moray, worker;

function setup(_, next)
{
	log.info('setup');

	moray = mod_worklib.createMoray();
	worker = mod_worklib.createWorker({ 'moray': moray });
	worker.start();
	next();
}

function submitJob(_, next)
{
	log.info('submitJob');

	moray.put(bktJobs, 'job-001', {
	    'jobId': 'job-001',
	    'phases': [ {}, {} ],
	    'inputKeys': [ 'key1', 'key2', 'key3', 'key4' ]
	});

	process.nextTick(next);
}

function checkJob(_, next)
{
	log.info('checkJob');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		mod_assert.equal('worker-000',
		    moray.get(bktJobs, 'job-001')['worker']);
		callback();
	}, next);
}

function checkPhase1(_, next)
{
	log.info('checkPhase1');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		var groups = moray.list(bktTgs).map(
		    moray.get.bind(moray, bktTgs));

		var count = 0;

		mod_assert.equal(moray.get(bktJobs, 'job-001')['state'],
		    'running');

		taskgroups = {};

		groups.forEach(function (group) {
			mod_assert.equal('job-001', group['jobId']);
			mod_assert.equal('dispatched', group['state']);
			mod_assert.ok(0 === group['phaseNum']);
			mod_assert.deepEqual([], group['results']);

			mod_assert.ok(!taskgroups.hasOwnProperty(
			    group['taskGroupId']));
			taskgroups[group['taskGroupId']] = group;
			count++;
		});

		mod_assert.equal(count, 3);

		callback();
	}, next);
}

function finishPhase1(_, next)
{
	log.info('finishPhase1');

	mod_jsprim.forEachKey(taskgroups, function (tgid, group) {
		group['results'] = group['inputKeys'].map(function (key) {
			return ({
			    'result': 'ok',
			    'input': key,
			    'outputs': [ '2' + key + '1', '2' + key + '2' ]
			});
		});

		moray.put(bktTgs, tgid, group);
	});

	next();
}

function checkPhase2(_, next)
{
	log.info('checkPhase2');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		var groups = moray.list(bktTgs).map(
		    moray.get.bind(moray, bktTgs));
		var count = 0;

		mod_assert.equal(moray.get(bktJobs, 'job-001')['state'],
		    'running');

		taskgroups = {};

		groups.forEach(function (group) {
			if (group['phaseNum'] === 0)
				return;

			mod_assert.equal('job-001', group['jobId']);
			mod_assert.equal('dispatched', group['state']);
			mod_assert.ok(1 === group['phaseNum']);
			mod_assert.deepEqual([], group['results']);

			mod_assert.ok(!taskgroups.hasOwnProperty(
			    group['taskGroupId']));
			taskgroups[group['taskGroupId']] = group;
			count++;
		});

		mod_assert.equal(count, 3);

		callback();
	}, next);
}

function finishPhase2(_, next)
{
	log.info('finishPhase2');

	mod_jsprim.forEachKey(taskgroups, function (tgid, group) {
		group['results'] = group['inputKeys'].map(function (key) {
			return ({
			    'result': 'ok',
			    'input': key,
			    'outputs': [ '3' + key ]
			});
		});

		moray.put(bktTgs, tgid, group);
	});

	next();
}

function checkDone(_, next)
{
	mod_worklib.timedCheck(10, 1000, function (callback) {
		var job = moray.get(bktJobs, 'job-001');

		mod_assert.equal(job['jobId'], 'job-001');
		mod_assert.equal(job['state'], 'done');
		mod_assert.deepEqual(job['phases'], [ {}, {} ]);
		mod_assert.deepEqual(job['inputKeys'],
		    [ 'key1', 'key2', 'key3', 'key4' ]);

		mod_assert.deepEqual(job['outputKeys'].sort(), [
		    '32key11', '32key12', '32key21', '32key22',
		    '32key31', '32key32', '32key41', '32key42' ]);

		mod_assert.ok(!worker.mw_jobs.hasOwnProperty('job-001'));

		callback();
	}, next);
}

function teardown(_, next)
{
	log.info('teardown');
	worker.stop();
	next();
}
