/*
 * tst.basic.js: runs a basic job through the worker
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
	checkTaskGroups,
	updatePartial,
	updateRest,
	teardown
    ]
});


function setup(_, next)
{
	log.info('setup');

	taskgroups = {};
	jobdef = mod_worklib.jobSpec1Phase;
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

function checkTaskGroups(_, next)
{
	log.info('checkTaskGroups');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		mod_worklib.listTaskGroups(moray, jobdef['jobId'], 0,
		    tcWrap(function (err, groups) {
			if (err) {
				callback(err);
				return;
			}

			mod_assert.equal(3, groups.length);
			log.info('found groups', groups);

			var hosts = {}, keys = {};

			groups.forEach(function (group) {
				mod_assert.equal(jobdef['jobId'],
				    group['jobId']);
				mod_assert.equal('dispatched', group['state']);
				mod_assert.ok(0 === group['phaseNum']);
				mod_assert.deepEqual([], group['results']);

				group['inputKeys'].forEach(function (key) {
					mod_assert.ok(
					    !keys.hasOwnProperty(key));
					keys[key] = true;
				});

				mod_assert.ok(
				    !hosts.hasOwnProperty(group['host']));
				hosts[group['host']] = true;

				mod_assert.ok(!taskgroups.hasOwnProperty(
				    group['taskGroupId']));
				taskgroups[group['taskGroupId']] = group;
			});

			mod_assert.deepEqual(hosts,
			    { 'node0': true, 'node1': true, 'node2': true });

			mod_assert.deepEqual(keys, {
			    'key1': true,
			    'key2': true,
			    'key3': true,
			    'key4': true
			});

			callback();
		}, callback));
	}, next);
}

function updatePartial(_, next)
{
	log.info('updatePartial');

	for (var key in taskgroups) {
		if (taskgroups[key]['inputKeys'][0] == 'key1') {
			mod_worklib.completeTaskGroup(
			    moray, taskgroups[key], 1);
			break;
		}
	}

	mod_worklib.timedCheck(10, 1000, function (callback) {
		moray.get(bktJobs, jobdef['jobId'], tcWrap(function (err, job) {
			if (err) {
				callback(err);
				return;
			}

			mod_assert.equal(job['jobId'], jobdef['jobId']);
			mod_assert.deepEqual(job['phases'], jobdef['phases']);
			mod_assert.deepEqual(job['inputKeys'],
			    [ 'key1', 'key2', 'key3', 'key4' ]);
			mod_assert.deepEqual(job['outputKeys'], [ 'key10' ]);
			mod_assert.ok(worker.mw_jobs.hasOwnProperty(
			    jobdef['jobId']));
			callback();
		}, callback));
	}, next);
}

function updateRest(_, next)
{
	log.info('updateRest');

	mod_worklib.finishPhase(moray, jobdef['jobId'], 0);

	mod_worklib.timedCheck(10, 1000, function (callback) {
		moray.get(bktJobs, jobdef['jobId'], tcWrap(function (err, job) {
			if (err) {
				callback(err);
				return;
			}

			mod_assert.equal(job['jobId'], jobdef['jobId']);
			mod_assert.equal(job['state'], 'done');
			mod_assert.deepEqual(job['phases'], jobdef['phases']);
			mod_assert.deepEqual(job['inputKeys'],
			    [ 'key1', 'key2', 'key3', 'key4' ]);
			job['outputKeys'].sort();
			mod_assert.deepEqual(job['outputKeys'],
			    [ 'key10', 'key20', 'key30', 'key40' ]);
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
