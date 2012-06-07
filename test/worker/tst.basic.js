/*
 * tst.basic.js: runs a basic job through the worker
 */

var mod_assert = require('assert');
var mod_path = require('path');

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
	checkTaskGroups,
	updatePartial,
	updateRest,
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

	taskgroups = {};
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
	    'phases': [ {} ],
	    'inputKeys': [ 'key1', 'key2', 'key3', 'key4' ],
	    'results': []
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

function checkTaskGroups(_, next)
{
	log.info('checkTaskGroups');
	mod_worklib.timedCheck(10, 1000, function (callback) {
		var groups = moray.list(bktTgs).map(
		    moray.get.bind(moray, bktTgs));
		mod_assert.equal(3, groups.length);
		log.info('found groups', groups);

		var hosts = {}, keys = {};

		groups.forEach(function (group) {
			mod_assert.equal('job-001', group['jobId']);
			mod_assert.equal('dispatched', group['state']);
			mod_assert.ok(0 === group['phaseNum']);
			mod_assert.deepEqual([], group['results']);

			group['inputKeys'].forEach(function (key) {
				mod_assert.ok(!keys.hasOwnProperty(key));
				keys[key] = true;
			});

			mod_assert.ok(!hosts.hasOwnProperty(group['host']));
			hosts[group['host']] = true;

			mod_assert.ok(!taskgroups.hasOwnProperty(
			    group['taskGroupId']));
			taskgroups[group['taskGroupId']] = group;
		});

		mod_assert.deepEqual(hosts,
		    { 'node0': true, 'node1': true, 'node2': true });

		mod_assert.deepEqual(keys,
		    { 'key1': true, 'key2': true, 'key3': true, 'key4': true });

		callback();
	}, next);
}

function updatePartial(_, next)
{
	var group, key, donekey;

	log.info('updatePartial');

	/*
	 * Report partial completion for one of the task groups.
	 */
	for (key in taskgroups) {
		group = taskgroups[key];
		donekey = group['inputKeys'][0];

		group['results'].push({
		    'result': 'ok',
		    'input': group['inputKeys'][0],
		    'outputs': []
		});

		moray.put(bktTgs, key, group);
		break;
	}

	mod_worklib.timedCheck(10, 1000, function (callback) {
		var job = moray.get(bktJobs, 'job-001');

		mod_assert.equal(job['jobId'], 'job-001');
		mod_assert.deepEqual(job['phases'], [ {} ]);
		mod_assert.deepEqual(job['inputKeys'],
		    [ 'key1', 'key2', 'key3', 'key4' ]);

		mod_assert.equal(job['results'].length, 1);

		mod_assert.deepEqual(job['results'], [ {
		    'input': donekey,
		    'result': 'ok',
		    'outputs': []
		} ]);

		mod_assert.ok(worker.mw_jobs.hasOwnProperty('job-001'));

		callback();
	}, next);
}

function updateRest(_, next)
{
	log.info('updateRest');

	mod_jsprim.forEachKey(taskgroups, function (tgid, group) {
		group['results'] = group['inputKeys'].map(function (key) {
			return ({
			    'result': 'ok',
			    'input': key,
			    'outputs': []
			});
		});

		moray.put(bktTgs, tgid, group);
	});

	mod_worklib.timedCheck(10, 1000, function (callback) {
		var job = moray.get(bktJobs, 'job-001');

		mod_assert.equal(job['jobId'], 'job-001');
		mod_assert.deepEqual(job['phases'], [ {} ]);
		mod_assert.deepEqual(job['inputKeys'],
		    [ 'key1', 'key2', 'key3', 'key4' ]);

		mod_assert.equal(job['results'].length, 4);
		job['results'].sort(function (a, b) {
			return (a['input'] < b['input'] ? -1 :
			    a['input'] > b['input'] ? 1 : 0);
		});

		mod_assert.deepEqual(job['results'], [ {
		    'input': 'key1',
		    'result': 'ok',
		    'outputs': []
		}, {
		    'input': 'key2',
		    'result': 'ok',
		    'outputs': []
		}, {
		    'input': 'key3',
		    'result': 'ok',
		    'outputs': []
		}, {
		    'input': 'key4',
		    'result': 'ok',
		    'outputs': []
		} ]);

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
