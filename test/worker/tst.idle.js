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

mod_vasync.pipeline({
    'funcs': [
	setup,
	submitJob,
	checkJob,
	checkJob2,
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
	worker = mod_worklib.createWorker({
	    'moray': moray,
	    'saveInterval': 8 * 1000
	});
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

	next();
}

function checkJob(_, next)
{
	log.info('checkJob');
	mod_worklib.timedCheck(10, 1000, function (callback) {
		mod_assert.equal('worker-000',
		    moray.get(bktJobs, 'job-001')['worker']);
		callback();
	}, function () {
		moray.put(bktJobs, 'job-001', 'foo');
		mod_assert.equal(moray.get(bktJobs, 'job-001'), 'foo');
		next();
	});
}

function checkJob2(_, next)
{
	log.info('checkJob2');
	mod_worklib.timedCheck(10, 1000, function (callback) {
		mod_assert.equal('worker-000',
		    moray.get(bktJobs, 'job-001')['worker']);
		callback();
	}, next);
}

function teardown(_, next)
{
	log.info('teardown');
	worker.stop();
	next();
}
