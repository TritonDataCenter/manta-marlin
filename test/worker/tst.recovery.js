/*
 * tst.recovery.js: tests recovery from an intermediate job state
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
	submitTaskGroups,
	checkJob,
	finishPhase3,
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

	taskgroups = {};
	moray = mod_worklib.createMoray();
	worker = mod_worklib.createWorker({ 'moray': moray });
	worker.start();
	next();
}

/*
 * As a fairly general case, we define a job with three phases, each of which
 * has four input and output keys.  We pretend like the job crashed during phase
 * 2 of 3, after the worker has finished writing *some* of the assignments,
 * which have been partially processed.
 */
function submitTaskGroups(_, next)
{
	log.info('submitTaskGroups');

	moray.put(bktTgs, 'tg-001', {
	    'taskGroupId': 'tg-001',
	    'jobId': 'job-001',
	    'host': 'mynode1',
	    'phaseNum': 0,
	    'inputKeys': [ 'key1', 'key2', 'key3' ],
	    'results': [ {
		'input': 'key1',
		'result': 'ok',
		'outputs': [ 'key1_out' ]
	    }, {
		'input': 'key2',
		'result': 'ok',
		'outputs': [ 'key2_out' ]
	    }, {
		'input': 'key3',
		'result': 'ok',
		'outputs': [ 'key3_out' ]
	    } ]
	});

	moray.put(bktTgs, 'tg-002', {
	    'taskGroupId': 'tg-002',
	    'jobId': 'job-001',
	    'host': 'mynode2',
	    'phaseNum': 0,
	    'inputKeys': [ 'key4' ],
	    'results': [ {
		'input': 'key4',
		'result': 'ok',
		'outputs': [ 'key4_out' ]
	    } ]
	});

	moray.put(bktTgs, 'tg-003', {
	    'taskGroupId': 'tg-003',
	    'jobId': 'job-001',
	    'host': 'mynode3',
	    'phaseNum': 1,
	    'inputKeys': [ 'key1_out', 'key2_out', 'key4_out' ],
	    'results': [ {
		'input': 'key1_out',
		'result': 'ok',
		'outputs': [ 'key1_out2' ]
	    } ]
	});

	moray.put(bktJobs, 'job-001', {
	    'jobId': 'job-001',
	    'phases': [ {}, {}, {} ],
	    'inputKeys': [ 'key1', 'key2', 'key3', 'key4' ]
	});

	next();
}

function checkJob(_, next)
{
	log.info('checkJob');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		var job, groups, newgroup, i;

		/*
		 * Check that the worker finished assigning phase 2, and
		 * recognized the partial assignment for phase 2.
		 */
		job = moray.get(bktJobs, 'job-001');
		mod_assert.equal('worker-000', job['worker']);
		mod_assert.equal('running', job['state']);

		groups = moray.list(bktTgs).map(moray.get.bind(moray, bktTgs));
		mod_assert.equal(Object.keys(groups).length, 4);

		for (i = 0; i < groups.length; i++) {
			if (groups[i]['phaseNum'] == 1 &&
			    groups[i]['taskGroupId'] != 'tg-003') {
				newgroup = groups[i];
				break;
			}
		}

		mod_assert.deepEqual(newgroup['inputKeys'], [ 'key3_out' ]);
		mod_assert.deepEqual(newgroup['results'], []);
		mod_assert.equal(newgroup['jobId'], 'job-001');

		/*
		 * Write records to finish phase 2.
		 */
		groups.forEach(function (group) {
			group['results'] = group['inputKeys'].map(function (key)
{
				return ({
				    'result': 'ok',
				    'input': key,
				    'outputs': [ key + '2' ]
				});
			});

			moray.put(bktTgs, group['taskGroupId'], group);
		});

		callback();
	}, next);
}

function finishPhase3(_, next)
{
	log.info('finishPhase3');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		/*
		 * Wait for new task groups to be assigned for phase 3.
		 */
		var groups, keyset, keys;

		groups = moray.list(bktTgs).map(moray.get.bind(moray, bktTgs));
		keyset = {};

		groups.forEach(function (group) {
			if (group['phaseNum'] != 2)
				return;

			group['inputKeys'].forEach(function (key) {
				keyset[key] = true;
			});
		});

		keys = Object.keys(keyset);
		keys.sort();

		mod_assert.deepEqual(keys,
		    [ 'key1_out2', 'key2_out2', 'key3_out2', 'key4_out2' ]);

		/*
		 * Write records indicating these have been completed.
		 */
		groups.forEach(function (group) {
			group['results'] = group['inputKeys'].map(function (key)
{
				return ({
				    'result': 'ok',
				    'input': key,
				    'outputs': [ key + '_final' ]
				});
			});

			moray.put(bktTgs, group['groupId'], group);
		});

		callback();
	}, next);
}

function checkDone(_, next)
{
	log.info('checkDone');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		var job = moray.get(bktJobs, 'job-001');
		mod_assert.equal(job['state'], 'done');
		callback();
	}, next);
}

function teardown(_, next)
{
	log.info('teardown');
	worker.stop();
	next();
}
