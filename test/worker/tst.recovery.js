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


var moray, worker, jobdef;

function setup(_, next)
{
	log.info('setup');

	taskgroups = {};
	jobdef = mod_worklib.jobSpec3Phase;
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
	    'jobId': jobdef['jobId'],
	    'host': 'mynode1',
	    'phaseNum': 0,
	    'inputKeys': [ 'key1', 'key2', 'key3' ],
	    'state': 'done',
	    'results': [ {
		'machine': 'mymac1',
		'input': 'key1',
		'result': 'ok',
		'outputs': [ 'key1_out' ],
		'startTime': mod_jsprim.iso8601(new Date(Date.now() - 1)),
		'doneTime': mod_jsprim.iso8601(new Date())
	    }, {
		'machine': 'mymac1',
		'input': 'key2',
		'result': 'ok',
		'outputs': [ 'key2_out' ],
		'startTime': mod_jsprim.iso8601(new Date(Date.now() - 1)),
		'doneTime': mod_jsprim.iso8601(new Date())
	    }, {
		'machine': 'mymac1',
		'input': 'key3',
		'result': 'ok',
		'outputs': [ 'key3_out' ],
		'startTime': mod_jsprim.iso8601(new Date(Date.now() - 1)),
		'doneTime': mod_jsprim.iso8601(new Date())
	    } ]
	});

	moray.put(bktTgs, 'tg-002', {
	    'taskGroupId': 'tg-002',
	    'jobId': jobdef['jobId'],
	    'host': 'mynode2',
	    'phaseNum': 0,
	    'inputKeys': [ 'key4' ],
	    'state': 'done',
	    'results': [ {
		'machine': 'mymac1',
		'input': 'key4',
		'result': 'ok',
		'outputs': [ 'key4_out' ],
		'startTime': mod_jsprim.iso8601(new Date(Date.now() - 1)),
		'doneTime': mod_jsprim.iso8601(new Date())
	    } ]
	});

	moray.put(bktTgs, 'tg-003', {
	    'taskGroupId': 'tg-003',
	    'jobId': jobdef['jobId'],
	    'host': 'mynode3',
	    'phaseNum': 1,
	    'inputKeys': [ 'key1_out', 'key2_out', 'key4_out' ],
	    'state': 'running',
	    'results': [ {
		'machine': 'mymac1',
		'input': 'key1_out',
		'result': 'ok',
		'outputs': [ 'key1_out2' ],
		'startTime': mod_jsprim.iso8601(new Date(Date.now() - 1)),
		'doneTime': mod_jsprim.iso8601(new Date())
	    } ]
	});

	moray.put(bktJobs, jobdef['jobId'], jobdef);

	next();
}

function checkJob(_, next)
{
	log.info('checkJob');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		var job, tgids, groups, newgroup, i;

		/*
		 * Check that the worker finished assigning phase 2, and
		 * recognized the partial assignment for phase 2.
		 */
		job = moray.get(bktJobs, jobdef['jobId']);
		mod_assert.equal('worker-000', job['worker']);
		mod_assert.equal('running', job['state']);

		tgids = {};
		groups = mod_worklib.listTaskGroups(moray, jobdef['jobId'], 1);
		groups = groups.concat(mod_worklib.listTaskGroups(
		    moray, jobdef['jobId'], 0));

		/*
		 * If there are too many groups here, it's likely that the
		 * worker ignored the ones we already wrote and redispatched
		 * them (which is wrong).  This may indicate an error validating
		 * the task group we wrote out.
		 */
		mod_assert.equal(groups.length, 4);

		for (i = 0; i < groups.length; i++) {
			if (groups[i]['phaseNum'] == 1 &&
			    groups[i]['taskGroupId'] != 'tg-003')
				newgroup = groups[i];

			tgids[groups[i]['taskGroupId']] = true;
		}

		mod_assert.ok(tgids.hasOwnProperty('tg-001'));
		mod_assert.ok(tgids.hasOwnProperty('tg-002'));
		mod_assert.ok(tgids.hasOwnProperty('tg-003'));

		mod_assert.deepEqual(newgroup['inputKeys'], [ 'key3_out' ]);
		mod_assert.deepEqual(newgroup['results'], []);
		mod_assert.equal(newgroup['jobId'], jobdef['jobId']);

		mod_worklib.finishPhase(moray, jobdef['jobId'], 1);
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

		groups = mod_worklib.listTaskGroups(moray, jobdef['jobId'], 2);
		keyset = {};

		groups.forEach(function (group) {
			group['inputKeys'].forEach(function (key) {
				keyset[key] = true;
			});
		});

		keys = Object.keys(keyset);
		keys.sort();

		mod_assert.deepEqual(keys,
		    [ 'key1_out1', 'key2_out1', 'key3_out1', 'key4_out1' ]);

		mod_worklib.finishPhase(moray, jobdef['jobId'], 2);
		callback();
	}, next);
}

function checkDone(_, next)
{
	log.info('checkDone');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		var job = moray.get(bktJobs, jobdef['jobId']);
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
