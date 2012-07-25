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
var tcWrap = mod_worklib.tcWrap;
var moray, worker, jobdef, taskgroups;

mod_worklib.pipeline({
    'funcs': [
	setup,
	setupMoray,
	submitTaskGroup1,
	submitTaskGroup2,
	submitTaskGroup3,
	submitJob,
	checkJob,
	checkJobGroups,
	finishPhase3,
	checkDone,
	teardown
    ]
});


function setup(_, next)
{
	log.info('setup');

	taskgroups = {};
	jobdef = mod_worklib.jobSpec3Phase;
	moray = mod_worklib.createMoray();
	worker = mod_worklib.createWorker({ 'moray': moray });
	moray.wipe(next);
}

function setupMoray(_, next)
{
	moray.setup(next);
}

/*
 * As a fairly general case, we define a job with three phases, each of which
 * has four input and output keys.  We pretend like the job crashed during phase
 * 2 of 3, after the worker has finished writing *some* of the assignments,
 * which have been partially processed.
 */
function submitTaskGroup1(_, next)
{
	log.info('submitTaskGroup1');

	moray.put(bktTgs, 'tg-001', {
	    'taskGroupId': 'tg-001',
	    'jobId': jobdef['jobId'],
	    'host': 'mynode1',
	    'phaseNum': 0,
	    'inputKeys': [ {
		'key': 'key1',
		'objectid': 'objid1',
		'zonename': 'fakezone0'
	    }, {
		'key': 'key2',
		'objectid': 'objid2',
		'zonename': 'fakezone1'
	    }, {
		'key': 'key3',
		'objectid': 'objid3',
		'zonename': 'fakezone2'
	    } ],
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
	}, next);
}

function submitTaskGroup2(_, next)
{
	log.info('submitTaskGroup2');

	moray.put(bktTgs, 'tg-002', {
	    'taskGroupId': 'tg-002',
	    'jobId': jobdef['jobId'],
	    'host': 'mynode2',
	    'phaseNum': 0,
	    'inputKeys': [ {
		'key': 'key4',
		'objectid': 'objid4',
		'zonename': 'fakezone3'
	    } ],
	    'state': 'done',
	    'results': [ {
		'machine': 'mymac1',
		'input': 'key4',
		'result': 'ok',
		'outputs': [ 'key4_out' ],
		'startTime': mod_jsprim.iso8601(new Date(Date.now() - 1)),
		'doneTime': mod_jsprim.iso8601(new Date())
	    } ]
	}, next);
}

function submitTaskGroup3(_, next)
{
	log.info('submitTaskGroup3');

	moray.put(bktTgs, 'tg-003', {
	    'taskGroupId': 'tg-003',
	    'jobId': jobdef['jobId'],
	    'host': 'mynode3',
	    'phaseNum': 1,
	    'inputKeys': [ {
		'key': 'key1_out',
		'objectid': 'objid2',
		'zonename': 'fakezone1'
	    }, {
		'key': 'key2_out',
		'objectid': 'objid2',
		'zonename': 'fakezone1'
	    }, {
		'key': 'key4_out',
		'objectid': 'objid2',
		'zonename': 'fakezone1'
	    } ],
	    'state': 'running',
	    'results': [ {
		'machine': 'mymac1',
		'input': 'key1_out',
		'result': 'ok',
		'outputs': [ 'key1_out1' ],
		'startTime': mod_jsprim.iso8601(new Date(Date.now() - 1)),
		'doneTime': mod_jsprim.iso8601(new Date())
	    } ]
	}, next);
}

function submitJob(_, next)
{
	log.info('submitJob');
	moray.put(bktJobs, jobdef['jobId'], jobdef, next);
}

function checkJob(_, next)
{
	log.info('checkJob');

	/*
	 * Unlike most tests, we don't want the worker to start in this test
	 * until after we've verified that the Moray records have been written,
	 * since the whole point is to test starting up in that state.
	 */
	worker.start();

	mod_worklib.timedCheck(10, 1000, function (callback) {
		moray.get(bktJobs, jobdef['jobId'], tcWrap(function (err, job) {
			mod_assert.equal('worker-000', job['worker']);
			mod_assert.equal('running', job['state']);
			callback();
		}, callback));
	}, next);
}

function checkJobGroups(_, next)
{
	log.info('checkJobGroups');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		/*
		 * Check that the worker finished assigning phase 2, and
		 * recognized the partial assignment for phase 2.
		 */
		mod_worklib.listTaskGroups(moray, jobdef['jobId'], undefined,
		    tcWrap(function (err, groups) {
			var newgroup, i;
			var tgids = {};

			/*
			 * If there are too many groups here, it's likely that
			 * the worker ignored the ones we already wrote and
			 * redispatched them (which is wrong).  This may
			 * indicate an error validating the task group we wrote
			 * out.  If running against a remote Moray instance,
			 * it's also possible that there were stale task groups
			 * still in it.
			 */
			mod_assert.equal(groups.filter(function (p) {
				return (p['phaseNum'] < 2);
			}).length, 4);

			for (i = 0; i < groups.length; i++) {
				if (groups[i]['phaseNum'] == 1 &&
				    groups[i]['taskGroupId'] != 'tg-003')
					newgroup = groups[i];

				tgids[groups[i]['taskGroupId']] = true;
			}

			mod_assert.ok(tgids.hasOwnProperty('tg-001'));
			mod_assert.ok(tgids.hasOwnProperty('tg-002'));
			mod_assert.ok(tgids.hasOwnProperty('tg-003'));

			mod_assert.equal(newgroup['inputKeys'].length, 1);
			mod_assert.equal(newgroup['inputKeys'][0]['key'],
			    'key3_out');
			mod_assert.deepEqual(newgroup['results'], []);
			mod_assert.equal(newgroup['jobId'], jobdef['jobId']);
			mod_worklib.finishPhase(moray, jobdef['jobId'], 1);
			callback();
		}, callback));
	}, next);
}

function finishPhase3(_, next)
{
	log.info('finishPhase3');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		/*
		 * Wait for new task groups to be assigned for phase 3.
		 */
		mod_worklib.listTaskGroups(moray, jobdef['jobId'], 2,
		    tcWrap(function (err, groups) {
			var keyset, keys;

			keyset = {};

			groups.forEach(function (group) {
				mod_assert.equal(group['phaseNum'], 2);

				group['inputKeys'].forEach(function (key) {
					keyset[key['key']] = true;
				});
			});

			keys = Object.keys(keyset);
			keys.sort();

			mod_assert.deepEqual(keys, [ 'key1_out1',
			    'key2_out1', 'key3_out1', 'key4_out1' ]);

			mod_worklib.finishPhase(moray, jobdef['jobId'], 2);
			callback();
		    }, callback));
	}, next);
}

function checkDone(_, next)
{
	log.info('checkDone');

	mod_worklib.timedCheck(10, 1000, function (callback) {
		moray.get(bktJobs, jobdef['jobId'], tcWrap(function (err, job) {
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
