/*
 * tst.assign.js: tests failure to assign a job due to a test-and-set failure.
 */

var mod_assert = require('assert');

var mod_jsprim = require('jsprim');
var mod_vasync = require('vasync');
var mod_worklib = require('./workerlib');

var log = mod_worklib.log;
var bktJobs = mod_worklib.jobsBucket;
var tcWrap = mod_worklib.tcWrap;
var moray, worker, jobdef;

mod_worklib.pipeline({
    'funcs': [
	setup,
	setupMoray,
	checkJob,
	teardown
    ]
});


function setup(_, next)
{
	log.info('setup');

	jobdef = mod_worklib.jobSpec1Phase;
	moray = mod_worklib.createMoray();

	/*
	 * In order to test a write conflict, we have to sneak a write to the
	 * job record in between when the worker reads the record and when it
	 * tries to write back its own assignment.  This is tricky, since it
	 * reads the record and writes back the assignment on the same tick.  We
	 * cheat by interposing on moray.saveJob to queue the worker's request
	 * until we successfully rewrite the job record.
	 */
	var realSave = moray.saveJob;
	var workerArgs;

	moray.saveJob = function tstAssignSave(writejob, options, callback) {
		mod_assert.ok(workerArgs === undefined,
		    'job worker invoked moray.saveJob multiple times');
		workerArgs = [ writejob, options, callback ];
	};

	moray.on('job', function (meta, job) {
		var newjob = Object.create(job);
		newjob['worker'] = 'someone-else';
		log.info('test found job', job);
		realSave.call(moray, newjob, { 'etag': meta.etag },
		    function (err) {
			if (err) {
				log.fatal(err, 'failed to overwrite ' +
				    'job record (did the worker\'s overwrite ' +
				    'it first?)');
				next(err);
			}

			log.info('test successfully clobbered job %s',
			    job['jobId']);
			mod_assert.ok(workerArgs !== undefined,
			    'worker didn\'t try to save job');
			moray.saveJob = realSave;
			moray.saveJob.apply(moray, workerArgs);
		    });
	});

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

	mod_worklib.timedCheck(30, 100, function (callback) {
		worker.stats(tcWrap(function (err, stats) {
			if (err) {
				next(err);
				return;
			}

			mod_assert.equal(stats['asgn_failed'], 1,
			    'worker did not report a failed assignment');

			moray.get(bktJobs, jobdef['jobId'],
			    tcWrap(function (suberr, job) {
				if (suberr) {
					next(suberr);
					return;
				}

				mod_assert.equal('someone-else', job['worker'],
				    'worker erroneously stole the job!');
				next();
			    }, callback));
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
