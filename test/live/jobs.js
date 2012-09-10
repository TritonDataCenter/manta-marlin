/*
 * test/live/jobs.js: test job definitions used by multiple tests
 */

var mod_assert = require('assert');
var mod_http = require('http');
var mod_jsprim = require('jsprim');
var mod_url = require('url');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_testcommon = require('../common');

var VError = mod_verror.VError;
var exnAsync = mod_testcommon.exnAsync;
var log = mod_testcommon.log;

exports.jobTestRun = jobTestRun;
exports.jobSubmit = jobSubmit;
exports.populateData = populateData;

exports.jobM = {
    'job': {
	'phases': [ { 'type': 'storage-map', 'exec': 'wc' } ]
    },
    'inputs': [
	'/test/stor/obj1',
	'/test/stor/obj2',
	'/test/stor/obj3'
    ],
    'timeout': 15 * 1000,
    'expected_outputs': [
	'/test/stor/obj1.out',
	'/test/stor/obj2.out',
	'/test/stor/obj3.out'
    ],
    'expected_tasks': [ {
	'phaseNum': 0,
	'key': '/test/stor/obj1',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': '/test/stor/obj1.out'
    }, {
	'phaseNum': 0,
	'key': '/test/stor/obj2',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': '/test/stor/obj2.out'
    }, {
	'phaseNum': 0,
	'key': '/test/stor/obj3',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': '/test/stor/obj3.out'
    } ],
    'verify': function (testspec, jobresult) {
	mod_assert.equal(jobresult['taskinput'].length, 0);
	mod_assert.equal(jobresult['taskoutput'].length, 0);
	mod_assert.equal(jobresult['task'].length, 3);
	mod_assert.equal(jobresult['jobinput'].length, 3);
    }
};

function jobTestRun(api, testspec, callback)
{
	var interval = testspec['timeout'];

	jobSubmit(api, testspec, function (err, jobid) {
		if (err) {
			callback(err);
			return;
		}

		mod_testcommon.timedCheck(Math.ceil(interval / 1000), 1000,
		    jobTestVerify.bind(null, api, testspec, jobid), callback);
	});
}

function jobSubmit(api, testspec, callback)
{
	var jobdef, funcs, jobid;

	jobdef = {
	    'owner': 'test',
	    'phases': testspec['job']['phases']
	};

	funcs = [
	    function (_, stepcb) {
		log.info('submitting job', jobdef);
		api.jobCreate(jobdef, function (err, result) {
			jobid = result;
			stepcb(err);
		});
	    }
	];

	testspec['inputs'].forEach(function (key) {
		funcs.push(function (_, stepcb) {
			log.info('job "%s": adding key %s', jobid, key);
			api.jobAddKey(jobid, key, stepcb);
		});
	});

	funcs.push(function (_, stepcb) {
		log.info('job "%s": ending input', jobid);
		api.jobEndInput(jobid, stepcb);
	});

	mod_vasync.pipeline({ 'funcs': funcs }, function (err) {
		log.info('job "%s": job submission complete', jobid);
		callback(err, jobid);
	});
}

function jobTestVerify(api, testspec, jobid, callback)
{
	api.jobFetchDetails(jobid, exnAsync(function (err, jobresult) {
		if (err) {
			callback(err);
			return;
		}

		/* This is really verifying that jobSubmit worked. */
		var inputs = jobresult['jobinput'].map(
		    function (rec) { return (rec['value']['key']); });
		var expected_inputs = testspec['inputs'].slice(0);
		mod_assert.deepEqual(inputs.sort(), expected_inputs.sort());

		var job = jobresult['job']['value'];
		mod_assert.deepEqual(testspec['job']['phases'], job['phases']);

		/* Wait for the job to be completed. */
		mod_assert.equal(job['state'], 'done');

		/* Sanity-check the rest of the job record. */
		mod_assert.ok(job['worker']);
		mod_assert.ok(!job['timeCancelled']);
		mod_assert.ok(job['timeInputDone'] >= job['timeCreated']);
		mod_assert.ok(job['timeDone'] >= job['timeCreated']);

		var outputs = [];

		mod_jsprim.forEachKey(jobresult['task'],
		    function (taskid, record) {
			if (record['value']['phaseNum'] !=
			    testspec['job']['phases'].length - 1)
				return;

			if (!record['value']['timeCommitted'] ||
			    record['value']['result'] != 'ok')
				return;

			record['value']['firstOutputs'].forEach(function (out) {
				outputs.push(out['key']);
			});
		    });

		var expected_outputs = testspec['expected_outputs'].slice(0);
		mod_assert.deepEqual(outputs.sort(), expected_outputs.sort());

		/* XXX check expected tasks */

		if (testspec['verify'])
			testspec['verify'](testspec, jobresult);

		callback();
	}, callback));
}

function populateData(keys, callback)
{
	if (!process.env['MANTA_URL']) {
		callback(new Error('MANTA_URL env var must be set.'));
		return;
	}

	var url = mod_url.parse(process.env['MANTA_URL']);
	mod_vasync.forEachParallel({
	    'inputs': keys,
	    'func': function (key, subcallback) {
		var data = 'sample data for key ' + key;
		var req = mod_http.request({
		    'method': 'put',
		    'host': url['hostname'],
		    'port': parseInt(url['port'], 10) || 80,
		    'path': key,
		    'headers': {
			'content-length': data.length
		    }
		});
		req.write(data);
		req.on('response', function (response) {
			if (response.statusCode == 204) {
				subcallback();
				return;
			}

			subcallback(new Error('wrong status code for key ' +
			    key + ': ' + response.statusCode));
		});
	    }
	}, callback);
}

/*
 * TODO more test cases:
 * - Job configurations:
 *   - two-map-phase job
 *   - map-reduce job
 *   - map-map-reduce-reduce job
 *   - single-phase reduce job
 * - Input variations:
 *   - 0 input keys
 *   - non-existent key
 *   - directory
 * - Other features
 *   - user code fails on some inputs (e.g., "grep" job)
 *   - uses assets for both M and R phases
 *   - phase emits more than 5 keys (using taskoutput records)
 *   - cancellation
 */
