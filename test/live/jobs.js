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

exports.jobSubmit = jobSubmit;
exports.jobTestRun = jobTestRun;
exports.jobTestVerifyTimeout = jobTestVerifyTimeout;
exports.populateData = populateData;

exports.jobM = {
    'job': {
	'phases': [ { 'type': 'storage-map', 'exec': 'wc' } ]
    },
    'inputs': [
	'/poseidon/stor/obj1',
	'/poseidon/stor/obj2',
	'/poseidon/stor/obj3'
    ],
    'timeout': 15 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./
    ],
    'expected_tasks': [ {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj1',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./
	]
    }, {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj2',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./
	]
    }, {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj3',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./
	]
    } ],
    'verify': function (testspec, jobresult) {
	mod_assert.equal(jobresult['taskinput'].length, 0);
	mod_assert.equal(jobresult['taskoutput'].length, 0);
	mod_assert.equal(jobresult['task'].length, 3);
	mod_assert.equal(jobresult['jobinput'].length, 3);
    }
};

/* Like jobM, but makes use of separate external task output objects */
exports.jobMX = {
    'job': {
	'phases': [ {
	    'type': 'storage-map',
	    'exec': 'cat > /var/tmp/tmpfile; ' +
		'for i in 1 2 3 4 5 6 7 8; do ' +
		'    wc < /var/tmp/tmpfile | mpipe; ' +
		'done'
	} ]
    },
    'inputs': [
	'/poseidon/stor/obj1',
	'/poseidon/stor/obj2',
	'/poseidon/stor/obj3'
    ],
    'timeout': 30 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./
    ],
    'expected_tasks': [ {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj1',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 8,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./
	]
    }, {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj2',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 8,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./
	]
    }, {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj3',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 8,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./,
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./,
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./,
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./,
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./
	]
    } ],
    'verify': function (testspec, jobresult) {
	mod_assert.equal(jobresult['taskinput'].length, 0);
	mod_assert.equal(jobresult['taskoutput'].length, 9);
	mod_assert.equal(jobresult['task'].length, 3);
	mod_assert.equal(jobresult['jobinput'].length, 3);
    }
};


exports.jobMM = {
    'job': {
	'phases': [
	    { 'type': 'storage-map', 'exec': 'wc' },
	    { 'type': 'storage-map', 'exec': 'wc' }
	]
    },
    'inputs': [
	'/poseidon/stor/obj1',
	'/poseidon/stor/obj2',
	'/poseidon/stor/obj3'
    ],
    'timeout': 30 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.1\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.1\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.1\./
    ],
    'expected_tasks': [ {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj1',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./
	]
    }, {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj2',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./
	]
    }, {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj3',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./
	]
    }, {
	'phaseNum': 1,
	'key': /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.1\./
	]
    }, {
	'phaseNum': 1,
	'key': /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.1\./
	]
    }, {
	'phaseNum': 1,
	'key': /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./,
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.1\./
	]
    } ],
    'verify': function (testspec, jobresult) {
	mod_assert.equal(jobresult['taskinput'].length, 0);
	mod_assert.equal(jobresult['taskoutput'].length, 0);
	mod_assert.equal(jobresult['jobinput'].length, 3);
    }
};

exports.jobR = {
    'job': {
	'phases': [ { 'type': 'reduce', 'exec': 'wc' } ]
    },
    'inputs': [
	'/poseidon/stor/obj1',
	'/poseidon/stor/obj2',
	'/poseidon/stor/obj3'
    ],
    'timeout': 15 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/reduce\.0\./
    ],
    'expected_tasks': [ {
	'phaseNum': 0,
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [ /\/poseidon\/jobs\/.*\/stor\/reduce\.0\./ ]
    } ],
    'verify': function (testspec, jobresult) {
	mod_assert.equal(jobresult['taskinput'].length, 3);
	mod_assert.equal(jobresult['taskoutput'].length, 0);
	mod_assert.equal(jobresult['task'].length, 1);
	mod_assert.equal(jobresult['jobinput'].length, 3);
    }
};

exports.jobMR = {
    'job': {
	'phases': [
	    { 'type': 'storage-map', 'exec': 'wc' },
	    { 'type': 'reduce', 'exec': 'wc' }
	]
    },
    'inputs': [
	'/poseidon/stor/obj1',
	'/poseidon/stor/obj2',
	'/poseidon/stor/obj3'
    ],
    'timeout': 30 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/reduce\.1\./
    ],
    'expected_tasks': [ {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj1',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./
	]
    }, {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj2',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./
	]
    }, {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj3',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./
	]
    }, {
	'phaseNum': 1,
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/reduce\.1\./
	]
    } ],
    'verify': function (testspec, jobresult) {
	mod_assert.equal(jobresult['taskinput'].length, 3);
	mod_assert.equal(jobresult['taskoutput'].length, 0);
	mod_assert.equal(jobresult['task'].length, 4);
	mod_assert.equal(jobresult['jobinput'].length, 3);
    }
};

exports.jobMMRR = {
    'job': {
	'phases': [
	    { 'type': 'storage-map', 'exec': 'wc' },
	    { 'type': 'storage-map', 'exec': 'wc' },
	    { 'type': 'reduce', 'exec': 'wc' },
	    { 'type': 'reduce', 'exec': 'wc' }
	]
    },
    'inputs': [
	'/poseidon/stor/obj1',
	'/poseidon/stor/obj2',
	'/poseidon/stor/obj3'
    ],
    'timeout': 60 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/reduce\.3\./
    ],
    'expected_tasks': [ {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj1',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./
	]
    }, {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj2',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./
	]
    }, {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj3',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./
	]
    }, {
	'phaseNum': 1,
	'key': /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.1\./
	]
    }, {
	'phaseNum': 1,
	'key': /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.0\./,
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.1\./
	]
    }, {
	'phaseNum': 1,
	'key': /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.0\./,
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.1\./
	]
    }, {
	'phaseNum': 2,
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [ /\/poseidon\/jobs\/.*\/stor\/reduce\.2\./ ]
    }, {
	'phaseNum': 3,
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [ /\/poseidon\/jobs\/.*\/stor\/reduce\.3\./ ]
    } ],
    'verify': function (testspec, jobresult) {
	mod_assert.equal(jobresult['taskinput'].length, 4);
	mod_assert.equal(jobresult['taskoutput'].length, 0);
	mod_assert.equal(jobresult['jobinput'].length, 3);
    }
};

function jobTestRun(api, testspec, callback)
{
	jobSubmit(api, testspec, function (err, jobid) {
		if (err) {
			callback(err);
			return;
		}

		jobTestVerifyTimeout(api, testspec, jobid, callback);
	});
}

function jobSubmit(api, testspec, callback)
{
	var jobdef, funcs, jobid;

	jobdef = {
	    'owner': '78d9fb71-a851-4287-8ce7-5a5090e86b17',
	    'phases': testspec['job']['phases']
	};

	if (testspec['input'])
		jobdef['input'] = testspec['input'];

	funcs = [
	    function (_, stepcb) {
		log.info('submitting job', jobdef);
		api.jobCreate(jobdef, function (err, result) {
			jobid = result;
			stepcb(err);
		});
	    }
	];

	if (!testspec['input']) {
		testspec['inputs'].forEach(function (key) {
			funcs.push(function (_, stepcb) {
				log.info('job "%s": adding key %s', jobid, key);
				api.jobAddKey(jobid, key, stepcb);
			});
		});

		funcs.push(function (_, stepcb) {
			log.info('job "%s": ending input', jobid);
			api.jobEndInput(jobid, { 'retry': { 'retries': 3 } },
			    stepcb);
		});
	}

	mod_vasync.pipeline({ 'funcs': funcs }, function (err) {
		log.info('job "%s": job submission complete', jobid);
		callback(err, jobid);
	});
}

function jobTestVerifyTimeout(api, testspec, jobid, callback)
{
	var interval = testspec['timeout'];

	mod_testcommon.timedCheck(Math.ceil(interval / 1000), 1000,
	    jobTestVerify.bind(null, api, testspec, jobid), callback);
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
		if (!testspec['input'])
			mod_assert.ok(job['timeInputDone'] >=
			    job['timeCreated']);
		mod_assert.ok(job['timeDone'] >= job['timeCreated']);

		/* Check expected job outputs. */
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

		mod_jsprim.forEachKey(jobresult['taskoutput'],
		    function (_, record) {
			outputs.push(record['value']['key']);
		    });

		outputs.sort();

		var expected_outputs = testspec['expected_outputs'].slice(0);
		expected_outputs.sort();

		mod_assert.equal(outputs.length, expected_outputs.length);
		for (var i = 0; i < outputs.length; i++) {
			if (typeof (expected_outputs[i]) == 'string')
				mod_assert.equal(expected_outputs[i],
				    outputs[i],
				    'output ' + i + ' doesn\'t match');
			else
				mod_assert.ok(
				    expected_outputs[i].test(outputs[i]));
		}

		/* Check expected task records. */
		var ntasks = Object.keys(jobresult['task']).length;
		var expected_tasks = testspec['expected_tasks'].slice(0);
		expected_tasks.forEach(function (etask) {
			for (var key in jobresult['task']) {
				var task = jobresult['task'][key]['value'];
				if (jobTaskMatches(etask, task))
					return;
			}

			throw (new VError('no matching task for %j (of %j)',
			    etask, jobresult['task']));
		});

		mod_assert.equal(ntasks, expected_tasks.length);

		if (testspec['verify'])
			testspec['verify'](testspec, jobresult);

		callback();
	}, callback));
}

function jobTaskMatches(etask, task)
{
	for (var prop in etask) {
		if (prop != 'firstOutputs' && prop != 'key') {
			if (etask[prop] !== task[prop])
				return (false);
			continue;
		}

		if (prop == 'key') {
			if (typeof (etask[prop]) == 'string') {
				if (etask[prop] != task[prop])
					return (false);
			} else {
				if (!etask[prop].test(task[prop]))
					return (false);
			}

			continue;
		}

		var expected = etask[prop];
		var actual = task[prop];
		if (expected.length != actual.length)
			return (false);

		actual.sort();
		expected.sort();

		for (var i = 0; i < expected.length; i++) {
			if ((typeof (expected[i]) == 'string' &&
			    expected[i] != actual[i]['key']) ||
			    (typeof (expected[i]) != 'string' &&
			    !expected[i].test(actual[i]['key'])))
				return (false);
		}
	}

	return (true);
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
			'content-length': data.length,
			'x-marlin': true
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
