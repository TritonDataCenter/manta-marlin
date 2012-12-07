/*
 * tst.pipe.js: tests piping job output to another job
 */

var mod_assert = require('assert');

var vasync = require('vasync');

var test = require('../common');
var jobs = require('./jobs');

var timeout, client;
var srcjobid, pipedjobid;
var testjob, pipedjob;

test.pipeline({ 'funcs': [
    setup,
    submitFirst,
    submitPiped,
    verifyJobs,
    teardown
] });

function setup(_, next)
{
	testjob = jobs.jobMX;
	testjob['timeout'] *= 2;

	var expected_outputs = [];
	var expected_tasks = [];

	testjob['expected_outputs'].forEach(function (out) {
		var source = out.source ? out.source : out;
		var pipedout = new RegExp('/poseidon/jobs/.*/stor' + source);

		expected_outputs.push(pipedout);

		expected_tasks.push({
		    'phaseNum': 0,
		    'key': out,
		    'state': 'done',
		    'result': 'ok',
		    'nOutputs': 1,
		    'firstOutputs': [ pipedout ]
		});
	});

	pipedjob = {
	    'job': {
		'phases': [ {
		    'type': 'storage-map',
		    'exec': 'wc'
		} ]
	    },
	    'inputs': [],
	    'timeout': testjob['timeout'],
	    'expected_outputs': expected_outputs,
	    'expected_tasks': expected_tasks,
	    'verify': function (spec, result, callback) {
		mod_assert.equal(result['taskinput'].length, 0);
		mod_assert.equal(result['taskoutput'].length, 0);
		mod_assert.equal(result['task'].length, expected_tasks.length);
		mod_assert.equal(result['jobinput'].length, 0);

		var stats = result['job']['value']['stats'];
		mod_assert.equal(stats['nErrors'], 0);
		mod_assert.equal(stats['nTasksCommittedFail'], 0);
		mod_assert.equal(stats['nInputsRead'],
		    testjob['expected_outputs'].length);

		callback();
	    }
	};

	test.setup(function (c) {
		client = c;
		next();
	});
}

function submitFirst(_, next)
{
	jobs.populateData(client.manta, testjob['inputs'], function (err) {
		if (err) {
			next(err);
			return;
		}

		jobs.jobSubmit(client, testjob, function (suberr, jobid) {
			srcjobid = jobid;
			next(suberr);
		});
	});
}

function submitPiped(_, next)
{
	pipedjob['input'] = srcjobid;

	jobs.jobSubmit(client, pipedjob, function (err, jobid) {
		pipedjobid = jobid;
		next(err);
	});
}

function verifyJobs(_, next)
{
	vasync.forEachParallel({
	    'inputs': [ srcjobid, pipedjobid ],
	    'func': verifyOneJob
	}, next);
}

function verifyOneJob(jobid, callback)
{
	var spec = jobid == pipedjobid ? pipedjob : testjob;
	jobs.jobTestVerifyTimeout(client, spec, jobid, callback);
}

function teardown(_, next)
{
	test.teardown(client, next);
}
