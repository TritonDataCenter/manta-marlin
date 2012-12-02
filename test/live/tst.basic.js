/*
 * tst.basic.js: basic functional testing
 */

var test = require('../common');
var jobs = require('./jobs');
var client;

test.pipeline({ 'funcs': [
    setup,
    runTest.bind(null, jobs.jobM),
    runTest.bind(null, jobs.jobMX),
    runTest.bind(null, jobs.jobR),
    runTest.bind(null, jobs.jobMM),
    runTest.bind(null, jobs.jobMR),
    runTest.bind(null, jobs.jobMMRR),
    runTest.bind(null, jobs.jobMRRoutput),
    runTest.bind(null, jobs.jobMasset),
    teardown
] });

function setup(_, next)
{
	test.setup(function (c) {
		client = c;
		next();
	});
}

function runTest(testjob, _, next)
{
	jobs.populateData(client.manta, testjob['inputs'], function (err) {
		if (err) {
			next(err);
			return;
		}

		jobs.jobTestRun(client, testjob, next);
	});
}

function teardown(_, next)
{
	test.teardown(client, next);
}
