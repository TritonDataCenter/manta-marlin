/*
 * tst.basic.js: basic functional testing
 */

var test = require('../common');
var jobs = require('./jobs');
var client;

test.pipeline({ 'funcs': [
    setup,
    putData,
    runTest,
    teardown
] });

function setup(_, next)
{
	test.setup(function (c) {
		client = c;
		next();
	});
}

function putData(_, next)
{
	jobs.populateData(jobs.jobM['inputs'], next);
}

function runTest(_, next)
{
	jobs.jobTestRun(client, jobs.jobM, next);
}

function teardown(_, next)
{
	test.teardown(client, next);
}
