/*
 * tst.basic.js: basic functional testing
 */

var test = require('../common');
var jobs = require('./jobs');
var client;

test.pipeline({ 'funcs': [
    setup,
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

function teardown(_, next)
{
	test.teardown(client, next);
}

function runTest(_, next)
{
	jobs.jobTestRun(client, jobs.jobM, next);
}
