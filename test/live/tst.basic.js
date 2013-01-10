/*
 * tst.basic.js: basic functional testing
 */

var test = require('../common');
var jobs = require('./jobs');
var client;

var funcs = [ setup ];

if (process.argv.length > 2) {
	process.argv.slice(2).forEach(function (name) {
		if (!jobs.hasOwnProperty(name)) {
			console.error('no such test: %s', name);
			process.exit(1);
		}

		funcs.push(runTest.bind(null, jobs[name]));
	});
} else {
	jobs.jobsAll.forEach(function (testjob) {
		funcs.push(runTest.bind(null, testjob));
	});
}

funcs.push(teardown);

test.pipeline({ 'funcs': funcs });

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
