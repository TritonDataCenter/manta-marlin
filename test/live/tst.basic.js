/*
 * tst.basic.js: basic functional testing
 */

var mod_assert = require('assert');
var mod_getopt = require('posix-getopt');

var test = require('../common');
var jobs = require('./jobs');
var client;

var funcs = [ setup ];
var strict = true;

var parser, option;

parser = new mod_getopt.BasicParser('S', process.argv);

while ((option = parser.getopt()) !== undefined) {
	switch (option.option) {
	case 'S':
		strict = false;
		break;

	default:
	    /* error message already emitted by getopt */
	    mod_assert.equal('?', option.option);
	    break;
	}
}

if (process.argv.length > parser.optind()) {
	process.argv.slice(parser.optind()).forEach(function (name) {
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

		jobs.jobTestRun(client, testjob, { 'strict': strict }, next);
	});
}

function teardown(_, next)
{
	test.teardown(client, next);
}
