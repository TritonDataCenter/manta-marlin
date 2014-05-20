/*
 * tst.concurrent.js: tests concurrent jobs
 */

var mod_assert = require('assert');
var mod_getopt = require('posix-getopt');
var mod_vasync = require('vasync');

var test = require('../common');
var jobcommon = require('./common');
var jobs = require('./jobs');
var client;

var strict = true;
var tests = jobs.jobsAll;
var concurrency = 5;

var parser, option;
parser = new mod_getopt.BasicParser('Sc:', process.argv);

if (process.env['MARLIN_TESTS_STRICT'] !== undefined) {
	strict = process.env['MARLIN_TESTS_STRICT'] !== 'false';
}

while ((option = parser.getopt()) !== undefined) {
	switch (option.option) {
	case 'S':
		strict = false;
		break;

	case 'c':
		concurrency = parseInt(option.optarg, 10);
		mod_assert.ok(!isNaN(concurrency),
		    '-c argument must be a number');
		break;

	default:
	    /* error message already emitted by getopt */
	    mod_assert.equal('?', option.option);
	    process.exit(2);
	    break;
	}
}

if (process.argv[parser.optind()] == 'stress') {
	test.log.info('only running stress tests');
	tests = jobs.jobsStress;
}

test.pipeline({ 'funcs': [
    setup,
    runTests,
    teardown
] });

function setup(_, next)
{
	test.setup(function (c) {
		client = c;
		next();
	});
}

function runTests(_, next)
{
	var timeout = tests.reduce(function (sum, testjob) {
		return (sum + testjob['timeout']);
	}, 0);

	test.log.info('using timeout = %s', timeout);

	tests.forEach(function (testjob) {
		testjob['timeout'] = timeout;
	});

	var queue = mod_vasync.queue(runOneTest, concurrency);
	tests.forEach(function (testcase) {
		queue.push(testcase, function (err) {
			if (err)
				next(err);
		});
	});
	queue.drain = next;
}

function runOneTest(testjob, callback)
{
	jobcommon.populateData(client.manta, testjob,
	    testjob['inputs'], function (err) {
		if (err)
			callback(err);
		else
			jobcommon.jobTestRun(client, testjob,
			    { 'strict': strict }, callback);
	});
}

function teardown(_, next)
{
	test.teardown(client, next);
}
