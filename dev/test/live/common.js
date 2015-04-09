/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * test/live/common.js: test job helpers
 */

var mod_assert = require('assert');
var mod_child = require('child_process');
var mod_getopt = require('posix-getopt');
var mod_fs = require('fs');
var mod_http = require('http');
var mod_path = require('path');
var mod_url = require('url');

var mod_extsprintf = require('extsprintf');
var mod_jsprim = require('jsprim');
var mod_manta = require('manta');
var mod_memorystream = require('memorystream');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_marlin = require('../../lib/marlin');
var mod_mautil = require('../../lib/util');
var mod_testcommon = require('../common');

/* jsl:import ../../../common/lib/errors.js */
require('../../lib/errors');

var sprintf = mod_extsprintf.sprintf;
var VError = mod_verror.VError;
var StringInputStream = mod_testcommon.StringInputStream;
var log = mod_testcommon.log;

/*
 * MANTA_USER should be set to an operator (e.g., "poseidon") in order to run
 * the tests, but most of the tests will run as DEFAULT_USER.
 */
var DEFAULT_USER = 'marlin_test';

/* Public interface */
exports.DEFAULT_USER = DEFAULT_USER;
exports.jobTestRunner = jobTestRunner;
exports.jobTestCaseRun = jobTestCaseRun;

/*
 * Library function that implements a testcase runner.  The caller passes:
 *
 *    testcases		the set of testcases to choose from, as an object with
 *    			values of the test case object passed to
 *    			jobTestCaseRun().
 *
 *    argv		process arguments, allowing users to specify common
 *    			arguments like "-S" to mean "non-strict mode"
 *
 *    concurrency	number of testcases to run concurrently
 */
function jobTestRunner(testcases, argv, concurrency)
{
	var parser, option, opts, cases;
	var queue, api, timeout;

	parser = new mod_getopt.BasicParser('S', argv);

	/*
	 * By default, we run in strict mode.  Users can override that using the
	 * MARLIN_TESTS_STRICT environment variable or the -S command-line
	 * option.  For details on what "strict" means, see jobTestCaseRun().
	 */
	opts = { 'strict': true };
	if (process.env['MARLIN_TESTS_STRICT'] == 'false')
		opts.strict = false;

	while ((option = parser.getopt()) !== undefined) {
		switch (option.option) {
		case 'S':
			opts.strict = false;
			break;

		default:
		    /* error message already emitted by getopt */
		    mod_assert.equal('?', option.option);
		    break;
		}
	}

	/*
	 * Test cases are supposed to have a label so that jobTestCaseRun can
	 * report which test its running, but for convenience most of them
	 * aren't present directly in the test case definition.  Fill them in
	 * here.
	 */
	mod_jsprim.forEachKey(testcases,
	    function (name, testcase) { testcase['label'] = name; });

	/*
	 * Figure out which cases the user selected.  If none, run all of them.
	 */
	cases = [];
	if (argv.length > parser.optind()) {
		argv.slice(parser.optind()).forEach(function (name) {
			if (!testcases.hasOwnProperty(name))
				throw (new VError('no such test: "%s"', name));
			cases.push(testcases[name]);
		});
	} else {
		mod_jsprim.forEachKey(testcases,
		    function (_, testcase) { cases.push(testcase); });
	}

	/*
	 * The "concurrency" argument determines how many test cases we can run
	 * concurrently.  When concurrency > 1, we allow each test to take
	 * longer to account for the fact that they're all sharing resources,
	 * but the total test time shouldn't change.
	 */
	if (concurrency > 1) {
		timeout = cases.reduce(function (sum, testjob) {
			return (sum + testjob['timeout']);
		}, 0);
		log.info('using timeout = %s', timeout);
		cases.forEach(function (testcase) {
			testcase['timeout'] = timeout;
		});
	}

	/*
	 * To implement all possible values for "concurrency", we run the tests
	 * through a vasync queue with the corresponding concurrency.
	 */
	queue = mod_vasync.queue(function (testcase, queuecb) {
		jobTestCaseRun(api, testcase, opts, function (err) {
			if (err) {
				err = new VError(err, 'TEST FAILED: "%s"',
				    testcase['label']);
				log.fatal(err);
				throw (err);
			}

			queuecb();
		});
	}, concurrency);

	/*
	 * Finally, run the tests.
	 */
	mod_testcommon.pipeline({
	    'funcs': [
		function setup(_, next) {
			mod_testcommon.setup(function (c) {
				api = c;
				next();
			});
		},
		function runTests(_, next) {
			queue.on('end', next);
			queue.push(cases);
			queue.close();
		},
		function teardown(_, next) {
			mod_testcommon.teardown(api, next);
		}
	    ]
	});
}

/*
 * Run a single job test case.
 *
 *     api		the test context, constructed by mod_testcommon.setup().
 *
 *     testspec		test job to run, as described below.
 *
 *     options		the only supported option is:
 *
 *	    strict	If false, skip checks that assume that the supervisor
 *			and agent haven't crashed.  This is useful to test
 *			crash-recovery by making sure that jobs still complete
 *			as expected.
 *
 *     callback		Invoked upon successful completion.  (On failure, an
 *     			exception is thrown.)
 *
 * Test cases are defined (mostly) declaratively in terms of inputs, expected
 * outputs, expected errors, and the like.  The idea is to keep these as
 * self-contained as possible so that they can be tested in a number of
 * different configurations: one-at-a-time or all at once; in series or in
 * parallel; using the marlin client to submit jobs or using the front door; or
 * in a stress mode that simulates failed Moray requests.  The only parts that
 * are implemented today are the one-at-a-time, all-at-once in series, and
 * all-at-once in parallel.
 *
 * The test suite asserts a number of correctness conditions for each job,
 * including:
 *
 *     o The job completed within the allowed timeout.
 *
 *     o The job completed with the expected errors and outputs (optionally
 *       including output contents).
 *
 *     o The job completed without any job reassignments (which would indicate a
 *       worker crash) or retries (which would indicate an agent crash), unless
 *       being run in "stress" mode.
 *
 *     o The various job counters (inputs, tasks, errors, and outputs) match
 *       what we expect.
 *
 *     o If the job wasn't cancelled, then there should be no objects under the
 *       job's directory that aren't accounted for as output objects.
 *
 * Each test case specifies the following fields:
 *
 *	job
 *
 *         Job definition, including "phases" (with at least "type" and "exec",
 *         and optionally "memory", "disk", and "image) and "assets" (which
 *         should be an object mapping paths to asset content).
 *
 *	inputs
 *
 *         Names of job input objects.  These will be created automatically
 *         before submitting the job and they will be submitted as inputs to the
 *         job.  By default, the objects are populated with a basic, unique
 *         string ("auto-generated content for key ..."), but a few special
 *         patterns trigger different behavior:
 *
 *		.*dir		a directory is created instead of a key
 *
 *		.*0bytes	object is created with no content
 *
 *	timeout
 *
 *         Milliseconds to wait for a job to complete, after which the test case
 *         is considered failed.  This should describe how long the test should
 *         take when run by itself (i.e., on an uncontended system), but should
 *         include time taken to reset zones.  The "concurrent" tests will
 *         automatically bump this timeout appropriately.
 *
 * Each test must also specify either "errors" and "expected_outputs" OR
 * "error_count":
 *
 *	errors
 *
 *         Array of objects describing the errors expected from the job.  Each
 *         property of the error may be a string or a regular expression that
 *         will be compared with the actual errors emitted.  It's not a failure
 *         if the actual errors contain properties not described here.  There
 *         must be a 1:1 mapping between the job's errors and these expected
 *         errors.
 *
 *	expected_outputs
 *
 *         Array of strings or regular expressions describing the *names* of
 *         expected output objects.  There must be a 1:1 mapping between
 *         the job's outputs and these expected outputs, but they may appear in
 *         any order.
 *
 *	error_count
 *
 *         Two-element array denoting the minimum and maximum number of errors
 *         allowed, used for probabilistic tests.
 *
 * Tests may also specify a few other fields, but these are less common:
 *
 *      account
 *
 *         The SDC account login name under which to run the job.  Most jobs run
 *         under DEFAULT_USER.  This is expanded for '%jobuser%' in object names
 *         and error messages, and for '%user%' as well unless "account_objects"
 *         is also specified.
 *
 *      account_objects
 *
 *         The SDC account login name to replace inside object names for the
 *         string '%user%'.  This is only useful for tests that use multiple
 *         accounts, which are pretty much just the authn/authz tests.
 *
 *	expected_output_content
 *
 *         Array of strings or regular expressions describing the contents of
 *         the expected output objects.  If specified, there must be a 1:1
 *         mapping between the job's output contents and these expected outputs,
 *         but they may appear in any order.
 *
 *	extra_inputs
 *
 *         Array of names of job input objects that should be submitted as
 *         inputs, but should not go through the creation process described for
 *         "inputs" above.
 *
 *	extra_objects
 *
 *         Array of strings or regular expressions describing objects that may
 *         be present under the job's directory after the job has completed.
 *         The test suite automatically checks that there are no extra objects
 *         under the job directory for most jobs, but some jobs create objects
 *         as side effects, and these need to be whitelisted here.
 *
 *      legacy_auth
 *
 *         Submit the job in legacy-authz/authn mode, in which only legacy
 *         credentials are supplied with the job and only legacy authorization
 *         checks can be applied by Marlin.
 *
 *	metering_includes_checkpoints
 *
 *         If true, the test suite (when run in the appropriate mode) will
 *         verify that metering data includes at least one checkpoint record.
 *
 *	pre_submit(api, callback)
 *
 *         Callback to be invoked before the job is submitted.  "callback"
 *         should be invoked when the job is ready to be submitted.
 *
 *	post_submit(api, jobid)
 *
 *         Callback to be invoked once the job has been submitted.
 *
 *	verify(verify)
 *
 *         Callback to be invoked once the job has completed to run additional
 *         checks.
 *
 *	skip_input_end
 *
 *         If true, then the job's input stream will not be closed after the job
 *         is submitted.  You'll probably want to use post_submit to do
 *         something or else the job will likely hit its timeout.
 *
 *      user
 *
 *         The SDC account subuser under which to run the job.  Most jobs run
 *         under the DEFAULT_USER account (not a subuser).
 */
function jobTestCaseRun(api, testspec, options, callback)
{
	var login, inputs, dirs, objects;

	login = testspec['account_objects'] || testspec['account'] ||
	    DEFAULT_USER;

	/*
	 * Expand the named parameters in all of the inputs.
	 */
	inputs = testspec['inputs'].map(function (input) {
		return (replaceParams(testspec, input));
	});

	/*
	 * Partition out the objects (as opposed to directories) from the list
	 * of inputs.  Sort them and uniquify them.
	 */
	objects = inputs.filter(function (input) {
		return (!mod_jsprim.endsWith(input, 'dir'));
	}).sort();
	objects = objects.filter(function (objname, i) {
		return (i === 0 || objname != objects[i - 1]);
	});

	/*
	 * Now partition out the directories from the list of inputs.  Combine
	 * that with the dirnames of the objects (found above).  This is the
	 * full list of directories we need to precreate.  As with objects, we
	 * sort and uniquify them.
	 */
	dirs = inputs.filter(function (input) {
		return (mod_jsprim.endsWith(input, 'dir'));
	}).concat(objects.map(function (input) {
		return (mod_path.dirname(input));
	})).sort();
	dirs = dirs.filter(function (dirname, i) {
		return (i === 0 || dirname != dirs[i - 1]);
	});

	mod_vasync.waterfall([
	    function ensureAccount(subcallback) {
		log.info('making sure account "%s" exists', login);
		mod_testcommon.ensureAccount(login,
		    function (err) { subcallback(err); });
	    },
	    function populateDirs(subcallback) {
		log.info('populating directories', dirs);
		mod_vasync.forEachPipeline({
		    'inputs': dirs,
		    'func': function doMkdir(dirname, subsubcb) {
			api.manta.mkdirp(dirname, subsubcb);
		    }
		}, function (err) { subcallback(err); });
	    },
	    function populateInputs(subcallback) {
		log.info('populating objects', objects);
		populateData(api.manta, testspec, objects, subcallback);
	    },
	    function submitAndVerify(subcallback) {
		log.info('submitting job');
		jobTestSubmitAndVerify(api, testspec, options, subcallback);
	    }
	], callback);
}

/*
 * Populate the input data that will be required to execute a job.
 *
 *     manta		Manta client
 *
 *     testspec		Test case whose data should be populated
 *
 *     keys		Objects to auto-create
 *
 *     callback		Invoked upon completion (with potential error)
 */
function populateData(manta, testspec, keys, callback)
{
	var final_err, dirs, queue;

	if (keys.length === 0) {
		setImmediate(callback);
		return;
	}

	dirs = keys.filter(
	    function (key) { return (mod_jsprim.endsWith(key, 'dir')); });
	mod_assert.ok(dirs.length === 0);
	queue = mod_vasync.queuev({
	    'concurrency': 15,
	    'worker': function (key, subcallback) {
		    if (final_err) {
			    subcallback();
			    return;
		    }

		    var data;
		    if (mod_jsprim.endsWith(key, '0bytes'))
			data = '';
		    else
			data = 'auto-generated content for key /someuser' +
			    mod_path.normalize(key.substr(key.indexOf('/', 1)));

		    var stream = new StringInputStream(data);

		    log.info('PUT key "%s"', key);
		    manta.put(key, stream, { 'size': data.length },
		        function (err) {
				if (err)
					final_err = err;
				subcallback();
			});
	    }
	});

	queue.drain = function () { callback(final_err); };
	keys.forEach(function (key) {
		if (key.indexOf('notavalid') == -1)
			queue.push(key);
	});
}

/*
 * Submits a single job, waits for it to finish, and then verifies the results.
 * Arguments are:
 *
 *     api		the test context, constructed by mod_testcommon.setup().
 *
 *     testspec		test job to run, as described in ./jobs.js.
 *
 *     options		the only supported option is:
 *
 *	    strict	If false, skip checks that assume that the supervisor
 *			and agent haven't crashed.  This is useful to test
 *			crash-recovery by making sure that jobs still complete
 *			as expected.
 *
 *     callback		Invoked upon successful completion.  (On failure, an
 *     			exception is thrown.)
 */
function jobTestSubmitAndVerify(api, testspec, options, callback)
{
	jobSubmit(api, testspec, function (err, jobid) {
		if (err) {
			callback(err);
			return;
		}

		jobTestVerifyTimeout(api, testspec, jobid, options, callback);
	});
}

/*
 * Given a test case (one of the test case objects in ./jobs.js) and a string
 * value contained within the test case, replace references to '%user%' with the
 * user that we're actually going to use.
 */
function replaceParams(testspec, str)
{
	mod_assert.ok(arguments.length >= 2);
	var jobuser = testspec['account'] || DEFAULT_USER;
	var user = testspec['account_objects'] || testspec['account'] ||
	    DEFAULT_USER;

	if (typeof (str) == 'string')
		return (str.replace(/%user%/g, user).replace(
		    /%jobuser%/g, jobuser));

	mod_assert.equal(str.constructor.name, 'RegExp');
	return (new RegExp(str.source.replace(/%user%/g, user).replace(
	    /%jobuser%/g, jobuser)));
}

/*
 * Submit the given job.  This test harness uses the Marlin client to submit the
 * job directly, rather than relying on a working Muskie consumer.  But a number
 * of things have to be done first, including generating a Muskie authentication
 * token (which requires signing a URL for the "tokens" endpoint and then using
 * it), uploading assets, submitting the job, and finally adding job inputs.
 */
function jobSubmit(api, testspec, callback)
{
	var mahi, jobdef, login, url, funcs, private_key, signed_path, jobid;

	login = testspec['account'] || DEFAULT_USER;
	url = mod_url.parse(process.env['MANTA_URL']);
	mahi = mod_testcommon.mahiClient();

	jobdef = {
	    'phases': testspec['job']['phases'],
	    'name': 'mrtest: ' + testspec['label'],
	    'options': {
		'frequentCheckpoint': true
	    }
	};

	jobdef['phases'].forEach(function (p) {
		p['exec'] = replaceParams(testspec, p['exec']);
		if (p['assets'])
			p['assets'] = p['assets'].map(
			    replaceParams.bind(null, testspec));
	});

	funcs = [
	    function (_, stepcb) {
		log.info('generating auth block');
		mod_mautil.makeInternalAuthBlock({
		    'mahi': mahi,
		    'account': login,
		    'user': testspec['user'] || null,
		    'legacy': testspec['legacy_auth'],
		    'tag': 'marlin test suite'
		}, function (err, auth) {
			if (err) {
				log.error(err, 'generating auth block');
				stepcb(err);
				return;
			}

			log.debug('auth block', auth);
			jobdef['auth'] = auth;
			jobdef['owner'] = auth['uuid'];
			stepcb();
		});
	    },
	    function (_, stepcb) {
		var path = process.env['MANTA_KEY'] ||
			mod_path.join(process.env['HOME'], '.ssh/id_rsa');
		log.info('reading private key from %s', path);
		mod_fs.readFile(path, function (err, contents) {
			private_key = contents.toString('utf8');
			stepcb(err);
		});
	    },
	    function (_, stepcb) {
		log.info('creating signed URL');

		mod_manta.signUrl({
		    'algorithm': 'rsa-sha256',
		    'expires': Date.now() + 86400 * 1000,
		    'host': url['host'],
		    'keyId': process.env['MANTA_KEY_ID'],
		    'method': 'POST',
		    'path': sprintf('/%s/tokens', login),
		    'user': login,
		    'sign': mod_manta.privateKeySigner({
			'algorithm': 'rsa-sha256',
			'key': private_key,
			'keyId': process.env['MANTA_KEY_ID'],
			'log': log,
			'user': login
		    })
		}, function (err, path) {
			signed_path = path;
			stepcb(err);
		});
	    },
	    function (_, stepcb) {
		log.info('creating auth token', signed_path);

		var req = mod_http.request({
		    'method': 'POST',
		    'path': signed_path,
		    'host': url['hostname'],
		    'port': parseInt(url['port'], 10) || 80
		});

		req.end();

		req.on('response', function (response) {
			log.info('auth token response: %d',
			    response.statusCode, response.headers);

			if (response.statusCode != 201) {
				stepcb(new VError(
				    'wrong status code for auth token'));
				return;
			}

			var body = '';
			response.on('data', function (chunk) {
				body += chunk;
			});
			response.on('end', function () {
				var token = JSON.parse(body)['token'];
				jobdef['auth']['token'] = token;
				stepcb();
			});
		});
	    },
	    function (_, stepcb) {
		log.info('submitting job', jobdef);
		api.jobCreate(jobdef, { 'istest': true },
		    function (err, result) {
			jobid = result;
			stepcb(err);
		    });
	    }
	];

	if (testspec['job']['assets']) {
		mod_jsprim.forEachKey(testspec['job']['assets'],
		    function (key, content) {
			key = replaceParams(testspec, key);
			funcs.push(function (_, stepcb) {
				log.info('submitting asset "%s"', key);
				var stream = new mod_memorystream(content,
				    { 'writable': false });
				api.manta.put(key, stream,
				    { 'size': content.length }, stepcb);
			});
		    });
	}

	funcs.push(function (_, stepcb) {
		var final_err;

		if (testspec['inputs'].length === 0 &&
		    (!testspec['extra_inputs'] ||
		    testspec['extra_inputs'].length === 0)) {
			stepcb();
			return;
		}

		var queue = mod_vasync.queuev({
			'concurrency': 15,
			'worker': function (key, subcallback) {
				if (final_err) {
					subcallback();
					return;
				}

				log.info('job "%s": adding key %s',
				    jobid, key);
				api.jobAddKey(jobid, key,
				    function (err) {
					if (err)
						final_err = err;
					subcallback();
				    });
			}
		});

		testspec['inputs'].forEach(function (key) {
			queue.push(replaceParams(testspec, key));
		});

		if (testspec['extra_inputs']) {
			testspec['extra_inputs'].forEach(function (key) {
				queue.push(replaceParams(testspec, key));
			});
		}

		queue.drain = function () { stepcb(final_err); };
	});

	if (!testspec['skip_input_end']) {
		funcs.push(function (_, stepcb) {
			log.info('job "%s": ending input', jobid);
			api.jobEndInput(jobid, { 'retry': { 'retries': 3 } },
			    stepcb);
		});
	}

	if (testspec['pre_submit'])
		funcs.unshift(testspec['pre_submit']);

	mod_vasync.pipeline({ 'funcs': funcs, 'arg': api }, function (err) {
		if (!err)
			log.info('job "%s": job submission complete', jobid);
		callback(err, jobid);

		if (testspec['post_submit'])
			testspec['post_submit'](api, jobid);
	});
}

/*
 * Waits for the job to complete, then asserts the expected end conditions.
 * It's convenient to write the verification code as a set of assertions, so we
 * we allow jobTestVerify to either throw an exception or invoke its callback
 * with an error on failure.  The error indicates whether we should keep trying.
 * The idea is that we'll retry until the job is completed, but most failures
 * after that point should trigger an immediate test failure.
 */
function jobTestVerifyTimeout(api, testspec, jobid, options, callback)
{
	var interval = testspec['timeout'];

	mod_testcommon.timedCheck(Math.ceil(interval / 1000), 1000,
	    function (subcallback) {
		jobTestVerify(api, testspec, jobid, options, function (err) {
			if (err)
				err.noRetry = !err.retry;
			subcallback(err);
		});
	    }, callback);
}

/*
 * Runs through a bunch of checks common to all jobs, starting with the
 * assertion that the job has completed.  If the job hasn't finished running,
 * then we'll retry this whole process.  Most other errors result an immediate
 * test case failure.
 *
 * To keep the code relatively clean, we fetch everything about the job (its
 * state; the lists of inputs, errors, outputs; output object contents; the list
 * of objects under the job's directory; and metering records) before doing any
 * other verification.  That way, jobTestVerifyResult() has everything it might
 * need handy.
 */
function jobTestVerify(api, testspec, jobid, options, callback)
{
	mod_vasync.pipeline({
	    'arg': {
		'strict': options['strict'],
		'api': api,
		'testspec': testspec,
		'jobid': jobid,
		'job': undefined,
		'objects_found': [],
		'inputs': [],
		'outputs': [],
		'errors': [],
		'content': [],
		'metering': null
	    },
	    'funcs': [
		jobTestVerifyFetchJob,
		jobTestVerifyFetchInputs,
		jobTestVerifyFetchErrors,
		jobTestVerifyFetchOutputs,
		jobTestVerifyFetchOutputContent,
		jobTestVerifyFetchObjectsFound,
		jobTestVerifyFetchMeteringRecords,
		jobTestVerifyResult
	    ]
	}, callback);
}

/*
 * Fetch the job record and verify that the job is done.  This is the main error
 * that can cause a retry.
 */
function jobTestVerifyFetchJob(verify, callback)
{
	verify['api'].jobFetch(verify['jobid'], function (err, job) {
		if (!err && job['value']['state'] != 'done') {
			err = new VError('job is not finished');
			err.retry = true;
		}

		if (!err)
			verify['job'] = job['value'];

		callback(err);
	});
}

/*
 * Fetch the names of the job's inputs.
 */
function jobTestVerifyFetchInputs(verify, callback)
{
	var req = verify['api'].jobFetchInputs(verify['jobid']);
	req.on('key', function (key) { verify['inputs'].push(key); });
	req.on('end', callback);
	req.on('error', callback);
}

/*
 * Fetch all job errors.
 */
function jobTestVerifyFetchErrors(verify, callback)
{
	var req = verify['api'].jobFetchErrors(verify['jobid']);
	req.on('err', function (error) { verify['errors'].push(error); });
	req.on('end', callback);
	req.on('error', callback);
}

/*
 * Fetch the job's outputs object names.
 */
function jobTestVerifyFetchOutputs(verify, callback)
{
	var pi = verify['job']['phases'].length - 1;
	var req = verify['api'].jobFetchOutputs(verify['jobid'], pi,
	    { 'limit': 1000 });
	req.on('key', function (key) { verify['outputs'].push(key); });
	req.on('end', callback);
	req.on('error', callback);
}

/*
 * Fetch the contents of all output objects.
 */
function jobTestVerifyFetchOutputContent(verify, callback)
{
	var manta = verify['api'].manta;
	var concurrency = 15;
	var errors = [];
	var queue;

	queue = mod_vasync.queue(
	    function fetchOneOutputContent(objectpath, qcallback) {
		log.info('fetching output "%s"', objectpath);
		manta.get(objectpath, function (err, mantastream) {
			if (err) {
				log.warn(err, 'error fetching output "%s"',
				    objectpath);
				errors.push(err);
				qcallback();
				return;
			}

			var data = '';
			mantastream.on('data', function (chunk) {
				data += chunk.toString('utf8');
			});

			mantastream.on('end', function () {
				verify['content'].push(data);
				qcallback();
			});
		});
	    }, concurrency);
	queue.on('end', function () {
		if (errors.length === 0) {
			callback();
		} else {
			callback(new VError(errors[0],
			    'first of "%d" errors', errors.length));
		}
	});
	verify['outputs'].forEach(function (o) { queue.push(o); });
	queue.close();
}

/*
 * Fetch the full list of objects under /%user/jobs/%jobid/stor to make sure
 * we've cleaned up intermediate objects.
 */
function jobTestVerifyFetchObjectsFound(verify, callback)
{
	/*
	 * It would be good if this were in node-manta as a library function,
	 * but until then, we use the command-line version to avoid reinventing
	 * the wheel.
	 */
	var cmd = mod_path.join(__dirname,
	    '../../node_modules/manta/bin/mfind');

	mod_child.execFile(process.execPath, [ cmd, '-i', '-t', 'o',
	    replaceParams(verify['testspec'],
	        '/%user%/jobs/' + verify['jobid'] + '/stor') ],
	    function (err, stdout, stderr) {
		if (err) {
			callback(new VError(err, 'failed to list job ' +
			    'objects: stderr = %s', JSON.stringify(stderr)));
			return;
		}

		var lines = stdout.split('\n');
		mod_assert.equal(lines[lines.length - 1], '');
		lines = lines.slice(0, lines.length - 1);

		verify['objects_found'] = verify['objects_found'].concat(lines);
		verify['objects_found'].sort();
		callback();
	    });
}

/*
 * Fetch metering records from the agent log, if available.
 */
function jobTestVerifyFetchMeteringRecords(verify, callback)
{
	var agentlog = process.env['MARLIN_METERING_LOG'];

	if (!agentlog) {
		log.warn('not checking metering records ' +
		    '(MARLIN_METERING_LOG not set)');
		callback();
		return;
	}

	var barrier = mod_vasync.barrier();
	verify['metering'] = {};
	[ 'deltas', 'cumulative' ].forEach(function (type) {
		barrier.start(type);

		var reader = new (mod_marlin.MarlinMeterReader)({
		    'stream': mod_fs.createReadStream(agentlog),
		    'summaryType': type,
		    'aggrKey': [ 'jobid', 'taskid' ],
		    'startTime': verify['job']['timeCreated'],
		    'resources': [ 'time', 'nrecords', 'cpu', 'memory',
		        'vfs', 'zfs', 'vnic0' ]
		});

		reader.on('end', function () {
			verify['metering'][type] =
			    reader.reportHierarchical()[verify['jobid']];
			barrier.done(type);
		});
	});

	barrier.on('drain', callback);
}

/*
 * Wrapper for jobTestVerifyResultSync that causes us to retry in certain cases.
 */
function jobTestVerifyResult(verify, callback)
{
	try {
		jobTestVerifyResultSync(verify);
	} catch (ex) {
		if (ex.name == 'AssertionError')
			ex.message = ex.toString();
		var err = new VError(ex, 'verifying job "%s" (test "%s")',
		    verify['jobid'], verify['job']['name']);

		/*
		 * Cancelled jobs may take a few extra seconds for the metering
		 * records to be written out.  Retry this whole fetch process
		 * for up to 10 seconds to deal with that case.
		 */
		if (verify['job']['timeCancelled'] !== undefined &&
		    Date.now() - Date.parse(
		    verify['job']['timeDone']) < 10000) {
			err.retry = true;
			log.error(err,
			    'retrying due to error on cancelled job');
		} else {
			log.fatal(err);
		}

		callback(err);
		return;
	}

	callback();
}

/*
 * Verifies the standard job postconditions.
 */
function jobTestVerifyResultSync(verify)
{
	var job = verify['job'];
	var inputs = verify['inputs'];
	var outputs = verify['outputs'];
	var joberrors = verify['errors'];
	var testspec = verify['testspec'];
	var strict = verify['strict'];
	var foundobj = verify['objects_found'];
	var expected_inputs, expected_outputs, i;

	/* Verify the behavior of jobSubmit, jobFetch, and jobFetchInputs. */
	mod_assert.deepEqual(testspec['job']['phases'], job['phases']);
	expected_inputs = testspec['inputs'].map(
	    replaceParams.bind(null, testspec));
	if (testspec['extra_inputs'])
		expected_inputs = expected_inputs.concat(
		    testspec['extra_inputs'].map(
		    replaceParams.bind(null, testspec)));

	/*
	 * We don't really need to verify this for very large jobs, and it's
	 * non-trivial to do so.
	 */
	if (expected_inputs.length <= 1000)
		mod_assert.deepEqual(expected_inputs.sort(), inputs.sort());

	/* Wait for the job to be completed. */
	mod_assert.equal(job['state'], 'done');

	/* Sanity-check the rest of the job record. */
	mod_assert.ok(job['worker']);
	mod_assert.ok(job['timeInputDone'] >= job['timeCreated']);
	mod_assert.ok(job['timeDone'] >= job['timeCreated']);

	/*
	 * Check job execution and jobFetchOutputs.  Only probabilistic tests
	 * are allowed to omit expected_outputs.
	 */
	if (testspec['expected_outputs']) {
		expected_outputs = testspec['expected_outputs'].map(
		    replaceParams.bind(null, testspec)).sort();
		outputs.sort();
		mod_assert.equal(outputs.length, expected_outputs.length);

		for (i = 0; i < outputs.length; i++) {
			if (typeof (expected_outputs[i]) == 'string')
				mod_assert.equal(expected_outputs[i],
				    outputs[i],
				    'output ' + i + ' doesn\'t match');
			else
				mod_assert.ok(
				    expected_outputs[i].test(outputs[i]),
				    'output ' + i + ' doesn\'t match');
		}
	} else {
		mod_assert.ok(testspec['error_count'],
		    'tests must specify either "error_count" ' +
		    'or "expected_outputs"');
	}

	/* Check job execution and jobFetchErrors */
	if (!strict)
		/* Allow retried errors in non-strict mode. */
		joberrors = joberrors.filter(
		    function (error) { return (!error['retried']); });

	if (!testspec['error_count']) {
		mod_assert.equal(joberrors.length, testspec['errors'].length);
	} else {
		mod_assert.ok(joberrors.length >= testspec['error_count'][0]);
		mod_assert.ok(joberrors.length <= testspec['error_count'][1]);
	}

	testspec['errors'].forEach(function (expected_error, idx) {
		var okay = false;

		joberrors.forEach(function (actual_error) {
			var match = true;
			var exp;

			for (var k in expected_error) {
				exp = replaceParams(
				    testspec, expected_error[k]);
				if (typeof (expected_error[k]) == 'string') {
					/*
					 * In non-strict modes, cancelled jobs
					 * are allowed to fail with EM_INTERNAL
					 * instead of EM_JOBCANCELLED.
					 */
					if (!strict &&
					    exp['code'] == EM_JOBCANCELLED &&
					    actual_error['code'] ==
					    EM_INTERNAL &&
					    (k == 'code' || k == 'message')) {
						log.warn('expected %s, ' +
						    'but got %s, which is ' +
						    'okay in non-strict mode',
						    exp[k], actual_error[k]);
						continue;
					}
					if (actual_error[k] !== exp) {
						match = false;
						break;
					}
				} else {
					if (!exp.test(actual_error[k])) {
						match = false;
						break;
					}
				}
			}

			if (match)
				okay = true;
		});

		if (!okay)
			throw (new VError('no match for error %d: %j %j',
			    idx, expected_error, joberrors));
	});

	/* Check stat counters. */
	var stats = job['stats'];

	if (strict) {
		mod_assert.equal(stats['nAssigns'], 1);
		mod_assert.equal(stats['nRetries'], 0);
	}

	mod_assert.equal(stats['nErrors'], verify['errors'].length);
	mod_assert.equal(stats['nInputsRead'], verify['inputs'].length);
	mod_assert.equal(stats['nJobOutputs'], verify['outputs'].length);
	mod_assert.equal(stats['nTasksDispatched'],
	    stats['nTasksCommittedOk'] + stats['nTasksCommittedFail']);

	/*
	 * For every failed task, there should be either a retry or an error.
	 * But there may be errors for inputs that were never dispatched as
	 * tasks (e.g., because the object didn't exist).
	 */
	mod_assert.ok(stats['nTasksCommittedFail'] <=
	    stats['nRetries'] + stats['nErrors']);

	/*
	 * Check metering records.  This is not part of the usual tests, but can
	 * be run on-demand by running the test suite in the global zone (where
	 * we have access to the metering records).  This also assumes that all
	 * tasks executed on this server.
	 */
	if (verify['metering'] !== null) {
		var sum, taskid;

		if (testspec['metering_includes_checkpoints']) {
			sum = 0;
			for (taskid in verify['metering']['deltas']) {
				if (verify['metering']['deltas'][taskid][
				    'nrecords'] > 1)
					sum++;
			}

			if (sum === 0) {
				log.error('expected metering checkpoints',
				    verify['metering']['deltas']);
				throw (new Error(
				    'expected metering checkpoints'));
			}
		}

		/*
		 * The "deltas" and "cumulative" reports should be identical
		 * except for the "nrecords" column.
		 */
		for (taskid in verify['metering']['deltas']) {
			if (verify['metering']['deltas'][
			    taskid]['nrecords'] != 1)
				log.info('checkpoint records found');
			delete (verify['metering']['deltas'][
			    taskid]['nrecords']);
		}

		var cumul = verify['metering']['cumulative'];
		for (taskid in cumul) {
			mod_assert.equal(1, cumul[taskid]['nrecords']);
			delete (cumul[taskid]['nrecords']);
		}

		mod_assert.deepEqual(cumul, verify['metering']['deltas']);

		/*
		 * For cancelled jobs, it's possible that some of the tasks
		 * never even ran.  Ditto jobs with agent dispatch errors.
		 */
		var expected_count = verify['job']['stats']['nTasksDispatched'];
		for (i = 0; i < joberrors.length; i++) {
			if (joberrors[i]['message'].indexOf(
			    'failed to dispatch task') != -1)
				--expected_count;
		}
		if (verify['job']['timeCancelled'] === undefined &&
		    expected_count > 0)
			mod_assert.equal(Object.keys(cumul).length,
			    expected_count);
	}

	/*
	 * Check for intermediate objects.  We don't do this in non-strict mode
	 * because agent restarts can result in extra copies of stderr and core
	 * files.
	 */
	if (strict && job['timeCancelled'] === undefined) {
		/*
		 * "foundobj" may not contain all output objects, since some of
		 * those may have been given names somewhere else.  But it must
		 * not contain any objects that aren't final outputs or stderr
		 * objects.
		 */
		var dedup = {};
		outputs.forEach(function (o) { dedup[o] = true; });
		joberrors.forEach(function (e) {
			if (e['stderr'])
				dedup[e['stderr']] = true;
			if (e['core'])
				dedup[e['core']] = true;
		});
		foundobj = foundobj.filter(function (o) {
			if (dedup[o])
				return (false);

			/*
			 * If the test case explicitly allows some extra
			 * objects, ignore them here.
			 */
			if (testspec['extra_objects']) {
				var pattern, extra;
				for (j = 0;
				    j < testspec['extra_objects'].length; j++) {
					extra = testspec['extra_objects'][j];
					pattern = replaceParams(
					    testspec, extra);
					if (pattern.test(o)) {
						log.info(
						    'ignoring extra object', o);
						return (false);
					}
				}
			}

			/*
			 * It's possible to see last-phase output objects that
			 * are not final outputs because the corresponding task
			 * actually failed.  These are legit: no sense removing
			 * them.
			 */
			var match = /.*\.(\d+)\.[0-9a-z-]{36}$/.exec(o);
			if (!match)
				return (true);

			if (parseInt(match[1], 10) ==
			    testspec['job']['phases'].length - 1)
				return (false);

			/*
			 * There's one other corner case, which is when we
			 * successfully emit an intermediate object, but fail to
			 * actually forward it onto the next phase.  The only
			 * way this can happen today is when the object is
			 * designated for a non-existent reducer.  We ignore
			 * this case, effectively saying that the semantics of
			 * this are that the intermediate object is made
			 * available to the user in this error case.
			 */
			var j, e;
			for (j = 0; j < joberrors.length; j++) {
				e = joberrors[j];
				if (e['input'] == o &&
				    e['code'] == EM_INVALIDARGUMENT &&
				    /* JSSTYLED */
				    /reducer "\d+" specified, but only/.test(
				    e['message']))
					return (false);
			}

			return (true);
		});

		if (foundobj.length !== 0) {
			log.error('extra objects found', foundobj);
			throw (new Error('extra objects found'));
		}
	}

	/* Check custom verifiers. */
	if (testspec['verify'])
		testspec['verify'](verify);

	/* Check output content */
	if (!testspec['expected_output_content'])
		return;

	var bodies = verify['content'];
	var expected = testspec['expected_output_content'].map(function (o) {
		var n;
		if (typeof (o) == 'string')
			n = o.replace(/\$jobid/g, verify['jobid']);
		else
			n = new RegExp(o.source.replace(
			    /\$jobid/g, verify['jobid']));
	        return (replaceParams(testspec, n));
	});
	bodies.sort();
	expected.sort();

	mod_assert.equal(bodies.length, expected.length,
	    'wrong number of output files');

	for (i = 0; i < expected.length; i++) {
		if (typeof (expected[i]) == 'string')
			mod_assert.equal(bodies[i], expected[i],
			    sprintf('output "%d" didn\'t match ' +
			        '(expected "%s", got "%s")', i, expected[i],
				bodies[i]));
		else
			mod_assert.ok(expected[i].test(bodies[i]),
			    sprintf('output "%d" didn\'t match ' +
			        '(expected "%s", got "%s")', i, expected[i],
				bodies[i]));
	}
}
