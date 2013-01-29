/*
 * test/live/jobs.js: test job definitions used by multiple tests
 */

var mod_assert = require('assert');
var mod_fs = require('fs');
var mod_http = require('http');
var mod_path = require('path');
var mod_stream = require('stream');
var mod_url = require('url');
var mod_util = require('util');

var mod_extsprintf = require('extsprintf');
var mod_jsprim = require('jsprim');
var mod_manta = require('manta');
var mod_memorystream = require('memorystream');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_testcommon = require('../common');

var sprintf = mod_extsprintf.sprintf;
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
    'errors': []
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
    'timeout': 60 * 1000,
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
    'errors': []
};

exports.jobMmpipeAnon = {
    'job': {
	'phases': [ {
	    'type': 'storage-map',
	    'exec': 'echo foo; echo bar | mpipe'
	} ]
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
    'expected_output_content': [ 'bar\n', 'bar\n', 'bar\n' ],
    'errors': []
};

exports.jobMmpipeNamed = {
    'job': {
	'phases': [ {
	    'type': 'storage-map',
	    'exec': 'echo foo; echo bar | mpipe -p /poseidon/stor/extra/out1'
	} ]
    },
    'inputs': [
	'/poseidon/stor/obj1',
	'/poseidon/stor/obj2',
	'/poseidon/stor/obj3'
    ],
    'timeout': 15 * 1000,
    'expected_outputs': [
	'/poseidon/stor/extra/out1',
	'/poseidon/stor/extra/out1',
	'/poseidon/stor/extra/out1'
    ],
    'expected_output_content': [ 'bar\n', 'bar\n', 'bar\n' ],
    'errors': []
};

exports.jobMmcat = {
    'job': {
	'phases': [ {
	    'type': 'storage-map',
	    'exec': 'echo foo; mcat /poseidon/stor/obj1'
	} ]
    },
    'inputs': [
	'/poseidon/stor/obj1',
	'/poseidon/stor/obj2',
	'/poseidon/stor/obj3'
    ],
    'timeout': 15 * 1000,
    'expected_outputs': [
	'/poseidon/stor/obj1',
	'/poseidon/stor/obj1',
	'/poseidon/stor/obj1'
    ],
    'expected_output_content': [
	'auto-generated content for key /poseidon/stor/obj1',
	'auto-generated content for key /poseidon/stor/obj1',
	'auto-generated content for key /poseidon/stor/obj1'
    ],
    'errors': []
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
    'timeout': 60 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.1\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj2\.1\./,
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj3\.1\./
    ],
    'errors': []
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
    'timeout': 30 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/reduce\.0\./
    ],
    'errors': []
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
    'timeout': 60 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/reduce\.1\./
    ],
    'errors': []
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
    'timeout': 90 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/reduce\.3\./
    ],
    'errors': []
};

exports.jobM500 = {
    'job': {
	'phases': [ { 'type': 'storage-map', 'exec': 'wc' } ]
    },
    'inputs': [],
    'timeout': 90 * 1000,
    'expected_outputs': [],
    'errors': []
};

exports.jobMRRoutput = {
    'job': {
	'phases': [ {
	    'type': 'storage-map',
	    'exec': 'for i in {1..10}; do echo $i; done | msplit -n 3'
	}, {
	    'type': 'reduce',
	    'count': 3,
	    'exec': 'awk \'{sum+=$1} END {print sum}\''
	}, {
	    'type': 'reduce',
	    'exec': 'awk \'{sum+=$1} END {print sum}\''
	} ]
    },
    'inputs': [ '/poseidon/stor/obj1' ],
    'timeout': 90 * 1000,
    'expected_outputs': [ /\/poseidon\/jobs\/.*\/stor\/reduce\.2\./ ],
    'expected_output_content': [ '55\n' ],
    'errors': []
};

var asset_body = [
    '#!/bin/bash\n',
    '\n',
    'echo "sarabi" "$*"\n'
].join('\n');

exports.jobMasset = {
    'job': {
	'assets': {
	    '/poseidon/stor/test_asset.sh': asset_body
	},
	'phases': [ {
	    'assets': [ '/poseidon/stor/test_asset.sh' ],
	    'type': 'storage-map',
	    'exec': '/assets/poseidon/stor/test_asset.sh 17'
	} ]
    },
    'inputs': [ '/poseidon/stor/obj1' ],
    'timeout': 15 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./
    ],
    'expected_output_content': [ 'sarabi 17\n' ],
    'errors': []
};

exports.jobM0bi = {
    'job': {
	'phases': [ { 'type': 'storage-map', 'exec': 'wc' } ]
    },
    'inputs': [ '/poseidon/stor/0bytes' ],
    'timeout': 15 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/0bytes\.0\./
    ],
    'expected_output_content': [ '0 0 0\n' ],
    'errors': []
};

/*
 * It's surprising that this output is different than the analogous 1-phase map
 * job, but it is, because GNU wc's output is different when you "wc <
 * 0-byte-file" than when you "emit_zero_byte_stream | wc".
 */
exports.jobR0bi = {
    'job': {
	'phases': [ { 'type': 'reduce', 'exec': 'wc' } ]
    },
    'inputs': [ '/poseidon/stor/0bytes' ],
    'timeout': 15 * 1000,
    'expected_outputs': [ /\/poseidon\/jobs\/.*\/stor\/reduce\.0\./ ],
    'expected_output_content': [ '      0       0       0\n' ],
    'errors': []
};

exports.jobM0bo = {
    'job': {
	'phases': [ { 'type': 'storage-map', 'exec': 'true' } ]
    },
    'inputs': [ '/poseidon/stor/obj1' ],
    'timeout': 15 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./
    ],
    'expected_output_content': [ '' ],
    'errors': []
};

exports.jobMcore = {
    'job': {
	'phases': [ {
	    'type': 'storage-map',
	    'exec': 'node -e "process.abort();"'
	} ]
    },
    'inputs': [ '/poseidon/stor/obj1' ],
    'timeout': 20 * 1000,
    'expected_outputs': [],
    'errors': [ {
	'phaseNum': '0',
	'what': 'phase 0: map input "/poseidon/stor/obj1"',
	'key': '/poseidon/stor/obj1',
	'p0key': '/poseidon/stor/obj1',
	'code': 'EJ_USER',
	'message': 'user command or child process dumped core'
    } ]
};

exports.jobR0inputs = {
    'job': {
	'phases': [ {
	    'type': 'reduce',
	    'exec': 'wc'
	} ]
    },
    'inputs': [],
    'timeout': 15 * 1000,
    'expected_outputs': [ /\/poseidon\/jobs\/.*\/stor\/reduce\.0\./ ],
    'errors': [],
    'expected_output_content': [ '      0       0       0\n' ]
};

exports.jobMenv = {
    'job': {
	'phases': [ {
	    'type': 'storage-map',
	    'exec': 'env | egrep ^MANTA_ | sort'
	} ]
    },
    'inputs': [ '/poseidon/stor/obj1' ],
    'timeout': 10 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./
    ],
    'errors': [],
    'expected_output_content': [
	'MANTA_INPUT_FILE=/manta/poseidon/stor/obj1\n' +
	'MANTA_INPUT_OBJECT=/poseidon/stor/obj1\n' +
	'MANTA_JOB_ID=$jobid\n' +
	'MANTA_NO_AUTH=true\n' +
	'MANTA_OUTPUT_BASE=/poseidon/jobs/$jobid/stor/' +
	    'poseidon/stor/obj1.0.\n' +
	'MANTA_URL=http://localhost:8080/\n'
    ]
};

exports.jobRenv = {
    'job': {
	'phases': [ {
	    'type': 'reduce',
	    'count': 3,
	    /* Workaround MANTA-992 */
	    'exec': 'cat > /dev/null; env | egrep ^MANTA_ | sort'
	} ]
    },
    'inputs': [],
    'timeout': 30 * 1000,
    'expected_outputs': [
	/\/poseidon\/jobs\/.*\/stor\/reduce\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/reduce\.0\./,
	/\/poseidon\/jobs\/.*\/stor\/reduce\.0\./
    ],
    'errors': [],
    'expected_output_content': [
	'MANTA_JOB_ID=$jobid\n' +
	'MANTA_NO_AUTH=true\n' +
	'MANTA_OUTPUT_BASE=/poseidon/jobs/$jobid/stor/reduce.0.\n' +
	'MANTA_REDUCER=0\n' +
	'MANTA_URL=http://localhost:8080/\n',

	'MANTA_JOB_ID=$jobid\n' +
	'MANTA_NO_AUTH=true\n' +
	'MANTA_OUTPUT_BASE=/poseidon/jobs/$jobid/stor/reduce.0.\n' +
	'MANTA_REDUCER=1\n' +
	'MANTA_URL=http://localhost:8080/\n',

	'MANTA_JOB_ID=$jobid\n' +
	'MANTA_NO_AUTH=true\n' +
	'MANTA_OUTPUT_BASE=/poseidon/jobs/$jobid/stor/reduce.0.\n' +
	'MANTA_REDUCER=2\n' +
	'MANTA_URL=http://localhost:8080/\n'
    ]
};

exports.jobMcancel = {
    'job': {
	'phases': [
	    { 'type': 'storage-map', 'exec': 'wc && sleep 3600' }
	]
    },
    'inputs': [
	'/poseidon/stor/obj1',
	'/poseidon/stor/obj2',
	'/poseidon/stor/obj3'
    ],
    'timeout': 30 * 1000,
    'expected_outputs': [],
    'post_submit': function (api, jobid) {
	setTimeout(function () {
		log.info('cancelling job');
		api.jobCancel(jobid, function (err) {
			if (err) {
				log.fatal('failed to cancel job');
				throw (err);
			}

			log.info('job cancelled');
		});
	}, 10 * 1000);
    },
    'errors': []
};

exports.jobsAll = [
    exports.jobM,
    exports.jobMX,
    exports.jobMmpipeAnon,
    exports.jobMmpipeNamed,
    exports.jobMmcat,
    exports.jobM0bi,
    exports.jobR0bi,
    exports.jobM0bo,
    exports.jobR,
    exports.jobR0inputs,
    exports.jobMM,
    exports.jobMR,
    exports.jobMMRR,
    exports.jobMRRoutput,
    exports.jobMcancel,
    exports.jobMasset,
    exports.jobMcore,
    exports.jobMenv,
    exports.jobRenv
];

function initJobs()
{
	var job = exports.jobM500;

	for (var i = 0; i < 500; i++) {
		var key = '/poseidon/stor/obj' + i;
		var okey = '/poseidon/jobs/.*/stor' + key;

		job['inputs'].push(key);
		job['expected_outputs'].push(new RegExp(okey));
	}
}

initJobs();

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
	var jobdef, login, url, funcs, private_key, signed_path, jobid;

	login = process.env['MANTA_USER'];
	url = mod_url.parse(process.env['MANTA_URL']);

	if (!login) {
		process.nextTick(function () {
			callback(new VError(
			    'MANTA_USER must be specified in the environment'));
		});
		return;
	}

	jobdef = {
	    'auth': {
		'login': login,
		'groups': [ 'operator' ] /* XXX */
	    },
	    'phases': testspec['job']['phases']
	};

	if (testspec['input'])
		jobdef['input'] = testspec['input'];

	funcs = [
	    function (_, stepcb) {
		log.info('looking up user "%s"', login);
		mod_testcommon.loginLookup(login, function (err, owner) {
			jobdef['auth']['uuid'] = owner;
			jobdef['owner'] = owner;
			stepcb(err);
		});
	    },
	    function (_, stepcb) {
		/*
		 * XXX It sucks that we're hardcoding the path to a particular
		 * key here given that node-manta.git has magic for extracting
		 * the right key from the agent or ~/.ssh based on the
		 * fingerprint.
		 */
		var path = mod_path.join(process.env['HOME'], '.ssh/id_rsa');
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
			    response.statusCode);

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
				jobdef['authToken'] = token;
				stepcb();
			});
		});
	    },
	    function (_, stepcb) {
		log.info('submitting job', jobdef);
		api.jobCreate(jobdef, function (err, result) {
			jobid = result;
			stepcb(err);
		});
	    }
	];

	if (testspec['job']['assets']) {
		mod_jsprim.forEachKey(testspec['job']['assets'],
		    function (key, content) {
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

		if (testspec['inputs'].length === 0) {
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
			queue.push(key);
		});

		queue.drain = function () { stepcb(final_err); };
	});

	funcs.push(function (_, stepcb) {
		log.info('job "%s": ending input', jobid);
		api.jobEndInput(jobid, { 'retry': { 'retries': 3 } },
		    stepcb);
	});

	mod_vasync.pipeline({ 'funcs': funcs }, function (err) {
		if (!err)
			log.info('job "%s": job submission complete', jobid);
		callback(err, jobid);

		if (testspec['post_submit'])
			testspec['post_submit'](api, jobid);
	});
}

function jobTestVerifyTimeout(api, testspec, jobid, callback)
{
	var interval = testspec['timeout'];

	mod_testcommon.timedCheck(Math.ceil(interval / 1000), 1000,
	    function (subcallback) {
		jobTestVerify(api, testspec, jobid, function (err) {
			if (err)
				err.noRetry = !err.retry;
			subcallback(err);
		});
	    }, callback);
}

function jobTestVerify(api, testspec, jobid, callback)
{
	mod_vasync.pipeline({
	    'arg': {
		'api': api,
		'testspec': testspec,
		'jobid': jobid,
		'job': undefined,
		'inputs': [],
		'outputs': [],
		'errors': [],
		'content': []
	    },
	    'funcs': [
		jobTestVerifyFetchJob,
		jobTestVerifyFetchInputs,
		jobTestVerifyFetchErrors,
		jobTestVerifyFetchOutputs,
		jobTestVerifyFetchOutputContent,
		jobTestVerifyResult
	    ]
	}, callback);
}

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

function jobTestVerifyFetchInputs(verify, callback)
{
	var req = verify['api'].jobFetchInputs(verify['jobid']);
	req.on('key', function (key) { verify['inputs'].push(key); });
	req.on('end', callback);
	req.on('error', callback);
}

function jobTestVerifyFetchErrors(verify, callback)
{
	var req = verify['api'].jobFetchErrors(verify['jobid']);
	req.on('err', function (error) { verify['errors'].push(error); });
	req.on('end', callback);
	req.on('error', callback);
}

function jobTestVerifyFetchOutputs(verify, callback)
{
	var pi = verify['job']['phases'].length - 1;
	var req = verify['api'].jobFetchOutputs(verify['jobid'], pi,
	    { 'limit': 1000 });
	req.on('key', function (key) { verify['outputs'].push(key); });
	req.on('end', callback);
	req.on('error', callback);
}

function jobTestVerifyFetchOutputContent(verify, callback)
{
	var manta = verify['api'].manta;

	mod_vasync.forEachParallel({
	    'inputs': verify['outputs'],
	    'func': function (objectpath, subcallback) {
		log.info('fetching output "%s"', objectpath);
		manta.get(objectpath, function (err, mantastream) {
			if (err) {
				subcallback(err);
				return;
			}

			var data = '';
			mantastream.on('data', function (chunk) {
				data += chunk.toString('utf8');
			});

			mantastream.on('end', function () {
				verify['content'].push(data);
				subcallback();
			});
		});
	    }
	}, callback);
}

function jobTestVerifyResult(verify, callback)
{
	try {
		jobTestVerifyResultSync(verify);
	} catch (ex) {
		if (ex.name == 'AssertionError')
			ex.message = ex.toString();
		var err = new VError(ex, 'verifying job "%s"', verify['jobid']);
		log.fatal(err);
		callback(err);
		return;
	}

	callback();
}

function jobTestVerifyResultSync(verify)
{
	var job = verify['job'];
	var inputs = verify['inputs'];
	var outputs = verify['outputs'];
	var errors = verify['errors'];
	var testspec = verify['testspec'];

	/* verify jobSubmit, jobFetch, jobFetchInputs */
	mod_assert.deepEqual(testspec['job']['phases'], job['phases']);
	mod_assert.deepEqual(testspec['inputs'].slice(0).sort(), inputs.sort());

	/* Wait for the job to be completed. */
	mod_assert.equal(job['state'], 'done');

	/* Sanity-check the rest of the job record. */
	mod_assert.ok(job['worker']);
	mod_assert.ok(job['timeInputDone'] >= job['timeCreated']);
	mod_assert.ok(job['timeDone'] >= job['timeCreated']);

	/* Check job execution and jobFetchOutputs */
	var expected_outputs = testspec['expected_outputs'].slice(0);
	expected_outputs.sort();
	outputs.sort();
	mod_assert.equal(outputs.length, expected_outputs.length);

	for (var i = 0; i < outputs.length; i++) {
		if (typeof (expected_outputs[i]) == 'string')
			mod_assert.equal(expected_outputs[i], outputs[i],
			    'output ' + i + ' doesn\'t match');
		else
			mod_assert.ok(expected_outputs[i].test(outputs[i]),
			    'output ' + i + ' doesn\'t match');
	}

	/* Check job execution and jobFetchErrors */
	mod_assert.equal(errors.length, testspec['errors'].length);
	testspec['errors'].forEach(function (expected_error, idx) {
		var okay = false;

		errors.forEach(function (actual_error) {
			var match = true;

			for (var k in expected_error) {
				if (actual_error[k] !== expected_error[k]) {
					match = false;
					break;
				}
			}

			if (match)
				okay = true;
		});

		if (!okay)
			throw (new VError('no match for error %d: %j %j',
			    idx, expected_error, errors));
	});

	/* Check stat counters. */
	var stats = job['stats'];
	mod_assert.equal(stats['nAssigns'], 1);
	mod_assert.equal(stats['nInputsRead'], testspec['inputs'].length);
	mod_assert.equal(stats['nJobOutputs'], outputs.length);

	if (job['timeCancelled'] === undefined)
		mod_assert.ok(stats['nTasksDispatched'],
		    stats['nTasksCommittedOk'] + stats['nTasksCommittedFail']);

	/* Check output content */
	if (!testspec['expected_output_content'])
		return;

	var bodies = verify['content'];
	var expected = testspec['expected_output_content'].map(
	    function (o) { return (o.replace(/\$jobid/g, verify['jobid'])); });
	var j;
	for (i = 0; i < expected.length; i++) {
		for (j = 0; j < bodies.length; j++) {
			if (expected[i] == bodies[j])
				break;
		}

		if (j == bodies.length)
			log.fatal('expected content not found',
			    expected[i], bodies);
		mod_assert.ok(j < bodies.length, 'expected content not found');
		log.info('output matched', bodies[j]);
		bodies.splice(j, 1);
	}
}

function jobTestVerifyOutputs(api, testspec, outputs, callback)
{
	var funcs = [];

	if (!testspec['expected_output_content']) {
		callback();
		return;
	}

	outputs.forEach(function (output) {
		funcs.push(function (_, stepcb) {
			log.info('fetching output "%s"', output);
			api.manta.get(output, function (err, mantastream) {
				if (err) {
					stepcb(err);
					return;
				}

				var data = '';
				mantastream.on('data', function (chunk) {
					data += chunk.toString('utf8');
				});

				mantastream.on('end', function () {
					stepcb(null, data);
				});
			});
		});
	});

	mod_vasync.pipeline({
	    'funcs': funcs
	}, function (err, results) {
		if (err) {
			callback(err);
			return;
		}

		var bodies = results.successes;
		var expected = testspec['expected_output_content'];
		var i, j;

		for (i = 0; i < expected.length; i++) {
			for (j = 0; j < bodies.length; j++) {
				if (expected[i] == bodies[j])
					break;
			}

			if (j == bodies.length)
				log.fatal('expected content not found',
				    expected[i], bodies);
			mod_assert.ok(j < bodies.length,
			    'expected content not found');
			log.info('output matched', bodies[j]);
			bodies = bodies.splice(j, 1);
		}

		callback();
	});
}

function populateData(manta, keys, callback)
{
	log.info('populating keys', keys);

	if (keys.length === 0) {
		callback();
		return;
	}

	var final_err;

	var queue = mod_vasync.queuev({
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
			data = 'auto-generated content for key ' + key;

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

	keys.forEach(function (key) { queue.push(key); });
	queue.drain = function () { callback(final_err); };
}

function StringInputStream(contents)
{
	mod_stream.Stream();

	this.s_data = contents;
	this.s_paused = false;
	this.s_done = false;

	this.scheduleEmit();
}

mod_util.inherits(StringInputStream, mod_stream.Stream);

StringInputStream.prototype.pause = function ()
{
	this.s_paused = true;
};

StringInputStream.prototype.resume = function ()
{
	this.s_paused = false;
	this.scheduleEmit();
};

StringInputStream.prototype.scheduleEmit = function ()
{
	var stream = this;

	process.nextTick(function () {
		if (stream.s_paused || stream.s_done)
			return;

		stream.emit('data', stream.s_data);
		stream.emit('end');

		stream.s_data = null;
		stream.s_done = true;
	});
};

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
