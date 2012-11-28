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
    'verify': function (testspec, jobresult, callback) {
	mod_assert.equal(jobresult['taskinput'].length, 0);
	mod_assert.equal(jobresult['taskoutput'].length, 0);
	mod_assert.equal(jobresult['task'].length, 3);
	mod_assert.equal(jobresult['jobinput'].length, 3);
	callback();
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
    'verify': function (testspec, jobresult, callback) {
	mod_assert.equal(jobresult['taskinput'].length, 0);
	mod_assert.equal(jobresult['taskoutput'].length, 9);
	mod_assert.equal(jobresult['task'].length, 3);
	mod_assert.equal(jobresult['jobinput'].length, 3);
	callback();
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
    'timeout': 60 * 1000,
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
    'verify': function (testspec, jobresult, callback) {
	mod_assert.equal(jobresult['taskinput'].length, 0);
	mod_assert.equal(jobresult['taskoutput'].length, 0);
	mod_assert.equal(jobresult['jobinput'].length, 3);
	callback();
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
    'timeout': 30 * 1000,
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
    'verify': function (testspec, jobresult, callback) {
	mod_assert.equal(jobresult['taskinput'].length, 3);
	mod_assert.equal(jobresult['taskoutput'].length, 0);
	mod_assert.equal(jobresult['task'].length, 1);
	mod_assert.equal(jobresult['jobinput'].length, 3);
	callback();
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
    'timeout': 60 * 1000,
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
    'verify': function (testspec, jobresult, callback) {
	mod_assert.equal(jobresult['taskinput'].length, 3);
	mod_assert.equal(jobresult['taskoutput'].length, 0);
	mod_assert.equal(jobresult['task'].length, 4);
	mod_assert.equal(jobresult['jobinput'].length, 3);
	callback();
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
    'timeout': 90 * 1000,
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
    'verify': function (testspec, jobresult, callback) {
	mod_assert.equal(jobresult['taskinput'].length, 4);
	mod_assert.equal(jobresult['taskoutput'].length, 0);
	mod_assert.equal(jobresult['jobinput'].length, 3);
	callback();
    }
};

exports.jobM500 = {
    'job': {
	'phases': [ { 'type': 'storage-map', 'exec': 'wc' } ]
    },
    'inputs': [],
    'timeout': 45 * 1000,
    'expected_outputs': [],
    'expected_tasks': []
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
    'expected_tasks': [ {
	'phaseNum': 0,
	'key': '/poseidon/stor/obj1',
	'state': 'done',
	'result': 'ok',
	'nOutputs': 3,
	'firstOutputs': [
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./,
	    /\/poseidon\/jobs\/.*\/stor\/poseidon\/stor\/obj1\.0\./
	]
    }, {
	'phaseNum': 1,
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [ /\/poseidon\/jobs\/.*\/stor\/reduce\.1\./ ]
    }, {
	'phaseNum': 1,
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [ /\/poseidon\/jobs\/.*\/stor\/reduce\.1\./ ]
    }, {
	'phaseNum': 1,
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [ /\/poseidon\/jobs\/.*\/stor\/reduce\.1\./ ]
    }, {
	'phaseNum': 2,
	'state': 'done',
	'result': 'ok',
	'nOutputs': 1,
	'firstOutputs': [ /\/poseidon\/jobs\/.*\/stor\/reduce\.2\./ ]
    } ],
    'expected_outputs': [ /\/poseidon\/jobs\/.*\/stor\/reduce\.2\./ ],
    'verify': function (testspec, jobresult, callback) {
	var output = '';
	mod_jsprim.forEachKey(jobresult['task'], function (taskid, record) {
		if (record['value']['phaseNum'] != 2 ||
		    record['value']['state'] != 'done' ||
		    record['value']['nOutputs'] != 1)
			return;

		output = record['value']['firstOutputs'][0]['key'];
	});

	mod_assert.ok(output, 'expected output not found');
	mod_testcommon.manta.get(output, function (err, stream) {
		if (err) {
			callback(err);
			return;
		}

		var data = '';
		stream.on('data', function (chunk) {
			data += chunk.toString('utf8');
		});
		stream.on('end', function () {
			mod_assert.equal('55\n', data);
			callback();
		});
	});
    }
};

function initJobs()
{
	var job = exports.jobM500;

	for (var i = 0; i < 500; i++) {
		var key = '/poseidon/stor/obj' + i;
		var okey = '/poseidon/jobs/.*/stor' + key;

		job['inputs'].push(key);
		job['expected_outputs'] = new RegExp(okey);
		job['expected_tasks'].push({
		    'phaseNum': 0,
		    'key': key,
		    'state': 'done',
		    'result': 'ok',
		    'nOutputs': 1,
		    'firstOutputs': [ new RegExp(okey) ]
		});
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

	jobdef = {
	    'phases': testspec['job']['phases']
	};

	login = process.env['MANTA_USER'];
	url = mod_url.parse(process.env['MANTA_URL']);

	if (!login) {
		process.nextTick(function () {
			callback(new VError(
			    'MANTA_USER must be specified in the environment'));
		});
		return;
	}

	if (testspec['input'])
		jobdef['input'] = testspec['input'];

	funcs = [
	    function (_, stepcb) {
		log.info('looking up user "%s"', login);
		mod_testcommon.loginLookup(login, function (err, owner) {
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

	if (!testspec['input']) {
		funcs.push(function (_, stepcb) {
			var final_err;

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
	}

	mod_vasync.pipeline({ 'funcs': funcs }, function (err) {
		if (!err)
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

		if (!testspec['verify']) {
			callback();
			return;
		}

		/*
		 * On success, the "verify" function must invoke its callback.
		 * However, if it's going to fail, it may either throw an
		 * exception synchronously or emit an error to the callback.
		 */
		testspec['verify'](testspec, jobresult, function (err2) {
			mod_assert.ok(!err2, 'job verify failed');
			callback();
		});
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

function populateData(manta, keys, callback)
{
	log.info('populating keys', keys);

	var final_err;

	var queue = mod_vasync.queuev({
	    'concurrency': 15,
	    'worker': function (key, subcallback) {
		    if (final_err) {
			    subcallback();
			    return;
		    }

		    var data = 'auto-generated content for key ' + key;
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
