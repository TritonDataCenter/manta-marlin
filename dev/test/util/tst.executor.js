/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * tst.executor.js: basic test for executor
 */

var mod_assert = require('assert');
var mod_fs = require('fs');
var mod_path = require('path');

var mod_bunyan = require('bunyan');
var mod_contract = require('illumos_contract');
var mod_vasync = require('vasync');

var mod_executor = require('../../lib/agent/executor');

var tmpdir = process.env['TMPDIR'] || '/tmp';
var stdinfile = mod_path.join(tmpdir, 'tst.executor.js.stdin');
var stdoutfile = mod_path.join(tmpdir, 'tst.executor.js.stdout');
var stderrfile = mod_path.join(tmpdir, 'tst.executor.js.stderr');
var stdin, stdout, stderr;

var log = new mod_bunyan({
    'name': 'tst.executor.js',
    'level': process.env['LOG_LEVEL'] || 'debug'
});

mod_contract.set_template({
	'type': 'process',
	'critical': {
	    'pr_empty': true,
	    'pr_hwerr': true,
	    'pr_core': true
	},
	'fatal': {
	    'pr_hwerr': true,
	    'pr_core': true
	},
	'param': {
	    'noorphan': true
	}
});

mod_vasync.pipeline({ 'funcs': [
    setupFiles,
    testEnv,		/* tests stdout, correct environment setup */
    setupFiles,
    testStatusOk,	/* tests stdin, stdout, stderr */
    setupFiles,
    testFdInput,	/* tests piped stdin */
    setupFiles,
    testStatusFail,	/* tests non-zero status code */
    setupFiles,
    testCore		/* tests dumping core */
] }, function (err) {
	if (err) {
		log.fatal(err, 'test failed');
		throw (err);
	}

	log.info('test passed');
	process.exit(0); /* XXX illumos_contract holds this open */
});

function setupFiles(_, next)
{
	var barrier = mod_vasync.barrier();

	mod_fs.writeFileSync(stdinfile, 'sample data');
	barrier.start('stdin');
	stdin = mod_fs.createReadStream(stdinfile);
	stdin.on('open', function () {
		stdin.pause();
		barrier.done('stdin');
	});

	barrier.start('stdout');
	stdout = mod_fs.createWriteStream(stdoutfile);
	stdout.on('open', function () { barrier.done('stdout'); });

	barrier.start('stderr');
	stderr = mod_fs.createWriteStream(stderrfile);
	stderr.on('open', function () { barrier.done('stderr'); });

	barrier.on('drain', next);
}

function runTest(exec, teststdin, env, callback)
{
	var executor = new mod_executor({
	    'exec': exec,
	    'env': env,
	    'log': log,
	    'stdout': stdout,
	    'stderr': stderr,
	    'stdin': teststdin
	});

	executor.start();
	stdout.destroy();
	stderr.destroy();

	executor.on('done', function (result) {
		var stdout_contents = mod_fs.readFileSync(stdoutfile).
		    toString('utf8');
		var stderr_contents = mod_fs.readFileSync(stderrfile).
		    toString('utf8');
		callback(result, stdout_contents, stderr_contents);
	});
}

function testEnv(_, next)
{
	runTest('env | grep ENVVAR_ | sort', stdin, {
	    'ENVVAR_ONE': 'value_one',
	    'ENVVAR_TWO': 'value_two'
	}, function (result, outstr, errstr) {
		mod_assert.deepEqual(result, {
		    'code': 0,
		    'signal': null,
		    'core': false,
		    'error': undefined
		});

		mod_assert.equal([
		    'ENVVAR_ONE=value_one',
		    'ENVVAR_TWO=value_two', ''].join('\n'), outstr);
		mod_assert.equal('', errstr);
		next();
	});
}

function testStatusOk(_, next)
{
	runTest('wc', stdin, {}, function (result, outstr, errstr) {
		mod_assert.deepEqual(result, {
		    'code': 0,
		    'signal': null,
		    'core': false,
		    'error': undefined
		});

		mod_assert.equal('       0       2      11\n', outstr);
		mod_assert.equal('', errstr);
		next();
	});
}

function testFdInput(_, next)
{
	stdin.destroy();

	var fd = mod_fs.openSync(stdinfile, 'r');
	runTest('wc', fd, {}, function (result, outstr, errstr) {
		mod_assert.deepEqual(result, {
		    'code': 0,
		    'signal': null,
		    'core': false,
		    'error': undefined
		});

		mod_assert.equal('       0       2      11\n', outstr);
		mod_assert.equal('', errstr);
		next();
	});
}

function testStatusFail(_, next)
{
	runTest('/usr/bin/nawk {', stdin, {},
	    function (result, outstr, errstr) {
		mod_assert.deepEqual(result, {
		    'code': 2,
		    'signal': null,
		    'core': false,
		    'error': undefined
		});

		mod_assert.equal('', outstr);
		mod_assert.equal([
		    '/usr/bin/nawk: syntax error at source line 1',
		    ' context is',
		    '\t >>> { <<< ',
		    '/usr/bin/nawk: illegal statement at source line 1',
		    '\tmissing }', ''].join('\n'), errstr);
		next();
	    });
}

function testCore(_, next)
{
	runTest('ulimit -c 0; node -e \'process.abort()\'', stdin, {
	    'PATH': process.env['PATH']
	}, function (result, outstr, errstr) {
		mod_assert.deepEqual(result, {
		    'core': true,
		    'code': null,
		    'signal': 'SIGKILL',
		    'error': undefined
		});

		mod_assert.equal('', outstr);
		mod_assert.equal('', errstr);
		next();
	});
}
