/*
 * tst.mtee.js: mtee tests
 */

var mod_assert = require('assert');
var mod_fs = require('fs');
var mod_child_process = require('child_process');

var mod_bunyan = require('bunyan');
var mod_vasync = require('vasync');

var common = require('../common');

var mtee = null;
var tmpdir = process.env['TMPDIR'] || '/tmp';

var log = new mod_bunyan({
    'name': 'tst.mtee.js',
    'level': process.env['LOG_LEVEL'] || 'debug'
});

mod_vasync.pipeline({ 'funcs': [
	findMtee,
	testNoOpts,
	testBasic
] }, function (err) {
	if (err) {
		log.fatal(err, 'test failed');
		throw (err);
	}

	log.info('test passed');
});

function runTest(opts, callback)
{
	var spawn = mod_child_process.spawn(mtee, opts.opts);

	var stdout = '';
	spawn.stdout.on('data', function (data) {
		stdout += data;
	});

	var stderr = '';
	spawn.stderr.on('data', function (data) {
		stderr += data;
	});

	var error = null;
	spawn.on('error', function (err) {
		error = err;
	});

	spawn.stdin.on('error', function (err) {
		error = err;
	});

	spawn.on('close', function (code) {
		var file = '';
		if (opts.file) {
			file = mod_fs.readFileSync(opts.file, 'utf8');
		}
		var result = {
			stdout: stdout,
			stderr: stderr,
			file: file,
			code: code,
			error: error
		};
		callback(result);
	});

	process.nextTick(function () {
		spawn.stdin.write(opts.stdin || '');
		spawn.stdin.end();
	});
}

function findMtee(_, next)
{
	common.findbin(function (err, bin) {
		if (err) {
			next(err);
			return;
		}
		mtee = bin + '/mtee';
		next();
	});
}

function testNoOpts(_, next)
{
	log.info('running testNoOpts');
	runTest({
		stdin: '',
		opts: []
	}, function (result) {
		mod_assert.equal(2, result.code);
		next();
	});
}

function testBasic(_, next)
{
	var sin = '1\n2\n3\n4\n';
	var file = '/var/tmp/tst.mtee.js-testBasic';
	log.info('running testBasic');
	runTest({
		stdin: sin,
		opts: ['-t', file],
		file: file
	}, function (result) {
		mod_assert.equal(0, result.code);
		mod_assert.equal(sin, result.stdout);
		mod_assert.deepEqual({
			'code': 0,
			'file': sin,
			'stdout': sin,
			'stderr': '',
			'error': null
		}, result);
		next();
	});
}
