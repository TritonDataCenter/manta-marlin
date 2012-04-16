/*
 * tst.fsutil.js: tests filesystem utility functions in lib/fsutil.js
 */

var mod_assert = require('assert');
var mod_fs = require('fs');

var mod_asyncutil = require('../lib/asyncutil');
var mod_fsutil = require('../lib/fsutil');
var mod_jsutil = require('../lib/jsutil');

var tmpdir = '/var/tmp/tst.fsutil.' + process.pid;

/*
 * Creates a directory tree with some depth, files, and directories.
 */
function makeTreeSync(dir)
{
	mod_fs.mkdirSync(dir);
	mod_fs.mkdirSync(dir + '/empty1');
	mod_fs.mkdirSync(dir + '/empty2', 0700);
	mod_fs.mkdirSync(dir + '/burns', 0750);
	mod_fs.writeFileSync(dir + '/burns/snpp', 'excellent');
	mod_fs.mkdirSync(dir + '/burns/lil_lisa');
	mod_fs.writeFileSync(dir + '/burns/lil_lisa/slurry', 'even worse');
	mod_fs.chmodSync(dir + '/burns/lil_lisa/slurry', 0444);
	mod_fs.writeFileSync(dir + '/burns/lil_lisa/plant', 'fishing net');
	mod_fs.writeFileSync(dir + '/burns/lil_lisa/cost', 'millions of cans');
	mod_fs.writeFileSync(dir + '/burns/moes', 'occasionally');
	mod_fs.writeFileSync(dir + '/fat_tony', 'milk contract');
	mod_fs.writeFileSync(dir + '/marge', 'pretzel wagon');
}

/*
 * Checks that the given tree exactly matches the one created by makeTree
 */
function checkTree(dir)
{
	checkDirExists(dir, 040755, [ 'burns', 'empty1', 'empty2',
	    'fat_tony', 'marge' ]);
	checkDirExists(dir + '/empty1', 040755, []);
	checkDirExists(dir + '/empty2', 040700, []);
	checkDirExists(dir + '/burns', 040750, [ 'lil_lisa', 'moes', 'snpp' ]);
	checkDirExists(dir + '/burns/lil_lisa', 040755,
	    [ 'cost', 'plant', 'slurry' ]);

	checkFileContents(dir + '/burns/snpp', 0100644, 'excellent');
	checkFileContents(dir + '/burns/lil_lisa/slurry', 0100444,
	    'even worse');
	checkFileContents(dir + '/burns/lil_lisa/plant', 0100644,
	    'fishing net');
	checkFileContents(dir + '/burns/lil_lisa/cost', 0100644,
	    'millions of cans');
	checkFileContents(dir + '/burns/moes', 0100644, 'occasionally');
	checkFileContents(dir + '/fat_tony', 0100644, 'milk contract');
	checkFileContents(dir + '/marge', 0100644, 'pretzel wagon');

	console.log('tree %s okay', dir);
}

function checkDirExists(dir, mode, expected)
{
	var stat = mod_fs.statSync(dir);
	mod_assert.ok(stat.isDirectory(), dir + ' is a directory');
	mod_assert.equal(mode, stat.mode, dir + ' permissions');

	var actual = mod_fs.readdirSync(dir);
	actual.sort();
	mod_assert.deepEqual(actual, expected, dir + ' contents');
}

function checkFileContents(file, mode, expected)
{
	var stat = mod_fs.statSync(file);
	mod_assert.ok(stat.isFile(), file + ' is a file');
	mod_assert.equal(mode, stat.mode, file + ' permissions');

	var actual = mod_fs.readFileSync(file);
	mod_assert.equal(actual, expected, file + ' contents');
}

function setup(_, next)
{
	console.log('using tmpdir %s', tmpdir);
	mod_fs.mkdirSync(tmpdir);
	makeTreeSync(tmpdir + '/source');
	checkTree(tmpdir + '/source');
	next();
}

function copyOk(_, next)
{
	console.log('copy %s/source -> %s/dest', tmpdir, tmpdir);
	mod_fsutil.copyTree(tmpdir + '/source', tmpdir + '/dest',
	    function (err) {
		if (err)
			throw (err);

		checkTree(tmpdir + '/dest');
		next();
	    });
}

function copyFailExists(_, next)
{
	console.log('mkdir %s/nope; cp %s/source -> %s/nope (should fail)',
	    tmpdir, tmpdir, tmpdir);
	mod_fs.mkdirSync(tmpdir + '/nope');
	mod_fsutil.copyTree(tmpdir + '/source', tmpdir + '/nope',
	    function (err) {
		if (!err)
			throw (new Error('expected failure'));

		console.error('saw expected failure on copy ' +
		    '(destination exists): %s', err);
		next();
	    });
}

function copyFailSymlink(_, next)
{
	makeTreeSync(tmpdir + '/contains_symlink');
	checkTree(tmpdir + '/contains_symlink');
	console.log('create symlink; attempt copy');
	mod_fs.symlinkSync(tmpdir + '/source',
	    tmpdir + '/contains_symlink/mylink');
	mod_fsutil.copyTree(tmpdir + '/contains_symlink', tmpdir + '/newcopy',
	    function (err) {
		if (!err)
			throw (new Error('expected failure'));

		console.error('saw expected failure on copy ' +
		    '(contains symlink): %s', err);
		next();
	    });
}

mod_asyncutil.asPipeline({
    'funcs': [
	setup,
	copyOk,
	copyFailExists,
	copyFailSymlink
    ]
}, function (err) {
	if (err)
		throw (err);

	console.error('removing %s', tmpdir);
	mod_fsutil.rmTree(tmpdir, function () {
		mod_assert.throws(function () {
			mod_fs.statSync(tmpdir);
		}, /no such file/);
	});
});
