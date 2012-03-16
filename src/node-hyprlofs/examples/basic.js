/*
 * basic.js: exercise the entry points for the hyprlofs bindings
 */

var mod_fs = require('fs');
var mod_hyprlofs = require('..');
var mod_path = require('path');

var tmpdir = '/var/tmp/hylofs.basic/' + process.pid;
var stages = [];
var fs;

/*
 * Process the next asynchronous stage.
 */
function stageDone(i, err)
{
	if (err) {
		console.log('FAILED: %s', err.message);
		process.exit(1);
	}

	if (i >= 0)
		console.log('done.');

	if (i + 1 == stages.length)
		return;

	stages[i + 1](function (err) { stageDone(i + 1, err); });
}

/*
 * Verify that the mount contains exactly the given files.
 */
function checkFiles(files, callback)
{
	find(tmpdir, function (err, foundfiles) {
		if (err)
			return (callback(err));

		var expected = JSON.stringify(files.sort());
		var found = JSON.stringify(foundfiles.sort());

		if (expected != found)
			return (callback(new Error('found "' + found + '", ' +
			    'expected "' + expected + '"')));

		process.stdout.write('(' + files.length + ' files) ');
		return (callback());
	});
}

/*
 * Iterate all files under the given directory tree.
 */
function find(dir, callback)
{
	findFiles(dir, '', [], callback);
}

function findFiles(dir, prefix, files, callback)
{
	mod_fs.readdir(dir, function (err, dirents) {
		if (err) {
			if (err['code'] == 'ENOTDIR') {
				files.push(prefix);
				return (callback(null, files));
			}

			return (callback(err));
		}

		if (dirents.length === 0)
			return (callback(null, files));

		var i = 0;
		var errs = [];

		dirents.forEach(function (ent) {
			var newprefix = prefix.length === 0 ? ent :
			    prefix + '/' + ent;

			i++;
			
			findFiles(dir + '/' + ent, newprefix,
			    files, function (err) {
				if (err)
					errs.push(err);
				if (--i === 0)
					callback(errs.length > 0 ?
					    errs[0] : null, files);
			});
		});
	});
}

/*
 * Setup: mkdir and mount.
 */
stages.push(function (callback) {
	process.stdout.write('Creating ' + tmpdir + ' ... ');
	mod_fs.mkdir(tmpdir, callback);
});

stages.push(function (callback) {
	fs = new mod_hyprlofs.Filesystem(tmpdir);
	process.stdout.write('Mounting hyprlofs at ' + tmpdir + ' ... ');
	fs.mount(callback);
});

/*
 * Check basic operation: add, remove, clear, unmount.
 */
stages.push(function (callback) {
	process.stdout.write('Adding mappings ... ');
	fs.addMappings([
	    [ '/etc/release',	'my_release' ],
	    [ '/usr/bin/cat',	'my_cat' ],
	    [ '/usr/bin/grep',	'my_grep' ],
	    [ '/bin/bash',	'some_other_bash' ],
	], callback);
});

stages.push(function (callback) {
	process.stdout.write('Checking for mappings ... ');
	checkFiles([ 'my_release', 'my_cat', 'my_grep', 'some_other_bash' ],
	    callback);
});

stages.push(function (callback) {
	process.stdout.write('Removing some of the mappings ... ');
	fs.removeMappings([ 'my_grep', 'my_cat' ], callback);
});

stages.push(function (callback) {
	process.stdout.write('Adding other mappings ... ');
	fs.addMappings([
	    [ '/usr/bin/ls', 'my_ls' ]
	], callback);
});

stages.push(function (callback) {
	process.stdout.write('Checking mappings ... ');
	checkFiles([ 'my_release', 'my_ls', 'some_other_bash' ], callback);
});

stages.push(function (callback) {
	process.stdout.write('Clearing mappings ... ');
	fs.removeAll(callback);
});

stages.push(function (callback) {
	process.stdout.write('Checking mappings ... ');
	checkFiles([], callback);
});

stages.push(function (callback) {
	process.stdout.write('Unmounting ' + tmpdir + ' ... ');
	fs.unmount(callback);
});

/*
 * Check mount-after-unmount.
 */
stages.push(function (callback) {
	process.stdout.write('Remounting hyprlofs at ' + tmpdir + ' ... ');
	fs.mount(callback);
});

stages.push(function (callback) {
	process.stdout.write('Adding mappings ... ');
	fs.addMappings([
	    [ '/etc/release',	'my_release' ],
	    [ '/usr/bin/cat',	'my_cat' ],
	    [ '/usr/bin/grep',	'my_grep' ],
	    [ '/bin/bash',	'some_other_bash' ],
	], callback);
});

stages.push(function (callback) {
	process.stdout.write('Checking for mappings ... ');
	checkFiles([ 'my_release', 'my_cat', 'my_grep', 'some_other_bash' ],
	    callback);
});

/*
 * Check unmounting with files mapped.
 */
stages.push(function (callback) {
	process.stdout.write('Unmounting ' + tmpdir + ' again ... ');
	fs.unmount(callback);
});

stages.push(function (callback) {
	process.stdout.write('Checking for mappings ... ');
	checkFiles([], callback);
});

/*
 * Cleanup.
 */
stages.push(function (callback) {
	process.stdout.write('Removing ' + tmpdir + ' ... ');
	mod_fs.rmdir(tmpdir, callback);
});

stageDone(-1);
