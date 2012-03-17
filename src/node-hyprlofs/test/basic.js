/*
 * basic.js: exercise the entry points for the hyprlofs bindings
 */

var mod_assert = require('assert');
var mod_fs = require('fs');
var mod_hyprlofs = require('hyprlofs');

var tmpdir = '/var/tmp/hylofs.basic/' + process.pid;
var stages = [];
var fs;

/*
 * Example files
 */
var file_paths = {
	'my_release': '/etc/release',
	'my_cat': '/usr/bin/cat',
	'my_grep': '/usr/bin/grep',
	'my_ls': '/usr/bin/ls',
	'some/other/bash': '/usr/bin/bash'
};

function makeMappings(paths)
{
	return (paths.map(function (path) {
		return ([ file_paths[path], path ]);
	}));
}

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

	stages[i + 1](function (suberr) { stageDone(i + 1, suberr); });
}

/*
 * Verify that the mount contains exactly the given files.
 */
function checkFiles(files, callback)
{
	var expected_paths = {};

	files.forEach(function (file) {
		expected_paths[file] = file_paths[file];
	});

	fs.listMappings(function (err, mappings) {
		if (files.length === 0 && mappings === undefined &&
		    err && err['code'] == 'ENOTTY') {
			err = undefined;
			mappings = [];
		}

		if (err)
			return (callback(err));

		var i, entry, path;

		for (i = 0; i < mappings.length; i++) {
			entry = mappings[i];
			mod_assert.ok(Array.isArray(entry));
			mod_assert.equal(entry.length, 2);

			/*
			 * Currently, hyprlofs GET includes the full path to
			 * this zone's root directory instead of the full path
			 * within this zone.  Since we have no way of actually
			 * knowing what our root directory really is, we just
			 * look at the *end* of each path.
			 */
			if (!expected_paths.hasOwnProperty(entry[1]))
				break;

			path = entry[0].substr(entry[0].length -
			    expected_paths[entry[1]].length);

			if (expected_paths[entry[1]] != path)
				break;
		}

		if (i < mappings.length)
			return (callback(new Error('expected ' +
			    JSON.stringify(makeMappings(files)) +
			    '; got ' + JSON.stringify(mappings))));

		return (find(tmpdir, function (suberr, foundfiles) {
			if (suberr)
				return (callback(suberr));

			var expected = JSON.stringify(files.sort());
			var found = JSON.stringify(foundfiles.sort());

			if (expected != found)
				return (callback(new Error('found "' +
				    found + '", expected "' + expected + '"')));

			process.stdout.write('(' + files.length + ' files) ');
			return (callback());
		}));
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

		return (dirents.forEach(function (ent) {
			var newprefix = prefix.length === 0 ? ent :
			    prefix + '/' + ent;

			i++;

			findFiles(dir + '/' + ent, newprefix,
			    files, function (suberr) {
				if (suberr)
					errs.push(suberr);
				if (--i === 0)
					callback(errs.length > 0 ?
					    errs[0] : null, files);
			});
		}));
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
	    [ '/bin/bash',	'some/other/bash' ]
	], callback);
});

stages.push(function (callback) {
	process.stdout.write('Checking for mappings ... ');
	checkFiles([ 'my_release', 'my_cat', 'my_grep', 'some/other/bash' ],
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
	checkFiles([ 'my_release', 'my_ls', 'some/other/bash' ], callback);
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
	    [ '/bin/bash',	'some/other/bash' ]
	], callback);
});

stages.push(function (callback) {
	process.stdout.write('Checking for mappings ... ');
	checkFiles([ 'my_release', 'my_cat', 'my_grep', 'some/other/bash' ],
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
