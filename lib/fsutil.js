/*
 * lib/fsutil.js: filesystem utilities
 */

var mod_fs = require('fs');
var mod_path = require('path');
var mod_util = require('util');
var mod_asyncutil = require('./asyncutil');

/*
 * Public interface
 */
exports.copyTree = copyTree;
exports.rmTree = rmTree;

/*
 * Copy a recursive directory hierarchy, like "cp -R", except that only regular
 * files and directories are supported (not symlinks, pipes, or devices).  Like
 * "cp", this operation fails if the destination already exists.  If any part of
 * the operation fails, the rest of it continues and the partial copy will still
 * be present.
 */
function copyTree(src, dst, callback)
{
	mod_fs.lstat(src, function (err, stat) {
		if (stat.isFile())
			copyFile(src, dst, stat, callback);
		else if (stat.isDirectory())
			copyDirectory(src, dst, stat, callback);
		else
			callback(new Error('copyTree: unsupported file ' +
			    'type: ' + src));
	});
}

function copyFile(src, dst, stat, callback)
{
	/*
	 * "open" with O_EXCL doesn't exist until Node 0.8.  Until then this
	 * check is subject to races.
	 */
	mod_fs.lstat(dst, function (err) {
		if (!err || err['code'] != 'ENOENT') {
			callback(new Error('copyTree: destination ' +
			    'exists: ' + dst));
			return;
		}

		var input = mod_fs.createReadStream(src);
		var output = mod_fs.createWriteStream(dst, {
		    'mode': stat.mode
		});

		mod_util.pump(input, output, function (suberr) {
			if (suberr)
				callback(new Error('copyTree: failed to ' +
				    'copy "' + src + '": ' + suberr.message));
			else
				callback();
		});
	});
}

function copyDirectory(src, dst, stat, callback)
{
	mod_fs.mkdir(dst, stat.mode, function (err) {
		if (err) {
			callback(new Error('copyTree: mkdir "' +
			    dst + '": ' + err.message));
			return;
		}

		mod_fs.readdir(src, function (suberr, files) {
			if (err) {
				callback(new Error('copyTree: readdir "' +
				    src + '":' + suberr.message));
				return;
			}

			mod_asyncutil.asForEach({
			    'inputs': files,
			    'func': function (filename, subcallback) {
				var srcfile = mod_path.join(src, filename);
				var dstfile = mod_path.join(dst, filename);
				copyTree(srcfile, dstfile, subcallback);
			    }
			}, callback);
		});
	});
}

/*
 * Recursively remove an entire directory tree, like "rm -rf".
 */
function rmTree(dir, callback)
{
	if (mod_path.normalize(dir) == '/') {
		callback(new Error('refusing to remove "/"'));
		return;
	}

	mod_fs.lstat(dir, function (err, stat) {
		if (err) {
			if (err['code'] == 'ENOENT')
				callback();
			else
				callback(err);
			return;
		}

		if (!stat.isDirectory()) {
			mod_fs.unlink(dir, callback);
			return;
		}

		mod_fs.readdir(dir, function (suberr, files) {
			if (err) {
				callback(err);
				return;
			}

			mod_asyncutil.asForEach({
			    'inputs': files,
			    'func': function (filename, subcallback) {
				var newfile = mod_path.join(dir, filename);
				rmTree(newfile, subcallback);
			    }
			}, function (suberr2) {
				if (suberr2) {
					callback(suberr2);
					return;
				}

				mod_fs.rmdir(dir, callback);
			});
		});
	});
}
