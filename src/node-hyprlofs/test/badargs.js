/*
 * test/badargs.js: tests bad arguments to hyprlofs module functions
 */

var mod_assert = require('assert');
var mod_hyprlofs = require('hyprlofs');
var mod_fs = require('fs');

var tmpdir = '/var/tmp/hyprlofs.badargs.' + process.pid;
var fs;

mod_fs.mkdirSync(tmpdir);
fs = new mod_hyprlofs.Filesystem(tmpdir);
fs.mount(function (callback) {
	mod_assert.throws(function () {
		fs.addMappings();
	}, /expected array/);

	mod_assert.throws(function () {
		fs.addMappings({}, function () {});
	}, /expected array/);

	mod_assert.throws(function () {
		fs.addMappings(undefined);
	}, /expected array/);

	mod_assert.throws(function () {
		fs.addMappings([]);
	}, /expected callback/);

	mod_assert.throws(function () {
		fs.addMappings([], false);
	}, /expected callback/);

	mod_assert.throws(function () {
		fs.addMappings([ undefined ], function () {});
	}, /invalid mappings/);

	mod_assert.throws(function () {
		fs.addMappings([ [1, 2, 3] ], function () {});
	}, /invalid mappings/);

	mod_assert.throws(function () {
		fs.addMappings([ [1] ], function () {});
	}, /invalid mappings/);

	mod_assert.throws(function () {
		fs.addMappings([ [] ], function () {});
	}, /invalid mappings/);


	mod_assert.throws(function () {
		fs.removeMappings();
	}, /expected array/);

	mod_assert.throws(function () {
		fs.removeMappings({}, function () {});
	}, /expected array/);

	mod_assert.throws(function () {
		fs.removeMappings(undefined);
	}, /expected array/);

	mod_assert.throws(function () {
		fs.removeMappings([]);
	}, /expected callback/);

	mod_assert.throws(function () {
		fs.removeMappings([], false);
	}, /expected callback/);


	mod_assert.throws(function () {
		fs.listMappings();
	}, /expected callback/);

	mod_assert.throws(function () {
		fs.removeAll();
	}, /expected callback/);

	mod_assert.throws(function () {
		fs.mount();
	}, /expected callback/);

	mod_assert.throws(function () {
		fs.unmount();
	}, /expected callback/);

	var newfs = new mod_hyprlofs.Filesystem('/var/tmp/nope');

	newfs.removeAll(function (err) {
		console.error('saw expected error: %j', err);
		mod_assert.ok(/No such file/.test(err.message));
	});

	fs.unmount(function () {
		fs.removeAll(function (err) {
			console.error('saw expected error: %j', err);
			mod_assert.equal(err['code'], 'ENOTTY');
			mod_fs.rmdirSync(tmpdir);
		});
	});
});
