/*
 * stress.js: exercise a number of hyprlofs add/clear cycles
 */

var mod_fs = require('fs');
var mod_hyprlofs = require('hyprlofs');

var batch_size = 1000;
var niters = 100;
var done = 0;
var tmpdir_files = '/var/tmp/hylofs.stress/files' + process.pid;
var tmpdir_fs = '/var/tmp/hylofs.stress/fs' + process.pid;
var stages = [];
var fs, mappings;

var ts_start, ts_end;
var time_add = 0;
var time_clear = 0;

/*
 * Process the next asynchronous stage.
 */
function stageDone(i, err)
{
	if (err) {
		console.log('FAILED: %s', err.message);
		process.exit(1);
	}

	if (i + 1 == stages.length)
		return;

	stages[i + 1](function (suberr) { stageDone(i + 1, suberr); });
}

function addMappings(callback)
{
	fs.addMappings(mappings, callback);
}

function clearMappings(callback)
{
	fs.removeAll(callback);
}

function stress(callback) {
	if (done % 1000 === 0)
		process.stdout.write('.');

	var tick = Date.now();
	addMappings(function (err) {
		time_add += Date.now() - tick;

		if (err) {
			callback(err);
			return;
		}

		tick = Date.now();

		clearMappings(function (clear_err) {

			time_clear += Date.now() - tick;

			if (clear_err)
				return (callback(clear_err));

			if (++done < niters)
				return (stress(callback));

			console.log('. done (' + done + ' iters)');
			return (callback());
		});
	});
}

function printMemory(callback)
{
	console.log('Memory usage: %j', process.memoryUsage());
	callback();
}

/*
 * Setup: mkdir, create files, and mount.
 */
stages.push(function setup(callback) {
	process.stdout.write('Creating ' + tmpdir_fs + ' ... ');
	mod_fs.mkdirSync(tmpdir_fs);
	mod_fs.mkdirSync(tmpdir_files);

	mappings = [];
	for (var i = 0; i < batch_size; i++) {
		mod_fs.writeFileSync(tmpdir_files + '/file' + i, '');
		mappings.push([ tmpdir_files + '/file' + i, 'file_' + i ]);
	}

	process.stdout.write('done.\n');
	callback();
});

stages.push(function mount(callback) {
	fs = new mod_hyprlofs.Filesystem(tmpdir_fs);
	process.stdout.write('Mounting hyprlofs at ' + tmpdir_fs + ' ... ');
	fs.mount(function (err) {
		if (err) {
			callback(err);
			return;
		}

		process.stdout.write('done.\n');
		callback();
	});
});

stages.push(function run_stress(callback) {
	process.stdout.write('Beginning stress test ... ');
	ts_start = Date.now();
	stress(function (err) {
		ts_end = Date.now();
		callback(err);
	});
});

stages.push(printMemory);

stages.push(function print_results(callback) {
	process.stdout.write('Completed ' + done + ' iterations of ' +
	    batch_size + ' files in ' + (ts_end - ts_start) + ' ms\n');
	process.stdout.write('    avg iter time:  ' +
	    (1000 * (ts_end - ts_start) / done) + 'us\n');
	process.stdout.write('    avg add time:   ' +
	    (1000 * time_add / done) + 'us\n');
	process.stdout.write('    avg clear time: ' +
	    (1000 * time_clear / done) + 'us\n');
	callback();
});

stages.push(printMemory);

/*
 * Cleanup.
 */
stages.push(function unmount(callback) {
	process.stdout.write('Unmounting ' + tmpdir_fs + ' ... ');
	fs.unmount(function (err) {
		if (err) {
			callback(err);
			return;
		}

		process.stdout.write('done.\n');
		callback();
	});
});

stages.push(function cleanup(callback) {
	process.stdout.write('Removing ' + tmpdir_fs + ' ... ');
	mod_fs.rmdirSync(tmpdir_fs);
	process.stdout.write('done.\n');

	process.stdout.write('Removing ' + tmpdir_files + ' ... ');
	for (var i = 0; i < batch_size; i++)
		mod_fs.unlinkSync(tmpdir_files + '/file' + i);

	mod_fs.rmdirSync(tmpdir_files);
	process.stdout.write('done.\n');
	callback();
});

stageDone(-1);
