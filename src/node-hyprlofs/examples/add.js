/*
 * examples/add.js: basic usage of addMappings
 */

var mod_hyprlofs = require('hyprlofs');

if (process.argv.length < 3) {
	console.error('usage: node add.js mountpoint');
	process.exit(1);
}

var start = Date.now();
var fs = new mod_hyprlofs.Filesystem(process.argv[2]);

fs.addMappings([
    [ '/etc/ssh/sshd_config', 'somefile1' ],
    [ '/etc/ssh/sshd_config', 'somefile2' ]
], function (err) {
	if (err) {
		console.error('fatal error: %s', err.message);
		process.exit(1);
	}

	console.log('successfully added mappings (%s ms)',
	    Date.now() - start);
});

console.log('dispatched request');
