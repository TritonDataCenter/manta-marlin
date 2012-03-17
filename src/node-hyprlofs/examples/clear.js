/*
 * examples/remove.js: removes all mappings
 */

var mod_hyprlofs = require('hyprlofs');

if (process.argv.length < 3) {
	console.error('usage: node clear.js mountpoint');
	process.exit(1);
}

var start = Date.now();
var fs = new mod_hyprlofs.Filesystem(process.argv[2]);
fs.removeAll(function (err) {
	if (err) {
		console.error('fatal error: %s', err.message);
		process.exit(1);
	}

	console.log('successfully cleared mappings (%s ms)',
	    Date.now() - start);
});

console.log('dispatched request');
