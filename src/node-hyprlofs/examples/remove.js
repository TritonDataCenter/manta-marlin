/*
 * examples/remove.js: removes all mappings
 */

var mod_hyprlofs = require('../');

if (process.argv.length < 3) {
	console.error('usage: node remove.js mountpoint');
	process.exit(1);
}


