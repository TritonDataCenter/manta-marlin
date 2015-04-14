/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * test-proxy-client: one possible client for the proxy server.  See
 * test-proxy-proxy.js.
 */

var mod_http = require('http');

var port = 8080;

var request = mod_http.get({
    'port': port,
    'path': '/abcd'
});

request.end();

request.on('response', function (response) {
	if (process.argv.length > 2 && process.argv[2] == '-p') {
		response.pause();
		setTimeout(function () {}, 1000);
		return;
	}

	var nbytes = 0;
	response.on('data', function (chunk) {
		nbytes += chunk.toString('utf8').length;
	});

	response.on('end', function () {
		console.log(nbytes);
	});
});
