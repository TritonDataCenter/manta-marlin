/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * test-proxy-upstream: sample upstream server for testing the proxy.  See
 * test-proxy-proxy.js
 */

var http = require('http');

var data = '';
var i;
for (i = 0; i < 10 * 1024 * 1024; i++)
	data += 'a';

http.createServer(function (req, res) {
	res.writeHead(200, { 'content-type': 'text/plain' });
	res.end(data);
}).listen(8087, '127.0.0.1');
console.log('server running at 127.0.0.1:8087');
