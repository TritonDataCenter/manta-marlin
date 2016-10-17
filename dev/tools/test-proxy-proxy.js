/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * test-proxy-proxy: simple HTTP proxy server to test the http proxy code in
 * common/lib/util.js.  Assumes that there's a server running on localhost
 * similar to the stock Node server, except that it should probably return
 * larger objects (e.g., 1MB).  See test-proxy-upstream.js for an example.
 */

var bunyan = require('bunyan');
var restify = require('restify');
var mod_localutil = require('../../common/lib/util');

var log;
var noutstanding = 0;
var baseport = 9100;

function main()
{
	var server;

	if (process.argv.length > 2) {
		baseport = parseInt(process.argv[2], 10);
		if (isNaN(baseport)) {
			throw (new Error('bad base port'));
		}
	}

	log = new bunyan({ 'name': 'testproxy', 'level': 'trace' });
	server = restify.createServer({
	    'name': 'testproxy',
	    'noWriteContinue': true,
	    'version': [ '1.0.0' ],
	    'log': log.child({
	      'serializers': restify.bunyan.serializers
	    })
	});
	server.pre(restify.pre.sanitizePath());
	server.use(restify.requestLogger());
	server.use(restify.queryParser());
	server.on('after', restify.auditLogger({ 'log': log }));
	/* JSSTYLED */
	server.get(/.*/, proxy);

	server.listen(baseport + 1, function () {
	    console.log('%s listening at %s', server.name, server.url);
	});
}

function proxy(request, response, next)
{
	var proxyargs;

	proxyargs = {
	    'request': request,
	    'response': response,
	    'server': {
		'host': '127.0.0.1',
		'port': baseport
	    }
	};

	log.info('maHttpProxy dispatched');
	noutstanding++;
	mod_localutil.maHttpProxy(proxyargs, function (err, res) {
		noutstanding--;
		log.info({ 'noutstanding': noutstanding },
		    'maHttpProxy callback called');
		next();
	});
}

main();
