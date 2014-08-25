/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * lib/worker/server.js: worker executable
 */

var mod_bunyan = require('bunyan');
var mod_kang = require('kang');
var mod_panic = require('panic');
var mod_restify = require('restify');

var mod_mautil = require('../util');
var mod_worker = require('./worker');

var mwLogLevel = process.env['LOG_LEVEL'] || 'info';
var mwWorkerInstance;

function main()
{
	var log, conf, worker, server;

	if (process.argv.length < 3) {
		console.error('usage: %s %s config.json', process.argv[0],
		    process.argv[1]);
		process.exit(2);
	}

	if (!process.env['NO_ABORT_ON_CRASH']) {
		mod_panic.enablePanicOnCrash({
		    'skipDump': true,
		    'abortOnPanic': true
		});
	}

	log = new mod_bunyan({ 'name': 'jobsupervisor', 'level': mwLogLevel });
	conf = mod_mautil.readConf(log, mod_worker.mwConfSchema,
	    process.argv[2]);

	worker = mwWorkerInstance = new mod_worker.mwWorker({
	    'conf': conf,
	    'log': log
	});

	server = mod_restify.createServer({ 'serverName': 'MarlinWorker' });

	/* JSSTYLED */
	server.get(/\/kang\/.*/, mod_kang.knRestifyHandler({
	    'port': conf['port'],
	    'uri_base': '/kang',
	    'service_name': 'marlin',
	    'component': 'jobworker',
	    'version': '0.0.1',
	    'ident': conf['instanceUuid'],
	    'list_types': worker.kangListTypes.bind(worker),
	    'list_objects': worker.kangListObjects.bind(worker),
	    'get': worker.kangGetObject.bind(worker),
	    'schema': worker.kangSchema.bind(worker),
	    'stats': worker.kangStats.bind(worker)
	}));

	server.post('/quiesce', function (request, response, next) {
		worker.quiesce(true, function (err) {
			if (!err)
				response.send(204);
			next(err);
		});
	});

	server.post('/unquiesce', function (request, response, next) {
		worker.quiesce(false, function (err) {
			if (!err)
				response.send(204);
			next(err);
		});
	});

	server.listen(conf['port'], function () {
		var addr = server.address();
		log.info('server listening at http://%s:%d',
		    addr['address'], addr['port']);
		worker.start();
	});
}

main();
