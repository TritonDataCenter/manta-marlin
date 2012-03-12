/*
 * Copyright (c) 2012, Joyent, Inc. All rights reserved.
 *
 * Main entry-point for the Boilerplate API.
 */

var restify = require('restify');
var Logger = require('bunyan');

var log = new Logger({
	name: 'manta-compute',
	level: 'debug',
	serializers: {
		err: Logger.stdSerializers.err,
		req: Logger.stdSerializers.req,
		res: restify.bunyan.serializers.response
	}
});

var server = restify.createServer({
	name: 'Manta Compute Engine',
	log: log
});

server.listen(8080, function () {
	log.info({url: server.url}, '%s listening', server.name);
});
