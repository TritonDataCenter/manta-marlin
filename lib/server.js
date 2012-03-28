/*
 * lib/server.js: implements the Marlin Jobs API
 * Copyright (c) 2012, Joyent, Inc. All rights reserved.
 */

var mod_restify = require('restify');
var mod_getopt = require('posix-getopt');

var mod_mljob = require('./job');

var mlUsage = [
    'Usage: node server.js [-l port]',
    '',
    'Start the Marlin Job API server.',
    '',
    '    -l port    listen port for HTTP server'
].join('\n');

var mlPort = 80;
var mlServer;

function main()
{
	var parser = new mod_getopt.BasicParser('l:', process.argv);
	var option;

	while ((option = parser.getopt()) !== undefined) {
		if (option.error || option.option == '?')
			usage();

		if (option.option == 'l') {
			mlPort = parseInt(option.optarg, 10);
			if (isNaN(mlPort))
				usage('invalid port');
		}
	}

	mlServer = mod_restify.createServer({
	    'name': 'Marlin Job API'
	});

	mlServer.on('uncaughtException', function (_0, _1, _2, err) {
		throw (err);
	});

	mlServer.use(mod_restify.acceptParser(mlServer.acceptable));
	mlServer.use(mod_restify.queryParser());
	mlServer.use(mod_restify.bodyParser({ mapParams: false }));

	mlServer.post('/jobs', mod_mljob.jobSubmit);
	mlServer.get('/jobs', mod_mljob.jobList);
	mlServer.get('/jobs/:jobid', mod_mljob.jobLookup);

	mlServer.listen(mlPort, function () {
		console.log('server listening at %s', mlServer.url);
	});
}

function usage(message)
{
	if (message)
		console.error('error: %s', message);

	console.error(mlUsage);
	process.exit(2);
}

main();
