/*
 * lib/agent/zone-agent.js: per-zone agent, responsible for running and
 * monitoring compute zone jobs and servicing HTTP requests from the user.
 */

var mod_bunyan = require('bunyan');
var mod_restify = require('restify');

var mazServerName = 'marlin_zone-agent';
var mazPort = 8080;

var mazUdsPath;
var mazLog;
var mazServer;
var mazClient;

function usage(errmsg)
{
	if (errmsg)
		console.error(errmsg);

	console.error('usage: node zone-agent.js socket_path');
	process.exit(2);
}

function main()
{
	mazLog = new mod_bunyan({ 'name': mazServerName });

	if (process.argv.length < 3)
		usage();

	mazUdsPath = process.argv[2];
	mazServer = mod_restify.createServer({
	    'name': mazServerName,
	    'log': mazLog
	});

	mazServer.use(mod_restify.acceptParser(mazServer.acceptable));
	mazServer.use(mod_restify.queryParser());
	mazServer.use(mod_restify.bodyParser({ mapParams: false }));

	/* XXX add routes */

	mazServer.on('after', mod_restify.auditLogger({ 'log': mazLog }));

	mazServer.on('error', function (err) {
		mazLog.fatal(err, 'failed to start server: %s', err.mesage);
		process.exit(1);
	});

	mazServer.listen(mazPort, function () {
		mazLog.info('server listening on port %d', mazPort);
	});

	mazClient = mod_restify.createJsonClient({
	    'url': mazUdsPath,
	    'log': mazLog
	});

	mazTaskInit();
}

function mazTaskInit()
{
	mazClient.get('/params', function (err, request, response, obj) {
		if (err) {
			mazLog.error(err, 'initial params request failed: %s',
			    err.message);
			/* XXX retry? */
			return;
		}

		if (response.statusCode != 200) {
			mazLog.error('initial params request got unexpected ' +
			    'status: %s (body: %s)', response.statusCode,
			    response.body);
			/* XXX retry? */
			return;
		}

		mazLog.info('job params', obj);
	});
}

main();
