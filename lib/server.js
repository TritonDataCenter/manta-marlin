/*
 * lib/server.js: implements the Marlin Jobs API
 * Copyright (c) 2012, Joyent, Inc. All rights reserved.
 */

var mod_bunyan = require('bunyan');
var mod_getopt = require('posix-getopt');
var mod_jsprim = require('jsprim');
var mod_restify = require('restify');
var mod_uuid = require('node-uuid');

var mod_moray = require('moray-client');
var mod_config = require('./config');
var mod_schema = require('./schema');
var mod_marlin_moray = require('./worker/moray');

var mlUsage = [
    'Usage: node server.js [-l port]',
    '',
    'Start the Marlin Job API server.',
    '',
    '    -l port    listen port for HTTP server'
].join('\n');

var mlBktJobs = mod_config.mcBktJobs;
var mlBktTaskGroups = mod_config.mcBktTaskGroups;
var mlPort = 80;
var mlLog;
var mlClient;
var mlServer;

function usage(message)
{
	if (message)
		console.error('error: %s', message);

	console.error(mlUsage);
	process.exit(2);
}

function main()
{
	if (!process.env['MORAY_URL'])
		usage('MORAY_URL must be set in the environment.');

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

	mlLog = new mod_bunyan({ 'name': 'marlinapi' });

	mlServer = mod_restify.createServer({
	    'log': mlLog,
	    'name': 'marlinapi'
	});

	mlClient = mod_moray.createClient({
	    'log': mlLog,
	    'url': process.env['MORAY_URL']
	});

	mlServer.on('uncaughtException', function (_0, _1, _2, err) {
		throw (err);
	});

	mlServer.use(mod_restify.acceptParser(mlServer.acceptable));
	mlServer.use(mod_restify.queryParser());
	mlServer.use(mod_restify.bodyParser({ mapParams: false }));

	mlServer.post('/jobs', jobValidate, jobSubmit);
	mlServer.get('/jobs', jobsList);
	mlServer.get('/jobs/:jobid', jobGet);

	mlServer.on('after', mod_restify.auditLogger({ 'log': mlLog }));
	/* XXX add debug metadata -- see agent.js */

	mlLog.info('setting up Moray buckets');
	mod_marlin_moray.RemoteMoraySetup({ 'moray': mlClient },
	    function (err) {
		if (err) {
			/* XXX should retry if it was a connection problem */
			mlLog.fatal(err, 'failed to setup Moray');
			throw (err);
		}

		mlServer.listen(mlPort, function () {
			mlLog.info('server listening at %s', mlServer.url);
		});
	    });
}

function jobValidate(request, response, next)
{
	var error = mod_jsprim.validateJsonObject(mod_schema.sHttpJobInput,
	    request.body);

	if (error) {
		next(new mod_restify.ConflictError(error.message));
	} else {
		next();
	}
}

/*
 * XXX pass through RequestId on the way in
 * XXX pass through etag on the way out
 */
function jobSubmit(request, response, next)
{
	var input = request.body;

	input['jobId'] = mod_uuid.v4();
	input['createTime'] = mod_jsprim.iso8601(new Date());
	input['state'] = 'queued';
	input['doneKeys'] = [];
	input['outputKeys'] = [];
	input['discardedKeys'] = [];

	mlClient.put(mlBktJobs, input['jobId'], input, function (err, meta) {
		if (err) {
			next(morayError(err, 'submitting job'));
			return;
		}

		response.header('Location', '/jobs/' + input['jobId']);
		response.send(204);
		next();
	});
}

/*
 * XXX needs work.  Should probably use "search" API and present additional
 * properties like "state".  But that API isn't paginated -- is that a problem?
 * Should this public API be paginated?
 */
function jobsList(request, response, next)
{
	var list = mlClient.keys(mlBktJobs);
	var allkeys = [];
	var sent = false;

	list.on('keys', function (newkeys) {
		mod_jsprim.forEachKey(newkeys, function (key, value) {
			allkeys.push({
			    'jobId': key,
			    'etag': value['etag'],
			    'mtime': value['mtime']
			});
		});
	});

	list.on('end', function done() {
		if (sent)
			return;
		response.send(allkeys);
		next();
	});

	list.on('error', function (err) {
		next(morayError(err, 'listing jobs'));
		sent = true;
	});
}

/*
 * XXX:
 * - pass through incoming if[-none]-match, x-api-request-id headers
 * - should we pass through outgoing etag, mtime in headers?
 */
function jobGet(request, response, next)
{
	var jobid = request.params['jobid'];

	mlClient.get(mlBktJobs, jobid, function (err, metadata, obj) {
		if (err) {
			next(morayError(err, 'fetching job'));
			return;
		}

		response.send(jobPublicView(obj));
		next();
	});
}

function jobPublicView(job)
{
	return ({
	    'jobId': job['jobId'],
	    'jobName': job['jobName'],
	    'phases': job['phases'],
	    'inputKeys': job['inputKeys'],
	    'createTime': job['createTime'],
	    'state': job['state'],
	    'doneKeys': job['doneKeys'],
	    'outputKeys': job['outputKeys'],
	    'discardedKeys': job['discardedKeys']
	});
}

function morayError(err, action)
{
	mlLog.error(err, 'error while %s', action, err);

	if (err['code'] && err['code'] == 'ResourceNotFound')
		err = new mod_restify.ResourceNotFoundError();
	else
		err = new mod_restify.ServiceNotAvailable();

	return (err);
}

main();
