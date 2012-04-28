/*
 * mock-manta.js: mock Manta endpoint for testing (backed up filesystem)
 */

var mod_fs = require('fs');
var mod_path = require('path');
var mod_url = require('url');

var mod_bunyan = require('bunyan');
var mod_mkdirp = require('mkdirp');
var mod_restify = require('restify');

var mod_mautil = require('./util');

/*
 * Static configuration
 */
var maServerName = 'mock-manta';
var maMantaRoot = '/var/tmp/mock-manta';
var maPort = 8081;

/*
 * Dynamic server state
 */
var maLog;
var maServer;

function main()
{
	maLog = new mod_bunyan({
	    'name': maServerName,
	    'level': 'trace'
	});

	maServer = mod_restify.createServer({
	    'name': maServerName,
	    'log': maLog
	});

	maServer.on('uncaughtException', mod_mautil.maRestifyPanic);
	maServer.on('after', mod_restify.auditLogger({ 'log': maLog }));

	maServer.on('error', function (err) {
		maLog.fatal(err, 'failed to start server: %s', err.message);
		process.exit(1);
	});

	maServer.get('/.*', maMantaPath, maMantaGet);
	maServer.put('/.*', maMantaPath, maMantaPut);
	maServer.del('/.*', maMantaNyi);
	maServer.head('/.*', maMantaNyi);
	maServer.post('/.*', maMantaNyi);

	mod_mkdirp(maMantaRoot, function (err) {
		if (err) {
			maLog.fatal(err, 'error creating "%s"', maMantaRoot);
			process.exit(1);
		}

		maServer.listen(maPort, function () {
			maLog.info('server listening on port %d', maPort);
		});
	});
}

/*
 * Check for attempts to access outside the mock Manta hierarchy.  These checks
 * assume no symlinks under the hierarchy.  If symlinks became allowed, we would
 * have to be more careful to avoid races.
 */
function maMantaPath(request, response, next)
{
	var path, fspath;

	path = mod_url.parse(request.url).pathname;

	if (path.length === 0) {
		next(new mod_restify.InvalidArgumentError(
		    'invalid object key: "/"'));
		return;
	}

	if (path[path.length - 1] == '/') {
		next(new mod_restify.InvalidArgumentError(
		    'object name cannot end with "/"'));
		return;
	}

	fspath = mod_path.normalize(mod_path.join(maMantaRoot, path));

	if (fspath.substr(0, maMantaRoot.length) != maMantaRoot) {
		next(new mod_restify.InvalidArgumentError(
		    'invalid object key'));
		return;
	}

	request.maPath = fspath;
	request.log.info('maMantaPath', fspath);
	next();
}

function maMantaGet(request, response, next)
{
	var path, instream, started;

	path = request.maPath;
	instream = mod_fs.createReadStream(path);
	started = false;

	instream.on('error', function (err) {
		request.log.error(err);

		if (started)
			return;

		started = true;
		if (err['code'] == 'ENOENT') {
			response.send(404);
			next();
		} else {
			next(err);
		}
	});

	instream.on('open', function () {
		started = true;
		response.writeHead(200);
		instream.pipe(response);

		/*
		 * This really should be on response end, which is when the
		 * response has flushed everything written by the time the
		 * instream emits "end", but it's unclear how to do that.
		 */
		instream.on('end', next);
	});
}

function maMantaPut(request, response, next)
{
	var path, outstream, started;

	path = request.maPath;
	outstream = mod_fs.createWriteStream(path);
	started = false;

	outstream.on('error', function (err) {
		if (started)
			return;

		if (err['code'] == 'ENOENT')
			err = new mod_restify.NotFoundError();

		started = true;
		next(err);
	});

	outstream.on('open', function () {
		request.log.trace('output file opened');

		if (!started) {
			started = true;
			response.writeHead(201);
		}
	});

	request.pipe(outstream);

	/* See the comment in maMantaGet above. */
	request.on('end', function () {
		request.log.trace('request end');

		if (!started) {
			started = true;
			response.writeHead(201);
		}

		response.end();
		next();
	});
}

function maMantaNyi(request, response, next)
{
	next(new mod_restify.BadRequestError('not yet implemented'));
}

main();
