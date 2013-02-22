/*
 * tst.httpcat.js: tests HttpCatStream
 */

var mod_assert = require('assert');
var mod_bunyan = require('bunyan');
var mod_restify = require('restify');
var mod_vasync = require('vasync');

var mod_httpcat = require('../../lib/http-cat-stream');

var serv1, serv2, log, cache, stream, buffer;

function handle(server, request, response, next)
{
	response.send(server.address().port + request.url);
	next();
}

function setup(_, next)
{
	log = new mod_bunyan({ 'name': 'tst.httpcat.js', 'level': 'trace' });
	log.info('setup');

	serv1 = mod_restify.createServer({
	    'name': 'tst.httpcat.js-1'
	});

	serv2 = mod_restify.createServer({
	    'name': 'tst.httpcat.js-2'
	});

	serv1.get('/.*', handle.bind(null, serv1));
	serv2.get('/.*', handle.bind(null, serv2));

	cache = new mod_httpcat.ObjectCache(function (url) {
		return (mod_restify.createClient({
		    'url': url,
		    'connectTimeout': 500,
		    'retry': {
			'retries': 2,
			'minTimeout': 500,
			'maxTimeout': 500
		    },
		    'log': log.child({
			'component': 'cache-client-' + url,
			'level': 'info'
		    })
		}));
	}, log.child({ 'component': 'ObjectCache' }));

	serv1.listen(8123, function () {
		serv2.listen(8125, next);
	});
}

function runSuccess(_, next)
{
	log.info('runSuccess');

	stream = new mod_httpcat.HttpCatStream({
	    'clients': cache,
	    'log': log.child({ 'component': 'HttpCatStream1' })
	});

	stream.write({
	    'url': 'http://localhost:8123',
	    'uri': '/file1'
	});

	stream.write({
	    'url': 'http://localhost:8125',
	    'uri': '/file2'
	});

	stream.end({
	    'url': 'http://localhost:8123',
	    'uri': '/file3'
	});

	buffer = '';
	stream.on('data', function (chunk) { buffer += chunk; });
	stream.on('end', function () {
		mod_assert.equal(buffer.toString('utf8'), [
		    '"8123/file1"',
		    '"8125/file2"',
		    '"8123/file3"'
		].join(''));

		next();
	});
}

function runError(_, next)
{
	log.info('runError');

	stream = new mod_httpcat.HttpCatStream({
	    'clients': cache,
	    'log': log.child({ 'component': 'HttpCatStream2' })
	});

	stream.write({
	    'url': 'http://localhost:8123',
	    'uri': '/file1'
	});

	stream.write({
	    'url': 'http://localhost:8124',
	    'uri': '/file2'
	});

	stream.end({
	    'url': 'http://localhost:8123',
	    'uri': '/file3'
	});

	buffer = '';
	stream.on('data', function (chunk) { buffer += chunk; });
	stream.on('error', function (err) {
		log.info(err, 'expected error');

		mod_assert.equal(buffer.toString('utf8'), '"8123/file1"');

		next();
	});
}

function pause(_, next)
{
	log.info('pause');

	var nends = 0;
	stream = new mod_httpcat.HttpCatStream({
	    'clients': cache,
	    'log': log.child({ 'component': 'HttpCatStream2' })
	});

	stream.on('end', function () { nends++; });

	stream.pause();
	stream.end();
	mod_assert.equal(nends, 0);

	stream.resume();
	mod_assert.equal(nends, 1);

	stream.pause();
	stream.resume();
	mod_assert.equal(nends, 1);

	mod_assert.throws(function () { stream.end(); },
	    /*JSSTYLED*/
	    /"end" already invoked/);

	next();
}

function resume(_, next)
{
	var servLimited = mod_restify.createServer({
		'name': 'tsst.httpcat.js-timeout'
	});

	var resLength = 1000000;
	var resumed = false;

	/*
	 * Server that (1) can handle resumes and (2) doesn't send all the
	 * data at once.
	 */
	servLimited.get('/.*', function (req, res, snext) {
		var range = req.headers['range'];
		/*JSSTYLED*/
		var regex = /bytes=(\d+)-/;
		var match = range.match(regex);
		var startAt = match[1];
		var sentBytes = parseInt(startAt, 10);

		res.writeHead(200, {
			'content-length': resLength - sentBytes
		});

		res.socket.setTimeout(100);

		function writeData() {
			var numBytes = Math.min(1000, resLength - sentBytes);
			res.write(new Buffer(numBytes));
			sentBytes += numBytes;
			if (sentBytes === resLength) {
				res.end();
				snext();
			} else {
				process.nextTick(writeData);
			}
		}
		writeData();
	});

	servLimited.listen(8127, function () {
		stream = new mod_httpcat.HttpCatStream({
			'clients': cache,
			'log': log.child({ 'component': 'HttpCatStream1' })
		});

		stream.end({
			'url': 'http://localhost:8127',
			'uri': '/file1'
		});

		/*
		 * Client pauses the stream to artificially induce a timeout.
		 */
		var pausedOnce = false;
		stream.on('data', function (chunk) {
			if (!pausedOnce) {
				stream.pause();
				pausedOnce = true;
				setTimeout(function () {
					stream.resume();
				}, 500);
			}
		});

		stream.on('resume', function () {
			resumed = true;
		});

		stream.on('end', function () {
			servLimited.close();
			mod_assert.ok(resumed);
			next();
		});
	});
}

function etagMismatch(_, next)
{
	var servLimited = mod_restify.createServer({
		'name': 'tsst.httpcat.js-timeout'
	});

	var resLength = 1000000;

	/*
	 * Server that (1) can handle resumes, (2) doesn't send all the
	 * data at once, (3) emits different etags each time.
	 */
	var etagNumber = 0;
	servLimited.get('/.*', function (req, res, snext) {
		var range = req.headers['range'];
		/*JSSTYLED*/
		var regex = /bytes=(\d+)-/;
		var match = range.match(regex);
		var startAt = match[1];
		var sentBytes = parseInt(startAt, 10);

		res.writeHead(200, {
			'content-length': resLength - sentBytes,
			'etag': etagNumber
		});
		++etagNumber;

		res.socket.setTimeout(100);

		function writeData() {
			var numBytes = Math.min(1000, resLength - sentBytes);
			try {
				res.write(new Buffer(numBytes));
			} catch (e) {
				// If writing to the result fails, that's
				// alright.  We expect errors from the client.
				res.end();
				snext();
			}
			sentBytes += numBytes;
			if (sentBytes === resLength) {
				res.end();
				snext();
			} else {
				process.nextTick(writeData);
			}
		}
		writeData();
	});

	servLimited.listen(8127, function () {
		stream = new mod_httpcat.HttpCatStream({
			'clients': cache,
			'log': log.child({ 'component': 'HttpCatStream1' })
		});

		stream.end({
			'url': 'http://localhost:8127',
			'uri': '/file1'
		});

		/*
		 * Client pauses the stream to artificially induce a timeout.
		 */
		var pausedOnce = false;
		stream.on('data', function (chunk) {
			if (!pausedOnce) {
				stream.pause();
				pausedOnce = true;
				setTimeout(function () {
					stream.resume();
				}, 500);
			}
		});

		stream.on('error', function (err) {
			servLimited.close();
			next();
		});

		stream.on('end', function () {
			mod_assert.fail('Failed to throw etag mismatch error');
			servLimited.close();
			next();
		});
	});
}

function md5Mismatch(_, next)
{
	var servLimited = mod_restify.createServer({
		'name': 'tsst.httpcat.js-timeout'
	});

	/*
	 * Server that sends a bad etag.
	 */
	servLimited.get('/.*', function (req, res, snext) {
		res.writeHead(200, {
			'content-length': 26,
			'content-md5': 'foobarbaz'
		});
		res.end('abcdefghijklmnopqrstuvwxyz');
		res.end();
		snext();
	});

	servLimited.listen(8127, function () {
		stream = new mod_httpcat.HttpCatStream({
			'clients': cache,
			'log': log.child({ 'component': 'HttpCatStream1' })
		});

		stream.end({
			'url': 'http://localhost:8127',
			'uri': '/file1'
		});

		stream.on('error', function (err) {
			servLimited.close();
			next();
		});

		stream.on('end', function () {
			mod_assert.fail('Failed to throw error');
			servLimited.close();
			next();
		});
	});
}

function teardown(_, next)
{
	log.info('tearDown');
	serv1.close();
	serv2.close();
	next();
}

mod_vasync.pipeline({ 'funcs': [
    setup,
    runSuccess,
    runError,
    pause,
    resume,
    etagMismatch,
    md5Mismatch,
    teardown
] }, function (err) {
	if (err) {
		log.fatal(err, 'test failed');
		throw (err);
	}

	log.info('test passed');
});
