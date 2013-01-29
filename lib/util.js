/*
 * lib/util.js: general-purpose utility functions.  These should generally be
 * pushed into an existing external package.
 */

var mod_assert = require('assert');
var mod_fs = require('fs');
var mod_http = require('http');
var mod_jsprim = require('jsprim');
var mod_memorystream = require('memorystream');
var mod_path = require('path');
var mod_uuid = require('node-uuid');
var mod_util = require('util');
var mod_verror = require('verror');

var VError = mod_verror.VError;

exports.maRestifyPanic = maRestifyPanic;
exports.maHttpProxy = maHttpProxy;
exports.maWrapIgnoreError = maWrapIgnoreError;
exports.maIgnoreError = maIgnoreError;
exports.Throttler = Throttler;
exports.makeEtag = makeEtag;
exports.readConf = readConf;
exports.SaveGeneration = SaveGeneration;
exports.pathExtractFirst = pathExtractFirst;
exports.CVError = CVError;
exports.mantaSignNull = mantaSignNull;
exports.mantaFileSave = mantaFileSave;
exports.EventThrottler = EventThrottler;

function maRestifyPanic(request, response, route, err)
{
	request.log.fatal(err, 'FATAL ERROR handling %s %s', request.method,
	    request.url);
	throw (err);
}

/*
 * Forwards an HTTP request to another server for processing. Arguments:
 *
 * 	request, response	Local HTTP request, response
 *
 * 	server			Arguments describing the remote server, exactly
 * 				as passed to http.request(), including
 * 				socketPath, host, port, and so on.  Additional
 * 				headers may also be specified here.
 *
 *      continue		Function to be invoked either when 100-continue
 *      (optional)		is received from the server (if the "expect:
 *      			100-continue" header is set), or when the
 *      			response is received (if that header is not
 *      			set).
 *
 * Callback is invoked as usual as callback([err]).  This implementation assumes
 * a restify request and response, including "request.log" and "response.send"
 * methods.  In the event of error, the response is always completed by the time
 * the callback is invoked (i.e. it is not required (nor valid) to write the
 * error to the response from the callback).
 */
function maHttpProxy(args, callback)
{
	var request = args['request'];
	var response = args['response'];
	var server_args = mod_jsprim.deepCopy(args['server']);
	var continuefunc = args['continue'];
	var state, key, subrequest, checkDone;

	mod_assert.ok(request.log,
	    'expected "log" on request (not using restify?)');
	mod_assert.ok(response.send,
	    'expected "send" on response (not using restify?)');

	state = {
	    'summary': request.method + ' ' + request.url,
	    'error': false,
	    'wroteHeader': false,
	    'request_args': server_args
	};

	server_args['method'] = request.method;

	if (!server_args.hasOwnProperty('path'))
		server_args['path'] = request.url;

	if (!server_args.hasOwnProperty('headers'))
		server_args['headers'] = {};

	for (key in request.headers) {
		if (key.toLowerCase() == 'connection')
			continue;
		server_args['headers'][key] = request.headers[key];
	}

	checkDone = function (where, err) {
		request.log.trace('proxy: ' + where);

		if (state['error'])
			/*
			 * We've already emitted an error. Ignore subsequent
			 * errors and completion.
			 */
			return;

		if (!err) {
			callback();
			return;
		}

		state['error'] = true;
		request.log.error(err, 'error proxying request "%s": %s',
		    state['summary'], where);

		if (!state['wroteHeader']) {
			response.send(err);
			callback(err);
		} else {
			response.end();
		}
	};

	subrequest = mod_http.request(server_args);
	subrequest.on('error', checkDone.bind(null, 'subrequest error'));
	request.on('error', checkDone.bind(null, 'request error'));
	request.on('end', function () {
		request.log.trace('proxy: initial request end');
	});
	response.on('close', function () {
		request.log.error('response closed unexpectedly');
		subrequest.destroy();
	});

	if (server_args['headers']['expect'] == '100-continue') {
		request.pause();

		subrequest.on('continue', function () {
			if (continuefunc) {
				continuefunc();
				continuefunc = null;
			}

			response.writeContinue();
			request.pipe(subrequest);
			request.resume();
		});
	} else {
		request.pipe(subrequest);
	}

	subrequest.on('response', function (subresponse) {
		request.log.trace('proxy: subresponse received');

		state['wroteHeader'] = true;
		response.writeHead(subresponse.statusCode, subresponse.headers);
		subresponse.pipe(response);
		subresponse.on('end', checkDone.bind(null, 'subresponse end'));
		subresponse.on('error',
		    checkDone.bind(null, 'subresponse error'));

		if (response.statusCode < 400 && continuefunc)
			continuefunc();

		response.on('close', function () {
			request.log.error('response closed unexpectedly');
			subresponse.destroy();
		});
	});

	return (state);
}

/*
 * Wrap an asynchronous function with one that ignores a certain class of system
 * errors (or all errors).   More precisely, given an asynchronous function
 * invoked as
 *
 *    func([arg1[, arg2 ...]], callback)
 *
 * returns a wrapper function that invokes "func" with arg1..argN and a callback
 * that passes through all errors whose "code" in "codes".  "codes" may be
 * omitted entirely, in which case *all* errors are ignored.  The former is
 * useful for calls to unlink, where an ENOENT error is often okay.  The latter
 * is useful for calls to close, where most errors are not actionable.
 */
function maWrapIgnoreError(func, codes)
{
	return (function ignoreErr() {
		var args = Array.prototype.slice.call(arguments, 0,
		    arguments.length - 1);
		var callback = arguments[arguments.length - 1];

		args.push(maIgnoreError(callback, codes));
		func.apply(null, args);
	});
}

/*
 * Returns a wrapper for the given callback function that does NOT pass through
 * any errors whose code is one of "codes".  If "codes" is not specified, all
 * errors are ignored.
 */
function maIgnoreError(callback, codes)
{
	return (function maIgnoreCb(err) {
		if (!codes || !err || !('code' in err)) {
			callback();
			return;
		}

		for (var i = 0; i < codes.length; i++) {
			if (err['code'] == codes[i]) {
				callback();
				return;
			}
		}

		callback(err);
	});
}

/*
 * Simple interface for making sure an asynchronous operation doesn't occur more
 * frequently than the given interval.
 */
function Throttler(interval)
{
	this.p_interval = interval;
	this.p_start = undefined;
	this.p_done = undefined;
	this.p_ongoing = false;
}

Throttler.prototype.start = function ()
{
	mod_assert.ok(!this.p_ongoing);
	this.p_start = new Date();
	this.p_ongoing = true;
};

Throttler.prototype.done = function ()
{
	mod_assert.ok(this.p_ongoing);
	this.p_done = new Date();
	this.p_ongoing = false;
};

Throttler.prototype.tooRecent = function ()
{
	if (this.p_ongoing)
		return (true);

	if (this.p_done && Date.now() - this.p_done.getTime() < this.p_interval)
		/* Last request was too recent to try again. */
		return (true);

	return (false);
};

Throttler.prototype.ongoing = function ()
{
	return (this.p_ongoing);
};


/*
 * Keeps track of events by name and provides a simple check for determining
 * whether that event should be throttled.  This is used to avoid spamming logs
 * with messages about persistent bad state.
 */
function EventThrottler(period)
{
	this.et_period = period;
	this.et_cleared = undefined;
	this.et_events = {};
}

EventThrottler.prototype.throttle = function (key)
{
	if (this.et_events.hasOwnProperty(key))
		return (true);

	this.et_events[key] = true;
	return (false);
};

EventThrottler.prototype.flush = function (timestamp)
{
	if (timestamp === undefined)
		timestamp = Date.now();

	if (this.et_cleared !== undefined &&
	    timestamp - this.et_cleared < this.et_period)
		return;

	this.et_cleared = timestamp;
	this.et_events = {};
};


/*
 * Make up an arbitrary etag for the given object.
 */
function makeEtag(obj)
{
	return (mod_uuid.v4());
}

/*
 * Synchronously reads a JSON file and validates that it matches the given
 * schema.
 */
function readConf(log, schema, filename)
{
	var contents, json, error;

	try {
		contents = mod_fs.readFileSync(filename);
		json = JSON.parse(contents);
		error = mod_jsprim.validateJsonObject(schema, json);

		if (error)
			throw (error);
	} catch (ex) {
		log.fatal(ex, 'failed to read configuration');
		throw (ex);
	}

	return (json);
}

/*
 * Keeps track of objects with "dirty" and "saved" generation numbers and an
 * asynchronous operation that updates them.
 */
function SaveGeneration()
{
	this.s_dirty = 0;
	this.s_saved = 0;
	this.s_pending = undefined;
}

SaveGeneration.prototype.markDirty = function ()
{
	this.s_dirty++;
};

SaveGeneration.prototype.saveStart = function ()
{
	mod_assert.ok(this.s_pending === undefined,
	    'attempted concurrent saves');
	this.s_pending = this.s_dirty;
};

SaveGeneration.prototype.saveOk = function ()
{
	mod_assert.ok(this.s_pending !== undefined,
	    'no save operation pending');
	this.s_saved = this.s_pending;
	this.s_pending = undefined;
};

SaveGeneration.prototype.saveFailed = function ()
{
	mod_assert.ok(this.s_pending !== undefined,
	    'no save operation pending');
	this.s_pending = undefined;
};

SaveGeneration.prototype.dirty = function ()
{
	return (this.s_dirty > this.s_saved);
};

SaveGeneration.prototype.pending = function ()
{
	return (this.s_pending !== undefined);
};


/*
 * Extracts the first component of "path".  This is used to extract either the
 * account uuid or the login name from an input key.
 */
function pathExtractFirst(path)
{
	var i, j;

	i = path.indexOf('/');
	j = path.indexOf('/', i + 1);
	return (path.substr(i + 1, j - i - 1));
}

/*
 * Common error class that supports a "code" field.
 */
function CVError(code, options)
{
	var args = Array.prototype.slice.call(arguments, 1);
	VError.apply(this, args);
	this.code = code;
}

mod_util.inherits(CVError, VError);
CVError.prototype.name = 'CVError';


/*
 * node-manta "sign" function that produces no signature.  This should only be
 * used when also specifying an authToken, in which case muskie doesn't need a
 * signature.
 */
function mantaSignNull(_, callback)
{
	callback(null, null);
}

/*
 * Upload a file to Manta.  Automatically does "mkdir -p" if the parent
 * directory does not exist.  Arguments:
 *
 *    client		Manta client object
 *
 *    filename		Path of file to upload.  This file should not be
 *    			modified during this operation.  (This function is only
 *    			called by consumers who know there is nothing else
 *    			operating on the file.)
 *
 *    key		Object key to create
 *
 *    headers		Additional headers for PUT request
 *
 *    log		Bunyan logger
 *
 *    stderr		Boolean indicating this is a stderr stream
 */
function mantaFileSave(args, callback, noretry)
{
	mod_assert.ok(args['client'], '"client" is required');
	mod_assert.ok(args['log'], '"log" is required');
	mod_assert.equal(typeof (args['filename']), 'string',
	    '"filename" must be a string');
	mod_assert.equal(typeof (args['key']), 'string',
	    '"key" must be a string');

	var filename = args['filename'];

	mod_fs.stat(filename, function (err, stat) {
		if (err) {
			callback(new VError(err, 'stat "%s"', filename));
			return;
		}

		var instream;

		if (stat['size'] === 0) {
			instream = new mod_memorystream('',
			    { 'writable': false });
			mantaStreamSave({
			    'client': args['client'],
			    'stream': instream,
			    'size': 0,
			    'key': args['key'],
			    'headers': args['headers'],
			    'stderr': args['stderr']
			}, callback, noretry);
			return;
		}

		var opened = false;
		instream = mod_fs.createReadStream(filename);

		instream.on('error', function (suberr) {
			callback(new VError(suberr, '%s "%s"',
			    opened ? 'read' : 'open', filename));
		});

		instream.on('open', function () {
			opened = true;

			mantaStreamSave({
			    'client': args['client'],
			    'stream': instream,
			    'size': stat['size'],
			    'key': args['key'],
			    'headers': args['headers'],
			    'stderr': args['stderr']
			}, callback, noretry);
		});
	});
}

function mantaStreamSave(args, callback, noretry)
{
	mod_assert.ok(args['client'], '"client" is required');
	mod_assert.ok(args['stream'], '"stream" is required');
	mod_assert.equal(typeof (args['size']), 'number',
	    '"size" must be a number');
	mod_assert.equal(typeof (args['key']), 'string',
	    '"key" must be a string');

	var client = args['client'];
	var key = args['key'];

	var options = {
	    'size': args['size'],
	    'headers': {}
	};

	if (args['headers']) {
		for (var header in args['headers'])
			options['headers'][header] = args['headers'][header];
	}

	options['headers']['x-marlin-stream'] =
	    args['stderr'] ? 'stderr': 'stdout';

	client.put(key, args['stream'], options, function (err) {
		if (!err) {
			callback();
			return;
		}

		if (noretry || err.name != 'DirectoryDoesNotExistError') {
			callback(err);
			return;
		}

		client.mkdirp(mod_path.dirname(key), function (err2) {
			if (err2) {
				callback(new VError(err2, 'failed to mkdirp'));
				return;
			}

			mantaStreamSave(args, callback, true);
		});
	});
}
