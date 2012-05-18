/*
 * lib/util.js: general-purpose utility functions.  These should generally be
 * pushed into an existing external package.
 */

var mod_assert = require('assert');
var mod_http = require('http');
var mod_jsprim = require('jsprim');

exports.maRestifyPanic = maRestifyPanic;
exports.maHttpProxy = maHttpProxy;
exports.maWrapIgnoreError = maWrapIgnoreError;
exports.maIgnoreError = maIgnoreError;

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
 * 	continue		Callback to be invoked on the forwarded
 * 	(optional)		request's "continue" event.
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
	server_args['path'] = request.url;

	if (!server_args.hasOwnProperty('headers'))
		server_args['headers'] = {};

	if (args['continue'])
		server_args['headers']['expect'] = '100-continue';

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
	request.pipe(subrequest);
	request.on('error', checkDone.bind(null, 'request error'));
	request.on('end', function () {
		request.log.trace('proxy: initial request end');
	});

	if (args['continue'])
		subrequest.on('continue', args['continue']);

	subrequest.on('response', function (subresponse) {
		request.log.trace('proxy: subresponse received');
		state['wroteHeader'] = true;
		response.writeHead(subresponse.statusCode, subresponse.headers);
		subresponse.pipe(response);
		subresponse.on('end', checkDone.bind(null, 'subresponse end'));
		subresponse.on('error',
		    checkDone.bind(null, 'subresponse error'));
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
		if (!codes || !('code' in err)) {
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
