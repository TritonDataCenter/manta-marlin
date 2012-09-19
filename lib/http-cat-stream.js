/*
 * lib/http-cat-stream.js: concatenate objects stored on a remote HTTP server
 */

var mod_assert = require('assert');
var mod_stream = require('stream');
var mod_util = require('util');

var mod_restify = require('restify');

/* Public interface */
exports.ObjectCache = ObjectCache;
exports.HttpCatStream = HttpCatStream;

/*
 * Generic object cache: this object provides get(key), which instantiates a new
 * object using "constructor" and caches it under the specified key.  Subsequent
 * requests for the same key return the same object unless it's been removed
 * with "remove".  This is used to implement an HTTP client cache indexed by
 * host.
 */
function ObjectCache(constructor, log)
{
	this.oc_cons = constructor;
	this.oc_cache = {};
	this.os_log = log;
}

ObjectCache.prototype.get = function (key)
{
	if (!this.oc_cache.hasOwnProperty(key)) {
		this.os_log.trace('creating new object for key "%s"', key);
		this.oc_cache[key] = this.oc_cons(key);
	}

	return (this.oc_cache[key]);
};

ObjectCache.prototype.remove = function (key, obj)
{
	if (arguments.length < 2 || this.oc_cache[key] == obj) {
		this.os_log.trace('removing object for key "%s"', key);
		delete (this.oc_cache[key]);
	}
};


/*
 * Custom stream object to concatenate the contents of multiple remote HTTP
 * resources.  Callers call write(params), where "params" is an object
 * with these properties:
 *
 *    url	restify-style URL for the remote server (NOT the remote path)
 *		e.g., "http://www.joyent.com:8080"
 *
 *    uri	path on the remote server to request, using GET
 *		e.g., "/favicon.png"
 *
 * This stream fetches these resources in order and emits each resource's
 * contents.  When each one completes, the next one is fetched, without emitting
 * an "end" event until the last resource has been fetched.
 */
function HttpCatStream(options)
{
	mod_assert.ok(options.hasOwnProperty('clients'),
	    'client cache is required');
	mod_assert.ok(options.hasOwnProperty('log', 'log is required'));

	this.writable = true;
	this.readable = true;
	this.hcs_paused = false;
	this.hcs_ended = false;
	this.hcs_log = options.log;
	this.hcs_cache = options.clients;  /* restify clients ObjectCache */
	this.hcs_urls = [];		   /* queue of resources to fetch */
	this.hcs_ndone = 0;		   /* number of completed resources */

	/*
	 * The following fields maintain state for whatever remote resource
	 * we're processing:
	 *
	 *    hcs_current		current resource, as passed to write()
	 *    hcs_request		ClientRequest object
	 *    hcs_response		ClientResponse object
	 *
	 * These are assigned in order as we process a resource:
	 *
	 *    STATE			INTERNAL STATE
	 *    initial state		all fields undefined
	 *    allocating socket		hcs_current assigned, others undefined
	 *    request sent		hcs_current, hcs_request assigned,
	 *				hcs_response undefined
	 *    response headers recvd	all fields assigned; data is streaming.
	 *
	 * Once the response is fully received, we go back to the initial state
	 * and process the next resource.  See kick() for details.
	 */
	this.hcs_current = undefined;
	this.hcs_request = undefined;
	this.hcs_response = undefined;
}

mod_util.inherits(HttpCatStream, mod_stream.Stream);

HttpCatStream.prototype.pause = function ()
{
	this.hcs_paused = true;

	if (this.hcs_response)
		this.hcs_response.pause();
};

HttpCatStream.prototype.resume = function ()
{
	this.hcs_paused = false;

	if (this.hcs_response)
		this.hcs_response.resume();
};

HttpCatStream.prototype.write = function (rqinfo)
{
	if (!this.writable)
		throw (new Error('stream is not writable'));

	this.hcs_urls.push(rqinfo);
	this.hcs_log.trace('enqueuing resource: %j', rqinfo);
	this.kick();
};

HttpCatStream.prototype.kick = function ()
{
	if (this.hcs_current !== undefined)
		return;

	if (this.hcs_urls.length === 0) {
		this.emit('drain');

		if (this.hcs_ended) {
			this.readable = false;
			this.emit('end');
		}

		return;
	}

	var stream = this;
	var next = this.hcs_urls.shift();
	var client = this.hcs_cache.get(next['url']);

	stream.hcs_current = next;
	this.hcs_log.trace('fetching %j', next);

	/*
	 * We treat any errors we see as fatal to this stream.  Connection
	 * errors are retried by the underlying client object, so we won't see
	 * them here.  Other errors occur after we've already emitted some data
	 * as part of a given request, in which case there's not much we can do
	 * short of retrying the request, hoping the data hasn't changed, and
	 * skipping all the data we already emitted.
	 *
	 * If the client itself emits an error, we remove it from the cache so
	 * someone else doesn't try to use it again.  We remove the listener
	 * that does this as soon as we get the request callback because errors
	 * intended for us after that will not be emitted on the client object
	 * (but errors from other consumers of the same client may be).
	 */
	var clientError = function (err) {
		stream.hcs_log.error(err, 'client error');
		stream.hcs_cache.remove(next['url'], client);
		stream.emit('error', err);
		stream.destroy();
	};

	client.on('error', clientError);

	client.get(next['uri'], function (err, request) {
		client.removeListener('error', clientError);

		if (err) {
			stream.hcs_log.error(err, 'client GET error');
			stream.emit('error', err);
			stream.destroy();
			return;
		}

		stream.hcs_request = request;

		request.on('error', function (err2) {
			stream.hcs_log.error(err2, 'request error');
			stream.emit('error', err2);
			stream.destroy();
			return;
		});

		request.on('result', function (err2, response) {
			if (err2) {
				stream.hcs_log.error(err2, 'request error');
				stream.emit('error', err2);
				stream.destroy();
				return;
			}

			stream.hcs_response = response;
			stream.hcs_log.trace('received response: %d',
			    response.statusCode);

			if (stream.hcs_paused)
				response.pause();

			stream.hcs_response = response;

			response.on('data', function (chunk) {
				stream.hcs_log.trace('received %d bytes',
				    chunk.length);
				stream.emit('data', chunk);
			});

			response.on('end', function () {
				stream.hcs_log.trace('finished processing %j',
				    stream.hcs_current);
				stream.hcs_current = undefined;
				stream.hcs_request = undefined;
				stream.hcs_response = undefined;
				stream.hcs_ndone++;
				stream.kick();
			});
		});
	});
};

HttpCatStream.prototype.end = function (rqinfo)
{
	if (arguments.length > 0)
		this.write(rqinfo);

	this.writable = false;
	this.hcs_ended = true;

	this.hcs_log.trace('end invoked');

	if (this.hcs_current === undefined && this.hcs_urls.length === 0) {
		this.readable = false;
		this.emit('end');
	}
};

HttpCatStream.prototype.destroy = function ()
{
	this.writable = false;

	if (this.hcs_request)
		this.hcs_request.destroy();

	if (this.hcs_response)
		this.hcs_response.destroy();
};

HttpCatStream.prototype.destroySoon = function ()
{
	this.end();
};
