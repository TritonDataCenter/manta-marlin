/*
 * lib/http-cat-stream.js: concatenate objects stored on a remote HTTP server
 */

var mod_assert = require('assert');
var mod_crypto = require('crypto');
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
	this.hcs_endemitted = false;
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
	 * and process the next resource.  See kick() and resumeGet() for
	 * details.
	 */
	this.hcs_current = undefined;
	this.hcs_request = undefined;
	this.hcs_response = undefined;
	this.hcs_bytes_read = undefined;
	this.hcs_resume_count = undefined;
	this.hcs_exp_md5 = undefined;
	this.hcs_exp_len = undefined;
	this.hcs_md5 = undefined;
	this.hcs_etag = undefined;
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

	if (this.hcs_response) {
		this.hcs_response.resume();
	} else if (this.hcs_ended && !this.hcs_endemitted &&
	    this.hcs_urls.length === 0 && this.hcs_current === undefined) {
		this.hcs_endemitted = true;
		this.emit('end');
	}
};

HttpCatStream.prototype.write = function (rqinfo)
{
	if (!this.writable)
		throw (new Error('stream is not writable'));

	this.hcs_urls.push(rqinfo);
	this.hcs_log.trace('enqueuing resource: %j', rqinfo);
	this.kick();
};

HttpCatStream.prototype.resumeGet = function (cb)
{
	if (this.hcs_current === undefined) {
		cb();
		return;
	}

	var stream = this;
	var url = stream.hcs_current['url'];
	var uri = stream.hcs_current['uri'];
	var client = stream.hcs_cache.get(url);
	var startRange = stream.hcs_bytes_read;

	/*
	 * We treat any errors we see as fatal to this stream.  Connection
	 * errors are retried by the underlying client object, so we won't see
	 * them here.  Other errors occur after we've already emitted some data
	 * as part of a given request, in which case we attempt to resume the
	 * download.
	 *
	 * If the client itself emits an error, we remove it from the cache so
	 * someone else doesn't try to use it again.  We remove the listener
	 * that does this as soon as we get the request callback because errors
	 * intended for us after that will not be emitted on the client object
	 * (but errors from other consumers of the same client may be).
	 */
	var clientError = function (err) {
		stream.hcs_log.error(err, 'client error');
		stream.hcs_cache.remove(url, client);
		cb(err);
	};

	client.on('error', clientError);

	var getOpts = {
		path: uri,
		headers: {
			'range': 'bytes=' + startRange + '-'
		}
	};

	client.get(getOpts, function (err, request) {
		client.removeListener('error', clientError);

		if (err) {
			stream.hcs_log.error(err, 'client GET error');
			cb(err);
			return;
		}

		stream.hcs_request = request;

		request.on('error', function (err2) {
			stream.hcs_log.error(err2, 'request error');
			cb(err);
			return;
		});

		request.on('result', function (err2, response) {
			if (err2) {
				stream.hcs_log.error(err2, 'request error');
				cb(err);
				return;
			}

			// The indication that this can be treated as the
			// start rather than a resume.
			if (stream.hcs_bytes_read === 0) {
				stream.hcs_response = response;
				stream.hcs_resume_count = 0;
				var headers = response.headers;
				stream.hcs_exp_md5 = headers['content-md5'];
				stream.hcs_exp_len = headers['content-length'];
				stream.hcs_md5 = mod_crypto.createHash('md5');
				stream.hcs_etag = response.headers['etag'];
			} else {
				var new_etag = response.headers['etag'];
				if (stream.hcs_etag !== new_etag) {
					var m = 'Get Resume, etag has ' +
						'changed.  Must abort request.';
					stream.hcs_log.error({
						uri: uri,
						prevEtag: stream.hcs_etag,
						newEtag: new_etag
					}, m);
					cb(new Error(m));
				}
				stream.hcs_response = response;
			}

			stream.hcs_log.debug('received response: %d',
			    response.statusCode);

			if (stream.hcs_paused)
				response.pause();

			response.on('data', function (chunk) {
				stream.hcs_log.trace('received %d bytes',
				    chunk.length);
				stream.hcs_bytes_read += chunk.length;
				stream.hcs_md5.update(chunk);
				stream.emit('data', chunk);
			});

			response.on('end', function () {
				stream.hcs_log.debug('got end event for %j,' +
						     ' read %d bytes',
						     stream.hcs_current,
						     stream.hcs_bytes_read);

				// MANTA-1006
				var exp_len = stream.hcs_exp_len;
				var cur_len = stream.hcs_bytes_read;
				var res_count = stream.hcs_resume_count;
				if (exp_len < cur_len) {
					stream.hcs_log.debug({
						uri: uri,
						resumeCount: res_count,
						bytesRead: cur_len,
						expectedBytes: exp_len
					}, 'resuming download');
					++stream.hcs_resume_count;
					stream.resumeGet(cb);
					return;
				}

				var md5 = stream.hcs_exp_md5;
				var cmd5 = stream.hcs_md5.digest('base64');
				if (md5 && md5 !== cmd5) {
					stream.hcs_log.error({
						uri: uri,
						expectedMd5: md5,
						calculatedMd5: cmd5,
						bytesRead: cur_len,
						expectedBytes: exp_len,
						etag: stream.hcs_etag
					}, 'md5 mismatch');
					cb(new Error('md5 mismatch'));
					return;
				}

				stream.hcs_request = undefined;
				stream.hcs_response = undefined;
				stream.hcs_resume_count = undefined;
				stream.hcs_exp_md5 = undefined;
				stream.hcs_exp_len = undefined;
				stream.hcs_md5 = undefined;
				stream.hcs_etag = undefined;
				cb();
			});
		});
	});
};

HttpCatStream.prototype.kick = function ()
{
	if (this.hcs_current !== undefined)
		return;

	if (this.hcs_urls.length === 0) {
		this.emit('drain');

		if (this.hcs_ended && !this.hcs_endemitted) {
			this.readable = false;
			this.hcs_endemitted = true;
			this.emit('end');
		}

		return;
	}

	var stream = this;
	var next = this.hcs_urls.shift();

	stream.hcs_current = next;
	stream.hcs_bytes_read = 0;
	this.hcs_log.debug('fetching %j', next);

	stream.resumeGet(function (err) {
		if (err) {
			stream.emit('error', err);
			stream.destroy();
			return;
		}

		stream.hcs_current = undefined;
		stream.hcs_bytes_read = undefined;
		stream.hcs_ndone++;
		stream.kick();
	});
};

HttpCatStream.prototype.end = function (rqinfo)
{
	if (this.hcs_ended)
		throw (new Error('"end" already invoked'));

	if (arguments.length > 0)
		this.write(rqinfo);

	this.writable = false;
	this.hcs_ended = true;

	this.hcs_log.trace('end invoked');

	if (this.hcs_current === undefined && this.hcs_urls.length === 0) {
		this.readable = false;

		if (!this.hcs_paused && !this.hcs_endemitted)
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
