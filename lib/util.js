/*
 * lib/util.js: general-purpose utility functions.  These should generally be
 * pushed into an existing external package.
 */

exports.maRestifyPanic = maRestifyPanic;

function maRestifyPanic(request, response, route, err)
{
	request.log.fatal(err, 'FATAL ERROR handling %s %s', request.method,
	    request.url);
	throw (err);
}
