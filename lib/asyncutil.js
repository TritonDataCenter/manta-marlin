/*
 * lib/asyncutil.js: routines for asynchronous control flow
 */

var mod_assert = require('assert');
var mod_util = require('util');

var mod_jsutil = require('./jsutil');

/*
 * Public interface
 */
exports.asParallel = asParallel;
exports.asForEach = asForEach;
exports.asPipeline = asPipeline;
exports.asMultiError = asMultiError; /* for testing only */

/*
 * Represents a collection of errors for the purpose of consumers that generally
 * only deal with one error.  Callers can extract the individual errors
 * contained in this object, but may also just treat it as a normal single
 * error, in which case a summary message will be printed.
 */
function asMultiError(errors)
{
	mod_assert.ok(errors.length > 0);
	this.ase_errors = errors;

	mod_jsutil.jsError.call(this, errors[0], 'first of %d error%s',
	    errors.length, errors.length == 1 ? '' : 's');
}

mod_util.inherits(asMultiError, mod_jsutil.jsError);


/*
 * Given a set of functions that complete asynchronously using the standard
 * callback(err, result) pattern, invoke them all and merge the results.  There
 * are three main use cases:
 *
 *    o Invoke N operations asynchronously, and the caller will report only one
 *      of the failed errors.  This is the most common, since most operations
 *      only deal well with a single error anyway.
 *
 *    o Invoke N operations asynchronously, and the caller will report all
 *      errors.  One case where this is desirable is user input validation,
 *      where the user may want to fix all errors at once with each attempt.
 *
 *    o Invoke N operations asynchronously, and the caller will simply use
 *      whatever values complete successfully.  This may be useful when querying
 *      the status of multiple services, for example, where the caller doesn't
 *      want to fail just because one of the services didn't respond.
 *
 * To summarize, we must provide both "err" and "result" from each individual
 * callback, but we also want to make it easy to check for "any errors" as well
 * as grab an arbitrary error.
 *
 * We invoke "callback" as callback(err, results), where "err" is a MultiError
 * (see above) and "results" is an object with the following fields:
 *
 *    operations	array corresponding to the input functions, with
 *
 *				func		input function
 *
 *				status		"pending", "ok", or "fail"
 *
 *				err		returned "err" value, if any
 *
 *				result		returned "result" value, if any
 *
 *    ndone		number of input operations that have completed
 *
 *    nerrors		number of input operations that have failed
 *
 * For case (1), the caller need only look at the "err" object.
 * For case (2), the caller can examine the MultiError object to see all of the
 * errors.
 * For case (3), the caller can examine the results of each operation directly.
 *
 * For debugging, this function actually returns the above object synchronously
 * as well (with ndone == nerrors == 0 and all operations "pending", of course).
 * This allows the state of pending operations to be examined using a debugger.
 */
function asParallel(args, callback)
{
	var funcs, rv, doneOne, i;

	funcs = args['funcs'];

	rv = {
	    'operations': new Array(funcs.length),
	    'ndone': 0,
	    'nerrors': 0
	};

	if (funcs.length === 0) {
		process.nextTick(function () { callback(null, rv); });
		return (rv);
	}

	doneOne = function (entry) {
		return (function (err, result) {
			mod_assert.equal(entry['status'], 'pending');

			entry['err'] = err;
			entry['result'] = result;
			entry['status'] = err ? 'fail' : 'ok';

			if (++rv['ndone'] < funcs.length)
				return;

			var errors = rv['operations'].filter(function (ent) {
				return (ent['status'] == 'fail');
			}).map(function (ent) { return (ent['err']); });

			if (errors.length > 0)
				callback(new asMultiError(errors));
			else
				callback();
		});
	};

	for (i = 0; i < funcs.length; i++) {
		rv['operations'][i] = {
			'func': funcs[i],
			'status': 'pending'
		};

		funcs[i](doneOne(rv['operations'][i]));
	}

	return (rv);
}

/*
 * Exactly like asParallel, except that the input is specified as a single
 * function to invoke on N different inputs (rather than N functions).  "args"
 * must have the following fields:
 *
 *	func		asynchronous function to invoke on each input value
 *
 *	inputs		array of input values
 */
function asForEach(args, callback)
{
	var func, funcs;

	func = args['func'];
	funcs = args['inputs'].map(function (input) {
		return (function (subcallback) {
			return (func(input, subcallback));
		});
	});

	return (asParallel({ 'funcs': funcs }, callback));
}

/*
 * Like asParallel, but invokes functions in sequence rather than in parallel
 * and aborts if any function exits with failure.  Arguments include:
 *
 *    funcs	invoke the functions in parallel
 *
 *    arg	first argument to each pipeline function
 */
function asPipeline(args, callback)
{
	var funcs, uarg, rv, next;

	funcs = args['funcs'];
	uarg = args['arg'];

	rv = {
	    'operations': funcs.map(function (func) {
		return ({ 'func': func, 'status': 'waiting' });
	    }),
	    'ndone': 0,
	    'nerrors': 0
	};

	if (funcs.length === 0) {
		process.nextTick(function () { callback(null, rv); });
		return (rv);
	}

	next = function (err, result) {
		var entry = rv['operations'][rv['ndone']++];

		mod_assert.equal(entry['status'], 'pending');

		entry['status'] = err ? 'fail' : 'ok';
		entry['err'] = err;
		entry['result'] = result;

		if (err)
			rv['nerrors']++;

		if (err || rv['ndone'] == funcs.length) {
			callback(err, rv);
		} else {
			var nextent = rv['operations'][rv['ndone']];
			nextent['status'] = 'pending';

			/*
			 * We invoke the next function on the next tick so that
			 * the caller (stage N) need not worry about the case
			 * that the next stage (stage N + 1) runs in its own
			 * context.
			 */
			process.nextTick(function () {
				nextent['func'](uarg, next);
			});
		}
	};

	rv['operations'][0]['status'] = 'pending';
	funcs[0](uarg, next);

	return (rv);
}
