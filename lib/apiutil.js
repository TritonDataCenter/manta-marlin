/*
 * lib/apiutil.js: utility functions for implementing REST APIs
 * Copyright (c) 2012, Joyent, Inc. All rights reserved.
 */

var mod_assert = require('assert');
var mod_jsv = require('JSV');
var mod_util = require('util');
var mod_restify = require('restify');

function JsonValidationError(report)
{
	/* Currently, we only do anything useful with the first error. */
	mod_assert.ok(report.errors.length > 0);
	var error = report.errors[0];

	/* The failed property is given by a URI with an irrelevant prefix. */
	var propname = error['uri'].substr(error['uri'].indexOf('#') + 2);

	var reason;

	/*
	 * Some of the default error messages are pretty arcane, so we define
	 * new ones here.
	 */
	switch (error['attribute']) {
	case 'type':
		reason = 'expected ' + error['details'];
		break;
	default:
		reason = error['message'].toLowerCase();
		break;
	}

	var message = reason + ': ' + propname;
	mod_restify.RestError.call(this, 409, 'JsonValidationError',
	    message, JsonValidationError);
	this.jsv_details = error;
}

mod_util.inherits(JsonValidationError, mod_restify.RestError);

exports.mlValidateSchema = function mlValidateSchema(schema, input)
{
	var env = mod_jsv.JSV.createEnvironment();
	var report = env.validate(input, schema);

	if (report.errors.length === 0)
		return (null);

	return (new JsonValidationError(report));
};

/*
 * Deep copy an acyclic *basic* Javascript object.  This only handles basic
 * scalars (strings, numbers, booleans) and arbitrarily deep arrays and objects
 * containing these.  This does *not* handle instances of other classes.
 */
function mlDeepCopy(obj)
{
	var ret, key;
	var marker = '__mlDeepCopy';

	if (obj && obj[marker])
		throw (new Error('attempted deep copy of cyclic object'));

	if (obj && obj.constructor == Object) {
		ret = {};
		obj[marker] = true;

		for (key in obj) {
			if (key == marker)
				continue;

			ret[key] = mlDeepCopy(obj[key]);
		}

		delete (obj[marker]);
		return (ret);
	}

	if (obj && obj.constructor == Array) {
		ret = [];
		obj[marker] = true;

		for (key = 0; key < obj.length; key++)
			ret.push(mlDeepCopy(obj[key]));

		delete (obj[marker]);
		return (ret);
	}

	/*
	 * It must be a primitive type -- just return it.
	 */
	return (obj);
}

exports.mlDeepCopy = mlDeepCopy;
