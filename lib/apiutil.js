/*
 * lib/apiutil.js: utility functions for implementing REST APIs
 * Copyright (c) 2012, Joyent, Inc. All rights reserved.
 */

var mod_assert = require('assert');
var mod_jsv = require('jsv');
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
