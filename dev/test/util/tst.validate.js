/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var mod_assert = require('assert');
var mod_test = require('../common');

/* BEGIN JSSTYLED */
var test_cases = [
    /* simplest valid case */
    [ null, {
	'name': '',
	'phases': [ { 'exec': 'wc' } ]
    } ],

    /* complex valid case */
    [ null, {
	'name': 'hello world',
	'phases': [ {
	    'exec': 'wc',
	    'assets': [ '/poseidon/stor/obj1' ],
	    'memory': 512,
	    'uarg': { 'billy-bob': true },
	    'image': '>0.0.0'
	}, {
	    'exec': 'wc',
	    'type': 'reduce',
	    'assets': [ '/poseidon/stor/obj1' ],
	    'memory': 1024,
	    'count': 10,
	    'uarg': { 'billy-bob': true },
	    'image': '>0.0.0'
	} ]
    } ],

    /* missing and bad values */
    [ /property "name".*required/,
      {} ],
    [ /property "phases".*required/,
      { 'name': '' } ],
    [ /property "phases".*number value found.*array is required/,
      { 'name': '', 'phases': 3 } ],
    [ /property "phases":.*minimum/,
      { 'name': '', 'phases': [] } ],
    [ /phases\[0\].exec.*required/,
      { 'name': '', 'phases': [ {} ] } ],
    [ /property "phases\[0\].exec.*number.*string is required/,
      { 'name': '', 'phases': [ { 'exec': 5 } ] } ],
    [ /property "name":.*number.*string is required/,
      { 'name': 3, 'phases': [ { 'exec': 'wc' } ] } ],

    /* extra fields should be rejected */
    [ /property "phases\[0\].junk": unsupported property/,
      { 'name': '', 'phases': [ { 'exec': 'wc', 'junk': true } ] } ],
    [ /property "junk": unsupported property/,
      { 'name': '', 'phases': [ { 'exec': 'wc' } ], 'junk': true } ],

    /* bad phase values */
    [ /property "phases\[0\].type"/,
      { 'name': '', 'phases': [ { 'exec': 'wc', 'type': 'junk' } ] } ],
    [ /property "phases\[0\].count":.*maximum value of/,
      { 'name': '', 'phases': [ { 'exec': 'wc', 'count': 1000 } ] } ],
    [ /property "phases\[0\].memory":/,
      { 'name': '', 'phases': [ { 'exec': 'wc', 'memory': 122 } ] } ],
    [ /property "phases\[0\].image": number.*string is required/,
      { 'name': '', 'phases': [ { 'exec': 'wc', 'image': 122 } ] } ],
    [ /property "phases\[0\].image": invalid semver range: ""/,
      { 'name': '', 'phases': [ { 'exec': 'wc', 'image': '' } ] } ],

    /* bad semantic values */
    [ /property "phases\[0\].image": unsupported version: "0.0.1"/,
      { 'name': '', 'phases': [ { 'exec': 'wc', 'image': '0.0.1' } ] } ]
];
/* END JSSSTYLED */

var client;
mod_test.pipeline({ 'funcs': [ setup, run, teardown ] });

function setup(_, next)
{
	mod_test.setup(function (c) { client = c; next(); });
}

function run(_, next)
{
	test_cases.forEach(function (testcase) {
		var input = testcase[1];

		[ true, false ].forEach(function (priv) {
			var err = client.jobValidate(input, priv);
			mod_test.log.info('testing', input, priv);
			if (err === null) {
				mod_test.log.info('validated okay');
				mod_assert.ok(testcase[0] === null,
				    'test case validated, but expected an error');
			} else {
				mod_test.log.info('validate failed', err.message);
				mod_assert.ok(testcase[0] !== null,
				    'test case failed, but expected it to validate');
				mod_assert.ok(testcase[0].test(err.message),
				    'expected error message ' + testcase[0].source);
			}
		});
	});

	next();
}

function teardown(_, next)
{
	mod_test.teardown(client, next);
}
