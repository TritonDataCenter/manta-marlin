/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * tst.big.js: Test cases that involve large numbers of inputs.
 */

var mod_livetests = require('./common');

var testcases = {
    'jobM500': {
	'job': {
	    'phases': [ { 'type': 'map', 'exec': 'wc' } ]
	},
	'inputs': [],
	'timeout': 90 * 1000,
	'expected_outputs': [],
	'errors': []
    },

    'jobMR1000': {
	'job': {
	    'phases': [
		{ 'type': 'map', 'exec': 'wc' },
		{ 'type': 'reduce', 'exec': 'wc' }
	    ]
	},
	'inputs': [],
	'timeout': 600 * 1000,
	'expected_outputs': [ /%user%\/jobs\/.*\/stor\/reduce\.1\./ ],
	'expected_output_content': [ '   1000    3000    9000\n' ],
	'errors': []
    },

    'jobM4RR1000': {
	'job': {
	    'phases': [
		{ 'type': 'map', 'exec': 'wc' },
		{ 'type': 'reduce', 'count': 4, 'exec': 'wc' },
		{ 'type': 'reduce',
		    'exec': 'awk \'{sum+=$1} END { print sum }\'' }
	    ]
	},
	'inputs': [],
	'timeout': 600 * 1000,
	'expected_outputs': [ /%user%\/jobs\/.*\/stor\/reduce\.2\./ ],
	'expected_output_content': [ '1000\n' ],
	'errors': []
    }
};

function main()
{
	var job, i, key, okey;

	job = testcases.jobM500;
	for (i = 0; i < 500; i++) {
		key = '/%user%/stor/obj' + i;
		okey = '/%user%/jobs/.*/stor' + key;

		job['inputs'].push(key);
		job['expected_outputs'].push(new RegExp(okey));
	}

	job = testcases.jobMR1000;
	for (i = 0; i < 1000; i++) {
		key = '/%user%/stor/obj' + i;
		job['inputs'].push(key);
	}

	job = testcases.jobM4RR1000;
	for (i = 0; i < 1000; i++) {
		key = '/%user%/stor/obj' + i;
		job['inputs'].push(key);
	}

	mod_livetests.jobTestRunner(testcases, process.argv, 1);
}

main();
