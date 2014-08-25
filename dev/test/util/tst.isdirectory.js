/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * tst.isdirectory.js: tests isMantaDirectory function
 */

var mod_assert = require('assert');
var mod_mautil = require('../../lib/util');

var test_cases = [
    [ false,	null						],
    [ false,	''						],
    [ false,	'text/html'					],
    [ false,	'text/html;type=directory'			],
    [ false,	'application/json'				],
    [ true,	'application/json;type=directory'		],
    [ true,	'application/json ;type=directory'		],
    [ true,	'application/json ; type=directory'		],
    [ true,	'application/json ; type =directory'		],
    [ true,	'application/json ; type = directory'		],
    [ true,	'application/json; type = directory; a=b'	],
    [ true,	'application/json; a=b; type=directory; c=d'	],
    [ false,	'application/json; a=b; c=d'			]
];

test_cases.forEach(function (testcase) {
	process.stdout.write('case "' + testcase[1] + '": ');
	var result = mod_mautil.isMantaDirectory(testcase[1]);
	process.stdout.write(result + '\n');
	mod_assert.equal(testcase[0], result);
});
