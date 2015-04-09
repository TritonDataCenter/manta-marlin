/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * tst.pathabbr.js: basic test for pathAbbreviate
 */

var mod_assertplus = require('assert-plus');
var pathAbbreviate = require('../../lib/path-abbrev').pathAbbreviate;

var name20;
var name255, name256, name257;
var path1023, path1024, path1025, path1026, pathbig;
var pathdirs1023, pathdirs1024, pathdirs1025, pathdirs1026;
var i, testcases;

/*
 * Populate the very long names and paths that we'll use for our test cases.
 */
name20 = '12345678901234567890';
name255 = name20;
for (i = 0; i < 11; i++) {
	name255 += name20;
}
name255 += '123456789012345';
mod_assertplus.equal(name255.length, 255);
name256 = name255 + '6';
mod_assertplus.equal(name256.length, 256);
name257 = name256 + '7';
mod_assertplus.equal(name257.length, 257);

path1024 = '/' + [ name255, name255, name255, name255 ].join('/');
mod_assertplus.ok(path1024.length, 1024);
path1023 = path1024.substr(0, 1023);
mod_assertplus.ok(path1023.length, 1023);
path1025 = path1024 + '/';
mod_assertplus.ok(path1025.length, 1025);
path1026 = path1025 + 'a';
mod_assertplus.ok(path1026.length, 1026);
pathbig = path1025 + name255;
mod_assertplus.equal(pathbig.length, 1025 + 255);

pathdirs1023 = '';
for (i = 0; i < 511; i++) {
	pathdirs1023 += '/' + (i % 10);
}
pathdirs1023 += '/';
mod_assertplus.equal(pathdirs1023.length, 1023);
pathdirs1024 = pathdirs1023 + (i % 10);
mod_assertplus.equal(pathdirs1024.length, 1024);
pathdirs1025 = pathdirs1024 + '/';
mod_assertplus.equal(pathdirs1025.length, 1025);
pathdirs1026 = pathdirs1025 + (i % 10);
mod_assertplus.equal(pathdirs1026.length, 1026);

testcases = [
/*
 * The first tests check PATH_MAX edge cases.  In all of these, NAME_MAX is not
 * violated.  Since the components are made up of NAME_MAX characters, this also
 * tests that the PATH_MAX truncation never causes us to violate the NAME_MAX
 * constraint.
 */
{
    'name': 'PATH_MAX edge-case (1023)',
    'path': path1023,
    'shortened': false
}, {
    'name': 'PATH_MAX edge-case (1024)',
    'path': path1024,
    'shortened': false
}, {
    'name': 'PATH_MAX edge-case (1025)',
    'path': path1025,
    'shortened': true
}, {
    'name': 'PATH_MAX edge-case (1026)',
    'path': path1026,
    'shortened': true
}, {
    'name': 'PATH_MAX edge-case (big path)',
    'path': pathbig,
    'shortened': true
},

/*
 * Now test cases where the PATH_MAX components are each too small to
 * abbreviate.
 */
{
    'name': 'PATH_MAX, small components (1023)',
    'path': pathdirs1023,
    'shortened': false
}, {
    'name': 'PATH_MAX, small components (1024)',
    'path': pathdirs1024,
    'shortened': false
}, {
    'name': 'PATH_MAX, small components (1025)',
    'path': pathdirs1025,
    'shortened': true
}, {
    'name': 'PATH_MAX, small components (1026)',
    'path': pathdirs1026,
    'shortened': true
},

/*
 * Now test various NAME_MAX edge cases.  These do not violate PATH_MAX.
 */
{
    'name': 'NAME_MAX directory edge-case (255)',
    'path': '/' + name255 + '/foo',
    'shortened': false
}, {
    'name': 'NAME_MAX directory edge-case (256)',
    'path': '/' + name256 + '/foo',
    'shortened': true
}, {
    'name': 'NAME_MAX directory edge-case (257)',
    'path': '/' + name257 + '/foo',
    'shortened': true
}, {
    'name': 'NAME_MAX basename edge-case (255)',
    'path': '/foo/' + name255,
    'shortened': false
}, {
    'name': 'NAME_MAX basename edge-case (256)',
    'path': '/foo/' + name256,
    'shortened': true
}, {
    'name': 'NAME_MAX basename edge-case (257)',
    'path': '/foo/' + name257,
    'shortened': true
}
];

testcases.forEach(function (tc) {
	var pa, pa2;

	mod_assertplus.string(tc.name);
	mod_assertplus.string(tc.path);
	mod_assertplus.bool(tc.shortened);

	console.error('test case: %s', tc.name);
	pa = pathAbbreviate({ 'path': tc.path });
	console.error(pa);
	mod_assertplus.ok(!(pa instanceof Error));
	mod_assertplus.equal(tc.shortened, pa.shortened);
	mod_assertplus.ok(pa.abbrpath.length <= 1024);

	/*
	 * If there was no shortening, then the returned path should be
	 * identical to the input.
	 */
	if (!pa.shortened)
		mod_assertplus.equal(pa.abbrpath, tc.path);

	/*
	 * Try abbreviating the one we got back.  It should not be shortened
	 * because it already obeys the constraints.
	 */
	pa2 = pathAbbreviate({ 'path': pa.abbrpath });
	mod_assertplus.equal(pa2.abbrpath, pa.abbrpath);
	mod_assertplus.ok(!pa2.shortened);
});

console.log('test okay');
