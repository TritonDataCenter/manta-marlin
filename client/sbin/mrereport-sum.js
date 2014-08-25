#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var mod_assert = require('assert');
var mod_path = require('path');
var mod_util = require('util');

var mod_lstream = require('lstream');

function main()
{
	var lstream = new mod_lstream();
	var summer = new Summer(lstream);

	process.stdin.pipe(lstream);
	process.stdin.on('end', function () { summer.printSummary(); });
	lstream.resume();
}

function Summer(lstream)
{
	this.s_arg0 = mod_path.basename(process.argv[1]);
	this.s_seen = {};
	this.s_users = {};
	this.s_jobs = {};
	this.s_tasks = {};
	this.s_errors = {};
	this.s_nwarnings = 0;
	this.s_line = 0;

	var accumulator = this;
	lstream.on('line', function (line) { accumulator.gotLine(line); });
}

Summer.prototype.warn = function ()
{
	var args = Array.prototype.slice.call(arguments);
	var msg = mod_util.format.apply(null, args);
	console.error('%s: line %s: %s', this.s_arg0, this.s_line, msg);
	this.s_nwarnings++;
};

Summer.prototype.gotLine = function (line)
{
	this.s_line++;

	var row;

	try {
		row = JSON.parse(line);
	} catch (ex) {
		this.warn(ex.name + ': ' + ex.message);
		return;
	}

	this.s_nwarnings += row['nwarnings'];
	this.walk(this.s_users, row['users']);
	this.walk(this.s_jobs, row['jobs']);
	this.walk(this.s_tasks, row['tasks']);
	this.walk(this.s_errors, row['errors']);
};

Summer.prototype.walk = function (ours, got)
{
	for (var key in got) {
		if (!ours.hasOwnProperty(key)) {
			ours[key] = got[key];
			continue;
		}

		if (typeof (got[key]) == 'number') {
			ours[key] += got[key];
			continue;
		}

		if (typeof (got[key]) != 'object')
			mod_assert.equal(ours[key], got[key]);

		this.walk(ours[key], got[key]);
	}
};

Summer.prototype.printSummary = function ()
{
	console.log(JSON.stringify({
	    'users': this.s_users,
	    'jobs': this.s_jobs,
	    'tasks': this.s_tasks,
	    'errors': this.s_errors,
	    'warnings': this.s_nwarnings
	}));
};

main();
