#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var mod_lstream = require('lstream');
var mod_path = require('path');
var mod_util = require('util');

function main()
{
	var start, end, lstream, accumulator;

	if (process.argv.length != 4) {
		console.error('usage: %s %s START_TIME END_TIME',
		    process.argv[0], process.argv[1]);
		process.exit(1);
	}

	start = new Date(process.argv[2]).toISOString();
	end = new Date(process.argv[3]).toISOString();
	lstream = new mod_lstream();
	accumulator = new Accumulator(lstream, start, end);

	process.stdin.pipe(lstream);
	process.stdin.on('end', function () { accumulator.printSummary(); });
	lstream.resume();
}

function Accumulator(lstream, start, end)
{
	this.a_arg0 = mod_path.basename(process.argv[1]);
	this.a_seen = {};
	this.a_users = {};
	this.a_jobs = {};
	this.a_tasks = {};
	this.a_errors = {};
	this.a_nwarnings = 0;
	this.a_nignored = 0;
	this.a_line = 0;
	this.a_start = start;
	this.a_end = end;

	var accumulator = this;
	lstream.on('line', function (line) { accumulator.gotLine(line); });
}

Accumulator.prototype.warn = function ()
{
	var args = Array.prototype.slice.call(arguments);
	var msg = mod_util.format.apply(null, args);
	console.error('%s: line %s: %s', this.a_arg0, this.a_line, msg);
	this.a_nwarnings++;
};

Accumulator.prototype.gotLine = function (line)
{
	this.a_line++;

	var row, table, val, j, tc;
	var start = this.a_start;
	var end = this.a_end;

	try {
		row = JSON.parse(line);
	} catch (ex) {
		this.warn(ex.name + ': ' + ex.message);
		return;
	}

	table = row['__table'];
	if (!this.a_seen.hasOwnProperty(table))
		this.a_seen[table] = 0;
	this.a_seen[table]++;

	val = row['_value'];

	switch (table) {
	case 'marlin_jobs_v2':
		if (val['timeCreated'] >= end)
			return;

		if (val['timeDone'] &&
		    val['timeDone'] < start)
			return;

		this.a_users[val['auth']['uuid']] = val['auth']['login'];
		this.a_jobs[val['jobId']] = val['auth']['uuid'];
		break;

	case 'marlin_tasks_v2':
		tc = val['timeCommitted'] || row['timecommitted'];
		if (!tc) {
			this.a_nignored++;
			return;
		}

		if (tc < start || end <= tc) {
			this.a_nignored++;
			return;
		}

		j = val['jobId'];
		var kind = val['result'] == 'ok' ? 'ok' :
		    val['wantRetry'] ? 'retryable' : 'fatal';
		if (!this.a_tasks.hasOwnProperty(j))
			this.a_tasks[j] = {};
		if (!this.a_tasks[j].hasOwnProperty(kind))
			this.a_tasks[j][kind] = 0;
		this.a_tasks[j][kind]++;
		break;

	case 'marlin_errors_v2':
		tc = val['timeCommitted'] || row['timecommitted'];
		if (!tc) {
			this.a_nignored++;
			return;
		}

		if (tc < start || end <= tc) {
			this.a_nignored++;
			return;
		}

		j = val['jobId'];
		var retr = val['retried'] ? 'retried' : 'fatal';
		var code = val['errorCode'];
		var msg = val['errorMessage'];

		if (val.hasOwnProperty('errorMessageInternal'))
			msg += ' (' + val['errorMessageInternal'] + ')';

		if (val.hasOwnProperty('input'))
			msg = msg.replace(val['input'], '...');

		if (code == 'TaskInitError') {
			/* JSSTYLED */
			msg = msg.replace(/error retrieving asset ".*"/,
			    'error retrieving asset "..."');
			msg = msg.replace(/first of \d+ errors?: /, '... ');
		}

		if (code == 'ResourceNotFoundError' &&
		    msg.indexOf('failed to load object') === 0)
			msg = 'failed to load object ... ';

		if (!this.a_errors.hasOwnProperty(j))
			this.a_errors[j] = {};
		if (!this.a_errors[j].hasOwnProperty(retr))
			this.a_errors[j][retr] = {};
		if (!this.a_errors[j][retr].hasOwnProperty(code))
			this.a_errors[j][retr][code] = {};
		if (!this.a_errors[j][retr][code].hasOwnProperty(msg))
			this.a_errors[j][retr][code][msg] = 0;
		this.a_errors[j][retr][code][msg]++;
		break;

	default:
		this.warn('don\'t know what to do with bucket "%s"', table);
		break;
	}
};

Accumulator.prototype.printSummary = function ()
{
	console.log(JSON.stringify({
	    'users': this.a_users,
	    'jobs': this.a_jobs,
	    'tasks': this.a_tasks,
	    'errors': this.a_errors,
	    'nwarnings': this.a_nwarnings
	}));
};

main();
