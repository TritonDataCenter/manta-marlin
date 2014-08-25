#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * report.js: given summary JSON on stdin, emit a textual report of Marlin
 * activity.  The format is described on "denenormalize" below.
 */

var mod_extsprintf = require('extsprintf');
var mod_getopt = require('posix-getopt');
var mod_jsprim = require('jsprim');
var sprintf = mod_extsprintf.sprintf;

function main()
{
	var parser, option;
	var conf = {
	    'showUserDetails': false,
	    'json': false
	};

	parser = new mod_getopt.BasicParser('ju', process.argv);
	while ((option = parser.getopt()) !== undefined) {
		switch (option.option) {
		case 'j':
			conf.json = true;
			break;

		case 'u':
			conf.showUserDetails = true;
			break;
		}
	}

	readStreamJson(process.stdin, function (err, data) {
		if (err)
			throw (err);

		if (conf.json)
			console.log(JSON.stringify(data));
		else
			report(conf, denormalize(data));
	});
}

function readStream(stream, callback)
{
	var data = '';
	stream.on('data', function (chunk) {
		data += chunk.toString('utf8');
	});
	stream.on('end', function () { callback(data); });
}

function readStreamJson(stream, callback)
{
	readStream(stream, function (data) {
		var obj;
		try {
			obj = JSON.parse(data);
		} catch (ex) {
			callback(ex);
			return;
		}

		callback(null, obj);
	});
}

function println()
{
	var args = Array.prototype.slice.call(arguments);
	var msg = sprintf.apply(null, args);
	console.log(msg);
}

/*
 * Incoming data comes in the form:
 *
 *    "users": {
 *        "userid": "login",
 *        ...
 *    },
 *    "jobs": {
 *        "jobid": "user_uuid",
 *        ...
 *    },
 *    "tasks": {
 *        "jobid": {
 *            "ok": count,
 *            "retryable": count,
 *            "fatal": count
 *        },
 *        ...
 *    },
 *    "errors": {
 *        "jobid": {
 *            "kind": { // "kind" indicates retried or not
 *                "code": {
 *                    "message": count
 *                }
 *            },
 *            ...
 *        }
 *    }
 *
 * This is easy to construct in a distributed way and then merge, but it's
 * easier to write a reporting tool based on a denormalized form.  The basic
 * unit is a block like this:
 *
 *    "ntasks": ...,
 *    "ntasks_ok":, ...,
 *    "ntasks_fail_retryable": ...,
 *    "ntasks_fail_fatal": ...
 *    "nerrors": ...,
 *    "nretries": ...,
 *    "retries_bycode": {
 *        "code": ...
 *    },
 *    "retries": [ {
 *        "code": ...,
 *        "message": ...,
 *        "count": ...
 *    }, ... ],
 *    "errors_bycode": {
 *        "code": ...
 *    },
 *    "errors": [ {
 *        "code": ...,
 *        "message": ...,
 *        "count": ...
 *    }, ... ]
 *
 * Then at the top level, we have:
 *
 *    "jobs": {
 *        "jobid": {
 *            "user_uuid": ...,
 *            "user_login": ...,
 *            properties from above
 *        },
 *        ...
 *    },
 *    "users": {
 *        "user_uuid": {
 *            "login": ...,
 *            "jobs": [ array of jobids ],
 *            properties from above
 *        }
 *    },
 *    "totals": {
 *        properties from above
 *    }
 */
function denormalize(data)
{
	var zero, tot, rv, k, v, u, j;
	var kind, code, msg;

	zero = {
	    'ntasks': 0,
	    'ntasks_ok': 0,
	    'ntasks_fail_retryable': 0,
	    'ntasks_fail_fatal': 0,
	    'nerrors': 0,
	    'nretries': 0,
	    'retries_bycode': {},
	    'retries': [],
	    'errors_bycode': {},
	    'errors': []
	};

	tot = mod_jsprim.deepCopy(zero);

	rv = {
	    'totals': tot,
	    'jobs': {},
	    'users': {}
	};

	for (k in data['users']) {
		v = data['users'][k];
		rv['users'][k] = mod_jsprim.deepCopy(zero);
		rv['users'][k]['login'] = v;
		rv['users'][k]['jobs'] = [];
	}

	for (k in data['jobs']) {
		v = data['jobs'][k];
		u = rv['users'][v];
		u['jobs'].push(k);
		rv['jobs'][k] = mod_jsprim.deepCopy(zero);
		rv['jobs'][k]['user_login'] = u['login'];
		rv['jobs'][k]['user_uuid'] = v;
	}

	for (k in data['tasks']) {
		if (!rv['jobs'].hasOwnProperty(k)) {
			console.error(
			    'warn:  task info for unknown job: "%s"', k);
			continue;
		}

		j = rv['jobs'][k];
		u = rv['users'][j['user_uuid']];
		v = data['tasks'][k];

		if (v.hasOwnProperty('ok')) {
			u['ntasks'] += v['ok'];
			u['ntasks_ok'] += v['ok'];
			j['ntasks'] += v['ok'];
			j['ntasks_ok'] += v['ok'];
			tot['ntasks'] += v['ok'];
			tot['ntasks_ok'] += v['ok'];
		}

		if (v.hasOwnProperty('fatal')) {
			u['ntasks'] += v['fatal'];
			u['ntasks_fail_fatal'] += v['fatal'];
			j['ntasks'] += v['fatal'];
			j['ntasks_fail_fatal'] += v['fatal'];
			tot['ntasks'] += v['fatal'];
			tot['ntasks_fail_fatal'] += v['fatal'];
		}

		if (v.hasOwnProperty('retryable')) {
			u['ntasks'] += v['retryable'];
			u['ntasks_fail_retryable'] += v['retryable'];
			j['ntasks'] += v['retryable'];
			j['ntasks_fail_retryable'] += v['retryable'];
			tot['ntasks'] += v['retryable'];
			tot['ntasks_fail_retryable'] += v['retryable'];
		}
	}

	for (k in data['errors']) {
		if (!rv['jobs'].hasOwnProperty(k)) {
			console.error(
			    'warn: error info for unknown job: "%s"', k);
			continue;
		}

		j = rv['jobs'][k];
		u = rv['users'][j['user_uuid']];
		v = data['errors'][k];

		for (kind in v) {
			for (code in v[kind]) {
				for (msg in v[kind][code]) {
					updErrors(j, v, kind, code, msg);
					updErrors(u, v, kind, code, msg);
					updErrors(tot, v, kind, code, msg);
				}
			}
		}
	}

	return (rv);
}

function updErrors(stats, errors, kind, code, message)
{
	var count = errors[kind][code][message];
	var bycode, list, i;

	if (kind == 'retried') {
		stats['nretries'] += count;
		bycode = stats['retries_bycode'];
		list = stats['retries'];
	} else {
		stats['nerrors'] += count;
		bycode = stats['errors_bycode'];
		list = stats['errors'];
	}

	if (!bycode.hasOwnProperty(code))
		bycode[code] = 0;
	bycode[code] += count;

	/*
	 * This could be more efficient, but the number of entries on each of
	 * these lists is expected to be small.
	 */
	for (i = 0; i < list.length; i++) {
		if (list[i]['code'] == code && list[i]['message'] == message)
			break;
	}

	if (i == list.length) {
		list.push({
		    'code': code,
		    'message': message,
		    'count': 0
		});
	}

	list[i]['count'] += count;
}

function report(conf, data)
{
	var users;

	println('SUMMARY');
	println('  %8d total tasks', data['totals']['ntasks']);
	println('  %8d     tasks ok', data['totals']['ntasks_ok']);
	println('  %8d     tasks failed that could be retried',
	    data['totals']['ntasks_fail_retryable']);
	println('  %8d     tasks failed with non-retryable errors',
	    data['totals']['ntasks_fail_fatal']);
	println('  %8d total errors', data['totals']['nerrors']);
	println('  %8d total retries', data['totals']['nretries']);

	if (data['totals']['nerrors'] > 0) {
		println('');
		println('ERRORS BY CAUSE');
		printErrorBlock(
		    data['totals']['errors_bycode'],
		    data['totals']['errors']);
	}

	if (data['totals']['nretries'] > 0) {
		println('');
		println('RETRIES BY CAUSE');
		printErrorBlock(
		    data['totals']['retries_bycode'],
		    data['totals']['retries']);
	}

	users = Object.keys(data['users']).sort(function (u1, u2) {
		return (data['users'][u2]['ntasks'] -
		    data['users'][u1]['ntasks']);
	});
	if (users.length > 0) {
		println('');
		println('SUMMARY BY USER');
		users.forEach(function (uuid) {
			var u = data['users'][uuid];
			println('%-30s %6d tasks %6d retries %6d errors',
			    u['login'], u['ntasks'], u['nretries'],
			    u['nerrors']);
		});
	}

	if (users.length > 0 && conf.showUserDetails) {
		println('');
		println('ERROR DETAILS BY USER');
		users.forEach(function (uuid) {
			var u = data['users'][uuid];
			println('%-30s %6d tasks %6d retries %6d errors',
			    u['login'], u['ntasks'], u['nretries'],
			    u['nerrors']);
			if (u['nerrors'] > 0) {
				printErrorBlock(
				    u['errors_bycode'], u['errors']);
				println('');
			}
		});
	}
}

function printErrorBlock(bycode, list)
{
	var codes, posns, last;

	posns = {};
	codes = Object.keys(bycode).sort(function (c1, c2) {
		return (bycode[c1] - bycode[c2]);
	});
	codes.forEach(function (c, i) { posns[c] = i; });

	list = list.slice(0).sort(function (a, b) {
		var d;

		d = posns[b['code']] - posns[a['code']];
		if (d === 0)
			d = b['count'] - a['count'];
		return (d);
	});

	last = null;
	list.forEach(function (e) {
		if (e['code'] !== last)
			println('  %8d %-24s ', bycode[e['code']], e['code']);
		println('  %8d     %s', e['count'], e['message']);
		last = e['code'];
	});
}

main();
