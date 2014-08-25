/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * test/util/tst.stataccumulator.js: tests kstat accumulator
 */

var mod_assert = require('assert');
var mod_util = require('util');

var mod_kstat = require('kstat');
var mod_zoneid = require('zoneid');

var mod_meter = require('../../lib/meter');
var StatAccumulator = mod_meter.StatAccumulator;

var MILLISEC = 1e3;
var MICROSEC = 1e6;
var NANOSEC  = 1e9;
var MAXCPUS  = 32;

function hrtime2msec(hrtime)
{
	return (hrtime[0] * MILLISEC + hrtime[1] / MICROSEC);
}

function spin_for(msec)
{
	console.log('spinning for %d milliseconds', msec);

	var hrstart = process.hrtime();
	var start = kstat.read()[0];
	var count = 1000;
	var i;

	for (;;) {
		if (kstat.read()[0]['data']['nsec_user'] -
		    start['data']['nsec_user'] >= msec * MICROSEC &&
		    hrtime2msec(process.hrtime(hrstart)) >= msec)
			return;

		for (i = 0; i < count; )
			i++;
	}
}

function elapsed(metrics)
{
	return (metrics['cpu']['data']['nsec_user'] +
	    metrics['cpu']['data']['nsec_sys']);
}

/*
 * In case the system is fully loaded while we're running, the upper bound on
 * CPU time elapsed while spinning for "bound" ms is NCPUS * "bound" ms.
 */
function not_much_more(value, bound)
{
	return (value < MAXCPUS * bound);
}

function in_range(value, expected)
{
	return (value >= expected && not_much_more(value, 2 * expected));
}

function print_checkpoint(cp)
{
	console.log('checkpoint: %s', mod_util.inspect(cp, false, 5));
}

var zoneid = mod_zoneid.getzoneid();
var kstat = new mod_kstat.Reader({
    'module': 'zones',
    'instance': zoneid,
    'class': 'zone_misc'
});
var stat = new StatAccumulator({
    'read': function () {
	var kstats = kstat.read();
	mod_assert.ok(Array.isArray(kstats));
	mod_assert.equal(kstats.length, 1);
	return ({ 'cpu': kstats[0] });
    }
});
var stats, prev, checkpoint, msects;

stat.zero(stat.read());

/* Initial reading: should be close to zero. */
stats = stat.stats();
mod_assert.ok(hrtime2msec(stats['hrtimestamp']) < MILLISEC);
mod_assert.ok(elapsed(stats) < NANOSEC);

/*
 * Spin for 1500ms, then check that we read about that much time in the
 * checkpoint.
 */
spin_for(1500);
checkpoint = stat.checkpoint();
print_checkpoint(checkpoint);
msects = hrtime2msec(checkpoint['metricsCumul']['hrtimestamp']);
mod_assert.ok(msects >= 1500 && msects < 3000);
mod_assert.ok(in_range(elapsed(checkpoint['metricsCumul']), 1500 * MICROSEC));
mod_assert.ok(!checkpoint.hasOwnProperty('metricsDelta'));

/*
 * Spin a little longer and check that there's a difference between the "delta"
 * and cumulative stats.
 */
spin_for(500);
checkpoint = stat.checkpoint();
print_checkpoint(checkpoint);
msects = hrtime2msec(checkpoint['metricsDelta']['hrtimestamp']);
mod_assert.ok(msects >= 500 && msects < 2000);
msects = hrtime2msec(checkpoint['metricsCumul']['hrtimestamp']);
mod_assert.ok(msects >= 2000 && msects < 4000);
mod_assert.ok(in_range(elapsed(checkpoint['metricsDelta']), 500 * MICROSEC));
mod_assert.ok(in_range(elapsed(checkpoint['metricsCumul']), 2000 * MICROSEC));

/*
 * Go through one more lap and check both delta and cumulative stats again.
 */
spin_for(2500);
checkpoint = stat.checkpoint();
print_checkpoint(checkpoint);
msects = hrtime2msec(checkpoint['metricsDelta']['hrtimestamp']);
mod_assert.ok(msects >= 2500 && msects < 4000);
msects = hrtime2msec(checkpoint['metricsCumul']['hrtimestamp']);
mod_assert.ok(msects >= 4500 && msects < 6000);
mod_assert.ok(in_range(elapsed(checkpoint['metricsDelta']), 2500 * MICROSEC));
mod_assert.ok(in_range(elapsed(checkpoint['metricsCumul']), 4500 * MICROSEC));

/*
 * Check that stat() shows since-zero stats.
 */
stats = stat.stats();
mod_assert.ok(hrtime2msec(stats['hrtimestamp']) >= 4500);
mod_assert.ok(hrtime2msec(stats['hrtimestamp']) < 6000);
mod_assert.ok(in_range(elapsed(stats), 4500 * MICROSEC));

/*
 * Now zero, spin for a bit, and check that the checkpoint started over.
 */
console.log('zero\'d accumulator');
stat.zero();
spin_for(800);
checkpoint = stat.checkpoint();
print_checkpoint(checkpoint);
msects = hrtime2msec(checkpoint['metricsCumul']['hrtimestamp']);
mod_assert.ok(msects >= 800 && msects < 2000);
mod_assert.ok(in_range(elapsed(checkpoint['metricsCumul']), 800 * MICROSEC));
mod_assert.ok(!checkpoint.hasOwnProperty('metricsDelta'));

stats = stat.stats();
mod_assert.ok(hrtime2msec(stats['hrtimestamp']) >= 800);
mod_assert.ok(hrtime2msec(stats['hrtimestamp']) < 2000);
mod_assert.ok(in_range(elapsed(stats), 800 * MICROSEC));
