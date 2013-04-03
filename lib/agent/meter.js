/*
 * lib/agent/meter.js: Marlin metering
 */

var mod_assertplus = require('assert-plus');

/* public interface */
exports.StatAccumulator = StatAccumulator;
exports.hrtimediff = hrtimediff;


/*
 * Cumulative kstat fields.  These are fields whose values represent cumulative
 * totals since some previous time, as opposed to "level" values (like "rss").
 */
var maKstatCumulativeFields = {};
[
    '100ms_ops',
    '10ms_ops',
    '10s_ops',
    '1s_ops',
    'anon_alloc_fail',
    'anonpgin',
    'delay_cnt',
    'execpgin',
    'fspgin',
    'n_pf_throttle',
    'nover',
    'nread',
    'nwritten',
    'nsec_user',
    'nsec_sys',
    'nsec_waitrq',
    'pagedout',
    'pgpgin',
    'reads',
    'rlentime',
    'rtime',
    'waittime',
    'wlentime',
    'writes',
    'wtime'
].forEach(function (field) { maKstatCumulativeFields[field] = true; });



/*
 * There is no defined way to represent negative deltas, so it's illegal to diff
 * B from A where the time denoted by B is later than the time denoted by A.  If
 * this becomes valuable, we can define a representation and extend the
 * implementation to support it.
 */
function hrtimediff(a, b)
{
	mod_assertplus.ok(a[0] >= 0 && a[1] >= 0 && b[0] >= 0 && b[1] >= 0,
	    'negative numbers not allowed in hrtimes');
	mod_assertplus.ok(a[1] < 1e9 && b[1] < 1e9,
	    'nanoseconds column overflow');
	mod_assertplus.ok(a[0] > b[0] || (a[0] == b[0] && a[1] >= b[1]),
	    'negative differences not allowed');

	var rv = [ a[0] - b[0], 0 ];

	if (a[1] >= b[1]) {
		rv[1] = a[1] - b[1];
	} else {
		rv[0]--;
		rv[1] = 1e9 - (b[1] - a[1]);
	}

	return (rv);
}

function StatAccumulator(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.func(args['read'], 'args.read');

	this.sa_read = args['read'];
	this.sa_zero = undefined;
	this.sa_last = undefined;
}

StatAccumulator.prototype.read = function ()
{
	var stats = this.sa_read();
	stats['hrtimestamp'] = process.hrtime();
	return (stats);
};

StatAccumulator.prototype.zero = function (stats)
{
	if (arguments.length === 0) {
		mod_assertplus.ok(this.sa_last !== undefined,
		    'cannot zero with no argument and no previous checkpoint');
		stats = this.sa_last;
	}

	mod_assertplus.ok(stats !== undefined);
	this.sa_zero = stats;
	this.sa_last = undefined;
};

StatAccumulator.prototype.stats = function ()
{
	mod_assertplus.ok(this.sa_zero !== undefined,
	    'accumulator must be zero\'d before stats()');
	var stats = this.read();
	if (stats instanceof Error)
		return (stats);

	return (statsDelta(stats, this.sa_zero));
};

StatAccumulator.prototype.checkpoint = function ()
{
	mod_assertplus.ok(this.sa_zero !== undefined,
	    'accumulator must be zero\'d before checkpoint()');

	var stats = this.read();
	if (stats instanceof Error)
		return (stats);

	var rv = {
	    'hrtimestamp': stats['hrtimestamp'],
	    'metricsCumul': statsDelta(stats, this.sa_zero)
	};

	if (this.sa_last !== undefined)
		rv['metricsDelta'] = statsDelta(stats, this.sa_last);
	this.sa_last = stats;
	return (rv);
};

function statsDelta(stats, zero)
{
	var hrtimestamp, diff;
	var k, d, o, n, f;

	hrtimestamp = hrtimediff(stats['hrtimestamp'], zero['hrtimestamp']);

	diff = {};
	diff['hrtimestamp'] = hrtimestamp;

	for (k in stats) {
		if (k == 'hrtimestamp')
			continue;

		d = diff[k] = {};
		o = zero[k];
		n = stats[k];

		/*
		 * If we're not looking at the same exact kstat, we've seen a
		 * bug at a lower-level somewhere.  We're searching by "class",
		 * and the "module" and "name" fields are fixed in the kernel
		 * source.  In the extraordinarily odd event that the instance
		 * number / zonename have changed, the "zone" class should have
		 * reported an error.  We're paranoid about this because we
		 * really don't want to report the wrong usage.
		 */
		mod_assertplus.equal(o['class'], n['class']);
		mod_assertplus.equal(o['module'], n['module']);
		mod_assertplus.equal(o['name'], n['name']);
		mod_assertplus.equal(o['instance'], n['instance']);
		mod_assertplus.equal(o['data']['zonename'],
		    n['data']['zonename']);

		d['snaptime'] = n['snaptime'] - o['snaptime'];
		mod_assertplus.ok(d['snaptime'] >= 0);

		d['data'] = {};
		for (f in o['data']) {
			if (!(f in n['data']))
				/* This is weird, but drive on. */
				continue;

			if (typeof (n['data'][f]) != 'number')
				continue;

			if (!maKstatCumulativeFields[f])
				d['data'][f] = n['data'][f];
			else
				d['data'][f] = n['data'][f] - o['data'][f];
		}
	}

	return (diff);
}
