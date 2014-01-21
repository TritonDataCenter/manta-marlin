/*
 * lib/meter.js: Marlin metering
 *
 * For details on the design of Marlin metering, see the documentation in
 * docs/index.restdown.
 */

var mod_assertplus = require('assert-plus');
var mod_events = require('events');
var mod_util = require('util');

var mod_lstream = require('lstream');
var mod_jsprim = require('jsprim');
var mod_verror = require('verror');

var mod_assert = mod_assertplus;
var EventEmitter = mod_events.EventEmitter;
var VError = mod_verror.VError;

/* public interface */
exports.StatAccumulator = StatAccumulator;
exports.hrtimediff = hrtimediff;
exports.MarlinMeterReader = MarlinMeterReader;

/*
 * Cumulative kstat fields.  These are fields whose values represent cumulative
 * totals since some previous time, as opposed to "level" values (like "rss").
 */
var maKstatCumulativeFields = {};
[
    /* cpu */
    'nsec_user',
    'nsec_sys',
    'nsec_waitrq',
    'forkfail_cap',
    'forkfail_noproc',
    'forkfail_nomem',
    'forkfail_misc',

    /* memory */
    'nover',
    'pagedout',
    'pgpgin',
    'anonpgin',
    'execpgin',
    'fspgin',
    'anon_alloc_fail',
    'n_pf_throttle',

    /* I/O (fields used by both VFS and ZFS) */
    'nread',
    'reads',
    'rtime',
    'rlentime',
    'nwritten',
    'writes',
    'wtime',
    'wlentime',
    'waittime',
    '10ms_ops',
    '100ms_ops',
    '1s_ops',
    '10s_ops',
    'delay_cnt',

    /* net */
    'ierrors',
    'rbytes64',
    'ipackets64',
    'obytes64',
    'opackets64',

    /* non-kstat fields */
    'nrecords',
    'time'
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

/*
 * Summarizes Marlin metering records from the given stream.  Arguments include:
 *
 *     summaryType	"deltas" | "cumulative"
 *
 *     			"deltas" causes the reader to process the deltas
 *     			reported in each TaskCheckpoint and TaskDone record.
 *     			"cumulative" causes only the cumulative stats from
 *     			TaskDone records to be read.
 *
 *			There are two cases where these differ: when the task
 *			never completes (because the agent crashes or the box
 *			panics), and when a long-running task spans multiple
 *			reporting periods (because "deltas" causes some of the
 *			task's execution time to be accounted for in each
 *			reporting period, while "cumulative" causes the entire
 *			execution time to be accounted for only in the period
 *			where the task actually finishes).
 *
 * 			These two modes are applicable for two different use
 * 			cases: if you want to know all of the resource used in a
 * 			given period (e.g., for billing), "deltas" is better,
 * 			since "cumulative" reports resources used in previous
 * 			billing periods and ignores resources used in the
 * 			current period for tasks that only finish in a later
 * 			period.  On the other hand, if you want to know
 * 			resourced used on a per-task basis (e.g., what's the
 * 			distribution of user-CPU-time used across all tasks),
 * 			you're better off with the "cumulative" report, since
 * 			the "deltas" report would include partial stats for any
 * 			tasks that span the reporting boundary, which would
 * 			skew the report.
 *
 *     aggrKey		[ 'owner' | 'jobid' | 'phase' | 'taskid' |
 *                        'hour' | 'minute' ... ]
 *
 *     			Summarize metrics broken down by the given key.  For
 *     			example, [ 'jobid '] breaks down resources used by
 *     			jobid, while [ 'jobid', 'hour' ] breaks down by jobid
 *     			and hour.
 *
 *     startTime,	ISO 8601 timestamp.
 *     endTime
 *     			Only show records between [ startTime, endTime ].
 *     			This option is not particularly efficient; you may be
 *     			better off filtering records before feeding them here.
 *
 *     resources	[ resource_name ... ]
 *
 *     			Resources to extract from each record, which can either
 *     			be a top-level resource or one of its subresources.
 *     			Top-level resources include "time" (elapsed time, in the
 *     			format specified by Node's process.hrtime()), "nrecords"
 *     			(the number of metering records found), "cpu", "memory",
 *     			"vfs", "zfs", and "vnic0".  Each of these except for
 *     			"time" and "nrecords" also has subresources, like
 *     			"cpu.nsec_user".
 *
 *     stream		Node 0.8-style ReadableStream
 *
 *     			Bunyan log stream.
 */
function MarlinMeterReader(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.stream, 'args.stream');

	EventEmitter.call(this);

	var mmr = this;
	this.mr_reader = new BunyanReader(args.stream);
	this.mr_aggr = new MeterAggregator(args);
	this.mr_aggrkey = args['aggrKey'].slice(0);

	this.mr_reader.on('warn', this.emit.bind(this, 'warn'));
	this.mr_reader.on('error', this.emit.bind(this, 'error'));
	this.mr_reader.on('end', this.emit.bind(this, 'end'));
	this.mr_aggr.on('warn', this.emit.bind(this, 'warn'));
	this.mr_aggr.on('error', this.emit.bind(this, 'error'));

	this.mr_reader.on('entry', function (entry) {
		if (!entry['computeAudit'])
			return;

		if (args['startTime'] !== null &&
		    entry['time'] < args['startTime'])
			return;

		if (args['endTime'] !== null &&
		    entry['time'] > args['endTime'])
			return;

		mmr.mr_aggr.aggregate(entry);
	});
}

mod_util.inherits(MarlinMeterReader, EventEmitter);

/*
 * Returns a hierarchical report of all data seen so far.  For example, a
 * report of cpu metrics by jobid and hour would return an object like this:
 *
 *  {
 *    "7ccf651f-9a73-4b79-b658-db307520f674": {
 *      "2013-04-03T19": {
 *        "cpu": {
 *          "nsec_user": 2026598497,
 *          "nsec_sys": 3719333494,
 *          "nsec_waitrq": 315426945,
 *          "avenrun_1min": 466,
 *          "avenrun_5min": 68,
 *          "avenrun_15min": 0,
 *        }, ... // other metrics, if they'd been selected
 *      }, ... // other hours
 *    }, ... // other jobids
 *  }
 *
 * The returned object should not be modified in any way.
 */
MarlinMeterReader.prototype.reportHierarchical = function ()
{
	return (this.mr_aggr.data());
};

/*
 * Returns a flattened report of all data seen so far.  The returned value is an
 * array, where each entry corresponds to a unique value of the aggregation key
 * (an N-tuple specified by "aggrKey" above).  The first N elements of the array
 * correspond to the N fields in "aggrKey" and the remaining element is a
 * hierarchical representaiton of resources selected.  For example, the above
 * report would be represented as:
 *
 * [
 *     [ '7ccf651f-9a73-4b79-b658-db307520f674', '2013-04-03T19', {
 *        "cpu": {
 *          "nsec_user": 2026598497,
 *          "nsec_sys": 3719333494,
 *          "nsec_waitrq": 315426945,
 *          "avenrun_1min": 466,
 *          "avenrun_5min": 68,
 *          "avenrun_15min": 0,
 *        }, ... // other metrics, if they'd been selected
 *     } ], ... // other tuples of (jobid, hour, resources)
 * ]
 */
MarlinMeterReader.prototype.reportFlattened = function ()
{
	return (mod_jsprim.flattenObject(
	    this.mr_aggr.data(), this.mr_aggrkey.length));
};

/*
 * Like reportFlattened, except that the resource values are also flattened.
 * This is desirable for tabular human-readable output.  Since the set of
 * columns is dynamic (e.g., the caller may select "cpu" and get
 * "cpu.nsec_user", "cpu.nsec_sys", and so on), this representation describes
 * the extra columns.  For example, the above report would look like this:
 *
 * {
 *   "datacolumns": [
 *     "cpu.avenrun_15min", "cpu.avenrun_1min", "cpu.avenrun_5min",
 *     "cpu.nsec_sys", "cpu.nsec_user", "cpu.nsec_waitrq"
 *   ],
 *   "flattened": [
 *     [
 *       "7ccf651f-9a73-4b79-b658-db307520f674", "2013-04-03T19",
 *       0, 466, 68,
 *       3719333494, 2026598497, 315426945
 *     ], ... // other tuples of (jobid, hour, resources)
 *   ]
 * }
 */
MarlinMeterReader.prototype.reportFlattenedFully = function ()
{
	/* Start with the above flattened representation. */
	var mmr = this;
	var flattened = this.reportFlattened();

	/* Compute the union of columns inside the "values" in each row. */
	var columns = {};
	flattened.forEach(function (elt) {
		var value = elt[elt.length - 1];
		mod_assert.equal(typeof (value), 'object');
		mod_assert.ok(value !== null);
		mmr.extractColumns(columns, value, '');
	});

	/* Flatten the rest of the object. */
	var colnames = Object.keys(columns).sort();
	flattened.forEach(function (elt) {
		var value = elt.pop();

		colnames.forEach(function (c) {
			elt.push(mod_jsprim.pluck(value, c) || 0);
		});
	});

	return ({
	    'datacolumns': colnames,
	    'flattened': flattened
	});
};

/* [private] */
MarlinMeterReader.prototype.extractColumns = function (columns, obj, prefix)
{
	for (var k in obj) {
		if (typeof (obj[k]) == 'number' || Array.isArray(obj[k])) {
			columns[prefix + k] = true;
			continue;
		}

		this.extractColumns(columns, obj[k], prefix + k + '.');
	}
};

/*
 * Returns debug stats (key-value pairs)
 */
MarlinMeterReader.prototype.stats = function ()
{
	var stats = this.mr_reader.stats();
	var mstats = this.mr_aggr.stats();

	for (var k in mstats)
		stats[k] = mstats[k];

	return (stats);
};


/*
 * See MarlinMeterReader.  The MeterAggregator takes incoming log records and
 * keeps summary information.
 */
function MeterAggregator(options)
{
	mod_assert.ok(options !== null);
	mod_assert.equal(typeof (options), 'object');
	mod_assert.ok(Array.isArray(options['aggrKey']));
	mod_assert.ok(Array.isArray(options['resources']));
	mod_assert.ok(options['summaryType'] == 'deltas' ||
	    options['summaryType'] == 'cumulative');

	this.ma_key = options['aggrKey'].slice(0);
	this.ma_resources = options['resources'].slice(0);
	this.ma_usedeltas = options['summaryType'] == 'deltas';
	this.ma_nrecords = 0;
	this.ma_nagentstarts = 0;
	this.ma_data = {};

	EventEmitter.call(this);
}

mod_util.inherits(MeterAggregator, EventEmitter);

MeterAggregator.prototype.aggregate = function (entry)
{
	if (!entry['computeAudit'])
		return;

	this.ma_nrecords++;

	if (entry['event'] == 'AgentStart') {
		this.ma_nagentstarts++;
		return;
	}

	/*
	 * We currently ignore TaskStart events entirely, but they could be used
	 * to keep track of which tasks are outstanding when an agent restart
	 * occurs.  This would only require adding code here, not enhancing the
	 * existing metering data.
	 */
	if (entry['event'] != 'TaskDone' && entry['event'] != 'TaskCheckpoint')
		return;

	if (!this.ma_usedeltas && entry['event'] != 'TaskDone')
		return;

	this.insert(this.computeKey(entry), this.extractResources(entry));
};

MeterAggregator.prototype.computeKey = function (entry)
{
	return (this.ma_key.map(function (field) {
		var value;

		switch (field) {
		case 'owner':
			value = entry['owner'];
			break;
		case 'taskid':
			value = entry['taskId'];
			break;
		case 'jobid':
			value = entry['jobId'];
			break;
		case 'phase':
			value = entry.hasOwnProperty('phase') ?
			    entry['phase'] : '-';
			break;
		case 'hour':
			value = entry['time'].substr(0, 13);
			break;
		case 'minute':
			value = entry['time'].substr(0, 16);
			break;
		default:
			throw (new VError('unsupported key: "%s"', field));
		}

		return (value);
	}));
};

MeterAggregator.prototype.extractResources = function (entry)
{
	var aggr = this;
	var rv = {};
	var metrics;
	var metricdata;

	if (this.ma_usedeltas && entry.hasOwnProperty('metricsDelta'))
		metrics = entry['metricsDelta'];
	else
		metrics = entry['metricsCumul'];

	if (typeof (metrics) != 'object') {
		this.emit('warn', new VError('bad log entry: %j', entry));
		return (undefined);
	}

	metricdata = {};
	mod_jsprim.forEachKey(metrics, function (key, value) {
		if (value.hasOwnProperty('snaptime'))
			/* kstat-based: flatten out the "data" field */
			metricdata[key] = value['data'];
		else
			metricdata[key] = value;
	});

	this.ma_resources.forEach(function (r) {
		if (r == 'nrecords') {
			rv[r] = 1;
			return;
		}

		var pluckkey = r == 'time' ? 'hrtimestamp' : r;

		var val = mod_jsprim.pluck(metricdata, pluckkey);
		if (val instanceof Error) {
			aggr.emit('warn', new VError(val,
			    'failed to extract "%s" from %j', r, metrics));
			return;
		}

		rv[r] = val;
	});

	return (rv);
};

MeterAggregator.prototype.insert = function (keyv, value)
{
	var obj, i;

	for (i = 0, obj = this.ma_data; i < keyv.length - 1; i++) {
		if (!obj.hasOwnProperty(keyv[i]))
			obj[keyv[i]] = {};

		obj = obj[keyv[i]];
	}

	obj[keyv[keyv.length - 1]] = this.aggregateValue(
	    obj[keyv[keyv.length - 1]], value);
};

MeterAggregator.prototype.aggregateValue = function (oldvalue, newvalue)
{
	if (!oldvalue)
		return (newvalue);

	if (typeof (newvalue) == 'number') {
		mod_assert.equal(typeof (oldvalue), 'number');
		return (oldvalue + newvalue);
	}

	if (Array.isArray(newvalue)) {
		/* hrtimestamp is an array of two-tuples */
		mod_assert.ok(Array.isArray(oldvalue));
		mod_assert.equal(oldvalue.length, 2);
		mod_assert.equal(newvalue.length, 2);
		oldvalue[0] += newvalue[0];
		oldvalue[1] += newvalue[1];
		if (oldvalue[1] > 1e9) {
			oldvalue[0]++;
			oldvalue[1] -= 1e9;
		}
		return (oldvalue);
	}

	mod_assert.equal(typeof (oldvalue), 'object');
	for (var k in newvalue) {
		/*
		 * Cumulative fields (like iops, nanoseconds of time, and so on)
		 * are added together because the agent reports deltas for those
		 * fields.  Other fields (like RSS used) aren't additive, so for
		 * those we just take the last value seen.
		 */
		var p = k.lastIndexOf('.');
		var basekey = p == -1 ? k : k.substr(p + 1);
		if (typeof (newvalue[k]) == 'object' ||
		    basekey in maKstatCumulativeFields)
			oldvalue[k] = this.aggregateValue(oldvalue[k],
			    newvalue[k]);
		else
			oldvalue[k] = newvalue[k];
	}

	return (oldvalue);
};

MeterAggregator.prototype.stats = function ()
{
	return ({
	    'metering records': this.ma_nrecords,
	    'agent (re)starts': this.ma_nagentstarts
	});
};

MeterAggregator.prototype.data = function ()
{
	return (this.ma_data);
};

/*
 * Stream-like object that reads lines from a stream and emits an 'entry' event
 * for each well-formed log entry.  The only argument for the event is the
 * parsed JSON record.
 */
function BunyanReader(stream)
{
	var reader = this;

	EventEmitter.call(this);
	this.br_stream = stream;

	this.br_linenum = 0;
	this.br_nonjson = 0;
	this.br_malformed = 0;
	this.br_lstream = new mod_lstream();
	this.br_lstream.on('end', function () { reader.emit('end'); });
	this.br_lstream.on('line', function (line) {
		++reader.br_linenum;

		if (line[0] != '{') {
			reader.br_nonjson++;
			return;
		}

		var entry;
		try {
			entry = JSON.parse(line);
		} catch (err) {
			reader.br_malformed++;
			reader.emit('warn', new VError(err,
			    'malformed record at line %d', reader.br_linenum));
			return;
		}

		reader.emit('entry', entry);
	});

	stream.pipe(this.br_lstream);
	this.br_lstream.resume();
}

mod_util.inherits(BunyanReader, EventEmitter);

BunyanReader.prototype.stats = function ()
{
	return ({
	    'log records': this.br_linenum,
	    'non-JSON records': this.br_nonjson,
	    'malformed records': this.br_malformed
	});
};
