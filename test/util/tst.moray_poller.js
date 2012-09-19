/*
 * test/util/tst.moray_poller.js: tests Moray polling interface
 */

var mod_assert = require('assert');
var mod_path = require('path');

var mod_jsprim = require('jsprim');
var mod_vasync = require('vasync');

var mod_moray = require('moray');

var mod_common = require('../common');
var mod_mamoray = require('../../lib/moray');

/* Configuration */
var bucket = 'marlin_tst_moray_poller_js';	/* test bucket name */
var first = [ 'moe', 'lenny' ];			/* first objects to PUT */
var second = [ 'carl', 'homer', 'duffman' ];	/* second objects to PUT */
var mutable = false;				/* which filter scheme to use */
var objects = {					/* sample data */
    'moe': {
	'name': 'Moe Szyslak',
	'age': 40
    },
    'lenny': {
	'name': 'Lenny Leonard',
	'age': 32
    },
    'carl': {
	'name': 'Carlton Carlson',
	'age': 37
    },
    'homer': {
	'name': 'Homer Simpson',
	'age': 36
    },
    'duffman': {
	'name': 'Duffman',
	'age': 22
    }
};

/* Global state */
var client;					/* moray client */
var poller;					/* moray poller being tested */
var records = [];				/* changed records received */
var saved = {};					/* objects we've saved */

/*
 * Wait state: at several points, we want to wait for a "poll" operation to
 * complete that we know contains all results before a given timestamp.  We set
 * wait_time = the timestamp and waiter = the callback function to invoke.  See
 * the poller's search-done event handler below, which drives this.
 */
var wait_time;
var waiter;

mod_common.pipeline({
    'funcs': [
	setupClient,

	/*
	 * Immutable object tests: ensure that we notice all new objects, and
	 * don't receive updates for objects that haven't been created.
	 */
	setupBucket,
	putFirstObjects,
	check,
	checkNoChanges,
	checkNoChanges,

	putSecondObjects,
	check,
	checkNoChanges,
	checkNoChanges,

	putChangedObjects,
	checkNoChanges,
	checkNoChanges,

	/*
	 * Wipe the bucket and run mutable object tests: ensure that we notice
	 * new objects as well as changes to existing objects, but don't receive
	 * updates for objects that haven't changed.
	 */
	function (_, next) { mutable = true; next(); },
	setupBucket,
	putFirstObjects,
	check,

	putChangedObjects,
	check,
	checkNoChanges,
	checkNoChanges,

	putChangedObjects,
	check,
	checkNoChanges,
	checkNoChanges,

	teardownClient
    ]
});

function setupClient(_, next)
{
	var url = process.env['MORAY_URL'];

	mod_assert.ok(url, '"MORAY_URL" must be specified in the environment');

	mod_common.log.info('connecting to moray at "%s"', url);

	client = mod_moray.createClient({
	    'url': url,
	    'log': mod_common.log.child({ 'component': 'moray-client' })
	});

	client.on('connect', next);

	poller = new mod_mamoray.MorayPoller({
	    'log': mod_common.log.child({ 'component': 'poller' }),
	    'client': function () { return (client); }
	});

	poller.on('record', function (record) {
		mod_common.log.debug('got record', record);
		records.push(record);
	});

	poller.on('search-done', function (_1, time) {
		if (waiter && time > wait_time) {
			var oldwaiter = waiter;
			waiter = wait_time = undefined;
			oldwaiter();
		} else if (waiter) {
			poller.poll();
		}
	});

	poller.addPoll({
	    'name': 'testbucket',
	    'bucket': bucket,
	    'mkfilter': mkfilter,
	    'options': {
		'sort': {
		    'attribute': 'name',
		    'order': 'ASC'
		}
	    },
	    'interval': 0
	});
}

/*
 * Function that generates Moray filter.  We test two schemes here: for the
 * first bunch of tests, where objects are assumed to be immutable, we filter on
 * the id.  For the second bunch, we use the mtime.
 */
function mkfilter(last_mtime, last_id)
{
	if (!mutable)
		return ('&(_id>=' + (last_id + 1) + ')(name=*)');
	return ('&(_mtime>=' + last_mtime + ')(name=*)');
}

function setupBucket(_, next)
{
	var schema = {
	    'name': { 'type': 'string' },
	    'age':  { 'type': 'number' }
	};

	saved = {};
	client.delBucket(bucket, function (err) {
		if (err && !/does not exist/.test(err.message)) {
			next(err);
			return;
		}

		client.putBucket(bucket, { 'index': schema }, next);
	});
}

function putFirstObjects(_, next)
{
	mod_common.log.info('putFirstObjects');
	mod_vasync.forEachParallel({
	    'inputs': first,
	    'func': function (key, callback) {
		saved[key] = true;
		client.putObject(bucket, key, objects[key], callback);
	    }
	}, next);
}

function check(_, next)
{
	mod_common.log.info('check');
	poller.poll();

	wait_time = Date.now();
	waiter = function () {
		mod_assert.equal(records.length, Object.keys(saved).length);
		var prev;

		while (records.length > 0) {
			var record = records.shift();
			var key = record['key'];
			var expected = typeof (saved[key]) == 'object' ?
			    saved[key] : objects[key];
			mod_assert.deepEqual(record['value'], expected);

			if (prev)
				mod_assert.ok(record['value']['name'] >= prev);

			prev = record['value']['name'];
			delete (saved[key]);
		}

		next();
	};
}

function checkNoChanges(_, next)
{
	mod_common.log.info('checkNoChanges');
	poller.poll();

	wait_time = Date.now();
	waiter = function () {
		mod_assert.equal(records.length, 0);
		next();
	};
}

function putSecondObjects(_, next)
{
	mod_common.log.info('putSecondObjects');
	mod_vasync.forEachParallel({
	    'inputs': second,
	    'func': function (key, callback) {
		saved[key] = true;
		client.putObject(bucket, key, objects[key], callback);
	    }
	}, next);
}

function putChangedObjects(_, next)
{
	mod_common.log.info('putChangedObjects');
	var changekey = first[0];
	var obj = mod_jsprim.deepCopy(objects[changekey]);
	obj['age'] = Math.floor(10000 * Math.random());
	saved[changekey] = obj;
	client.putObject(bucket, changekey, saved[changekey], next);
}

function teardownClient(_, next)
{
	mod_common.log.info('teardownClient');
	client.close();
	next();
}
