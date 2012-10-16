/*
 * test/util/tst.moray_queue.js: tests Moray queuing interface
 */

var mod_assert = require('assert');
var mod_path = require('path');

var mod_jsprim = require('jsprim');
var mod_vasync = require('vasync');

var mod_moray = require('moray');

var mod_common = require('../common');
var mod_mamoray = require('../../lib/moray');

var exnAsync = mod_common.exnAsync;

/* Configuration */
var bucket = 'marlin_tst_moray_queue_js';	/* test bucket name */
var maxpending = 5;				/* max nr of pending writes */

/* Global state */
var client;					/* moray client */
var queue;					/* moray poller being tested */

mod_common.pipeline({
    'funcs': [
	setupClient,

	setupBucket,
	checkWrite,

	setupBucket,
	checkThrottle,

	setupBucket,
	checkRetry,
	checkDirtyPending,
	checkDirtyInFlight,

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

	queue = new mod_mamoray.MorayWriteQueue({
	    'log': mod_common.log.child({ 'component': 'queue' }),
	    'client': function () { return (client); },
	    'buckets': {
		'team': bucket
	    },
	    'maxpending': maxpending
	});
}

function setupBucket(_, next)
{
	var options = {
	    'index': {
		'name': { 'type': 'string' },
		'wins':  { 'type': 'number' }
	    }
	};

	mod_common.resetBucket(client, bucket, options, next);
}

/*
 * Checks that a single write makes it to Moray eventually.
 */
function checkWrite(_, next)
{
	mod_common.log.info('checkWrite');

	var key = 'holy_rollers';

	queue.dirty('team', 'holy_rollers', {
	    'name': key,
	    'wins': 10
	});

	queue.flush();

	mod_common.timedCheck(10, 100, function (callback) {
		client.getObject(bucket, key, { 'noCache': true },
		    function (err, record) {
			if (err) {
				callback(err);
				return;
			}

			mod_assert.equal(record['key'], key);
			mod_assert.equal(record['value']['name'], key);
			mod_assert.equal(record['value']['wins'], 10);
			callback();
		    });
	}, next);
}

/*
 * Checks that when we do too many writes to Moray, not all of them are
 * dispatched at once.  We test this by doing a bunch of writes, flushing the
 * queue once, waiting for those writes to complete, and making sure that the
 * other writes haven't happened yet.  Then we flush again and make sure the
 * remaining writes are dispatched.
 */
function checkThrottle(_, next)
{
	var keys = [], funcs = [];
	var nkeys = maxpending * 2 + 1;
	var i, checkRange;

	mod_common.log.info('checkThrottle');

	for (i = 0; i < nkeys; i++) {
		keys.push('key' + i);

		queue.dirty('team', keys[i], {
		    'name': keys[i],
		    'wins': i
		});
	}

	checkRange = function (low, high, callback) {
		var req = client.findObjects(bucket, 'name=*');
		var nexpected = high - low;

		req.on('error', callback);

		req.on('record', function (record) {
			mod_assert.ok(nexpected > 0,
			    'too many records written');
			mod_assert.ok(
			    record['value']['wins'] < high,
			    'records written out of order');
			if (record['value']['wins'] >= low)
				--nexpected;
		});

		req.on('end', exnAsync(function () {
			mod_assert.ok(nexpected === 0,
			    'too few records written at once');
			callback();
		}, callback));
	};

	funcs.push(function dispatchFirst(_1, pipenext) {
		mod_assert.equal(maxpending, queue.flush());
		mod_assert.equal(0, queue.flush());
		mod_common.timedCheck(10, 100, function (callback) {
			checkRange(0, maxpending, callback);
		}, pipenext);
	});

	funcs.push(function dispatchNext(_1, pipenext) {
		mod_assert.equal(maxpending, queue.flush());
		mod_common.timedCheck(10, 100, function (callback) {
			checkRange(maxpending, 2 * maxpending, callback);
		}, pipenext);
	});

	funcs.push(function dispatchLast(_1, pipenext) {
		mod_assert.equal(1, queue.flush());
		mod_common.timedCheck(10, 100, function (callback) {
			checkRange(2 * maxpending, nkeys, callback);
		}, pipenext);
	});

	mod_vasync.pipeline({ 'funcs': funcs }, next);
}

/*
 * Check that failed writes are retried.  The failure we can easily test is
 * simply an etag conflict error.
 */
function checkRetry(_, next)
{
	mod_common.log.info('checkRetry');

	var funcs = [];
	var key = 'pin_pals';
	var etag;

	queue.dirty('team', key, {
	    'name': key,
	    'wins': 11
	});

	queue.flush();

	funcs.push(function waitObject(_1, pipenext) {
		mod_common.log.info('checkRetry -> waitObject');

		mod_common.timedCheck(10, 100, function (callback) {
			client.getObject(bucket, key, { 'noCache': true },
			    function (err, record) {
				if (err) {
					callback(err);
					return;
				}

				etag = record['_etag'];
				mod_assert.equal(record['value']['wins'], 11);
				callback();
			    });
		}, pipenext);
	});

	funcs.push(function waitFail(_1, pipenext) {
		mod_common.log.info('checkRetry -> waitFail');

		queue.dirty('team', key, { 'name': key, 'wins': 12 },
		    { 'etag': 'bogus' });

		mod_assert.equal(0, queue.stats()['nfailed']);
		mod_assert.equal(1, queue.flush());

		mod_common.timedCheck(30, 100, function (callback) {
			queue.flush();
			mod_assert.equal(3, queue.stats()['nfailed']);
			callback();
		}, pipenext);
	});

	funcs.push(function waitSuccess(_1, pipenext) {
		mod_common.log.info('checkRetry -> waitSuccess');

		mod_assert.ok(queue.pending('team', key));
		queue.dirty('team', key,
		    { 'name': key, 'wins': 12 }, { 'etag': etag });
		queue.flush();

		mod_common.timedCheck(10, 100, function (callback) {
			queue.flush();
			mod_assert.ok(!queue.pending('team', key),
			    'write is still pending');
			callback();
		}, pipenext);
	});

	mod_vasync.pipeline({ 'funcs': funcs }, next);
}

/*
 * Check that if we dirty a record with a pending (NOT in-flight) write, the
 * subsequent value takes precedence.
 */
function checkDirtyPending(_, next)
{
	mod_common.log.info('checkDirtyPending');

	var key = 'homewreckers';

	mod_assert.ok(!queue.pending('team', key));
	queue.dirty('team', key, {
	    'name': key,
	    'wins': 5
	});

	mod_assert.ok(queue.pending('team', key));
	queue.dirty('team', key, {
	    'name': key,
	    'wins': 7
	});

	mod_assert.equal(1, queue.flush());

	mod_common.timedCheck(10, 100, function (callback) {
		client.getObject(bucket, key, { 'noCache': true },
		    exnAsync(function (err, record) {
			if (err) {
				callback(err);
				return;
			}

			mod_assert.equal(7, record['value']['wins']);
			callback();
		    }, callback));
	}, next);
}

/*
 * Check that if we dirty a record with an in-flight write, the subsequent
 * value takes precedence.
 */
function checkDirtyInFlight(_, next)
{
	mod_common.log.info('checkDirtyInFlight');

	var key = 'stereotypes';

	mod_assert.ok(!queue.pending('team', key));
	queue.dirty('team', key, {
	    'name': key,
	    'wins': 5
	});

	mod_assert.equal(1, queue.flush());

	mod_assert.ok(queue.pending('team', key));
	queue.dirty('team', key, {
	    'name': key,
	    'wins': 7
	});

	mod_common.timedCheck(10, 100, function (callback) {
		client.getObject(bucket, key, { 'noCache': true },
		    exnAsync(function (err, record) {
			queue.flush();

			if (err) {
				callback(err);
				return;
			}

			mod_assert.equal(7, record['value']['wins']);
			callback();
		    }, callback));
	}, next);
}

function teardownClient(_, next)
{
	mod_common.log.info('teardownClient');
	client.close();
	next();
}
