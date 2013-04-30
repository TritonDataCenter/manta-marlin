/*
 * lib/moray.js: common Moray abstractions
 */

var mod_assert = require('assert');

/* Public interface. */
exports.poll = poll;

/*
 * Polls Moray for new records:
 *
 *    client	Moray client object
 *
 *    options	Moray findObjects options
 *
 *    now	timestamp of the request
 *
 *    log	bunyan logger
 *
 *    throttle	throttler to determine whether to skip this poll
 *
 *    bucket	bucket to poll
 *
 *    filter	Moray filter string
 *
 *    onrecord	callback to invoke on each found record, as callback(record)
 *
 *    [ondone]	optional callback to invoke upon successful completion, as
 *
 *			callback(bucket, now, count)
 */
function poll(args)
{
	mod_assert.equal(typeof (args['client']), 'object');
	mod_assert.equal(typeof (args['options']), 'object');
	mod_assert.equal(typeof (args['now']), 'number');
	mod_assert.equal(typeof (args['log']), 'object');
	mod_assert.equal(typeof (args['throttle']), 'object');
	mod_assert.equal(typeof (args['bucket']), 'string');
	mod_assert.equal(typeof (args['filter']), 'string');
	mod_assert.equal(typeof (args['onrecord']), 'function');

	var client = args['client'];
	var options = args['options'];
	var now = args['now'];
	var log = args['log'];
	var throttle = args['throttle'];
	var bucket = args['bucket'];
	var filter = args['filter'];
	var onrecord = args['onrecord'];
	var ondone = args['ondone'];
	var onerr = args['onerr'];

	var count = 0;
	var req;

	if (throttle.tooRecent())
		return (null);

	throttle.start();

	log.debug('poll start: %s', bucket, filter);

	req = client.findObjects(bucket, filter, options);

	req.on('error', function (err) {
		log.warn(err, 'poll failed: %s', bucket);
		throttle.done();

		if (onerr)
			onerr(bucket, now);
	});

	req.on('record', function (record) {
		count++;
		onrecord(record);
	});

	req.on('end', function () {
		log.debug('poll completed: %s', bucket);
		throttle.done();

		if (ondone)
			ondone(bucket, now, count);
	});

	return (req);
}
