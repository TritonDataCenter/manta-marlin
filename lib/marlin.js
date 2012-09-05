/*
 * lib/marlin.js: public interface to Marlin via Moray.  This is used by both
 * automated tests and developer tools.
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_fs = require('fs');
var mod_moray = require('moray');
var mod_path = require('path');
var mod_url = require('url');

var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var mod_verror = require('verror');
var VError = mod_verror.VError;

var mod_mautil = require('./util');
var mod_schema = require('./schema');

/* Public interface. */
exports.createClient = createClient;
exports.jobValidate = jobValidate;

/*
 * Arguments:
 *
 *    moray_url		Moray URL to use
 *
 *    log		Bunyan-style logger
 *
 *    [setup_jobs]	If true, setup the "jobs" bucket if it doesn't exist.
 *    default: false
 *
 *    [config_filename]	Name of configuration file to be read *synchronously*.
 * XXX all of these should take a requestId
 */
function createClient(conf, callback)
{
	mod_assert.equal(typeof (conf), 'object',
	    '"marlin.createClient: args object required');
	mod_assert.ok(typeof (conf['log']),
	    '"marlin.createClient: "log" required');

	var log = conf['log'];
	var filename = conf['config_filename'];

	if (!filename)
		filename = mod_path.join(__dirname, '../etc/config.coal.json');

	mod_fs.readFile(filename, function (err, contents) {
		if (err) {
			callback(new VError(err, 'failed to read "%s"',
			    filename));
			return;
		}

		var json;
		try {
			json = JSON.parse(contents);
		} catch (ex) {
			callback(new VError(ex, 'failed to parse "%s"',
			    filename));
			return;
		}

		if (conf['moray_url'])
			json['moray']['url'] = conf['moray_url'];

		var api = new MarlinApi({ 'conf': json, 'log': log });
		var onerr = function (merr) {
			callback(new VError(merr, 'failed to connect'));
		};
		api.ma_client.on('error', onerr);
		api.ma_client.once('connect', function () {
			api.ma_client.removeListener('error', onerr);

			if (!conf['setup_jobs']) {
				callback(null, api);
				return;
			}

			setupJobsBucket(api, callback);
		});
	});
}

function setupJobsBucket(api, callback)
{
	var bucket = api.ma_buckets['job'];
	var schema = mod_schema.sBktSchemas['job'];
	api.ma_client.putBucket(bucket, { 'index': schema }, function (err) {
		callback(err, err ? null : api);
	});
}

/*
 * Handle for a remote Marlin service.  Arguments include:
 *
 *    conf.moray		Moray storage configuration (see worker config)
 *
 *    conf.buckets		Bucket names (see worker config)
 *
 *    log		Bunyan logger
 *
 * The public API includes:
 *
 *    jobCreate
 *    jobCancel
 *    jobEndInput
 *    jobAddKey
 *    jobFetch
 *    jobFetchDetails
 *    jobFetchOutputs
 *    jobsList
 *
 * See the definitions below for details.
 */
function MarlinApi(args)
{
	var conf = args['conf'];
	mod_assert.equal(typeof (conf['moray']), 'object');
	mod_assert.equal(typeof (conf['buckets']), 'object');
	mod_assert.ok(conf['buckets'].hasOwnProperty('job'));
	mod_assert.ok(conf['buckets'].hasOwnProperty('jobinput'));
	mod_assert.ok(conf['buckets'].hasOwnProperty('task'));
	mod_assert.ok(conf['buckets'].hasOwnProperty('taskinput'));
	mod_assert.ok(conf['buckets'].hasOwnProperty('taskoutput'));

	var url = mod_url.parse(conf['moray']['url']);

	this.ma_client = mod_moray.createClient({
	    'host': url['hostname'],
	    'port': parseInt(url['port'], 10),
	    'log': args['log'],
	    'reconnect': true
	});

	this.ma_buckets = mod_jsprim.deepCopy(conf['buckets']);

	var api = this;
	var methods = [
	    jobCreate,
	    jobCancel,
	    jobEndInput,
	    jobAddKey,
	    jobFetch,
	    jobFetchDetails,
	    jobFetchOutputs,
	    jobsList
	];

	methods.forEach(function (func) {
		api[func.name] = func.bind(null, api);
	});
}

MarlinApi.prototype.close = function ()
{
	this.ma_client.close();
};

/*
 * Create a job with the following configuration:
 *
 *   [jobId]	unique job identifier
 *		[random uuid]
 *
 *   [jobName]	non-unique job name
 *		['']
 *
 *   phases	see lib/schema.js
 *
 *   [owner]	owning user
 *		['nobody']
 *
 * Upon completion, invokes callback(err, jobid).
 */
function jobCreate(api, conf, callback)
{
	mod_assert.ok(Array.isArray(conf['phases']),
	    'expected array: "phases"');

	var bucket = api.ma_buckets['job'];
	var key = conf['jobId'] || mod_uuid.v4();
	var value = {
	    'jobId': key,
	    'jobName': conf['jobName'] || '',
	    'owner': conf['owner'] || 'nobody',
	    'phases': mod_jsprim.deepCopy(conf['phases']),
	    'state': 'queued',
	    'timeCreated': mod_jsprim.iso8601(Date.now())
	};

	api.ma_client.putObject(bucket, key, value, function (err) {
		if (err)
			callback(new VError(err, 'failed to create job'));
		else
			callback(null, key);
	});
}

/*
 * Fetch the job record for job "jobid".
 */
function jobFetch(api, jobid, callback)
{
	var bucket = api.ma_buckets['job'];

	api.ma_client.getObject(bucket, jobid, { 'noCache': true },
	    function (err, record) {
		if (err)
			callback(new VError(err, 'failed to fetch job'));
		else
			callback(null, record);
	    });
}

function jobFetchAndUpdate(api, jobid, callback, updatef)
{
	var bucket = api.ma_buckets['job'];

	api.ma_client.getObject(bucket, jobid, { 'noCache': true },
	    function (err, job) {
		if (err) {
			callback(new VError(err, 'failed to fetch job'));
			return;
		}

		var newval = mod_jsprim.deepCopy(job['value']);
		updatef(newval);
		api.ma_client.putObject(bucket, jobid, newval,
		    { 'etag': job['_etag'] }, function (suberr) {
			if (suberr) {
				callback(new VError(suberr,
				    'failed to update job'));
				return;
			}

			callback(null, job);
		    });
	    });
}

/*
 * Cancel job "jobid".  Upon completion, invokes callback(err, record), where
 * "record" is the *previous* job record.
 */
function jobCancel(api, jobid, callback)
{
	jobFetchAndUpdate(api, jobid, callback, function (job) {
		job['timeCancelled'] = mod_jsprim.iso8601(Date.now());
	});
}

/*
 * End input for job "jobid".  Upon completion, invokes callback(err, record),
 * where "record" is the current job record.
 */
function jobEndInput(api, jobid, callback)
{
	jobFetchAndUpdate(api, jobid, callback, function (job) {
		job['timeInputDone'] = mod_jsprim.iso8601(Date.now());
	});
}

/*
 * Add input keys for job "jobid" (which does not need to exist yet).
 */
function jobAddKey(api, jobid, key, callback)
{
	var bucket = api.ma_buckets['jobinput'];
	var record = { 'jobId': jobid, 'key': key };

	api.ma_client.putObject(bucket, mod_uuid.v4(), record, function (err) {
		if (err)
			callback(new VError(err, 'failed to save jobinput'));
		else
			callback(null);
	});
}

/*
 * Fetch the job record and all other records associated with this job.
 */
function jobFetchDetails(api, jobid, callback)
{
	jobFetch(api, jobid, function (err, job) {
		if (err) {
			callback(new VError(err, 'failed to fetch job'));
			return;
		}

		var rv = {
		    'job': job
		};

		mod_vasync.forEachParallel({
		    'inputs': [ 'jobinput', 'task', 'taskinput', 'taskoutput' ],
		    'func': function (bucket, subcallback) {
			var req = api.ma_client.findObjects(
			    api.ma_buckets[bucket], 'jobId=' + jobid,
			    { 'noCache': true });

			rv[bucket] = [];

			req.on('error', function (suberr) {
				subcallback(new VError(suberr,
				    'error fetching "%s" records', bucket));
			});

			req.on('record', function (record) {
				rv[bucket].push(record);
			});

			req.on('end', subcallback);
		    }
		}, function (multierr) {
			callback(multierr, rv);
		});
	});
}

/*
 * Fetch committed output records for the given job and phase.
 */
function jobFetchOutputs(api, jobid, pi)
{
	var bucket = api.ma_buckets['task'];
	var filter = '&(jobId=' + jobid + ')(phaseNum=' + pi + ')' +
	    '(timeCommitted=*)(state=done)(nOutputs>0)';
	var rv, req, xtasks;

	rv = new mod_events.EventEmitter();
	xtasks = [];

	req = api.ma_client.findObjects(bucket, filter);
	req.on('error', function (err) { rv.emit('error', err); });

	req.on('record', function (record) {
		var task = record['value'];

		/*
		 * Recall that tasks may store the first few output keys inside
		 * the task record itself.  The rest are stored in external
		 * "taskoutput" records.
		 */
		if (task['firstOutputs']) {
			task['firstOutputs'].forEach(function (key) {
				rv.emit('key', key['key']);
			});

			if (task['nOutputs'] <= task['firstOutputs'].length)
				return;
		}

		/*
		 * XXX Instead of queueing up tasks with external taskoutput
		 * records and making another Moray request those outputs at the
		 * end, we should be issuing another Moray request as we process
		 * these.  That's because if there were a million tasks, each
		 * with many output keys (admittedly an extreme case), we'd have
		 * to buffer them in memory here.
		 */
		xtasks.push(task['taskId']);
	});

	req.on('end', function () {
		if (xtasks.length === 0) {
			rv.emit('end');
			return;
		}

		/*
		 * XXX These should be chunked into groups of some more
		 * manageable number.
		 */
		var tobucket = api.ma_buckets['taskoutput'];
		var tofilter = '|' + xtasks.map(function (taskid) {
			return ('(taskId=' + taskid + ')');
		});

		var toreq = api.ma_client.fidnObjects(tobucket, tofilter);
		toreq.on('error', function (err) { rv.emit('error', err); });
		toreq.on('record', function (record) {
			rv.emit('key', record['value']['key']);
		});
		toreq.on('end', function () {
			rv.emit('end');
		});
	});

	return (rv);
}

/*
 * List all jobs.  Returns an object that emits 'error', 'record', and 'end'.
 */
function jobsList(api, options)
{
	var bucket = api.ma_buckets['job'];
	var filter = 'jobId=*';
	var filters = [];
	var req;

	if (options && options['state'] &&
	    mod_schema.sJobStates.indexOf(options['state']) != -1)
		filters.push('(state=' + options['state'] + ')');

	if (options && options['owner'] && /^[\d\w-]+$/.test(options['owner']))
		/* XXX should be a common Manta username validator */
		filters.push('(owner=' + options['owner'] + ')');

	if (options && options['jobId'] && /^[\d\w-]+$/.test(options['jobId']))
		filters.push('(jobId=' + options['jobId'] + ')');

	if (filters.length > 0)
		filter = '&' + filters.join('');
	else
		filter = 'jobId=*';

	req = api.ma_client.findObjects(bucket, filter, { 'noCache': true });
	return (req);
}

/*
 * Validate the given job record.
 */
function jobValidate(job)
{
	return (mod_jsprim.validateJsonObject(mod_schema.sHttpJobInput, job));
}
