/*
 * lib/marlin.js: public interface to Marlin via Moray.  This is used by
 * automated tests, developer tools, and muskie.
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_fs = require('fs');
var mod_moray = require('moray');
var mod_path = require('path');
var mod_url = require('url');

var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_retry = require('retry');
var mod_vasync = require('vasync');
var mod_verror = require('verror');
var VError = mod_verror.VError;

var mod_mautil = require('./util');
var mod_schema = require('./schema');

/* Public interface. */
exports.createClient = createClient;
exports.jobValidate = jobValidate;

/*
 * Creates a new Marlin client.  "conf" arguments include:
 *
 *    moray		Moray configuration to use (including at least "url")
 *
 *    log		Bunyan-style logger
 *
 *    setup_jobs	If true, setup the "jobs" bucket if it doesn't exist.
 *    [false]
 *
 *    config_filename	Name of configuration file, which may be read
 *    [default]		synchronously.  If moray is specified, then this file is
 *    			only used for static Marlin system configuration (e.g.,
 *    			Moray bucket names) and the default is generally
 *    			appropriate.
 *
 * "callback" will be invoked upon error or when the client is connected (and
 * the "jobs" bucket has been created, if "setup_jobs" is true).  The callback
 * is invoked as callback(err, api), where "api" is a handle with the following
 * methods:
 *
 *    jobCreate			Create a new job
 *    jobFetch			Fetch an existing job's record
 *    jobCancel			Cancel an existing job
 *    jobAddKey			Add an input key to an existing job
 *    jobEndInput		Mark input complete for an existing job
 *    jobFetchDetails		Fetch an existing job and all related records
 *    jobFetchErrors		Fetch all failures from a job
 *    jobFetchInputs		Fetch a job's input keys
 *    jobFetchOutputs		Fetch a job's output keys
 *    jobFetchFailedJobInputs	Fetch the job input keys that failed
 *    jobsList			List jobs matching criteria
 *
 *    close			Close all open clients.
 *
 * See documentation below for each of these methods.  For all of the functions
 * that take an "options" argument, that argument may contain "log" for logging.
 * Otherwise, the client handle's log is used.
 */
function createClient(conf, callback)
{
	mod_assert.equal(typeof (conf), 'object',
	    '"marlin.createClient: args object required');
	mod_assert.ok(typeof (conf['log']),
	    '"marlin.createClient: "log" required');

	var log = conf['log'];
	var filename = conf['config_filename'];
	var moray;

	if (conf['moray'])
		moray = mod_jsprim.deepCopy(conf['moray']);

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

		if (moray)
			json['moray'] = moray;

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

			mod_vasync.forEachParallel({
			    'inputs': [ 'job', 'jobinput' ],
			    'func': function makeBucket(name, subcallback) {
				var bucket = api.ma_buckets[name];
				var config = mod_schema.sBktConfigs[name];
				api.ma_log.info('setting up %s bucket',
				    name, bucket, config);
				api.ma_client.putBucket(bucket,
				    config, subcallback);
			    }
			}, function (suberr) {
				api.ma_log.info('done setting up buckets');
				callback(suberr, suberr ? null : api);
			});
		});
	});
}

/*
 * Private constructor for Marlin client handle.  Arguments include:
 *
 *    conf.moray		Moray storage configuration (see worker config)
 *
 *    conf.buckets		Bucket names (see worker config)
 *
 *    log			Bunyan logger
 *
 * See the definitions below for details.
 */
function MarlinApi(args)
{
	mod_assert.equal(typeof (args['log']), 'object');

	var conf = args['conf'];
	mod_assert.equal(typeof (conf['moray']), 'object');
	mod_assert.equal(typeof (conf['buckets']), 'object');
	mod_assert.ok(conf['buckets'].hasOwnProperty('job'));
	mod_assert.ok(conf['buckets'].hasOwnProperty('jobinput'));
	mod_assert.ok(conf['buckets'].hasOwnProperty('task'));
	mod_assert.ok(conf['buckets'].hasOwnProperty('taskinput'));
	mod_assert.ok(conf['buckets'].hasOwnProperty('taskoutput'));

	var url = mod_url.parse(conf['moray']['url']);

	this.ma_log = args['log'];

	this.ma_client = mod_moray.createClient({
	    'host': url['hostname'],
	    'port': parseInt(url['port'], 10),
	    'log': args['log'].child({'component': 'moray'}),
	    'reconnect': true,
	    'retry': conf['moray']['retry']
	});

	this.ma_buckets = mod_jsprim.deepCopy(conf['buckets']);

	/*
	 * Purely for convenience, the actual implementations of these methods
	 * are defined in separate functions outside this class.  Each of these
	 * functions takes this object as their first argument.
	 */
	var api = this;
	var methods = [
	    jobCreate,
	    jobCancel,
	    jobEndInput,
	    jobAddKey,
	    jobFetch,
	    jobFetchDetails,
	    jobFetchErrors,
	    jobFetchFailedJobInputs,
	    jobFetchInputs,
	    jobFetchOutputs,
	    jobsList
	];

	methods.forEach(function (func) {
		api[func.name] = func.bind(null, api);
	});
}

MarlinApi.prototype.close = function ()
{
	this.ma_log.info('closing marlin client');
	this.ma_client.close();
};

/*
 * jobCreate(conf, options, callback): create a new job described by "conf":
 *
 *   phases	see lib/schema.js
 *		(required)
 *
 *   jobId	unique job identifier
 *   		[random uuid]
 *
 *   jobName	human-readable job label (need not be unique)
 *   		['']
 *
 *   owner	owning user
 *   		['nobody']
 *
 * Upon completion, callback is invoked as callback(err, jobid).
 */
function jobCreate(api, conf, options, callback)
{
	mod_assert.ok(Array.isArray(conf['phases']),
	    'expected array: "phases"');

	if (arguments.length == 3) {
		callback = options;
		options = {};
	}

	var log = options.log || api.ma_log;
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

	log.debug('job "%s": creating with value', key, conf);
	api.ma_client.putObject(bucket, key, value, function (err) {
		if (err) {
			log.warn(err, 'job "%s": failed to create', key);
			callback(new VError(err, 'failed to create job'));
		} else {
			log.debug('job "%s": created', key);
			callback(null, key);
		}
	});
}

/*
 * jobFetch(jobid, callback): Fetch the job record for job "jobid".
 *
 * Upon completion, "callback" is invoked as callback(err, record), where
 * "record" is the full Moray record for the job (not just the value itself).
 */
function jobFetch(api, jobid, options, callback)
{
	if (arguments.length == 3) {
		callback = options;
		options = {};
	}

	var bucket = api.ma_buckets['job'];
	var log = options.log || api.ma_log;

	log.debug('job "%s": fetching record', jobid);
	api.ma_client.getObject(bucket, jobid, { 'noCache': true },
	    function (err, record) {
		if (err) {
			log.warn(err, 'job "%s": failed to fetch', jobid);
			callback(new VError(err, 'failed to fetch job'));
		} else {
			log.debug('job "%s": fetched record', jobid, record);
			callback(null, record);
		}
	    });
}

/*
 * Private routine to read-modify-write a job record.
 */
function jobFetchAndUpdateTry(api, jobid, options, callback, updatef)
{
	var bucket = api.ma_buckets['job'];
	var log = options.log || api.ma_log;

	log.debug('job "%s": fetching job for update', jobid);
	api.ma_client.getObject(bucket, jobid, { 'noCache': true },
	    function (err, job) {
		if (err) {
			log.warn('job "%s": failed to fetch job', jobid);
			callback(new VError(err, 'failed to fetch job'));
			return;
		}

		log.debug('job "%s": fetched job', jobid, job['value']);

		var newval = mod_jsprim.deepCopy(job['value']);
		updatef(newval);

		log.debug('job "%s": saving updated job record', jobid, newval);
		api.ma_client.putObject(bucket, jobid, newval,
		    { 'etag': job['_etag'] }, function (suberr) {
			if (suberr) {
				log.warn(err, 'job "%s": failed to update',
				    jobid);
				callback(new VError(suberr,
				    'failed to update job'));
				return;
			}

			log.debug('job "%s": saved', jobid);
			callback(null, job);
		    });
	    });
}

function jobFetchAndUpdate(api, jobid, options, callback, updatef)
{
	if (!options.retry) {
		jobFetchAndUpdateTry(api, jobid, options, callback, updatef);
		return;
	}

	var op = mod_retry.operation(options.retry);
	var log = options.log || api.ma_log;

	op.attempt(function () {
		jobFetchAndUpdateTry(api, jobid, options, function (err, rec) {
			if (op.retry(err)) {
				log.debug(err, 'jobFetchAndUpdate: failed ' +
				    'but will retry');
				return;
			}

			callback(err, rec);
		}, updatef);
	});
}

/*
 * jobCancel(jobid, options, callback): Cancel job "jobid".
 *
 * Upon completion, invokes callback(err, record), where "record" is the
 * *previous* job record (which can be useful for warning on odd conditions.
 */
function jobCancel(api, jobid, options, callback)
{
	if (arguments.length == 3) {
		callback = options;
		options = {};
	}

	jobFetchAndUpdate(api, jobid, options, callback, function (job) {
		job['timeCancelled'] = mod_jsprim.iso8601(Date.now());
	});
}

/*
 * jobEndInput(jobid, options, callback): End input for job "jobid".
 *
 * Upon completion, invokes callback(err, record), where "record" is the
 * *previous* job record.  (See "jobCancel".)
 */
function jobEndInput(api, jobid, options, callback)
{
	if (arguments.length == 3) {
		callback = options;
		options = {};
	}

	jobFetchAndUpdate(api, jobid, options, callback, function (job) {
		job['timeInputDone'] = mod_jsprim.iso8601(Date.now());
	});
}

/*
 * jobAddKey(jobid, key, options, callback): Add key "key" as input for job
 *     "jobid".  The job need not exist for this call to succeed.
 *
 * Upon completion, invokes callback(err).
 */
function jobAddKey(api, jobid, key, options, callback)
{
	var bucket = api.ma_buckets['jobinput'];
	var record = { 'jobId': jobid, 'key': key };

	if (arguments.length == 4) {
		callback = options;
		options = {};
	}

	var log = options.log || api.ma_log;

	log.debug('job "%s": adding key "%s"', jobid, key);
	api.ma_client.putObject(bucket, mod_uuid.v4(), record, function (err) {
		if (err) {
			log.warn(err, 'job "%s": failed to add key "%s"',
			    jobid, key);
			callback(new VError(err, 'failed to save jobinput'));
		} else {
			log.debug('job "%s": added key "%s"', jobid, key);
			callback();
		}
	});
}

/*
 * jobFetchDetails(jobid, callback): Fetch the job record and all other records
 *     associated with this job.  Beware that that may be a very large number of
 *     records.
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
			    { 'noCache': true, 'limit': 5000 });

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
 * jobFetchErrors(jobid): Fetch failures from a given job.  Returns an event
 * emitter that emits "err" (to avoid conflicting with "error", which denotes an
 * actual error retrieving the list of errors), "error", and "end".  Each
 * emitted 'err' object has the following properties:
 *
 *    taskId	unique identifies this task, which represents a single attempt
 *    		to process this key
 *
 *    phaseNum	which phase was being processed
 *
 *    server	the physical server name where the user's task was executed
 *
 *    machine	the zonename where the user's task was executed
 *
 *    what	a human-readable summary of what failed, usually one of:
 *
 *	"reduce"				for a reduce task
 *	"map $key"				for a phase 0 map task
 *	"map $key (from job input key $p0key)"	for a phase i>0 map task
 *
 *		This can be derived from the other fields, but is provided as a
 *		convenience.
 *
 *    code	programmatic error code (e.g., "EJ_NOENT")
 *
 *    message	human-readable error message
 *
 * The following property MAY also be present:
 *
 *    stderr	name of a Manta object whose contents are the stderr from the
 *    		task's execution, if the task ran, exited non-zero, and the
 *    		stderr was non-empty
 *
 * The following properties MAY also be present, for map tasks only:
 *
 *    key	the key being processed when the task failed
 *
 *    p0key	the job input key which ultimately resulted in this task
 */
function jobFetchErrors(api, jobid, options)
{
	var bucket = api.ma_buckets['task'];
	var filter = '&(jobId=' + jobid + ')(error=*)';
	var log = options && options.log ? options.log : api.ma_log;
	var rv = new mod_events.EventEmitter();
	var req;

	log.debug('job "%s": fetching errors', jobid, filter);
	req = api.ma_client.findObjects(bucket, filter);

	req.on('error', function (err) {
		log.warn(err, 'job "%s": error fetching job errors', jobid);
		rv.emit('error', err);
	});

	req.on('record', function (record) {
		var value = record['value'];
		var error = value['error'] || {
		    'code': 'EJ_UNKNOWN',
		    'message': 'unknown error'
		};
		var summary = 'phase ' + value['phaseNum'] + ': ';

		if (value['key']) {
			summary += 'map key ' + JSON.stringify(value['key']);

			if (value['phaseNum'] > 0 && value['p0key'])
				summary += ' (from job input key ' +
				    JSON.stringify(value['p0key']) + ')';
		} else
			summary += 'reduce';

		var errobj = {
		    'taskId': value['taskId'],
		    'phaseNum': value['phaseNum'],
		    'server': value['server'],
		    'machine': value['machine'],
		    'key': value['key'],
		    'p0key': value['p0key'],
		    'what': summary,
		    'code': error['code'],
		    'message': error['message'],
		    'stderr': value['stderr']
		};

		rv.emit('err', errobj);
	});

	req.on('end', function () {
		log.debug('job "%s": finished fetching job errors', jobid);
		rv.emit('end');
	});

	return (rv);
}

/*
 * jobFetchInputs(jobid): Fetch input keys for the given job.  Returns an event
 * emitter that emits "error", "key" (with a string representation of the key as
 * input by the user), and "end".
 */
function jobFetchInputs(api, jobid, options)
{
	var bucket = api.ma_buckets['jobinput'];
	var filter = 'jobId=' + jobid;
	var log = options && options.log ? options.log : api.ma_log;
	var rv = new mod_events.EventEmitter();
	var req;

	log.debug('job "%s": fetching input keys', jobid, filter);

	req = api.ma_client.findObjects(bucket, filter);

	req.on('error', function (err) {
		log.warn(err, 'job "%s": error fetching job inputs', jobid);
		rv.emit('error', err);
	});

	req.on('record', function (record) {
		rv.emit('key', record['value']['key']);
	});

	req.on('end', function () {
		log.debug('job "%s": finished fetching job inputs', jobid);
		rv.emit('end');
	});

	return (rv);
}

/*
 * jobFetchOutputs(jobid, pi): Fetch committed output records for the given job
 *     phase.  Returns an event emitter that emits "error", "key", and "end"
 *     events.
 */
function jobFetchOutputs(api, jobid, pi, options)
{
	var bucket = api.ma_buckets['task'];
	var filter = '&(jobId=' + jobid + ')(phaseNum=' + pi + ')' +
	    '(timeCommitted=*)(state=done)(nOutputs>=1)';
	var rv, req, xtasks;
	var log = options && options.log ? options.log : api.ma_log;

	rv = new mod_events.EventEmitter();
	xtasks = [];

	log.debug('job "%s": fetching tasks for outputs', jobid, filter);
	req = api.ma_client.findObjects(bucket, filter);
	req.on('error', function (err) {
		log.warn(err,
		    'job "%s": error fetching tasks for outputs', jobid);
		rv.emit('error', err);
	});

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
		log.debug('job "%s": fetched tasks for outputs', jobid);

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

		log.debug('job "%s": fetching extra taskoutputs',
		    jobid, tofilter);

		var toreq = api.ma_client.findObjects(tobucket, tofilter);
		toreq.on('error', function (err) {
			log.warn(err,
			    'job "%s": error fetching taskoutputs', jobid);
			rv.emit('error', err);
		});
		toreq.on('record', function (record) {
			rv.emit('key', record['value']['key']);
		});
		toreq.on('end', function () {
			log.debug('job "%s": fetched taskoutputs', jobid);
			rv.emit('end');
		});
	});

	return (rv);
}

/*
 * jobFetchFailedJobInputs(jobid): Fetch the input keys for the given job which
 * failed for some reason, at any point during the job.  Note that jobs can fail
 * for reasons that can't be tracked to a particular job input key (e.g., if the
 * failure happens during or after any reduce phase), so not all errors will be
 * represented here.
 */
function jobFetchFailedJobInputs(api, jobid, options)
{
	var bucket = api.ma_buckets['task'];
	var filter = '&(jobId=' + jobid + ')(timeCommitted=*)' +
	    '(|(state=cancelled)(state=aborted)(&(state=done)(result=fail)))';
	var log = options && options.log ? options.log : api.ma_log;
	var rv = new mod_events.EventEmitter();
	var req;

	log.debug('job "%s": fetching failed job inputs', jobid, filter);

	req = api.ma_client.findObjects(bucket, filter);

	req.on('error', function (err) {
		log.warn(err, 'job "%s": error fetching failed inputs', jobid);
		rv.emit('error', err);
	});

	req.on('record', function (record) {
		if (record['value']['p0key'])
			rv.emit('key', record['value']['p0key']);
	});

	req.on('end', function () {
		log.debug('job "%s": finished fetching failed inputs', jobid);
		rv.emit('end');
	});

	return (rv);
}

/*
 * jobsList(options): List all jobs.  "options" may contain:
 *
 *    state		List only jobs in specified state.
 *
 *    owner		List only jobs with specified owner.
 *
 *    jobId		List only job with specified jobId.
 *
 *    log		Use the specified logger.  If unspecified, the client
 *    			handle's logger will be used.
 *
 * If any fields are invalid, they will be silently ignored.
 *
 * Returns an object that emits 'error', 'record', and 'end'.
 */
function jobsList(api, options)
{
	var bucket = api.ma_buckets['job'];
	var filter = 'jobId=*';
	var filters = [];
	var req, log;

	if (options && options['state'] &&
	    mod_schema.sJobStates.indexOf(options['state']) != -1)
		filters.push('(state=' + options['state'] + ')');

	if (options && options['owner'] && /^[\d\w-]+$/.test(options['owner']))
		filters.push('(owner=' + options['owner'] + ')');

	if (options && options['jobId'] && /^[\d\w-]+$/.test(options['jobId']))
		filters.push('(jobId=' + options['jobId'] + ')');

	if (filters.length > 0)
		filter = '&' + filters.join('');
	else
		filter = 'jobId=*';

	log = options && options.log ? options.log : api.ma_log;
	log.debug('listing jobs', filter);

	req = api.ma_client.findObjects(bucket, filter, { 'noCache': true });

	req.on('error', function (err) {
		log.warn(err, 'error listing jobs');
	});

	req.on('end', function () {
		log.debug('listed jobs');
	});

	return (req);
}

/*
 * Validate the given job record.
 */
function jobValidate(job)
{
	return (mod_jsprim.validateJsonObject(mod_schema.sHttpJobInput, job));
}
