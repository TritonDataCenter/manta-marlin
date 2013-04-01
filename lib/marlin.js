/*
 * lib/marlin.js: public interface to Marlin via Moray.	 This is used by
 * automated tests, developer tools, and muskie.
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_fs = require('fs');
var mod_moray = require('moray');
var mod_path = require('path');
var mod_url = require('url');
var mod_util = require('util');

var mod_extsprintf = require('extsprintf');
var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_retry = require('retry');
var mod_vasync = require('vasync');
var mod_verror = require('verror');
var VError = mod_verror.VError;

var mod_mautil = require('./util');
var mod_schema = require('./schema');

var sprintf = mod_extsprintf.sprintf;

/* Public interface. */
exports.createClient = createClient;
exports.jobValidate = jobValidate;

/*
 * Creates a new Marlin client.	 "conf" arguments include:
 *
 *    moray		Moray configuration to use (including at least "url")
 *
 *    log		Bunyan-style logger
 *
 *    setup_jobs	If true, setup the "jobs" bucket if it doesn't exist.
 *    [false]
 *
 *    config_filename	Name of configuration file, which may be read
 *    [default]		synchronously.	If moray is specified, then this file is
 *			only used for static Marlin system configuration (e.g.,
 *			Moray bucket names) and the default is generally
 *			appropriate.
 *
 * "callback" will be invoked upon error or when the client is connected (and
 * the "jobs" bucket has been created, if "setup_jobs" is true).  The callback
 * is invoked as callback(err, api), where "api" is a handle with the following
 * methods:
 *
 *    jobCreate			Create a new job
 *    jobDelete			Deletes a job and all of the related tasks
 *    jobFetch			Fetch an existing job's record
 *    jobFetchLog		Fetch detailed job history
 *    jobCancel			Cancel an existing job
 *    jobAddKey			Add an input key to an existing job
 *    jobEndInput		Mark input complete for an existing job
 *    jobFetchDetails		Fetch an existing job and all related records
 *    jobFetchErrors		Fetch all failures from a job
 *    jobFetchRetries		Fetch all retried failures from a job
 *    jobFetchInputs		Fetch a job's input keys
 *    jobFetchOutputs		Fetch a job's output keys
 *    jobFetchFailedJobInputs	Fetch the job input keys that failed
 *    jobsList			List jobs matching criteria
 *    taskDone			Mark a task finished (dev only)
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
		api.on('error', onerr);
		api.once('connect', function () {
			api.removeListener('error', onerr);

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
	mod_events.EventEmitter();

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

	this.ma_client.on('close', this.emit.bind(this, 'close'));
	this.ma_client.on('connect', this.emit.bind(this, 'connect'));
	this.ma_client.on('error', this.emit.bind(this, 'error'));

	this.ma_buckets = mod_jsprim.deepCopy(conf['buckets']);

	/*
	 * Purely for convenience, the actual implementations of these methods
	 * are defined in separate functions outside this class.  Each of these
	 * functions takes this object as their first argument.
	 */
	var api = this;
	var methods = [
	    jobCreate,
	    jobDelete,
	    jobCancel,
	    jobEndInput,
	    jobAddKey,
	    jobFetch,
	    jobFetchDetails,
	    jobFetchErrors,
	    jobFetchFailedJobInputs,
	    jobFetchInputs,
	    jobFetchLog,
	    jobFetchOutputs,
	    jobFetchRetries,
	    jobsList,
	    taskDone
	];

	methods.forEach(function (func) {
		api[func.name] = func.bind(null, api);
	});
}

mod_util.inherits(MarlinApi, mod_events.EventEmitter);

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
 *		[random uuid]
 *
 *   jobName	human-readable job label (need not be unique)
 *		['']
 *
 *   owner	owning user
 *		(required)
 *
 *   input	source job for job's input stream
 *		[none]
 *
 *   authToken	authn token
 *		(required)
 *
 *   auth	authentication object, including:
 *		(required)
 *
 *	login		effective user's login name
 *
 *	uuid		effective user's uuid
 *
 *	groups		effective user's groups
 *
 *	token		authn token
 *
 *   options	additional options (only for privileged users)
 *
 * Upon completion, callback is invoked as callback(err, jobid).
 */
function jobCreate(api, conf, options, callback)
{
	mod_assert.ok(Array.isArray(conf['phases']),
	    'expected array: "phases"');
	mod_assert.equal(typeof (conf['authToken']), 'string',
	    'expected string: "authToken"');
	mod_assert.equal(typeof (conf['owner']), 'string',
	    'expected string: "owner"');
	mod_assert.equal(typeof (conf['auth']), 'object',
	    'expected object: "auth"');

	if (conf['options']) {
		mod_assert.equal(typeof (conf['options']), 'object');

		if (!mod_jsprim.isEmpty(conf['options']))
			mod_assert.ok(conf['auth']['groups'].indexOf(
			    'operators') != -1);
	}

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
	    'auth': conf['auth'],
	    'authToken': conf['authToken'],
	    'owner': conf['owner'],
	    'phases': mod_jsprim.deepCopy(conf['phases']),
	    'state': 'queued',
	    'timeCreated': mod_jsprim.iso8601(Date.now()),
	    'options': conf['options'] || {}
	};

	if (conf['input'])
		value['input'] = conf['input'];

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
 * jobDelete(jobId, options, callback): deletes a job and all related tasks
 *
 * Upon completion, callback is invoked as callback(err).
 */
function jobDelete(api, jobId, options, callback)
{
	if (arguments.length == 3) {
		callback = options;
		options = {};
	}

	var filter = '(jobId=' + jobId + ')';
	var log = options.log || api.ma_log;
	var requests = [ {
		'bucket': api.ma_buckets['job'],
		'filter': filter,
		'operation': 'deleteMany'
	}, {
		'bucket': api.ma_buckets['jobinput'],
		'filter': filter,
		'operation': 'deleteMany'
	}, {
		'bucket': api.ma_buckets['task'],
		'filter': filter,
		'operation': 'deleteMany'
	}, {
		'bucket': api.ma_buckets['taskinput'],
		'filter': filter,
		'operation': 'deleteMany'
	}, {
		'bucket': api.ma_buckets['taskoutput'],
		'filter': filter,
		'operation': 'deleteMany'
	}];

	log.debug('job "%s": deleting job', jobId);
	api.ma_client.batch(requests, function (err) {
		if (err) {
			log.warn(err, 'job "%s": failed to delete', jobId);
			callback(new VError(err, 'failed to delete job'));
		} else {
			log.debug('job "%s": deleted', jobId);
			callback(null);
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
		var uerr = updatef(newval);

		if (uerr instanceof Error) {
			log.warn(uerr);
			callback(uerr);
			return;
		}

		log.debug('job "%s": saving updated job record', jobid, newval);
		api.ma_client.putObject(bucket, jobid, newval,
		    { 'etag': job['_etag'] }, function (suberr) {
			if (suberr) {
				log.warn(suberr, 'job "%s": failed to update',
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
		if (job['input'])
			return (new VError('job "%s" input is piped from ' +
			    '%s', job['jobId'], job['input']));

		job['timeInputDone'] = mod_jsprim.iso8601(Date.now());
		return (null);
	});
}

/*
 * jobAddKey(jobid, key, options, callback): Add key "key" as input for job
 *     "jobid".	 The job need not exist for this call to succeed.
 *
 * Upon completion, invokes callback(err).
 */
function jobAddKey(api, jobid, key, options, callback)
{
	var bucket = api.ma_buckets['jobinput'];
	var record;

	if (arguments.length == 4) {
		callback = options;
		options = {};
	}

	record = {
	    'jobId': jobid,
	    'input': key,
	    'timeCreated': options['timestamp'] ||
		mod_jsprim.iso8601(Date.now())
	};

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
 * jobFetchDetails(jobid, doall, limit, callback): Fetch the job record and
 *     other records associated with this job.	Beware that that may be a very
 *     large number of records if "limit" is high.
 */
function jobFetchDetails(api, jobid, doall, limit, callback)
{
	jobFetch(api, jobid, function (err, job) {
		if (err) {
			callback(new VError(err, 'failed to fetch job'));
			return;
		}

		var rv = {
		    'job': job
		};

		var inputs = [ 'jobinput', 'task', 'error', 'taskoutput' ];

		if (doall)
			inputs.push('taskinput');

		mod_vasync.forEachParallel({
		    'inputs': inputs,
		    'func': function (bucket, subcallback) {
			var req = api.ma_client.findObjects(
			    api.ma_buckets[bucket], 'jobId=' + jobid,
			    { 'noCache': true, 'limit': limit });

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
 * Simply creates the common options object to fetch and paginate from moray for
 * all jobFetch* APIs below (internal only).
 */
function jobMorayOptions(options)
{
	options = options || {};

	var m_opts = {
		limit: options.limit,
		noCache: true,
		sort: {
			attribute: '_id',
			order: options.sort_order || 'ASC'
		}
	};

	return (m_opts);
}


/*
 * jobFetchErrors(jobid): Fetch failures from a given job.  Returns an event
 * emitter that emits "err" (to avoid conflicting with "error", which denotes an
 * actual error retrieving the list of errors), "error", and "end".  Each
 * emitted 'err' object has the following properties:
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
 *    code	programmatic error code (e.g., "ResourceNotFoundError")
 *
 *    message	human-readable error message
 *
 * The following property MAY also be present:
 *
 *    taskId	unique identifies this task, which represents a single attempt
 *		to process this key
 *
 *    stderr	name of a Manta object whose contents are the stderr from the
 *		task's execution, if the task ran, exited non-zero, and the
 *		stderr was non-empty
 *
 * The following properties MAY also be present, for map tasks only:
 *
 *    key	the key being processed when the task failed
 *
 *    p0key	the job input key which ultimately resulted in this task
 */
function jobFetchErrors(api, jobid, options)
{
	return (jobFetchErrorsImpl(api, jobid, options,
	    /* Work around MANTA-1065. */
	    '(|(retried=false)(retried=FALSE))'));
}

/*
 * Like jobFetchErrors, but fetches only the errors that were retried.
 */
function jobFetchRetries(api, jobid, options)
{
	return (jobFetchErrorsImpl(api, jobid, options,
	    '(|(retried=true)(retried=TRUE))'));
}

function jobFetchErrorsImpl(api, jobid, options, extra)
{
	var bucket = api.ma_buckets['error'];
	var filter = sprintf('(&(jobId=%s)(timeCommitted=*)%s',
	    jobid, extra);
	var log = options && options.log ? options.log : api.ma_log;
	var moray_opts = jobMorayOptions(options);
	var rv = new mod_events.EventEmitter();
	var req;

	if (options && options.marker)
		filter += '(_id>=' + options.marker + ')';

	filter += ')';

	log.debug('job "%s": fetching errors', jobid, filter);
	req = api.ma_client.findObjects(bucket, filter, moray_opts);

	req.on('error', function (err) {
		log.warn(err, 'job "%s": error fetching job errors', jobid);
		rv.emit('error', err);
	});

	req.on('record', function (record) {
		var value = record['value'];
		var summary = 'phase ' + value['phaseNum'] + ': ';

		if (value['input']) {
			summary += 'map input ' +
			    JSON.stringify(value['input']);

			if (value['phaseNum'] > 0 && value['p0input'])
				summary += ' (from job input ' +
				    JSON.stringify(value['p0input']) + ')';
		} else
			summary += 'reduce';

		var errobj = {
		    'taskId': value['taskId'],
		    'phaseNum': value['phaseNum'],
		    'server': value['server'],
		    'machine': value['machine'],
		    'key': value['input'],
		    'p0key': value['p0input'],
		    'what': summary,
		    'code': value['errorCode'],
		    'message': value['errorMessage'] || 'unknown error',
		    'stderr': value['stderr']
		};

		rv.emit('err', errobj, record);
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
 *
 * In addition to the jobid parameter, 'options' may also take a 'marker', which
 * indicates where to start the search, as well as a limit, which indicates how
 * many results can come back in a single fetch operation.
 */
function jobFetchInputs(api, jobid, options)
{
	if (!options)
		options = {};
	var bucket = api.ma_buckets['jobinput'];
	var filter = '(&(jobId=' + jobid + ')';
	var log = options.log ? options.log : api.ma_log;
	var moray_opts = jobMorayOptions(options);
	var rv = new mod_events.EventEmitter();
	var req;

	if (options && options.marker)
		filter += '(_id>=' + options.marker + ')';

	filter += ')';

	log.debug('job "%s": fetching input keys', jobid, filter);

	req = api.ma_client.findObjects(bucket, filter, moray_opts);

	req.on('error', function (err) {
		log.warn(err, 'job "%s": error fetching job inputs', jobid);
		rv.emit('error', err);
	});

	req.on('record', function (record) {
		rv.emit('key', record['value']['input'], record);
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
 *
 * In addition to the jobid parameter, 'options' may also take a 'marker', which
 * indicates where to start the search, as well as a limit, which indicates how
 * many results can come back in a single fetch operation.  The marker is
 * just the taskid.
 */
function jobFetchOutputs(api, jobid, pi, options)
{
	var bucket = api.ma_buckets['taskoutput'];
	var filter = '(&(jobId=' + jobid + ')(phaseNum=' + pi + ')' +
	    '(timeCommitted=*)(valid=true)';
	var rv, req;
	var log = options.log ? options.log : api.ma_log;
	var moray_opts = jobMorayOptions(options);

	rv = new mod_events.EventEmitter();

	if (options && options.marker)
		filter += '(_id>=' + options.marker + ')';

	filter += ')';

	log.debug('job "%s": fetching taskoutputs', jobid, filter);
	req = api.ma_client.findObjects(bucket, filter, moray_opts);
	req.on('error', function (err) {
		log.warn(err,
		    'job "%s": error fetching taskoutputs', jobid);
		rv.emit('error', err);
	});

	req.on('record', function (record) {
		rv.emit('key', record['value']['output'], record);
	});

	req.on('end', function () {
		log.debug('job "%s": fetched taskoutputs', jobid);
		rv.emit('end');
	});

	return (rv);
}

/*
 * jobFetchFailedJobInputs(jobid): Fetch the input keys for the given job which
 * failed for some reason, at any point during the job.	 Note that jobs can fail
 * for reasons that can't be tracked to a particular job input key (e.g., if the
 * failure happens during or after any reduce phase), so not all errors will be
 * represented here.
 */
function jobFetchFailedJobInputs(api, jobid, options)
{
	var rv, req;

	rv = new mod_events.EventEmitter();
	req = jobFetchErrors(api, jobid, options);
	req.on('error', function (err) { rv.emit('error', err); });
	req.on('end', function () { rv.emit('end'); });
	req.on('err', function (error, record) {
		if (error['p0key'])
			rv.emit('key', error['p0key'], record);
	});

	return (rv);
}

/*
 * jobsList(options): List all jobs.  "options" may contain:
 *
 *    state		List only jobs in specified state.
 *
 *    cancelled		If true, list only jobs that are cancelled.
 *
 *    owner		List only jobs with specified owner.
 *
 *    jobId		List only job with specified jobId.
 *
 *    worker		List only jobs with specified worker.
 *
 *    doneSince		List only jobs completed in last specified number of
 *			milliseconds.
 *
 *    log		Use the specified logger.  If unspecified, the client
 *			handle's logger will be used.
 *
 * If any fields are invalid, they will be silently ignored.
 *
 * Returns an object that emits 'error', 'record', and 'end'.
 */
function jobsList(api, options)
{
	options = options || {};

	var bucket = api.ma_buckets['job'];
	var filter;
	var filters = [];
	var moray_opts = jobMorayOptions(options);
	var req, log;

	if (options['state'] &&
	    mod_schema.sJobStates.indexOf(options['state']) != -1)
		filters.push('(state=' + options['state'] + ')');

	if (options['owner'] && /^[\d\w-]+$/.test(options['owner']))
		filters.push('(owner=' + options['owner'] + ')');

	if (options['jobId'] && /^[\d\w-]+$/.test(options['jobId']))
		filters.push('(jobId=' + options['jobId'] + ')');

	if (options['worker'] &&
	    /^[\d\w-]+$/.test(options['worker']))
		filters.push('(worker=' + options['worker'] + ')');

	if (options['cancelled'])
		filters.push('(timeCancelled=*)');

	if (options['doneSince']) {
		var limit = mod_jsprim.iso8601(
		    Date.now() - options['doneSince'] * 1000);
		filters.push('(timeDone>=' + limit + ')');
	}

	if (filters.length > 0)
		filter = '(&' + filters.join('');
	else
		filter = '(jobId=*';

	if (options.marker)
		filter += '(_id>=' + options.marker + ')';

	filter += ')';

	log = options.log || api.ma_log;
	log.debug('listing jobs', filter);

	req = api.ma_client.findObjects(bucket, filter, moray_opts);

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
function jobValidate(job, isprivileged)
{
	var schema = isprivileged ?
	    mod_schema.sHttpJobInputPrivileged : mod_schema.sHttpJobInput;
	return (mod_jsprim.validateJsonObject(schema, job));
}

/*
 * jobFetchLog(jobid): Fetch job history information for job "jobid".
 *
 * Returns an event emitter that emits "log" events, each with:
 *
 *    time	millisecond-resolution timestamp
 *
 *    where	in which component an event happened
 *
 *    what	what object (job, task) was affected
 *
 *    message	human-readable summary of what happened
 *
 * These events are NOT stable for programmatic consumption.  If we want to
 * build on top of this interface, we should add a separate, stable field
 * identifying the event that happened (e.g., "key_dispatch").
 *
 * The log entries are not emitted in time order.
 *
 * "end" is emitted when the log is finished, and "error" is emitted on failures
 * that may result in incomplete logs.
 */
function jobFetchLog(api, jobid)
{
	var rv = new JobLogFetcher(api, jobid);
	rv.start();
	return (rv);
}

function JobLogFetcher(api, jobid)
{
	this.ml_api = api;	/* marlin API handle */
	this.ml_jobid = jobid;	/* jobid whose log we're fetching */
	this.ml_limit = 1000;	/* limit of records returned per query */

	this.ml_npending = 0;	/* number of pending queries */
	this.ml_ntasks = 0;	/* tasks returned by current query */
	this.ml_filter = sprintf('(jobId=%s)', jobid);	/* query filter */
	this.ml_options = {	/* query options */
	    'noCache': true,
	    'limit': this.ml_limit
	};

	this.ml_req_job = undefined;
	this.ml_req_tasks = undefined;

	mod_events.EventEmitter();
}

mod_util.inherits(JobLogFetcher, mod_events.EventEmitter);

JobLogFetcher.prototype.start = function ()
{
	mod_assert.ok(this.ml_req_job === undefined);
	mod_assert.ok(this.ml_req_tasks === undefined);
	mod_assert.ok(this.ml_npending === 0);

	var client = this.ml_api.ma_client;
	var bucket = this.ml_api.ma_buckets['job'];

	var req = this.ml_req_job = client.findObjects(bucket,
	    this.ml_filter, this.ml_options);
	this.ml_npending++;

	req.on('error', this.error.bind(this, 'error fetching job'));
	req.on('record', this.emitJob.bind(this));
	req.on('end', this.checkDone.bind(this));

	this.fetchTasks();
};

JobLogFetcher.prototype.fetchTasks = function ()
{
	mod_assert.ok(this.ml_req_tasks === undefined);

	var fetcher = this;
	var client = this.ml_api.ma_client;
	var bucket = this.ml_api.ma_buckets['task'];

	this.ml_ntasks = 0;
	this.ml_npending++;

	var req = this.ml_req_tasks = client.findObjects(bucket,
	    this.ml_filter, this.ml_options);

	req.on('error', this.error.bind(this, 'error fetching tasks'));

	req.on('record', function (record) {
		fetcher.ml_ntasks++;
		fetcher.emitTask(record);
	});

	req.on('end', function () {
		if (fetcher.ml_ntasks < fetcher.ml_limit) {
			fetcher.checkDone();
			return;
		}

		fetcher.ml_npending--;
		fetcher.ml_req_tasks = undefined;
		fetcher.fetchTasks();
	});
};

JobLogFetcher.prototype.checkDone = function ()
{
	mod_assert.ok(this.ml_npending > 0);

	if (--this.ml_npending === 0)
		this.emit('end');
};

JobLogFetcher.prototype.error = function (prefix, err)
{
	this.emit('error', new VError(err, prefix));
};

JobLogFetcher.prototype.emitJob = function (record)
{
	var job = record['value'];

	this.emitLogEntry(job, 'timeCreated', 'user', 'job', 'submitted');
	this.emitLogEntry(job, 'timeAssigned', 'marlin', 'job', 'assigned');
	this.emitLogEntry(job, 'timeInputDone', 'user', 'job', 'input done');
	this.emitLogEntry(job, 'timeCancelled', 'user', 'job', 'cancelled');
	this.emitLogEntry(job, 'timeDone', 'marlin', 'job', 'done');
};

JobLogFetcher.prototype.emitTask = function (record)
{
	var task = record['value'];
	var label;

	label = sprintf('ph%d "%s"', task['phaseNum'],
	    task['input'] || task['taskId']);

	this.emitLogEntry(task, 'timeDispatched', 'marlin', label,
	    'dispatched');
	this.emitLogEntry(task, 'timeInputDone', 'marlin', label,
	    'all input ready');
	this.emitLogEntry(task, 'timeStarted', task['machine'] || 'marlin',
	    label, 'started');
	this.emitLogEntry(task, 'timeDone', task['machine'] || 'marlin',
	    label, 'done');
	this.emitLogEntry(task, 'timeCommitted', 'marlin', label,
	    'committed');
};

JobLogFetcher.prototype.emitLogEntry = function (obj, field, where, what,
    message)
{
	if (!obj[field])
		return;

	this.emit('log', {
	    'time': Date.parse(obj[field]),
	    'what': what,
	    'where': where,
	    'message': message
	});
};

function taskDone(api, taskid, nout, ecode, emessage, callback)
{
	mod_vasync.pipeline({
	    'arg': {
		'api': api,
		'taskid': taskid,
		'nout': nout,
		'ecode': ecode,
		'emessage': emessage
	    },
	    'funcs': [
		taskDoneFetch,
		taskDoneWriteOthers,
		taskDoneWriteTask
	    ]
	}, callback);
}

function taskDoneFetch(arg, callback)
{
	var api = arg.api;
	var client = api.ma_client;
	var bucket = api.ma_buckets['task'];
	var taskid = arg.taskid;

	client.getObject(bucket, taskid, function (err, record) {
		if (err) {
			callback(err);
			return;
		}

		if (record['value']['state'] == 'done') {
			callback(new VError('task is already done'));
			return;
		}

		arg.record = record;
		callback();
	});
}

function taskDoneWriteOthers(arg, callback)
{
	var api = arg.api;
	var client = api.ma_client;
	var task = arg.record['value'];
	var bucket, uuid;

	if (arg.nout === undefined) {
		bucket = api.ma_buckets['error'];
		uuid = mod_uuid.v4();
		client.putObject(bucket, uuid, {
		    'errorId': uuid,
		    'jobId': task['jobId'],
		    'phaseNum': task['phaseNum'],
		    'errorCode': arg.ecode,
		    'errorMessage': arg.emessage,
		    'input': task['input'] || undefined,
		    'p0input': task['p0input'] || undefined,
		    'taskId': task['taskId'],
		    'server': task['server'],
		    'machine': task['machine'],
		    'prevRecordType': 'task',
		    'prevRecordId': task['taskId']
		}, function (err) {
			if (err)
				err = new VError(err, 'failed to save error');
			callback(err);
		});
		return;
	}

	if (arg.nout === 0) {
		callback();
		return;
	}

	var now, queue, i, done;

	now = mod_jsprim.iso8601(Date.now());
	bucket = api.ma_buckets['taskoutput'];
	done = false;
	queue = mod_vasync.queue(function (_, subcallback) {
		if (done)
			return;

		uuid = mod_uuid.v4();
		client.putObject(bucket, uuid, {
		    'jobId': task['jobId'],
		    'taskId': task['taskId'],
		    'phaseNum': task['phaseNum'],
		    'output': '/nobody/stor/nonexistent',
		    'timeCreated': now
		}, function (err) {
			subcallback();

			if (!err || done)
				return;

			callback(new VError(err, 'failed to save taskoutput'));
			done = true;
		});
	}, 10);


	for (i = 0; i < arg.nout; i++)
		queue.push(i);

	queue.drain = function () {
		if (!done)
			callback();
	};
}

function taskDoneWriteTask(arg, callback)
{
	var api = arg.api;
	var client = api.ma_client;
	var now = mod_jsprim.iso8601(Date.now());
	var bucket = arg.record['bucket'];
	var key = arg.record['key'];
	var task = arg.record['value'];

	task['state'] = 'done';
	task['timeDone'] = now;

	if (!task.hasOwnProperty('machine'))
		task['machine'] = 'mrjob_fake_machine';

	if (!task.hasOwnProperty('timeAccepted'))
		task['timeAccepted'] = now;

	if (!task.hasOwnProperty('timeStarted'))
		task['timeStarted'] = now;

	if (arg.nout !== undefined) {
		task['result'] = 'ok';
		task['nOutputs'] = arg.nout;
	} else {
		task['result'] = 'fail';
	}

	client.putObject(bucket, key, task, { 'etag': arg.record['_etag'] },
	    callback);
}
