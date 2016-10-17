/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * lib/marlin.js: public interface to Marlin via Moray.	 This is used by
 * automated tests, developer tools, and muskie.
 */

var mod_assert = require('assert');
var mod_assertplus = require('assert-plus');
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
var mod_semver = require('semver');
var mod_vasync = require('vasync');
var mod_verror = require('verror');
var VError = mod_verror.VError;

var mod_mautil = require('./util');
var mod_meter = require('./meter');
var mod_schema = require('./schema');

var sprintf = mod_extsprintf.sprintf;

/* Public interface. */
exports.createClient = createClient;
exports.jobValidate = jobValidateSchema;
exports.MarlinMeterReader = mod_meter.MarlinMeterReader;

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
 *    jobFetchPendingTasks	Fetch job tasks not yet completed
 *    jobFetchFailedJobInputs	Fetch the job input keys that failed
 *    jobArchiveStart		Start a job archival (wrasse only)
 *    jobArchiveDone		Complete a job archival (wrasse only)
 *    jobArchiveHeartbeat	Heartbeats a job during archival (wrasse only)
 *    jobArchiveReset		Resets archive status for a job
 *    jobValidate		Validate job input
 *    jobsList			List jobs matching criteria
 *    taskDone			Mark a task finished (dev only)
 *    taskFetchHistory		Fetch history of a given task
 *
 *    fetchArchiveStatus	Fetch job archival status.  This should only
 *    				be used for reporting to operators.
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

	var filename = conf['config_filename'];
	var moray;

	if (conf['moray'])
		moray = mod_jsprim.deepCopy(conf['moray']);

	/*
	 * See the comment around requires in "mrjob".
	 */
	if (!filename)
		filename = mod_path.join(__dirname,
		    '../../jobsupervisor/etc/config.coal.json');

	mod_fs.readFile(filename, function (err, contents) {
		if (!err || err['code'] != 'ENOENT') {
			createFinish(conf, moray, callback,
			    filename, err, contents);
			return;
		}

		if (!conf['config_filename'])
			filename = mod_path.join(__dirname,
			    '../etc/config.coal.json');
		mod_fs.readFile(filename,
		    createFinish.bind(null, conf, moray, callback, filename));
	});
}

function createFinish(conf, moray, callback, filename, err, contents)
{
	var log = conf['log'];

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
			    config, function (err2) {
				if (err2 && err2['name'] ==
				    'BucketVersionError') {
					log.warn(err2, 'bucket ' +
					    'schema out of date');
					err = null;
				}

				subcallback(err);
			    });
		    }
		}, function (suberr) {
			api.ma_log.info('done setting up buckets');
			callback(suberr, suberr ? null : api);
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
 *    conf.images		Supported image versions (see worker config)
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
	mod_assert.ok(Array.isArray(conf['images']));

	var url = mod_url.parse(conf['moray']['url']);

	this.ma_log = args['log'];

	this.ma_client = mod_moray.createClient({
	    'host': url['hostname'],
	    'port': parseInt(url['port'], 10),
	    'log': args['log'].child({'component': 'moray'}),
	    'retry': conf['moray']['retry'],
	    'unwrapErrors': true
	});

	this.ma_client.on('close', this.emit.bind(this, 'close'));
	this.ma_client.on('connect', this.emit.bind(this, 'connect'));
	this.ma_client.on('error', this.emit.bind(this, 'error'));

	this.ma_buckets = mod_jsprim.deepCopy(conf['buckets']);
	this.ma_images = conf['images'].slice(0);
	this.ma_limit_archiving = 10;
	this.ma_limit_archive_queued = 10;

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
	    jobFetchPendingTasks,
	    jobFetchRetries,
	    jobArchiveStart,
	    jobArchiveDone,
	    jobArchiveHeartbeat,
	    jobArchiveReset,
	    jobValidate,
	    jobsList,
	    taskDone,
	    taskFetchHistory,
	    fetchArchiveStatus
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
 *   name	human-readable job label (need not be unique)
 *		['']
 *
 *   owner	owning user
 *		(required)
 *
 *   auth	authentication object, including:
 *		(required)
 *
 *	login		effective user's login name
 *
 *	token		authn token
 *
 *	uuid		effective user's uuid
 *	(legacy)
 *
 *	groups		effective user's groups
 *	(legacy)
 *
 *	principal	access-control principal
 *
 *	conditions	access-control conditions
 *
 *		"login" and "token" are required.  Either "uuid" and "groups"
 *		(for legacy access-control behavior) OR "principal" and
 *		"conditions" (for modern access-control behavior) is required.
 *		If all are specified, modern behavior is used.
 *
 *   transient  indicates that the job is "transient", meaning its inputs will
 *   		be removed automatically and it may not be listed by default
 *
 *   options	additional options (only for privileged users)
 *
 * Upon completion, callback is invoked as callback(err, jobid).
 */
function jobCreate(api, conf, options, callback)
{
	mod_assert.ok(Array.isArray(conf['phases']),
	    'expected array: "phases"');
	mod_assert.equal(typeof (conf['owner']), 'string',
	    'expected string: "owner"');
	mod_assert.equal(typeof (conf['auth']), 'object',
	    'expected object: "auth"');

	if (conf['options']) {
		mod_assert.equal(typeof (conf['options']), 'object');

		/*
		 * Muskie should only allow operators to specify "options", but
		 * the test suite allows unprivileged users to do so.
		 */
		if (!mod_jsprim.isEmpty(conf['options'])) {
			mod_assert.ok((options && options.istest) ||
			    mod_mautil.jobIsPrivileged(conf['auth']),
			    'only operators can specify job options');
		}
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
	    'name': conf['name'] || '',
	    'auth': conf['auth'],
	    'authToken': conf['auth']['token'],
	    'owner': conf['owner'],
	    'phases': mod_jsprim.deepCopy(conf['phases']),
	    'state': 'queued',
	    'timeCreated': mod_jsprim.iso8601(Date.now()),
	    'transient': conf['transient'] || false,
	    'options': conf['options'] || {}
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
 * jobDelete(jobId, options, callback): deletes all tasks related to a
 * job, and when they are all cleaned up, deletes the actual job.  This is
 * explicitly not done transactionally, as we want to only cleanup manageable
 * chunks at a time; this API will "block" indeterminanetely and do this for
 * you, but the end result is that the job is completely eradicated.
 *
 * Upon completion, callback is invoked as callback(err, meta).
 */
function jobDelete(api, jobId, options, callback)
{
	if (arguments.length == 3) {
		callback = options;
		options = {};
	}

	var callback_invoked = false;
	var filter = '(jobId=' + jobId + ')';
	var limit = (options.options || {}).limit || 500;
	var log = options.log || api.ma_log;
	var timeout = (options.options || {}).timeout || (300 * 1000);

	var requests = [ {
		'bucket': api.ma_buckets['jobinput'],
		'filter': filter,
		'operation': 'deleteMany',
		'options': {
			'limit': limit
		}
	}, {
		'bucket': api.ma_buckets['error'],
		'filter': filter,
		'operation': 'deleteMany',
		'options': {
			'limit': limit
		}
	}, {
		'bucket': api.ma_buckets['task'],
		'filter': filter,
		'operation': 'deleteMany',
		'options': {
			'limit': limit
		}
	}, {
		'bucket': api.ma_buckets['taskinput'],
		'filter': filter,
		'operation': 'deleteMany',
		'options': {
			'limit': limit
		}
	}, {
		'bucket': api.ma_buckets['taskoutput'],
		'filter': filter,
		'operation': 'deleteMany',
		'options': {
			'limit': limit
		}
	}];

	function jobDeleteNextBatch() {
		api.ma_client.batch(requests, {'timeout': timeout}, cb);
	}

	function cb(err, meta) {
		if (callback_invoked)
			return;

		if (err) {
			log.warn(err, 'job "%s": failed to delete', jobId);
			callback(new VError(err, 'failed to delete job'));
			callback_invoked = true;
			return;
		}

		// First we check that all the internals are gone
		var internals_done = true;
		log.debug({
			meta: meta.etags // etags is a misnomer
		}, 'job "%s": deleted internals', jobId);
		meta.etags.forEach(function (m) {
			if (m.count < limit) {
				requests = requests.filter(function (r) {
					return (r.bucket !== m.bucket);
				});
			} else {
				internals_done = false;
			}
		});

		if (!internals_done) {
			log.debug('job "%s": deleting next batch', jobId);
			jobDeleteNextBatch();
			return;
		}

		// If they're all gone, we can safely drop the job and return
		api.ma_client.deleteObject(api.ma_buckets['job'], jobId,
		    function (err2) {
			    if (callback_invoked)
				    return;

			    callback_invoked = true;
			    if (err2) {
				    log.warn(err2, 'job "%s": failed to delete',
					     jobId);
				    err2 = new VError(err2, 'failed to ' +
						      'delete job');
				    callback(err2);
			    } else {
				    log.debug('job "%s": deleted', jobId);
				    callback(null);
			    }
		    });
	}

	log.debug('job "%s": deleting job', jobId);
	jobDeleteNextBatch();
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
 * *previous* job record (which can be useful for warning on odd conditions).
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

	if (options.job && options.job.worker)
		record['domain'] = options.job.worker;

	var log = options.log || api.ma_log;

	log.debug('job "%s": adding key "%s"%s', jobid, key,
	    record['domain'] ? ' with domain' : '');
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
 *    server	the logical server name where the user's task was executed,
 *		a.k.a the manta compute id
 *
 *    machine	the zonename where the user's task was executed
 *
 *    what	a human-readable summary of what failed, usually one of:
 *
 *	"reduce"				for a reduce task
 *	"map $input"				for a phase 0 map task
 *	"map $input (from job input $p0input)"	for a phase i>0 map task
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
 *    core	name of a Manta object representing the core file from the
 *		task's execution, if the task ran and one of its processes
 *		dumped core
 *
 * The following properties MAY also be present, for map tasks only:
 *
 *    input	the object being processed when the task failed
 *
 *    p0input	the job input object which ultimately resulted in this task
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
			summary += 'input ' + JSON.stringify(value['input']);

			if (value['phaseNum'] > 0 && value['p0input'])
				summary += ' (from job input ' +
				    JSON.stringify(value['p0input']) + ')';
		} else
			summary += 'reduce';

		var errobj = {
		    'taskId': value['taskId'],
		    'phaseNum': value['phaseNum'],
		    'server': value['mantaComputeId'],
		    'machine': value['machine'],
		    'input': value['input'],
		    'p0input': value['p0input'],
		    'what': summary,
		    'code': value['errorCode'],
		    'message': value['errorMessage'] || 'unknown error',
		    'stderr': value['stderr'],
		    'core': value['core']
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
 * jobFetchPendingTasks(jobid, options): Fetch tasks for a given job that are
 * not yet completed.  Returns an event emitter that emits "error", "task", and
 * "end" events.
 *
 * In addition to the jobid parameter, 'options' may also take a 'marker', which
 * indicates where to start the search, as well as a limit, which indicates how
 * many results can come back in a single fetch operation.  The marker is
 * just the taskid.
 */
function jobFetchPendingTasks(api, jobid, options)
{
	var bucket = api.ma_buckets['task'];
	var filter = '(&(jobId=' + jobid + ')(!(state=done))';
	var rv, req;
	var log = options.log ? options.log : api.ma_log;
	var moray_opts = jobMorayOptions(options);
	var copyfields = [
	    'taskId',
	    'phaseNum',
	    'state',
	    'timeDispatched',
	    'timeAccepted',
	    'rIdx',
	    'mantaComputeId',
	    'nattempts',
	    'input',
	    'nInputs'
	];

	rv = new mod_events.EventEmitter();

	if (options && options.marker)
		filter += '(_id>=' + options.marker + ')';

	filter += ')';

	log.debug('job "%s": fetching pending tasks', jobid, filter);
	req = api.ma_client.findObjects(bucket, filter, moray_opts);
	req.on('error', function (err) {
		log.warn(err, 'job "%s": error fetching pending tasks', jobid);
		rv.emit('error', err);
	});

	req.on('record', function (record) {
		var task = {
		    'id': record['_id']
		};
		copyfields.forEach(function (f) {
			if (record['value'].hasOwnProperty(f))
				task[f] = record['value'][f];
		});
		rv.emit('task', task);
	});

	req.on('end', function () {
		log.debug('job "%s": fetched pending tasks', jobid);
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
		if (error['p0input'])
			rv.emit('key', error['p0input'], record);
	});

	return (rv);
}

/*
 * jobArchiveStart(job, options, callback): wrasse uses this to mark a job
 * as starting archival (and locks).  Updates the `timeArchiveStarted` and
 * `wrasse` fields.
 *
 * options should include "wrasse" as the instance uuid
 *
 * Upon completion, invokes callback(err, record), where "record" is the
 * *previous* job record (which can be useful for warning on odd conditions).
 */
function jobArchiveStart(api, jobid, options, callback)
{
	mod_assert.equal(typeof (options), 'object',
	    'expected object: "options"');

	jobFetchAndUpdate(api, jobid, options, callback, function (job) {
		job['timeArchiveStarted'] = mod_jsprim.iso8601(Date.now());
		job['wrasse'] = options['wrasse'];
	});
}

/*
 * jobArchiveDone(job, options, callback): wrasse uses this to mark a job
 * as completely archived.  Only updates the `timeArchiveDone` field.
 *
 * Upon completion, invokes callback(err, record), where "record" is the
 * *previous* job record (which can be useful for warning on odd conditions).
 */
function jobArchiveDone(api, jobid, options, callback)
{
	if (arguments.length == 3) {
		callback = options;
		options = {};
	}

	jobFetchAndUpdate(api, jobid, options, callback, function (job) {
		job['timeArchiveDone'] = mod_jsprim.iso8601(Date.now());
	});
}

/*
 * jobArchiveHeartbeat(job, options, callback): wrasse uses this to heartbeat
 * a job (writes the job back so _time gets updated).
 *
 * Upon completion, invokes callback(err, record), where "record" is the
 * *previous* job record (which can be useful for warning on odd conditions).
 */
function jobArchiveHeartbeat(api, jobid, options, callback)
{
	if (arguments.length == 3) {
		callback = options;
		options = {};
	}

	jobFetchAndUpdate(api, jobid, options, callback, function () {});
}

/*
 * jobArchiveReset(job, options, callback): reset archive state.  This is used
 * to work around MANTA-2611.
 *
 * Upon completion, invokes callback(err, record), where "record" is the
 * *previous* job record (which can be useful for warning on odd conditions).
 */
function jobArchiveReset(api, jobid, options, callback)
{
	if (arguments.length == 3) {
		callback = options;
		options = {};
	}

	jobFetchAndUpdate(api, jobid, options, callback, function (job) {
		if (job['timeArchiveDone']) {
			return (new VError('job "%s": already archived',
			    job['jobId']));
		}

		if (job['state'] != 'done') {
			return (new VError('job "%s": expected state "done",' +
			    ' but found "%s"', job['jobId'], job['state']));
		}

		delete (job['timeArchiveStarted']);
		delete (job['wrasse']);
		return (null);
	});
}

/*
 * jobsList(options): List all jobs.  "options" may contain:
 *
 *    state		List only jobs in specified state.
 *
 *    name		List only jobs with a specified name.
 *
 *    transient		If true, list only transient jobs.  If false, list only
 *    			non-transient jobs.  (Leave unspecified or "null" for
 *    			all jobs.)
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
 *			seconds.
 *
 *    wrasse		List only jobs with specified wrasse instance.
 *
 *    archiveStartedBefore	List only jobs that have started archival before
 *				time T (seconds).
 *
 *    archived		If true, list only jobs that are archived. If false,
 *			list jobs that are done but not archived.
 *
 *    archivedBefore	List only jobs archived before the specified time
 *			(seconds).
 *
 *    mtime		List only jobs modified in the last specifiend number of
 *			seconds.
 *
 *    !mtime		List only jobs not modified in the last specified number
 *			of seconds.
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
	var limit;
	var moray_opts = jobMorayOptions(options);
	var req, log;

	if (options['state'] &&
	    mod_schema.sJobStates.indexOf(options['state']) != -1)
		filters.push('(state=' + options['state'] + ')');

	if (options['name'] &&
	    /^[\d\w-]+$/.test(options['name']))
		filters.push('(name=' + options['name'] + ')');

	if (typeof (options['transient']) == 'boolean')
		filters.push('(transient=' + options['transient'] + ')');

	if (options['owner'] && /^[\d\w-]+$/.test(options['owner']))
		filters.push('(owner=' + options['owner'] + ')');

	if (options['jobId'] && /^[\d\w-]+$/.test(options['jobId']))
		filters.push('(jobId=' + options['jobId'] + ')');

	if (options['worker'] &&
	    /^[\d\w-]+$/.test(options['worker']))
		filters.push('(worker=' + options['worker'] + ')');

	if (options['cancelled'])
		filters.push('(timeCancelled=*)');

	if (options['archived'] === true) {
		filters.push('(timeArchiveDone=*)');
	} else if (options['archived'] === false) {
		filters.push('(!(timeArchiveDone=*))');
	}

	if (options['archiveStartedBefore']) {
		limit = mod_jsprim.iso8601(
		    Date.now() - options['archiveStartedBefore'] * 1000);
		filters.push('(timeArchiveStarted<=' + limit + ')');
	}

	if (options['archivedBefore']) {
		limit = mod_jsprim.iso8601(
		    Date.now() - options['archivedBefore'] * 1000);
		filters.push('(timeArchiveDone<=' + limit + ')');
	}

	if (options['wrasse'] === null) {
		filters.push('(!(wrasse=*))');
	} else if (options['wrasse'] === true) {
		filters.push('(wrasse=*)');
	} else if (options['wrasse'] &&
	    /^[\d\w-]+$/.test(options['wrasse'])) {
		filters.push('(wrasse=' + options['wrasse'] + ')');
	} else if (options['!wrasse']) {
		filters.push('(!(wrasse=' + options['!wrasse'] + '))');
	}

	if (options['doneSince']) {
		limit = mod_jsprim.iso8601(
		    Date.now() - options['doneSince'] * 1000);
		filters.push('(timeDone>=' + limit + ')');
	}

	if (options['mtime']) {
		limit = Date.now() - (options['mtime'] * 1000);
		filters.push('(_mtime>=' + limit + ')');
	}

	if (options['!mtime']) {
		limit = Date.now() - (options['!mtime'] * 1000);
		filters.push('(_mtime<=' + limit + ')');
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
 * jobValidate(job, isprivileged): fully validate the given job input
 * Returns an error on failure, or null on success.
 */
function jobValidate(api, job, isprivileged)
{
	var error = jobValidateSchema(job, isprivileged);
	var ph, i, j;
	if (error)
		return (error);

	/*
	 * Work around json-schema#46, which allows "null" to pass for objects
	 * with required properties.  Note that this only applies to properties
	 * with type "object" (as in "phases[0]"), not "array" (as in "phases"
	 * or "phases[i].assets").
	 */
	if (job === null)
		return (new Error('job must not be null'));

	for (i = 0; i < job['phases'].length; i++) {
		ph = job['phases'][i];

		/* See comment above about json-schema#46. */
		if (ph === null)
			return (new VError(
			    'property "phases[%d]": must not be null', i));

		if (!ph.hasOwnProperty('image'))
			continue;

		if (!mod_semver.validRange(ph['image']))
			return (new VError(
			    'property "phases[%d].image": ' +
			    'invalid semver range: "%s"', i, ph['image']));

		for (j = 0; j < api.ma_images.length; j++) {
			if (mod_semver.satisfies(
			    api.ma_images[j], ph['image']))
				break;
		}

		if (j == api.ma_images.length)
			return (new VError('property "phases[%d].image": ' +
			    'unsupported version: "%s"', i, ph['image']));
	}

	return (null);
}

/*
 * Validate the given job record.  NOTE: This only validates basic semantics.
 * For full validation, use client.jobValidate above.
 */
function jobValidateSchema(job, isprivileged)
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
		    'server': task['mantaComputeId'],
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

function taskFetchHistory(api, taskid, callback)
{
	var taskhistory = {
	    'th_api': api,
	    'th_barrier': mod_vasync.barrier(),
	    'th_client': api.ma_client,
	    'th_errors': [],
	    'th_log': api.ma_log,
	    'th_jobid': null,
	    'th_objects': {
		'error': {},
		'jobinput': {},
	        'task': {},
		'taskinput': {},
		'taskoutput': {}
	    }
	};

	taskHistoryGet(taskhistory, 'task', taskid);

	taskhistory.th_barrier.on('drain', function () {
		if (taskhistory.th_errors.length > 0)
			callback(taskhistory.th_errors[0]);
		else
			callback(null, taskHistoryAssemble(taskhistory));
	});
}

function taskHistoryWalk(taskhistory, type, key, value)
{
	if (taskhistory.th_objects[type].hasOwnProperty(key))
		return;

	taskhistory.th_objects[type][key] = value;

	if (taskhistory.th_jobid === null)
		taskhistory.th_jobid = value['jobId'];

	if (value.hasOwnProperty('prevRecordType')) {
		taskhistory.th_log.debug('want prev for %s %s', type, key);
		taskHistoryGet(taskhistory,
		    value['prevRecordType'],
		    value['prevRecordId']);
	}

	if (value.hasOwnProperty('nextRecordType')) {
		taskhistory.th_log.debug('want next for %s %s', type, key);
		taskHistoryGet(taskhistory,
		    value['nextRecordType'],
		    value['nextRecordId']);
	}

	/*
	 * We only walk taskoutputs for "map" tasks.
	 */
	if (type == 'task' && value['input'] && value['result'] == 'ok') {
		taskhistory.th_log.debug('want outputs for %s %s', type, key);
		taskHistoryFind(taskhistory, 'taskoutput',
		    sprintf('(taskId=%s)', key));
	}

	/*
	 * The way Marlin maintains links for retries for map and reduce tasks
	 * is unfortunately different.
	 *
	 * For map tasks, the retry has prevRecord{Type,Id} fields that point
	 * back at the original task.  So if we're given the retry, we
	 * follow that pointer back to the previous attempt (above).  If we're
	 * given the first attempt, we do a "find" to find the retry (below).
	 *
	 * For reduce tasks, the original task has a retryTaskId property that
	 * points to the retry.  So if we're given the first attempt, we follow
	 * that pointer to the retry (below).  If we're given the retry, we do a
	 * "find" below to find the original (also below).
	 *
	 * This should be normalized, but this tool needs to work with the
	 * existing mechanism anyway.
	 */
	if (type == 'task' && value['timeRetried']) {
		if (value['retryTaskId']) {
			taskhistory.th_log.debug('want retry task for %s %s',
			    type, key);
			taskHistoryGet(taskhistory, 'task',
			    value['retryTaskId']);
		} else {
			taskhistory.th_log.debug('want retry task for %s %s',
			    type, key);
			taskHistoryFind(taskhistory, 'task',
			    sprintf('(prevRecordType=task)(prevRecordId=%s)',
			    key));
		}
	}

	if (type == 'task' && !value['input'] && value['nattempts'] > 1) {
		taskhistory.th_log.debug('want previous task for %s %s',
		    type, key);
		taskHistoryFind(taskhistory, 'task',
		    sprintf('(retryTaskId=%s)', value['taskId']));
	}

	if (type == 'taskinput' || type == 'taskoutput') {
		taskhistory.th_log.debug('want task for %s %s', type, key);
		taskHistoryGet(taskhistory, 'task', value['taskId']);
	}
}

function taskHistoryGet(taskhistory, type, key)
{
	if (taskhistory.th_objects[type].hasOwnProperty(key))
		return;

	var bucket = taskhistory.th_api.ma_buckets[type];
	var barrier = taskhistory.th_barrier;

	taskhistory.th_log.debug('fetching %s %s', type, key);
	barrier.start(type + ' ' + key);

	taskhistory.th_client.getObject(bucket, key, function (err, record) {
		if (err)
			taskhistory.th_errors.push(err);
		else
			taskHistoryWalk(taskhistory, type, key,
			    record['value']);

		barrier.done(type + ' ' + key);
	});
}

function taskHistoryFind(taskhistory, type, subfilter)
{
	var bucket = taskhistory.th_api.ma_buckets[type];
	var barrier = taskhistory.th_barrier;
	var filter, req;

	taskhistory.th_log.debug('fetching %s for %s', type, subfilter);
	mod_assert.ok(taskhistory.th_jobid !== null);
	barrier.start('find ' + type + ' ' + subfilter);

	filter = sprintf('(&(jobId=%s)%s)', taskhistory.th_jobid, subfilter);
	req = taskhistory.th_client.findObjects(bucket, filter);

	req.on('error', function (err) {
		taskhistory.th_errors.push(err);
		barrier.done('find ' + type + ' ' + subfilter);
	});

	req.on('record', function (record) {
		taskHistoryWalk(taskhistory, type, record['key'],
		    record['value']);
	});

	req.on('end', function () {
		barrier.done('find ' + type + ' ' + subfilter);
	});
}

/*
 * Sort all of the objects, first by phase, and then by order of records within
 * a phase: jobinputs, taskinputs, tasks, taskoutputs, errors.
 */
function taskHistoryAssemble(taskhistory)
{
	var bucketorder = [ 'jobinput', 'taskinput', 'task',
	    'taskoutput', 'error' ];
	var rv = [];
	var type, key;

	for (type in taskhistory.th_objects) {
		for (key in taskhistory.th_objects[type]) {
			rv.push({
			    'type': type,
			    'key': key,
			    'value': taskhistory.th_objects[type][key]
			});
		}
	}

	rv.sort(function (o1, o2) {
		var diff, b1, b2;

		/*
		 * Job inputs are always first, and they're sorted by input
		 * object name.
		 */
		if (o1.type == 'jobinput') {
			if (o2.type == 'jobinput')
				return (o1.value['input'].localeCompare(
				    o2.value['input']));
			return (-1);
		}

		if (o2.type == 'jobinput')
			return (1);

		/*
		 * Next, all records are sorted by phase number.
		 */
		mod_assert.ok(o1.value.hasOwnProperty('phaseNum'));
		mod_assert.ok(o2.value.hasOwnProperty('phaseNum'));
		diff = o1.value['phaseNum'] - o2.value['phaseNum'];
		if (diff !== 0)
			return (diff);

		/*
		 * Tasks in the same phase are sorted by the retry count.
		 */
		if (o1.type == 'task' && o2.type == 'task')
			return (o1.value['nattempts'] - o2.value['nattempts']);

		/*
		 * Taskinputs, taskoutputs, and errors are next sorted by their
		 * task's nattempts (since they're in the same phase).
		 */
		b1 = o1.type == 'error' ?
		    o1.value['prevRecordId'] : o1.value['taskId'];
		b1 = taskhistory.th_objects['task'][b1]['nattempts'];
		b2 = o2.type == 'error' ?
		    o2.value['prevRecordId'] : o2.value['taskId'];
		b2 = taskhistory.th_objects['task'][b2]['nattempts'];
		diff = b1 - b2;
		if (diff !== 0)
			return (diff);

		/*
		 * Within a task, sort records by the bucket sort order listed
		 * above.
		 */
		b1 = bucketorder.indexOf(o1.type);
		mod_assert.ok(b1 != -1);
		b2 = bucketorder.indexOf(o2.type);
		mod_assert.ok(b2 != -1);
		diff = b1 - b2;
		if (diff !== 0)
			return (diff);

		/*
		 * Within a task, taskoutputs are sorted by output object name.
		 */
		mod_assert.equal(o1.type, o2.type);
		if (o1.type == 'taskoutput')
			return (o1.value['output'].localeCompare(
			    o2.value['output']));

		/*
		 * Within a task, taskinputs are sorted by input object name.
		 */
		if (o1.type == 'taskinput')
			return (o1.value['input'].localeCompare(
			    o2.value['input']));

		/*
		 * At this point, we should be looking at two errors, and we
		 * don't currently define a sort order for them.
		 */
		mod_assert.equal(o1.type, 'error');
		return (0);
	});

	return (rv.map(taskHistorySanitize));
}

function taskHistorySanitize(entry)
{
	var allowed, rv;

	allowed = [
	    'jobId',
	    'taskId',
	    'taskInputId',
	    'phaseNum',
	    'state',
	    'result',
	    'timeDispatched',
	    'timeCreated',
	    'rIdx',
	    'mantaComputeId',
	    'nattempts',
	    'timeDispatchDone',
	    'timeAccepted',
	    'timeInputDone',
	    'nInputs',
	    'input',
	    'output',
	    'timeStarted',
	    'timeDone',
	    'machine',
	    'timeCommitted',
	    'errorCode',
	    'errorMessage',
	    'errorMessageInternal'
	];

	rv = {
	    'type': entry['type'],
	    'key': entry['key'],
	    'value': {}
	};

	allowed.forEach(function (f) {
		if (entry['value'].hasOwnProperty(f))
			rv['value'][f] = entry['value'][f];
	});

	return (rv);
}

/*
 * Fetches the current status of job archival, in terms of overall counts and
 * some example jobs.  The object passed to callback() contains:
 *
 *     nJobsTotal	(integer) count of total jobs in the bucket
 *     nJobsDone	(integer) count of jobs in state "done"
 *     nJobsArchived	(integer) count of jobs archived
 *     nJobsAssigned    (integer) count of jobs assigned to a wrasse but not
 *				  archived yet
 *
 *     jobNextDelete    (job)	  next job to be deleted
 *     jobLastArchived  (job)     last job archived
 *
 *     jobsArchiveAssigned   (array of jobs, abridged)
 *     jobsArchiveUnassigned (array of jobs, abridged)
 *
 * Each "job" above has:
 *
 *     "id"                  (string)
 *     "timeDone"            (ISO8601 date string)
 *     "timeArchiveStarted"  (ISO8601 date string, or null if not yet assigned)
 *     "timeArchiveDone"     (ISO8601 date string, or null if not yet archived)
 *
 * Note that before MANTA-2348, we did not have indexes on the fields required
 * to do this operation efficiently.  The first thing we do here is check for
 * the presence of these indexes.  If we don't find them, we do a much more
 * annoying, much more expensive check.
 */
function fetchArchiveStatus(api, options, callback)
{
	var bucket;

	mod_assertplus.object(options, 'options');
	mod_assertplus.func(callback, 'callback');

	if (options.forceLegacyFetch) {
		fetchArchiveStatusLegacy(api, callback);
		return;
	}

	bucket = api.ma_buckets['job'];
	api.ma_client.getBucket(bucket, function (err, config) {
		if (err) {
			callback(new VError(err, 'fetch bucket "%s"', bucket));
			return;
		}

		if (config.index.hasOwnProperty('timeArchiveStarted') &&
		    config.index.hasOwnProperty('timeArchiveDone') &&
		    config.index.hasOwnProperty('wrasse')) {
			fetchArchiveStatusWithIndexes(api, callback);
		} else {
			console.error('warning: archive-related indexes ' +
			    'missing on bucket: "%s"', bucket);
			fetchArchiveStatusLegacy(api, callback);
		}
	});
}

/*
 * Implements fetchArchiveStatus() when indexes are available for all desired
 * fields.  This is relatively cheap: we issue a number of separate queries to
 * count jobs in various stages through the archive process, and we also fetch a
 * few sample jobs from these stages.
 */
function fetchArchiveStatusWithIndexes(api, callback)
{
	var requests;

	/*
	 * This table describes a bunch of queries we want to make, and also
	 * holds the results of each query.  We'll execute these in parallel
	 * below and then take the results (by name) and form the desired output
	 * object.
	 */
	requests = {
	    'countAllJobs': {
		'count': -1,
		'results': null,
	        'filter': 'jobId=*',
		'limit': 1
	    },
	    'countDoneJobs': {
		'count': -1,
		'results': null,
	        'filter': '(&(jobId=*)(state=done))',
		'limit': 1
	    },
	    'listJobNextDelete': {
		'count': -1,
		'results': null,
	        'filter': '(&(jobId=*)(state=done)(timeArchiveDone=*))',
		'limit': 1,
		'sort': {
		    'attribute': 'timeArchiveDone',
		    'order': 'asc'
		}
	    },
	    'listJobLastArchived': {
		'count': -1,
		'results': null,
	        'filter': '(&(jobId=*)(state=done)(timeArchiveDone=*))',
		'limit': 1,
		'sort': {
		    'attribute': 'timeArchiveDone',
		    'order': 'desc'
		}
	    },
	    'listJobsArchiveAssigned': {
		'count': -1,
		'results': null,
		'filter': '(&(jobId=*)(state=done)(!(timeArchiveDone=*))' +
		    '(timeArchiveStarted=*))',
		'limit': api.ma_limit_archiving,
		'sort': {
		    'attribute': 'timeDone',
		    'order': 'asc'
		}
	    },
	    'listJobsArchiveUnassigned': {
		'count': -1,
		'results': null,
		'filter': '(&(jobId=*)(state=done)(!(timeArchiveStarted=*)))',
		'limit': api.ma_limit_archive_queued,
		'sort': {
		    'attribute': 'timeDone',
		    'order': 'asc'
		}
	    }
	};

	mod_vasync.forEachParallel({
	    'inputs': Object.keys(requests),
	    'func': function kickOffRequest(rqname, subcb) {
		archiveStatusFind(api, rqname, requests[rqname], subcb);
	    }
	}, function (err, results) {
		if (err) {
			callback(err);
			return;
		}

		archiveStatusFini(requests, callback);
	});
}

/*
 * Given a single request from the table in fetchArchiveStatusWithIndexes, make
 * the request and save the results.
 */
function archiveStatusFind(api, rqname, rqinfo, callback)
{
	var bucket, options, req;

	bucket = api.ma_buckets['job'];

	options = {};
	if (rqinfo.limit)
		options.limit = rqinfo.limit;
	if (rqinfo.sort)
		options.sort = rqinfo.sort;

	rqinfo.results = [];
	req = api.ma_client.findObjects(bucket, rqinfo.filter, options);
	req.on('error', function (err) {
		callback(new VError(err, 'archive: %s', rqname));
	});

	req.on('record', function (record) {
		var v = record.value;
		rqinfo.count = record._count;
		rqinfo.results.push(archiveJobInfo(v));
	});

	req.on('end', function () {
		if (rqinfo.count == -1)
			rqinfo.count = 0;
		callback();
	});
}

/*
 * Take the combined results of each of the requests described in
 * fetchArchiveStatusWithIndexes() and form the desired output object.
 */
function archiveStatusFini(requests, callback)
{
	var rv = {};
	rv.nJobsTotal = requests['countAllJobs'].count;
	rv.nJobsDone = requests['countDoneJobs'].count;
	rv.nJobsArchived = requests['listJobNextDelete'].count;
	rv.nJobsAssigned = requests['listJobsArchiveAssigned'].count;
	rv.jobLastArchived = requests['listJobLastArchived'].results[0] || null;
	rv.jobNextDelete = requests['listJobNextDelete'].results[0] || null;
	rv.jobsArchiveAssigned = requests['listJobsArchiveAssigned'].results;
	rv.jobsArchiveUnassigned =
	    requests['listJobsArchiveUnassigned'].results;
	callback(null, rv);
}

/*
 * Implements fetchArchiveStatus() for the case where we don't have the
 * requisite indexes.  This is a considerably more expensive operation, and it's
 * also much more annoying to implement.  We cannot rely on Moray's _count field
 * for any of the non-indexed fields (since that reflects the count reported by
 * the database, which doesn't know about the additional constraints).  We also
 * must specify a limit at least as large as the jobs table in order to make
 * sure Moray actually finds all matching records.
 */
function fetchArchiveStatusLegacy(api, callback)
{
	var client, bucket, rv;
	var countall, countdone;
	var maxarchiving, maxqueued, archivefilter;

	client = api.ma_client;
	bucket = api.ma_buckets['job'];
	rv = {
	    'nJobsTotal': -1,
	    'nJobsDone': -1,
	    'nJobsArchived': 0,
	    'nJobsAssigned': 0,
	    'jobLastArchived': null,
	    'jobNextDelete': null,
	    'jobsArchiveAssigned': [],
	    'jobsArchiveUnassigned': []
	};

	countall = {
	    'count': -1,
	    'results': null,
	    'filter': 'jobId=*',
	    'limit': 1
	};

	countdone = {
	    'count': -1,
	    'results': null,
	    /* XXX commonize filters */
	    'filter': '(&(jobId=*)(state=done))',
	    'limit': 1
	};

	console.error('warning: using legacy approach (this is expensive)');

	maxarchiving = api.ma_limit_archiving;
	maxqueued = api.ma_limit_archive_queued;
	archivefilter = '(&(jobId=*)(state=done))';

	mod_vasync.waterfall([
	    function fetchTotalCount(subcb) {
		archiveStatusFind(api, 'countAllJobs', countall, subcb);
	    },
	    function fetchDoneCount(subcb) {
		archiveStatusFind(api, 'countDoneJobs', countdone, subcb);
	    },
	    function fetchDoneJobs(subcb) {
		var req;

		console.error('warning: will scan through all %d completed ' +
		    'jobs', countdone.count);
		req = client.findObjects(bucket, archivefilter,
		    { 'limit': countdone.count });
		req.on('error', subcb);
		req.on('record', function (record) {
			var v = record['value'];

			if (v.timeArchiveDone) {
				rv.nJobsArchived++;
				if (rv.jobLastArchived === null ||
				    rv.jobLastArchived.timeArchiveDone <
				    v.timeArchiveDone) {
					rv.jobLastArchived = archiveJobInfo(v);
				}

				if (rv.jobNextDelete === null ||
				    rv.jobNextDelete.timeArchiveDone >
				    v.timeArchiveDone) {
					rv.jobNextDelete = archiveJobInfo(v);
				}
			} else if (v.timeArchiveStarted) {
				rv.nJobsAssigned++;
				legacyListInsert(rv.jobsArchiveAssigned, v,
				    v.timeDone, maxarchiving);
			} else {
				legacyListInsert(rv.jobsArchiveUnassigned, v,
				    v.timeDone, maxqueued);
			}
		});
		req.on('end', function () {
			rv.nJobsTotal = countall.count;
			rv.nJobsDone = countdone.count;
			rv.jobsArchiveAssigned = rv.jobsArchiveAssigned.map(
			    archiveJobInfo);
			rv.jobsArchiveUnassigned = rv.jobsArchiveUnassigned.map(
			    archiveJobInfo);
			subcb(null, rv);
		});
	    }
	], callback);
}

/*
 * Given a job's "value" (from its Moray record), return a summary appropriate
 * for fetchArchiveStatus().  See that function for the interface definition.
 */
function archiveJobInfo(v)
{
	mod_assertplus.string(v.jobId);

	return ({
	    'id': v['jobId'],
	    'timeDone': v['timeDone'],
	    'timeArchiveStarted': v['timeArchiveStarted'] || null,
	    'timeArchiveDone': v['timeArchiveDone'] || null
	});
}

/*
 * Assuming "list" is a list of the first "maxlen" jobs in ascending order by
 * "sortval", insert "v" into the list.  This is used to keep track of the N
 * oldest jobs.  This is O(N), but N is bounded to a small constant (10).
 */
function legacyListInsert(list, v, sortval, maxlen)
{
	var i;

	/*
	 * We start from the back of the list since by far the most likely case
	 * is that "sortval" is not going to be one of the first "maxlen"
	 * values.
	 */
	mod_assertplus.string(sortval);
	v._sortval = sortval;
	for (i = list.length - 1; i >= 0; i--) {
		if (sortval >= list[i]._sortval)
			break;
	}

	if (list.length == maxlen && i == list.length - 1) {
		return;
	}

	list.splice(i + 1, 0, v);
	if (list.length > maxlen)
		list.splice(maxlen, list.length - maxlen);
}
