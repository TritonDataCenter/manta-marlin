/*
 * lib/worker/worker.js: job worker implementation
 */

/*
 * Each Marlin deployment includes a fleet of job workers that are responsible
 * for managing the distributed execution of Marlin jobs.  The core of each
 * worker is a loop that looks for new and abandoned jobs, divides each job into
 * chunks called tasks, assigns these tasks to individual compute nodes,
 * monitors each node's progress, and collects the results.  While individual
 * workers are not resource-intensive, a fleet is used to support very large
 * numbers of jobs concurrently and to provide increased availability in the
 * face of failures and partitions.
 *
 * Jobs and tasks are represented as records within Moray instances, which are
 * themselves highly available.  At any given time, a job is assigned to at most
 * one worker, and this assignment is stored in the job's record in Moray.
 * Workers do not maintain any state which cannot be reconstructed from the
 * state stored in Moray.  This makes it possible for workers to pick up jobs
 * abandoned by other workers which have failed or become partitioned from the
 * Moray ring.  In order to detect such failures, workers must update job
 * records on a regular basis (even if there's no substantial state change) so
 * that failure to update the job record indicates that a worker has failed.
 *
 * All communication among the workers, compute nodes, and the web tier (through
 * which jobs are submitted and monitored) goes through Moray.  The Moray
 * interface is abstracted out so that it can be replaced with an alternative
 * mechanism for testing.
 *
 * Jobs run through the following states:
 *
 *                              +
 *                              | Discover new or abandoned job
 *                              v
 *                         UNASSIGNED
 *                              |
 *                              | Successfully write assignment record
 *                              v
 *                  +---- INITIALIZING
 *                  |           |
 *                  |           | Finish retrieving all existing records
 *                  v           v
 *                  + <----  RUNNING
 *                  |           |
 *                  |           | Last phase completes or
 *                  |           | job encounters fatal failure
 *    Job dropped   |           v
 *    because lock  |       FINISHING
 *    was lost      |           |
 *                  |           | Final save completes
 *                  |           v
 *                  +-------> Remove job from internal state
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_extsprintf = require('extsprintf');
var mod_fs = require('fs');
var mod_path = require('path');
var mod_util = require('util');

var mod_carrier = require('carrier');
var mod_jsprim = require('jsprim');
var mod_redis = require('redis');
var mod_manta = require('manta');
var mod_mkdirp = require('mkdirp');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_locator = require('./locator');
var mod_mamoray = require('../moray');
var mod_moray = require('./moray');
var mod_schema = require('../schema');
var mod_mautil = require('../util');

var sprintf = mod_extsprintf.sprintf;

/* Public interface */
exports.mwConfSchema = mwConfSchema;
exports.mwWorker = Worker;

var mwTmpfileRoot = '/var/tmp/marlin';

/*
 * Configuration file JSON schema
 */
var mwConfSchema = {
    'type': 'object',
    'properties': {
	'instanceUuid': mod_schema.sStringRequiredNonEmpty,
	'port': mod_schema.sTcpPortRequired,
	'manta': {
	    'required': true,
	    'type': 'object',
	    'properties': {
		'url': mod_schema.sStringRequiredNonEmpty,
		'connectTimeout': mod_schema.sIntervalRequired
	    }
	},
	'moray': {
	    'required': true,
	    'type': 'object',
	    'properties': {
		'indexing': {
		    'required': true,
		    'type': 'object',
		    'properties': {
			'urls': {
			    'required': true,
			    'type': 'array',
			    'items': mod_schema.sStringRequiredNonEmpty,
			    'minItems': 1
			},
			'reconnect': {
			    'required': true,
			    'type': 'object',
			    'properties': {
				'maxTimeout': mod_schema.sIntervalRequired,
				'retries': mod_schema.sIntervalRequired
			    }
			}
		    }
		},
		'storage': {
		    'required': true,
		    'type': 'object',
		    'properties': {
			'url': mod_schema.sStringRequiredNonEmpty,
			'reconnect': {
			    'required': true,
			    'type': 'object',
			    'properties': {
				'maxTimeout': mod_schema.sIntervalRequired,
				'retries': mod_schema.sIntervalRequired
			    }
			}
		    }
		}
	    }
	},
	'auth': {
	    'required': true,
	    'type': 'object',
	    'properties': {
		'host': mod_schema.sStringRequiredNonEmpty,
		'port': mod_schema.sTcpPortRequired,
		'options': {
		    'type': 'object'
		}
	    }
	},
	'locator': {
	    'type': 'string',
	    'enum': [ 'manta' ]
	},
	'buckets': {
	    'required': true,
	    'type': 'object',
	    'properties': {
		'job': mod_schema.sStringRequiredNonEmpty,
		'jobinput': mod_schema.sStringRequiredNonEmpty,
		'task': mod_schema.sStringRequiredNonEmpty,
		'taskinput': mod_schema.sStringRequiredNonEmpty,
		'taskoutput': mod_schema.sStringRequiredNonEmpty
	    }
	},
	'tunables': {
	    'maxPendingAuths': mod_schema.sIntervalRequired,
	    'maxPendingDeletes': mod_schema.sIntervalRequired,
	    'maxPendingLocates': mod_schema.sIntervalRequired,
	    'maxPendingPuts': mod_schema.sIntervalRequired,
	    'maxRecentRequests': mod_schema.sIntervalRequired,
	    'maxRecordsPerQuery': mod_schema.sIntervalRequired,
	    'timeJobAbandon': mod_schema.sIntervalRequired,
	    'timeJobSave': mod_schema.sIntervalRequired,
	    'timePoll': mod_schema.sIntervalRequired,
	    'timeTaskAbandon': mod_schema.sIntervalRequired,
	    'timeTick': mod_schema.sIntervalRequired
	}
    }
};


/*
 * Maintains state for a single job.  The logic for this class lives in the
 * worker class.  Arguments include:
 *
 *    conf	worker configuration
 *
 *    log	bunyan-style logger
 *
 *    record	initial job record, as returned from Moray
 */
function WorkerJobState(args)
{
	var j = args['record']['value'];

	this.j_job = j;				/* in-moray job record */
	this.j_id = j['jobId'];			/* immutable job id */
	this.j_log = args['log'];		/* job-specific logger */
	this.j_etag = args['record']['_etag'];	/* initial etag */

	this.j_state = 'unassigned';		/* current state (see above) */
	this.j_state_time = new Date();		/* time of last state change */
	this.j_dropped = undefined;		/* time the job was dropped */

	if (j['timeInputDone'])
		this.j_input_done = Date.parse(j['timeInputDone']);
	else
		this.j_input_done = undefined;

	this.j_input_read = undefined;		/* last time all inputs read */
	this.j_cancelled = j['timeCancelled'];	/* time job was cancelled */
	this.j_locates = 0;			/* nr of pending locates */
	this.j_auths = 0;			/* nr of pending auths */
	this.j_queued = 0;			/* nr of queued records */

	this.j_tasks = {};			/* set of all tasks */
	this.j_phases = j['phases'].map(
	    function (phase) { return (new WorkerJobPhase(phase)); });

	this.j_save_throttle = new mod_mautil.Throttler(
	    args['conf']['tunables']['timeJobSave']);
	this.j_save = new mod_mautil.SaveGeneration();

	/*
	 * Before initializing the job, we'll retrieve all of its records in all
	 * Marlin-related buckets.  We store these records in j_init_records
	 * until we have them all.  We store the set of buckets we haven't
	 * finished loading in j_init_waiting.
	 */
	this.j_init_start = undefined;

	this.j_init_records = {
	    'jobinput': {},
	    'task': {},
	    'taskinput': {},
	    'taskoutput': {}
	};

	this.j_init_waiting = {
	    'jobinput': undefined,
	    'task': undefined,
	    'taskinput': undefined,
	    'taskoutput': undefined
	};

	this.j_polls = {};
	for (var key in this.j_init_records)
		this.j_polls[key] = new mod_mamoray.PollState(
		    args['conf']['tunables']['timePoll'],
		    args['conf']['tunables']['maxRecentRequests']);
}

WorkerJobState.prototype.debugState = function ()
{
	return ({
	    'record': this.j_job,
	    'state': this.j_state,
	    'state_time': this.j_state_time,
	    'dropped': this.j_dropped,
	    'input_done': this.j_input_done,
	    'cancelled': this.j_cancelled,
	    'phases': this.j_phases,
	    'save_throttle': this.j_save_throttle,
	    'save_gen': this.j_save,
	    'init_start': this.j_init_start,
	    'init_records': this.j_init_records,
	    'init_waiting': this.j_init_waiting
	});
};


/*
 * Stores runtime state about each phase in a job.
 */
function WorkerJobPhase(phase)
{
	var count = phase.hasOwnProperty('count') ? phase['count'] : 1;

	this.p_type = phase['type'];	/* phase type (same as in job record) */
	this.p_npending = 0;		/* number of uncommitted tasks */

	if (this.p_type != 'reduce')
		return;

	this.p_reducers = new Array(count);	/* task records */

	for (var i = 0; i < count; i++) {
		this.p_reducers[i] = {
		    'r_task': undefined,	/* task record */
		    'r_ninput': 0,		/* total nr of input keys */
		    'r_anon_stream': undefined,	/* file storing list of keys */
		    'r_anon_filename': undefined,
		    'r_anon_bufs': []
		};
	}
}


/*
 * Stores information about a specific task within a job.
 */
function WorkerJobTask(jobid, pi, taskid)
{
	this.t_id = taskid;		/* unique task identifier */
	this.t_xoutputs = [];		/* output keys NOT inside task record */
	this.t_record = undefined;	/* last received record for this task */
	this.t_value = {		/* authoritative task record */
	    'jobId': jobid,
	    'taskId': taskid,
	    'phaseNum': pi
	};
}


/*
 * Stores current state associated with remote agents so that we can identify
 * when they've gone AWOL and time out the corresponding tasks.
 */
function WorkerAgent()
{
	/*
	 * In order to detect when an agent has gone AWOL, we keep track of two
	 * timestamps: a_last is the last time we heard anything from this
	 * agent, and a_dispatch is the time we started dispatching any tasks to
	 * this agent.  a_dispatch gets reset if the agent's pending task list
	 * goes to zero.
	 *
	 * If a_dispatch is zero, then there are no outstanding tasks, and
	 * there's nothing to do.  If a_dispatch is non-zero and MAX(a_last,
	 * a_dispatch) is more than TIMEOUT seconds ago, then we know we
	 * should have heard from the agent recently and haven't, so we time it
	 * out.  (Note that a_dispatch is NOT the last dispatch time -- namely,
	 * we don't update it when we dispatch tasks unless it's there were
	 * previously no tasks dispatched.  Otherwise, this wouldn't work.)
	 */
	this.a_last = 0;	/* time (msec since epoch) of last contact */
	this.a_dispatch = 0;	/* time since initial dispatch of current set */
	this.a_tasks = {};	/* set of uncompleted tasks assigned to agent */
}


/*
 * Manages all jobs owned by a single Marlin worker.  Arguments include:
 *
 *    conf		Configuration object matching the above schema.
 *
 *    log		Bunyan-style logger instance
 */
function Worker(args)
{
	var worker, conf, error;

	worker = this;
	conf = args['conf'];
	mod_assert.ok(args.hasOwnProperty('log'), '"log" is required');
	mod_assert.equal('object', typeof (conf), '"conf" must be an object');

	args['log'].info('worker configuration', args['conf']);
	error = mod_jsprim.validateJsonObject(mwConfSchema, args['conf']);
	if (error) {
		args['log'].fatal(error, 'invalid configuration');
		throw (error);
	}

	/* immutable configuration */
	this.w_uuid = conf['instanceUuid'];
	this.w_conf = mod_jsprim.deepCopy(conf);
	this.w_buckets = this.w_conf['buckets'];
	this.w_max_pending_locates = conf['tunables']['maxPendingLocates'];
	this.w_max_pending_auths = conf['tunables']['maxPendingAuths'];
	this.w_max_pending_deletes = conf['tunables']['maxPendingDeletes'];
	this.w_time_tick = conf['tunables']['timeTick'];
	this.w_time_poll = conf['tunables']['timePoll'];
	this.w_time_job_abandon = conf['tunables']['timeJobAbandon'];
	this.w_time_job_save = conf['tunables']['timeJobSave'];
	this.w_time_task_abandon = conf['tunables']['timeTaskAbandon'];
	this.w_tmproot = mwTmpfileRoot + '-' + this.w_uuid;

	this.w_names = {};
	mod_jsprim.forEachKey(this.w_buckets, function (name, bucket) {
		worker.w_names[bucket] = name;
	});

	this.w_poll_options = {
	    'limit': conf['tunables']['maxRecordsPerQuery'],
	    'noCache': true,
	    'sort': {
		'attribute': '_txn_snap',
		'order': 'ASC'
	    }
	};

	/* helper objects */
	this.w_log = args['log'].child({
	    'component': 'worker-' + this.w_uuid
	});

	this.w_redis = undefined;

	this.w_locator = mod_locator.createLocator(args['conf'], {
	    'log': this.w_log.child({ 'component': 'manta-locator' })
	});

	this.w_moray = mod_moray.createMoray({
	    'conf': args['conf'],
	    'log': this.w_log.child({ 'component': 'moray-client' })
	});

	this.w_queue = new mod_mamoray.MorayWriteQueue({
	    'log': this.w_log.child({ 'component': 'moray-queue' }),
	    'client': function () { return (worker.w_moray); },
	    'buckets': this.w_buckets,
	    'maxpending': conf['tunables']['maxPendingPuts']
	});

	this.w_poll = new mod_mamoray.PollState(conf['tunables']['timePoll'],
	    conf['tunables']['maxRecentRequests']);
	this.w_abandon_throttle = new mod_mautil.Throttler(
	    conf['tunables']['timePoll']);

	this.w_manta = mod_manta.createClient({
	    'connectTimeout': conf['manta']['connectTimeout'],
	    'url': conf['manta']['url'],
	    'headers': {
		'x-marlin': 'true'
	    },
	    'log': this.w_log.child({ 'component': 'manta-client' }),
	    'sign': function (_, callback) {
		/*
		 * This is not used because we're passing x-marlin
		 * above.  This must be replaced with a proper signature
		 * when that's available.
		 */
		callback(null, {
		    'user': 'poseidon',
		    'keyId': 'none',
		    'algorithm': 'rsa-sha1',
		    'signature': 'none'
		});
	    }
	});

	/* global dynamic state */
	this.w_pending_locates = 0;		/* nr of pending locate ops */
	this.w_pending_auths = 0;		/* nr of pending auth ops */
	this.w_pending_deletes = 0;		/* nr of pending delete ops */
	this.w_worker_start = undefined;	/* time worker started */
	this.w_worker_stopped = undefined;	/* time worker stopped */
	this.w_tick_start = undefined;		/* time last tick started */
	this.w_tick_done = undefined;		/* time last tick finished */
	this.w_timeout = undefined;		/* JS timeout handle */
	this.w_jobs = {};			/* all jobs, by jobId */
	this.w_tasks = {};			/* pending tasks, by taskId */
	this.w_agents = {};			/* remote agents, by server */

	this.w_stats = {			/* stat counters */
	    'asgn_failed': 0,			/* failed job assignments */
	    'asgn_restart': 0			/* jobs picked up on restart */
	};

	/* incoming message queues */
	this.w_jobinputs_in = [];	/* incoming "job input" records */
	this.w_tasks_in = [];		/* incoming "task" record updates */
	this.w_taskoutputs_in = [];	/* incoming "task output" records */

	/*
	 * Outgoing locate requests are specified simply by Manta key (as a
	 * string).  The responses are enqueued on w_locates_in as an object:
	 *
	 *	lr_key		Manta key located
	 *
	 *	lr_objectid	Manta object uuid
	 *
	 *	lr_error	Detailed error message
	 *
	 *	lr_locations	Array of locations, each an object:
	 *
	 *	    lrl_server		physical server uuid
	 *
	 *	    lrl_zonename	mako zone uuid
	 *
	 * The pending requests are stored as objects in w_locates with:
	 *
	 *	l_key		Manta key to be located (uses account uuid)
	 *
	 *      l_login		Display name for this key's user
	 *
	 *	l_origins	List of jobinput or taskoutput records that are
	 *			waiting on the result of this request, in the
	 *			same format as aq_origins below
	 */
	this.w_locates_out = [];	/* outgoing "locate" requests */
	this.w_locates_in = [];		/* incoming "locate" responses */
	this.w_locates = {};		/* pending "locate" requests */

	/*
	 * Similarly, we keep a set of pending auth requests, each with:
	 *
	 *    aq_type		'jobinput' | 'taskinput'
	 *
	 *    aq_origins	list of records waiting on this request, each
	 *    			with:
	 *
	 *			    o_record	actual Moray record object
	 *
	 *			    o_ridx	reducer index, if any
	 *
	 *    aq_issued		issue time (for debugging)
	 *
	 * And corresponding responses, each with:
	 *
	 *    ar_key		user-facing key name (uses login name)
	 *
	 *    ar_error		error, if any
	 *
	 *    ar_login		resolved login name
	 *
	 *    ar_account	resolved account name
	 */
	this.w_auths_out = [];		/* outgoing "auth" requests */
	this.w_auths_in = [];		/* incoming "auth" responses */
	this.w_auths = {};		/* pending "auth" requests */

	/*
	 * Finally, we also keep track of pending "delete" requests.
	 */
	this.w_deletes_out = [];	/* outgoing "delete" requests */
	this.w_deletes = {};		/* pending "delete" requests */
}

Worker.prototype.debugState = function ()
{
	return ({
	    'conf': this.w_conf,
	    'pending_locates': this.w_pending_locates,
	    'worker_start': this.w_worker_start,
	    'worker_stopped': this.w_worker_stopped,
	    'tick_start': this.w_tick_start,
	    'tick_done': this.w_tick_done,
	    'agents': this.w_agents,
	    'njobinputs_in': this.w_jobinputs_in.length,
	    'ntasks_in': this.w_tasks_in.length,
	    'ntaskoutputs_in': this.w_taskoutputs_in.length,
	    'nauths_in': this.w_auths_in.length,
	    'nauths_out': this.w_auths_out.length,
	    'nlocates_in': this.w_locates_in.length,
	    'nlocates_out': this.w_locates_out.length
	});
};

/*
 * Kang (introspection) entry point.
 */
Worker.prototype.kangStats = function (callback)
{
	return (mod_jsprim.deepCopy(this.w_stats));
};

/*
 * Fetch stats.  This call is used by the tests, but is asynchronous so that it
 * can be replaced in the future with a call to a remote worker.
 */
Worker.prototype.stats = function (callback)
{
	var stats = mod_jsprim.deepCopy(this.w_stats);
	process.nextTick(function () { callback(null, stats); });
	return (undefined);
};

/*
 * Start the worker: connect to Moray and start looking for work to do.
 */
Worker.prototype.start = function ()
{
	var log = this.w_log;
	var next = this.tick.bind(this);
	var worker = this;

	mod_assert.ok(this.w_worker_start === undefined);
	this.w_worker_start = new Date();

	this.initTmp();

	log.info('initializing moray');
	this.w_moray.setup(function (err) {
		if (err) {
			log.fatal(err, 'failed to initialize moray');
			throw (err);
		}

		var conf = worker.w_conf['auth'];
		var redis;

		log.info('initializing redis', conf);

		worker.w_redis = redis = mod_redis.createClient(conf['port'],
		    conf['host'], conf['options'] || {});

		redis.once('error', function (suberr) {
			log.fatal(suberr, 'failed to connect to redis');
			throw (suberr);
		});

		redis.once('end', function () {
			log.fatal('redis connection closed');
			throw (new Error('redis connection closed'));
		});

		redis.once('ready', function () {
			log.info('redis connected');
			worker.w_redis.removeAllListeners('error');

			log.info('starting worker');
			process.nextTick(next);
		});
	});
};

Worker.prototype.initTmp = function ()
{
	var worker = this;
	var files;

	mod_assert.ok(mod_jsprim.startsWith(this.w_tmproot, '/var/tmp/marlin'));

	this.w_log.info('initializing "%s"', this.w_tmproot);
	mod_mkdirp.sync(this.w_tmproot);

	files = mod_fs.readdirSync(this.w_tmproot);

	files.forEach(function (file) {
		worker.w_log.info('removing "%s"', file);
		mod_fs.unlinkSync(mod_path.join(worker.w_tmproot, file));
	});
};

/*
 * Stop the worker: stop polling on Moray and invoke "callback" when all pending
 * operations have stopped.
 */
Worker.prototype.stop = function (callback)
{
	this.w_log.info('shutting down worker');
	this.w_worker_stopped = new Date();

	if (this.w_timeout) {
		clearTimeout(this.w_timeout);
		this.w_timeout = undefined;
	}

	if (callback)
		this.w_moray.drain(callback);
};

/*
 * Process incoming messages and send queued outgoing messages.
 */
Worker.prototype.flush = function ()
{
	this.processQueues();
	this.w_queue.flush();
};

/*
 * The heart of the job worker: this function is invoked periodically to poll
 * Moray, evaluate timeouts, and evaluate job state.
 */
Worker.prototype.tick = function ()
{
	var now, worker, jobid;

	this.w_timeout = undefined;
	this.w_tick_start = new Date();
	now = this.w_tick_start.getTime();
	worker = this;

	/* Poll Moray for updates. */
	this.poll(now);

	this.flush();

	/* Check whether each job needs to be saved. */
	for (jobid in this.w_jobs)
		this.jobTick(this.w_jobs[jobid]);

	/* Look for agents that we haven't heard from in a while. */
	mod_jsprim.forEachKey(this.w_agents, function (host, agent) {
		var ntasks = 0;
		var last, reason;

		if (agent.a_dispatch === 0) {
			mod_assert.ok(mod_jsprim.isEmpty(agent.a_tasks));
			return;
		}

		last = Math.max(agent.a_last, agent.a_dispatch);

		if (now - last <= worker.w_time_task_abandon)
			return;

		delete (worker.w_agents[host]);

		mod_jsprim.forEachKey(agent.a_tasks, function (taskid) {
			ntasks++;
			worker.taskTimedOut(worker.w_tasks[taskid], now);
		});

		if (agent.a_last > agent.a_dispatch)
			reason = 'last response was';
		else
			reason = 'no response since dispatch';

		worker.w_log.info('agent "%s" has timed out (%s %d ms ago); ' +
		    'cancelling %d tasks', host, reason, now - last, ntasks);
	});

	this.w_tick_done = new Date();
	this.w_timeout = setTimeout(this.tick.bind(this), this.w_time_tick);
};

Worker.prototype.poll = function (now)
{
	var worker = this;
	var req, abandon_filter, updated_filter;

	/* Kick off poll for new and abandoned jobs. */
	abandon_filter = sprintf('(&(!(timeDone=*))' +
	    '(|(!(worker=*))(_mtime<=%s)))', now - this.w_time_job_abandon);
	mod_mamoray.poll({
	    'client': this.w_moray,
	    'options': this.w_poll_options,
	    'now': now,
	    'log': this.w_log,
	    'throttle': this.w_abandon_throttle,
	    'bucket': this.w_buckets['job'],
	    'filter': abandon_filter,
	    'onrecord': this.onRecord.bind(this)
	});

	/* Kick off poll for updated jobs that we own. */
	updated_filter = sprintf('(&(!(timeDone=*))(worker=%s)%s)',
	    this.w_uuid, this.w_poll.filter());
	req = mod_mamoray.poll({
	    'client': this.w_moray,
	    'options': this.w_poll_options,
	    'now': now,
	    'log': this.w_log,
	    'throttle': this.w_poll.p_throttle,
	    'bucket': this.w_buckets['job'],
	    'filter': updated_filter,
	    'onrecord': function (record) {
		if (worker.w_poll.record(record))
			worker.onRecord(record);
	    }
	});
	this.w_poll.request(updated_filter, req);

	/* Kick off poll for updated job-related records. */
	mod_jsprim.forEachKey(this.w_jobs, function (jobid, job) {
		var log = job.j_log;

		if (job.j_state == 'unassigned')
			return;

		mod_jsprim.forEachKey(job.j_polls, function (name, state) {
			var filter = sprintf('(&(jobId=%s)%s)',
			    jobid, state.filter());
			req = mod_mamoray.poll({
			    'client': worker.w_moray,
			    'options': worker.w_poll_options,
			    'now': now,
			    'log': log,
			    'throttle': state.p_throttle,
			    'bucket': worker.w_buckets[name],
			    'filter': filter,
			    'onrecord': function (record) {
				if (state.record(record))
					worker.onRecord(record);
			    },
			    'ondone': worker.onSearchDone.bind(worker, job)
			});

			state.request(filter, req);
		});
	});
};

/*
 * Invoked to process an incoming Moray record.
 */
Worker.prototype.onRecord = function (record)
{
	var name, error, job;

	name = this.w_names[record['bucket']];

	this.w_log.debug('record: "%s" "%s" etag %s',
	    name, record['key'], record['_etag']);

	/*
	 * If this is a task record from a remote agent, updated our last
	 * contact time for that agent.  We do this before just about anything
	 * else, even if we end up rejecting the record later for some other
	 * reason.
	 */
	if (name == 'task' && record['value']['server']) {
		var server = record['value']['server'];

		if (!this.w_agents.hasOwnProperty(server))
			this.w_agents[server] = new WorkerAgent();

		if (record['_mtime'] > this.w_agents[server].a_last)
			this.w_agents[server].a_last = record['_mtime'];
	}

	error = mod_jsprim.validateJsonObject(
	    mod_schema.sBktJsonSchemas[name], record['value']);
	if (error) {
		this.w_log.warn(error, 'onRecord: validation error', record);
		return;
	}

	if (name == 'job') {
		this.jobUpdate(record);
		return;
	}

	if (!this.w_jobs.hasOwnProperty(record['value']['jobId'])) {
		this.w_log.warn('onRecord: no such jobId', record);
		return;
	}

	job = this.w_jobs[record['value']['jobId']];
	if (job.j_state == 'unassigned' || job.j_state == 'finishing') {
		job.j_log.info('onRecord: ignoring update in job state "%s"',
		    job.j_state, record);
		return;
	}

	/*
	 * While a job is still in the "initializing" state, buffer all incoming
	 * records until all they've all been received.
	 */
	if (job.j_state == 'initializing') {
		job.j_init_records[name][record['key']] = record;
		return;
	}

	mod_assert.ok(job.j_state == 'running');

	if (name == 'taskinput') {
		job.j_log.warn('onRecord: ignoring update in job state "%s"',
		    job.j_state, record);
		return;
	}

	job.j_queued++;

	if (name == 'jobinput')
		this.w_jobinputs_in.push(record);
	else if (name == 'task')
		this.w_tasks_in.push(record);
	else if (name == 'taskoutput')
		this.w_taskoutputs_in.push(record);
};

/*
 * Invoked when a Moray search request completes.  This only affects jobs in the
 * "initializing" state, which need to know when they've loaded all existing
 * state for a job.
 */
Worker.prototype.onSearchDone = function (job, bucket, start, count)
{
	var worker = this;
	var name = this.w_names[bucket];
	var limit = this.w_poll_options['limit'];

	mod_assert.ok(name != 'job');

	if (name == 'jobinput') {
		job.j_input_read = start;

		if (job.j_input_done &&
		    job.j_input_read > job.j_input_done)
			delete (job.j_polls['jobinput']);
	}

	if (job.j_state !== 'initializing')
		return;

	if (start <= job.j_init_start)
		return;

	if (count === limit)
		/*
		 * We don't know we're done reloading the bucket until
		 * we get some number of records less than the limit.
		 */
		return;

	job.j_log.info('read all "%s" records', name);
	delete (job.j_init_waiting[name]);

	if (name == 'taskinput')
		delete (job.j_polls['taskinput']);

	if (mod_jsprim.isEmpty(job.j_init_waiting))
		worker.jobLoad(job);

	this.flush();
};

/*
 * Invoked when we receive a new job record that we don't already own.
 */
Worker.prototype.jobCreate = function (record)
{
	var job;

	job = record['value'];
	mod_assert.ok(!this.w_jobs.hasOwnProperty(job['jobId']));

	if (job['worker'] == this.w_uuid) {
		this.w_log.info('resuming our own job: %s', job['jobId']);
		this.w_stats['asgn_restart']++;
	}

	this.w_jobs[job['jobId']] = new WorkerJobState({
	    'conf': this.w_conf,
	    'log': this.w_log.child({ 'component': 'job-' + job['jobId'] }),
	    'record': record
	});

	this.jobAssign(this.w_jobs[job['jobId']]);
};

/*
 * Process an incoming Moray record for a job.
 */
Worker.prototype.jobUpdate = function (record)
{
	var jobid, job;

	jobid = record['value']['jobId'];

	if (!this.w_jobs.hasOwnProperty(jobid)) {
		if (!record['value']['timeCancelled']) {
			this.jobCreate(record);
		} else if (!record['value']['timeDone']) {
			record['value']['state'] = 'done';
			record['value']['timeDone'] =
			    mod_jsprim.iso8601(Date.now());
			this.w_queue.dirty('job', record['key'],
			    record['value']);
		}

		return;
	}

	/*
	 * We don't want to clobber job.j_job because we may have various pieces
	 * of dirty state in our local copy.  Fortunately, the only state
	 * updates we can receive from elsewhere are that the job was cancelled
	 * or that its input was completed.  We check for these specific cases
	 * and update both our internal state as well as our copy of the full
	 * job record to make sure we don't clobber these changes on the next
	 * save.
	 */
	job = this.w_jobs[jobid];

	if (job.j_etag == record['_etag'])
		return;

	job.j_etag = record['_etag'];

	if (job.j_state != 'unassigned' &&
	    record['value']['worker'] != this.w_uuid) {
		/* We've lost the lock! Drop the job immediately. */
		job.j_log.error('job lock lost (now assigned to %j)',
		    record['value']['worker']);
		this.jobCleanup(job);
		this.jobRemove(job);
		return;
	}

	if (job.j_cancelled) {
		job.j_log.warn('ignoring update (job cancelled');
		return;
	}

	if (record['value']['timeCancelled']) {
		job.j_cancelled = record['value']['timeCancelled'];
		job.j_job['timeCancelled'] = job.j_cancelled;
		job.j_log.info('job cancelled');

		if (!record['value']['timeDone']) {
			record['value']['state'] = 'done';
			record['value']['timeDone'] =
			    mod_jsprim.iso8601(Date.now());
			this.w_queue.dirty('job', job.j_id, record['value']);
		}

		this.jobCleanup(job);
		this.jobRemove(job);
		return;
	}

	if (job.j_input_done === undefined &&
	    record['value']['timeInputDone']) {
		job.j_input_done = Date.parse(record['value']['timeInputDone']);
		job.j_job['timeInputDone'] = record['value']['timeInputDone'];
		job.j_log.info('job input completed');

		if (job.j_input_read && job.j_input_read >= job.j_input_done)
			delete (job.j_polls['jobinput']);

		this.jobPropagateEnd(job, null);
		this.jobTick(job);
		return;
	}
};

Worker.prototype.jobCleanup = function (job)
{
	var pi, phase, ri, reducer;

	for (pi = 0; pi < job.j_phases.length; pi++) {
		phase = job.j_phases[pi];

		if (!phase.p_reducers)
			continue;

		for (ri = 0; ri < phase.p_reducers.length; ri++) {
			reducer = phase.p_reducers[ri];
			this.reducerFiniKeyFile(reducer, job);
		}
	}
};

/*
 * Invoked to completely remove a job from this worker.
 */
Worker.prototype.jobRemove = function (job)
{
	var npending, taskid, task, agent;

	if (job.j_state == 'unassigned')
		this.w_stats['asgn_failed']++;

	job.j_dropped = new Date();
	delete (this.w_jobs[job.j_id]);
	job.j_log.info('job removed');

	if (job.j_save_throttle.ongoing())
		job.j_log.info('job removed with pending save operation');

	if (job.j_auths)
		job.j_log.info('job removed with %d pending auth requests',
		    job.j_auths);

	if (job.j_locates)
		job.j_log.info('job removed with %d pending locate requests',
		    job.j_locates);

	if (job.j_queued)
		job.j_log.info('job removed with %d pending incoming records',
		    job.j_queued);

	npending = 0;
	for (taskid in job.j_tasks) {
		if (this.w_queue.pending('task', taskid))
			npending++;

		task = this.w_tasks[taskid];
		delete (this.w_tasks[taskid]);

		if (!task.t_record)
			continue;

		agent = this.w_agents[task.t_record['value']['server']];
		if (agent) {
			delete (agent.a_tasks[taskid]);
			if (mod_jsprim.isEmpty(agent.a_tasks))
				agent.a_dispatch = 0;
		}
	}

	if (npending > 0)
		job.j_log.info('job removed with %d pending task saves',
		    npending);
};

/*
 * Common function to transition from state S1 to state S2.
 */
Worker.prototype.jobTransition = function (job, s1, s2)
{
	if (s1 !== undefined)
		mod_assert.equal(job.j_state, s1);

	job.j_log.info('transitioning states from %s to %s', s1, s2);
	job.j_state = s2;
	job.j_state_time = new Date();

	this.jobTick(job);
};

/*
 * Invoked periodically while processing the job to check for expired timeouts.
 */
Worker.prototype.jobTick = function (job)
{
	mod_assert.ok(job.j_dropped === undefined);

	if (job.j_state == 'unassigned')
		return;

	/*
	 * If it's been too long since the last job save, mark the job dirty.
	 * After that, if the job is dirty and we're not already trying to save
	 * it, save it now.
	 */
	if (!job.j_save_throttle.tooRecent() && !job.j_save.dirty())
		job.j_save.markDirty();

	if (!job.j_save_throttle.ongoing() && job.j_save.dirty())
		this.jobSave(job);

	if (job.j_state == 'initializing' || job.j_state == 'finishing')
		/* We'll get kicked out of this state asynchronously. */
		return;

	mod_assert.equal(job.j_state, 'running');

	if (this.jobDone(job)) {
		job.j_save.markDirty();
		job.j_job['timeDone'] = mod_jsprim.iso8601(new Date());
		job.j_job['state'] = 'done';

		delete (job.j_polls['taskoutput']);
		delete (job.j_polls['task']);
		mod_assert.ok(mod_jsprim.isEmpty(job.j_polls));

		this.jobTransition(job, 'running', 'finishing');
	}
};

/*
 * Attempt to move this job from "unassigned" state to "initializing" by
 * updating the Moray job record to have job['worker'] == our uuid.
 */
Worker.prototype.jobAssign = function (job)
{
	mod_assert.equal(job.j_state, 'unassigned');

	if (job.j_job['worker']) {
		job.j_log.info('attempting to steal job from %s',
		    job.j_job['worker']);
	} else {
		job.j_log.info('attempting to take unassigned job');
	}

	job.j_job['state'] = 'running';
	job.j_job['worker'] = this.w_uuid;
	job.j_save.markDirty();
	this.jobSave(job);
};

Worker.prototype.jobSave = function (job)
{
	var worker = this;

	mod_assert.ok(!job.j_save_throttle.ongoing());
	job.j_save_throttle.start();
	job.j_log.debug('saving job (%j)', job.j_save);
	job.j_save.saveStart();

	this.w_moray.putObject(this.w_buckets['job'], job.j_id,
	    job.j_job, { 'etag': job.j_etag }, function (err) {
		if (err) {
			/*
			 * The most likely failures here are transient failures
			 * to connect to Moray and conflict errors resulting
			 * from concurrent modifications by muskie.  In both
			 * cases, we simply retry again.  The conflict case
			 * should be resolved when we see the updated job
			 * record from Moray and update our own copy and etag.
			 */
			job.j_save.saveFailed();
			job.j_log.warn(err, 'failed to save job');
		} else {
			job.j_save.saveOk();
			job.j_log.info('saved job (%j)', job.j_save);
		}

		job.j_save_throttle.done();

		if (job.j_state == 'unassigned' && !job.j_dropped) {
			if (err) {
				job.j_log.warn('failed to assign job');
				worker.jobRemove(job);
			} else {
				job.j_log.info('successfully assigned job');
				job.j_init_start = Date.now();
				worker.jobTransition(
				    job, 'unassigned', 'initializing');
			}
		}

		if (job.j_state == 'finishing' && !job.j_save.dirty())
			worker.jobRemove(job);
	});
};

/*
 * Invoked when we've finished loading all existing state on this job.
 */
Worker.prototype.jobLoad = function (job)
{
	mod_assert.ok(mod_jsprim.isEmpty(job.j_init_waiting));
	mod_assert.equal(job.j_state, 'initializing');

	/*
	 * We essentially replay the job from the beginning and see what work
	 * has already been done.  The first thing we need is an index of input
	 * records by phase and then key.
	 */
	var worker = this;
	var inputs = job.j_phases.map(function () { return ({}); });
	var taskoutputs = {};
	var addinput = function (pi, record) {
		if (!inputs[pi][record['value']['key']])
			inputs[pi][record['value']['key']] = [];
		inputs[pi][record['value']['key']].push(record);
	};

	mod_jsprim.forEachKey(job.j_init_records['task'],
	    function (taskid, record) {
		var pi = record['value']['phaseNum'];

		if (pi >= job.j_phases.length) {
			job.j_log.debug('reload: dropping record ' +
			    '(invalid phase)', record);
			delete (job.j_init_records['task'][taskid]);
			return;
		}

		if (job.j_job['phases'][pi]['type'] == 'reduce')
			return;

		addinput(pi, record);
	    });

	mod_jsprim.forEachKey(job.j_init_records['taskinput'],
	    function (_, record) {
		var taskid = record['value']['taskId'];
		var task = job.j_init_records['task'][taskid];

		if (!task) {
			job.j_log.debug('reload: dropping record ' +
			    '(invalid task)', record);
			return;
		}

		var pi = task['value']['phaseNum'];
		if (job.j_job['phases'][pi]['type'] != 'reduce') {
			job.j_log.debug('reload: dropping record ' +
			    '(not reduce phase)', record);
			return;
		}

		addinput(pi, record);
	    });

	mod_jsprim.forEachKey(job.j_init_records['taskoutput'],
	    function (_, record) {
		var taskid = record['value']['taskId'];
		var task = job.j_init_records['task'][taskid];

		if (!task) {
			job.j_log.debug('reload: dropping record ' +
			    '(invalid task)', record);
			return;
		}

		if (!taskoutputs[taskid])
			taskoutputs[taskid] = [];
		taskoutputs[taskid].push(record['value']);
	    });

	/*
	 * Now examine each jobinput record and look for matching task records
	 * in phase 0.  If there are any, drop the jobinput record because it's
	 * already been processed.
	 */
	mod_jsprim.forEachKey(job.j_init_records['jobinput'],
	    function (_, record) {
		var key = record['value']['key'];

		if (!inputs[0][key]) {
			job.j_log.debug('reload: replaying jobinput "%s"', key);
			job.j_queued++;
			worker.w_jobinputs_in.push(record);
			return;
		}

		var next = inputs[0][key].shift();
		job.j_log.debug('reload: skipping jobinput "%s" (task %s)',
		    key, next['value']['taskId']);
		if (inputs[0][key].length === 0)
			delete (inputs[0][key]);
	    });

	/*
	 * Similarly, examine each task to see what needs to be replayed.
	 */
	mod_jsprim.forEachKey(job.j_init_records['task'],
	    function (taskid, record) {
		var state = record['value']['state'];
		var pi = record['value']['phaseNum'];
		var task, nout;

		/*
		 * We maintain task objects for all tasks, completed or
		 * otherwise.  If the task was aborted or cancelled, there's
		 * nothing else to do.
		 */
		task = new WorkerJobTask(job.j_id, pi, taskid);
		job.j_tasks[taskid] = true;
		worker.w_tasks[taskid] = task;

		task.t_record = record;
		task.t_value = record['value'];

		if (taskoutputs[taskid])
			task.t_xoutputs = taskoutputs[taskid];

		nout = task.t_xoutputs.length +
		    (task.t_value['firstOutputs'] ?
		    task.t_value['firstOutputs'].length : 0);

		if (state == 'dispatched' || state == 'running' ||
		    (state == 'done' && nout < task.t_value['nOutputs'])) {
			/* XXX may require updating timeInputDone */
			job.j_phases[pi].p_npending++;
			job.j_log.debug('reload: task "%s": no action ' +
			    'required (task still running)', taskid);
			return;
		}

		if (state == 'cancelled' ||
		    (state == 'aborted' && record['value']['timeCommitted'])) {
			job.j_log.debug('reload: task "%s": no action ' +
			    'required (state "%s")', taskid, state);
			return;
		}

		if (state == 'aborted') {
			job.j_log.debug('reload: task "%s": replaying ' +
			    '(aborted)', taskid);
			job.j_queued++;
			worker.w_tasks_in.push(record);
			return;
		}

		mod_assert.equal(state, 'done');
		if (!record['value']['timeCommitted']) {
			/*
			 * XXX This could cause us to emit duplicate inputs for
			 * the next phase if we already saved some previously.
			 */
			job.j_log.debug('reload: task "%s": replaying ' +
			    '(not commited)', taskid);
			job.j_queued++;
			worker.w_tasks_in.push(record);
			return;
		}

		if (pi == job.j_phases.length - 1) {
			job.j_log.debug('reload: task "%s": no action ' +
			    'required (last phase, done, and committed)',
			    taskid);
			return;
		}

		/*
		 * XXX We should really be checking whether there was a next-
		 * phase input record for all of the outputs.  Otherwise we
		 * could miss some if we crashed at the wrong time.
		 */
		job.j_log.debug('reload: task "%s": done and committed ' +
		    '(assuming all output propagated)', taskid);
	    });

	this.j_init_records = undefined;
	job.j_log.debug('reload: complete');

	this.jobTransition(job, 'initializing', 'running');
};

/*
 * Returns true iff the job is complete.
 */
Worker.prototype.jobDone = function (job)
{
	var pi;

	if (job.j_state != 'running' && job.j_state != 'finishing')
		return (false);

	if (!job.j_input_done || !job.j_input_read ||
	    job.j_input_done > job.j_input_read)
		return (false);

	if (job.j_locates > 0 || job.j_queued > 0 || job.j_auths > 0)
		return (false);

	for (pi = 0; pi < job.j_phases.length; pi++) {
		if (job.j_phases[pi].p_npending > 0)
			return (false);
	}

	return (true);
};

/*
 * Most of the work done by this service goes through one of several queues
 * documented in Worker above.  This function is invoked periodically to
 * process work on each of these queues.
 */
Worker.prototype.processQueues = function ()
{
	var now, ent, key, loc, lreq, auth, areq, i, task, len, job;
	var err, dispatch;
	var changedtasks = {};
	var queries = [];

	now = mod_jsprim.iso8601(new Date());

	while (this.w_jobinputs_in.length > 0) {
		ent = this.w_jobinputs_in.shift();
		job = this.w_jobs[ent['value']['jobId']];

		if (!job) {
			this.w_log.warn('record references unknown job', ent);
			continue;
		}

		job.j_queued--;
		key = ent['value']['key'];
		this.keyResolveUser(key, ent);
	}

	while (this.w_auths_in.length > 0) {
		auth = this.w_auths_in.shift();
		areq = this.w_auths[auth.ar_key];
		mod_assert.ok(areq);

		for (i = 0; i < areq.aq_origins.length; i++) {
			ent = areq.aq_origins[i];
			job = this.w_jobs[ent.o_record['value']['jobId']];

			if (!job) {
				this.w_log.warn('record references unknown job',
				    ent.o_record);
				continue;
			}

			job.j_auths--;

			job.j_log.debug('resolve "%s"', auth.ar_key, auth);

			err = auth.ar_error;
			if (!err && auth.ar_account != job.j_job['owner']) {
				err = {
				    'code': 'EJ_ACCES',
				    'message': 'permission denied: "' +
					auth.ar_key + '"'
				};
			}

			if (!err) {
				this.keyLocate(
				    pathSwapFirst(auth.ar_key, auth.ar_account),
				    auth.ar_login, ent);
				continue;
			}

			dispatch = {
			    'd_job': job,
			    'd_pi': undefined,
			    'd_p0key': undefined,
			    'd_origin': ent.o_record,
			    'd_ridx': ent.o_ridx,
			    'd_key': auth.ar_key,
			    'd_account': null,
			    'd_locate': null,
			    'd_time': now,
			    'd_error': err
			};

			this.keyDispatch(dispatch);
		}

		this.w_pending_auths--;
		delete (this.w_auths[auth.ar_key]);
	}

	while (this.w_locates_in.length > 0) {
		loc = this.w_locates_in.shift();
		lreq = this.w_locates[loc.lr_key];
		mod_assert.ok(lreq);

		for (i = 0; i < lreq.l_origins.length; i++) {
			ent = lreq.l_origins[i];
			job = this.w_jobs[ent.o_record['value']['jobId']];

			if (!job) {
				this.w_log.warn('record references unknown job',
				    ent.o_record);
				continue;
			}

			job.j_locates--;

			dispatch = {
			    'd_job': job,
			    'd_pi': undefined,
			    'd_p0key': undefined,
			    'd_origin': ent.o_record,
			    'd_ri': ent.o_ridx,
			    'd_key': pathSwapFirst(lreq.l_key, lreq.l_login),
			    'd_account': mod_mautil.pathExtractFirst(
				lreq.l_key),
			    'd_locate': loc,
			    'd_time': now,
			    'd_error': undefined
			};

			this.keyDispatch(dispatch);
		}

		this.w_pending_locates--;
		delete (this.w_locates[loc.lr_key]);
	}

	while (this.w_tasks_in.length > 0) {
		ent = this.w_tasks_in.shift();
		task = this.w_tasks[ent['value']['taskId']];
		job = this.w_jobs[ent['value']['jobId']];

		if (!job) {
			this.w_log.warn('record references unknown job', ent);
			continue;
		}

		job.j_queued--;

		if (!task) {
			this.w_log.warn('record references unknown task', ent);
			continue;
		}

		if (task.t_record && task.t_record['_etag'] == ent['_etag'])
			continue;

		if (task.t_value['timeCommitted'])
			continue;

		/*
		 * If we've got a pending write for this task already, then
		 * we've either timed it out, cancelled it, or we've got a
		 * pending update to timeInputDone.  In the first two cases, we
		 * can ignore the external update because we've already given up
		 * on this task.  In the last case, the only relevant external
		 * update is a state change to "aborted", in which case we
		 * want to clobber our own change.
		 */
		if (this.w_queue.pending('task', task.t_id) &&
		    task.t_value['state'] == 'cancelled') {
			job.j_log.warn('task "%s": ignoring update because ' +
			    'a clobbering write is already pending',
			    task.t_id, ent);
			continue;
		}

		if (!ent['value']['timeInputDone'] &&
		    task.t_value['timeInputDone'])
			ent['value']['timeInputDone'] =
			    task.t_value['timeInputDone'];

		task.t_record = ent;
		task.t_value = ent['value'];
		changedtasks[task.t_id] = true;

		/*
		 * If there is a non-clobbering write pending, we must update
		 * the etag being used in the write.
		 */
		if (this.w_queue.pending('task', task.t_id)) {
			job.j_log.debug('task "%s": updating etag in cache',
			    task.t_id);
			this.w_queue.dirty('task', task.t_id, task.t_value,
			    { 'etag': ent['_etag'] });
		}
	}

	while (this.w_taskoutputs_in.length > 0) {
		ent = this.w_taskoutputs_in.shift();
		task = this.w_tasks[ent['value']['taskId']];
		job = this.w_jobs[ent['value']['jobId']];

		if (!job) {
			this.w_log.warn('record references unknown job', ent);
			continue;
		}

		job.j_queued--;

		if (!task) {
			this.w_log.warn(
			    'taskoutput record references unknown task', ent);
			continue;
		}

		/* See above. */
		if (this.w_queue.pending('task', task.t_id)) {
			job.j_log.warn('task "%s": ignoring new taskoutput ' +
			    'because a task write is already pending',
			    task.t_id, ent);
			continue;
		}

		task.t_xoutputs.push(ent['value']);
		changedtasks[task.t_id] = true;
	}

	for (key in changedtasks) {
		task = this.w_tasks[key];
		job = this.w_jobs[task.t_value['jobId']];
		len = task.t_xoutputs.length +
		    (task.t_value['firstOutputs'] ?
		    task.t_value['firstOutputs'].length : 0);

		/*
		 * Wait until the task is finished running and, if successful,
		 * we've collected all of its output.  Ignore cancelled tasks.
		 */
		if (task.t_value['state'] == 'running' ||
		    task.t_value['state'] == 'dispatched' ||
		    task.t_value['state'] == 'cancelled')
			continue;

		if (task.t_value['state'] == 'done' &&
		    task.t_value['nOutputs'] > len)
			continue;

		/*
		 * For now, we commit the result in both the "aborted" and
		 * "done" cases.  Muskie should report errors from committed,
		 * aborted tasks as well as output keys from committed, done
		 * tasks in the final phase.  When we later add retries, we
		 * won't necessarily commit here if the task is aborted.
		 */
		mod_assert.ok(task.t_value['state'] == 'aborted' ||
		    (task.t_value['state'] == 'done' &&
		    task.t_value['nOutputs'] <= len),
		    'task ' + task.t_id + ' is invalid');

		if (task.t_value['nOutputs'] < len)
			job.j_log.warn(
			    'task "%s" emitted extra output keys', task.t_id);

		this.taskCommit(task, now);
	}

	if (this.w_pending_locates < this.w_max_pending_locates &&
	    this.w_locates_out.length > 0) {
		queries = this.w_locates_out.splice(0,
		    this.w_max_pending_locates - this.w_pending_locates);
		this.w_pending_locates += queries.length;
		this.w_locator.locate(queries,
		    this.onLocate.bind(this, queries));
	}

	while (this.w_pending_auths < this.w_max_pending_auths &&
	    this.w_auths_out.length > 0) {
		key = this.w_auths_out.shift();
		this.w_pending_auths++;
		this.doResolve(key);
	}

	while (this.w_pending_deletes < this.w_max_pending_deletes &&
	    this.w_deletes_out.length > 0) {
		key = this.w_deletes_out.shift();
		this.doDelete(key, now);
	}

	/*
	 * By this point, we should have processed all incoming records, and
	 * either the output queues should be empty or we've reached internal
	 * limits.
	 */
	mod_assert.equal(this.w_jobinputs_in.length, 0);
	mod_assert.equal(this.w_locates_in.length, 0);
	mod_assert.equal(this.w_tasks_in.length, 0);
	mod_assert.equal(this.w_taskoutputs_in.length, 0);
	mod_assert.ok(this.w_locates_out.length === 0 ||
	    this.w_pending_locates === this.w_max_pending_locates);
};

/*
 * Create a new "task" record for phase "pi" of job "job".
 */
Worker.prototype.taskCreate = function (job, pi, now)
{
	mod_assert.ok(pi >= 0 && pi < job.j_phases.length);

	var task = new WorkerJobTask(job.j_id, pi, mod_uuid.v4());
	task.t_value['state'] = 'dispatched';
	task.t_value['timeDispatched'] = now;
	job.j_phases[pi].p_npending++;
	job.j_tasks[task.t_id] = true;
	this.w_tasks[task.t_id] = task;
	return (task);
};

Worker.prototype.taskAssignServer = function (task, server)
{
	mod_assert.equal(task.t_value['server'], server);

	if (!this.w_agents.hasOwnProperty(server))
		this.w_agents[server] = new WorkerAgent();

	if (!this.w_agents[server].a_dispatch)
		this.w_agents[server].a_dispatch = Date.now();

	this.w_agents[server].a_tasks[task.t_id] = true;
};

/*
 * Commit the given task.  If the task succeeded and isn't for the last phase of
 * the job, then propagate the outputs as inputs to the next phase.
 */
Worker.prototype.taskCommit = function (task, now)
{
	var record, job, i;

	record = task.t_value;
	record['timeCommitted'] = now;

	this.w_queue.dirty('task', task.t_id, record);

	job = this.w_jobs[record['jobId']];
	job.j_log.debug('committing task "%s"', task.t_id);
	job.j_phases[record['phaseNum']].p_npending--;

	if (record['server']) {
		var agent = this.w_agents[record['server']];
		delete (agent.a_tasks[task.t_id]);
		if (mod_jsprim.isEmpty(agent.a_tasks))
			agent.a_dispatch = 0;
	}

	if (record['phaseNum'] > 0)
		this.taskCleanup(task, job);

	if (record['result'] != 'ok' ||
	    record['phaseNum'] == job.j_phases.length - 1)
		return;

	/*
	 * If there's no output to propagate, check whether we should mark a
	 * subsequent reduce phase as done.
	 */
	if (record['nOutputs'] === 0)
		this.jobPropagateEnd(job, now);

	for (i = 0; i < record['firstOutputs'].length; i++)
		this.keyResolveUser(record['firstOutputs'][i]['key'],
		    task.t_record, record['firstOutputs'][i]['rIdx']);

	for (i = 0; i < task.t_xoutputs.length; i++)
		this.keyResolveUser(task.t_xoutputs[i]['key'], task.t_record,
		    task.t_xoutputs[i]['rIdx']);
};

Worker.prototype.taskCleanup = function (task, job)
{
	var key = task.t_value['key'];
	var phase, reducer, stream, i;
	var worker = this;

	/*
	 * Map tasks are easy because the key we have to delete is stored inside
	 * the record itself.
	 */
	if (key) {
		if (keyIsAnonymous(key, job.j_id))
			this.w_deletes_out.push(key);

		return;
	}

	phase = job.j_phases[task.t_value['phaseNum']];
	if (!phase) {
		this.w_log.warn('taskCleanup: invalid phase number', task);
		return;
	}

	this.w_log.debug('taskCleanup: deleting keys for reduce task "%s"',
	    task.t_id);

	for (i = 0; i < phase.p_reducers.length; i++) {
		reducer = phase.p_reducers[i];
		if (reducer.r_task && reducer.r_task.t_id == task.t_id)
			break;
	}

	mod_assert.ok(i < phase.p_reducers.length);

	stream = mod_fs.createReadStream(reducer.r_anon_filename);

	stream.on('open', function () {
		worker.reducerFiniKeyFile(reducer, job);
	});

	stream.on('error', function (err) {
		worker.w_log.error(err, 'failed to open "%s" for read',
		    reducer.r_anon_filename);
	});

	mod_carrier.carry(stream, function (filekey) {
		var keyname = filekey.toString('utf8');
		mod_assert.ok(keyIsAnonymous(keyname, job.j_id));
		worker.w_deletes_out.push(keyname);
		/* XXX flow control */
	});
};

Worker.prototype.taskTimedOut = function (task, now)
{
	var job;

	task.t_value['state'] = 'cancelled';

	job = this.w_jobs[task.t_value['jobId']];
	job.j_phases[task.t_value['phaseNum']].p_npending--;

	this.w_queue.dirty('task', task.t_id, task.t_value, {});
};

/*
 * Resolve the username associated with a Manta key.
 */
Worker.prototype.keyResolveUser = function (key, ent, ri)
{
	var job = this.w_jobs[ent['value']['jobId']];
	var type = ent['bucket'] == this.w_buckets['jobinput'] ?
	    'jobinput' : 'taskinput';
	var entry = {
	    'o_record': ent,
	    'o_ridx': ri
	};

	job.j_auths++;

	if (this.w_auths.hasOwnProperty(key)) {
		mod_assert.equal(this.w_auths[key].aq_type, type);
		this.w_auths[key].aq_origins.push(entry);
		job.j_log.debug('resolve "%s": piggy-backing onto existing ' +
		    'request', key);
		return;
	}

	this.w_auths[key] = {
	    'aq_type': type,
	    'aq_origins': [ entry ]
	};

	this.w_auths_out.push(key);
	job.j_log.debug('resolve "%s": new request', key);
};

Worker.prototype.doResolve = function (key)
{
	var areq, worker, component, redis_key;

	areq = this.w_auths[key];
	mod_assert.ok(areq);

	component = mod_mautil.pathExtractFirst(key);
	if (!component) {
		this.w_auths_in.push({
		    'ar_key': key,
		    'ar_error': {
			'code': 'EJ_NOENT',
			'message': 'malformed key: "' + key + '"'
		    }
		});

		return;
	}

	worker = this;
	areq.aq_issued = Date.now();
	redis_key = '/login/' + component;
	this.w_redis.get(redis_key, function (err, data) {
		var auth, record;

		auth = {};
		auth.ar_key = key;

		if (!err) {
			try {
				record = JSON.parse(data);
			} catch (ex) {
				err = ex;
			}
		}

		if (err) {
			auth.ar_error = {
			    'code': 'EJ_INTERNAL',
			    'message': err.message
			};
		} else if (record === null) {
			auth.ar_error = {
			    'code': 'EJ_NOENT',
			    'message': 'no such object: "' + key + '"'
			};
		} else {
			auth.ar_account = record['uuid'];
			auth.ar_login = component;
		}

		worker.w_auths_in.push(auth);
	});
};

Worker.prototype.doDelete = function (key, now)
{
	if (this.w_deletes[key]) {
		this.w_log.debug('delete "%s": already pending', key);
		return;
	}

	var worker = this;
	this.w_deletes[key] = now;
	worker.w_pending_deletes++;
	this.w_manta.unlink(key, function (err) {
		worker.w_pending_deletes--;
		delete (worker.w_deletes[key]);

		if (err) {
			worker.w_log.error(err, 'delete "%s": failed', key);
			return;
		}

		worker.w_log.debug('delete "%s": okay', key);
	});
};

/*
 * Enqueue a request to locate key "key", triggered by record "ent".
 */
Worker.prototype.keyLocate = function (key, login, ent)
{
	var job = this.w_jobs[ent.o_record['value']['jobId']];
	job.j_locates++;

	if (this.w_locates.hasOwnProperty(key)) {
		job.j_log.debug('locate "%s": piggy-backing onto existing ' +
		    'request', key);
		this.w_locates[key].l_origins.push(ent);
		return;
	}

	this.w_locates[key] = {
	    'l_key': key,
	    'l_login': login,
	    'l_origins': [ ent ]
	};

	this.w_locates_out.push(key);
	job.j_log.debug('locate "%s": enqueued new request', key);
};

/*
 * Dispatch a key to its map or reduce phase.  The "dispatch" argument has the
 * following fields:
 *
 *    d_job		Job object.
 *
 *    d_origin		The record that generated this key: either a job input
 *    			record or a task record from a previous phase.
 *
 *    d_ri		The reducer index, if any, for this key.
 *
 *    d_key		The key to be dispatched, as the user sees it (with an
 *    			unresolved account login name).
 *
 *    d_error		If set, an error has already occurred and the task
 *    			should be dispatched just to indicate the error.
 *
 *    d_time		Current time, to become the dispatch time for the task.
 *
 * If d_error is not set, then the following additional fields should be set:
 *
 *    d_account		The account uuid for the key's owner.  The agent needs
 *    			this to locate the key.
 *
 *    d_locate		Location result (see documentation above).
 *
 * This function fills in the following fields before delegating to the map- or
 * reduce-specific dispatch function:
 *
 *    d_pi		Phase number for the task to be dispatched.
 *
 *    d_error		If the key doesn't exist.
 */
Worker.prototype.keyDispatch = function (dispatch)
{
	var job, source;

	job = dispatch.d_job;
	mod_assert.ok(job);

	source = dispatch.d_origin;

	if (source['bucket'] == this.w_buckets['jobinput']) {
		dispatch.d_pi = 0;
	} else {
		mod_assert.equal(source['bucket'], this.w_buckets['task']);
		dispatch.d_pi = source['value']['phaseNum'] + 1;
		mod_assert.ok(dispatch.d_pi < job.j_phases.length);
	}

	if (!dispatch.d_error && dispatch.d_locate.lr_error) {
		var error = dispatch.d_locate.lr_error;

		dispatch.d_error = {
		    'code': error.code,
		    'message': sprintf('%s: "%s"', error.message,
			dispatch.d_key)
		};
	}

	if (job.j_phases[dispatch.d_pi].p_type == 'reduce')
		this.keyDispatchReduce(dispatch);
	else
		this.keyDispatchMap(dispatch);
};

/*
 * Implementation of keyDispatch() for map phases.  This is simple because we
 * always dispatch a new task.
 */
Worker.prototype.keyDispatchMap = function (dispatch)
{
	var task, record, server;

	task = this.taskCreate(dispatch.d_job, dispatch.d_pi, dispatch.d_time);
	record = task.t_value;
	record['key'] = dispatch.d_key;
	record['p0key'] = dispatch.d_pi === 0 ? dispatch.d_key :
	    dispatch.d_origin['value']['p0key'];

	if (dispatch.d_error) {
		record['error'] = dispatch.d_error;
		record['state'] = 'aborted';
		record['timeDone'] = dispatch.d_time;
		record['timeCommitted'] = dispatch.d_time;
		dispatch.d_job.j_phases[dispatch.d_pi].p_npending--;
	} else {
		var loc = dispatch.d_locate;
		server = mod_jsprim.randElt(loc.lr_locations);
		record['account'] = dispatch.d_account;
		record['objectid'] = loc.lr_objectid;
		record['server'] = server.lrl_server;
		record['zonename'] = server.lrl_zonename;
		this.taskAssignServer(task, server.lrl_server);
	}

	this.w_queue.dirty('task', task.t_id, record);
};

/*
 * Implementation of keyDispatch() for reduce phases.  If a task for this phase
 * hasn't yet been dispatched, do so now.  Then write a taskinput record for
 * this key.
 */
Worker.prototype.keyDispatchReduce = function (dispatch)
{
	var job, pi, ri;
	var task, reducer, record, phase;
	var worker = this;

	job = dispatch.d_job;
	pi = dispatch.d_pi;
	mod_assert.equal(job.j_phases[pi].p_type, 'reduce');

	phase = job.j_phases[pi];
	ri = dispatch.d_ri !== undefined ? dispatch.d_ri :
	    Math.floor(Math.random() * phase.p_reducers.length);

	if (ri >= phase.p_reducers.length) {
		if (!dispatch.d_error) {
			dispatch.d_error = {
			    'code': 'EJ_INVAL',
			    'message': 'specified reducer out of range: "' +
				ri + '"'
			};
		}

		ri = 0;
	}

	if (phase.p_reducers[ri].r_task === undefined) {
		reducer = phase.p_reducers[ri];
		task = this.taskCreate(job, pi, dispatch.d_time);
		reducer.r_task = task;

		if (pi > 0)
			this.reducerInitKeyFile(reducer, job);

		record = task.t_value;
		this.w_queue.dirty('task', task.t_id, record);

		if (mod_jsprim.isEmpty(this.w_agents)) {
			record['state'] = 'aborted';
			record['timeDone'] = dispatch.d_time;
			record['timeCommitted'] = dispatch.d_time;
			record['server'] = undefined;
			record['error'] = {
			    'code': 'EJ_NORESOURCES',
			    'message': 'no servers available to run task'
			};

			phase.p_npending--;
		} else {
			record['server'] = mod_jsprim.randElt(
			    Object.keys(this.w_agents));
		}
	}

	reducer = phase.p_reducers[ri];
	task = reducer.r_task;

	if (task.t_state == 'aborted')
		return;

	if (dispatch.d_error) {
		this.w_queue.dirty('taskoutput', mod_uuid.v4(), {
		    'jobId': job.j_id,
		    'taskId': task.t_id,
		    'error': dispatch.d_error,
		    'timeCreated': dispatch.d_time
		});

		worker.jobPropagateEnd(job, dispatch.d_time);

		return;
	}

	reducer.r_ninput++;

	if (pi > 0 && keyIsAnonymous(dispatch.d_key, job.j_id)) {
		if (reducer.r_anon_bufs)
			reducer.r_anon_bufs.push(dispatch.d_key);
		else
			reducer.r_anon_stream.write(dispatch.d_key + '\n');
	}

	this.w_queue.dirty('taskinput', mod_uuid.v4(), {
	    'jobId': job.j_id,
	    'taskId': task.t_id,
	    'key': dispatch.d_key,
	    'account': dispatch.d_account,
	    'objectid': dispatch.d_locate.lr_objectid,
	    'servers': dispatch.d_locate.lr_locations.map(function (l) {
		return ({
		    'server': l.lrl_server,
		    'zonename': l.lrl_zonename
		});
	    })
	});

	/*
	 * We want to do the rest of this only after any remaining keys for this
	 * phase are dispatched.  Those keys must be dispatched on this tick.
	 */
	process.nextTick(function () {
		worker.jobPropagateEnd(job, dispatch.d_time);
	});
};

/*
 * Examines the state of a job's phases to determine whether end-of-input should
 * be propagated.  This is invoked when we discover that a job's input key
 * stream has been ended by the user so that we can propagate that end-of-input
 * to any waiting reduce tasks.  It's also invoked when a task completes without
 * producing output to see if that should trigger the end of a subsequent reduce
 * task.
 */
Worker.prototype.jobPropagateEnd = function (job, now)
{
	var phase, pi, ri;
	var task;

	if (!job.j_input_done)
		return;

	/*
	 * This works by iterating the phases and determining whether there is
	 * or may in the future be more work to do.  If so, bail out, as some
	 * future event will cause us to reevaluate this.  If not, propagate
	 * end-of-input.
	 */
	for (pi = 0; pi < job.j_phases.length; pi++) {
		phase = job.j_phases[pi];

		if (!phase.p_reducers) {
			if (phase.p_npending > 0)
				/*
				 * The map task is still running.  End-of-stream
				 * will be propagated when it finishes.
				 */
				return;

			/*
			 * Skip over map tasks with no pending tasks since
			 * there's no further work to do there.
			 */
			continue;
		}

		/*
		 * See if this reduce task has already been ended.  This can't
		 * happen in the case that we just discovered the job's input
		 * stream has finished, but it could happen if we've been
		 * invoked in response to a map task that completed with no
		 * output, if that map task happened after a previous reduce
		 * task that had already finished.
		 */
		for (ri = 0; ri < phase.p_reducers.length; ri++) {
			task = phase.p_reducers[ri].r_task;
			if (task && task.t_value['timeInputDone'])
				break;
		}

		if (ri === phase.p_reducers.length)
			/*
			 * We've found a reduce phase to which to propagate the
			 * end-of-stream.  We only ever need to propagate this
			 * to one phase, since that phase must still be running
			 * and its completion will trigger another check.
			 */
			break;
	}

	if (pi < job.j_phases.length) {
		if (now === null)
			now = mod_jsprim.iso8601(new Date());
		this.reduceEndInput(job, pi, now);
	}
};

Worker.prototype.reduceEndInput = function (job, i, now)
{
	var worker = this;
	var phase = job.j_phases[i];

	mod_assert.ok(phase.p_reducers);
	phase.p_reducers.forEach(function (reducer) {
		var task = reducer.r_task;

		if (!task)
			return;

		if (reducer.r_task.t_value['timeInputDone'])
			return;

		if (reducer.r_anon_stream &&
		    reducer.r_anon_stream.writable &&
		    !reducer.r_anon_bufs)
			reducer.r_anon_stream.end();

		job.j_log.debug('marking input done for ' +
		    'reduce phase %d (task %s)', i, task.t_id);
		task.t_value['timeInputDone'] = now;
		task.t_value['nInputs'] = reducer.r_ninput;

		if (reducer.r_ninput === 0) {
			phase.p_npending--;
			task.t_value['state'] = 'done';
			task.t_value['result'] = 'ok';
			task.t_value['nOutputs'] = 0;
			task.t_value['timeDone'] = now;
			task.t_value['timeCommitted'] = now;
		}

		var options = {};
		if (task.t_record)
			options['etag'] = task.t_record['_etag'];

		worker.w_queue.dirty('task', task.t_id, task.t_value, options);
	});
};

Worker.prototype.reducerInitKeyFile = function (reducer, job)
{
	var task = reducer.r_task;

	reducer.r_anon_filename = sprintf('%s/%s', this.w_tmproot, task.t_id);
	reducer.r_anon_stream = mod_fs.createWriteStream(
	    reducer.r_anon_filename);

	var onErr = function (err) {
		job.j_log.error(err, 'failed to open "%s"',
		    reducer.r_anon_filename);
	};

	reducer.r_anon_stream.on('error', onErr);

	reducer.r_anon_stream.on('open', function () {
		reducer.r_anon_stream.removeListener('error', onErr);
		reducer.r_anon_stream.on('error', function (err) {
			job.j_log.error(err, 'error on "%s"',
			    reducer.r_anon_filename);
		});

		reducer.r_anon_bufs.forEach(function (key) {
			reducer.r_anon_stream.write(key + '\n');
		});

		if (reducer.r_task.t_value['timeInputDone'])
			reducer.r_anon_stream.end();

		reducer.r_anon_bufs = null;
	});
};

Worker.prototype.reducerFiniKeyFile = function (reducer, job)
{
	if (!reducer.r_anon_filename)
		return;

	job.j_log.info('deleting "%s"', reducer.r_anon_filename);

	mod_fs.unlink(reducer.r_anon_filename, function (err) {
		if (err) {
			job.j_log.warn(err, 'failed to delete "%s"',
			    reducer.r_anon_filename);
		}
	});
};

/*
 * Handle an incoming "locate" response.
 */
Worker.prototype.onLocate = function (keys, err, locations)
{
	var key, i;
	var worker = this;

	if (err)
		this.w_log.warn(err, 'failed to locate some keys');

	for (i = 0; i < keys.length; i++) {
		key = keys[i];

		this.w_log.debug('locate response for "%s"', key);

		if (locations[key]['error']) {
			this.w_log.warn(locations[key]['error'],
			    'error locating key "%s"', key);

			this.w_locates_in.push({
			    'lr_key': key,
			    'lr_objectid': undefined,
			    'lr_error': locations[key]['error'],
			    'lr_locations': []
			});

			continue;
		}

		mod_assert.ok(locations[key].length > 0);

		locations[key].forEach(function (loc) {
			if (worker.w_agents.hasOwnProperty(loc['host']))
				return;

			worker.w_agents[loc['host']] = new WorkerAgent();
		});

		this.w_locates_in.push({
			'lr_key': key,
			'lr_objectid': locations[key][0]['objectid'],
			'lr_error': undefined,
			'lr_locations': locations[key].map(function (l) {
			    return ({
				'lrl_server': l['host'],
				'lrl_zonename': l['zonename']
			    });
			})
		});
	}

	this.flush();
};

/*
 * Return a string constructed by swapping the first component of "path" with
 * "newfirst".  "path" should have previously been normalized, and it should
 * contain at least two components.  This is used to change a key from the
 * "/:account_uuid/path" form to the "/:login/path" form or vice versa.
 */
function pathSwapFirst(path, newfirst)
{
	var i, j;
	i = path.indexOf('/');
	j = path.indexOf('/', i + 1);

	if (i == -1 || j == -1)
		return (null);

	return ('/' + newfirst + path.substr(j));
}

/*
 * Returns true if the given key represents an "anonymous" intermediate object
 * created by Marlin.  These keys can be identified because they're stored
 * under "/:login/jobs/:jobid/stor" rather than "/:login/stor".
 */
function keyIsAnonymous(key, jobid)
{
	var re = new RegExp('^/[^/]+/jobs/' + jobid + '/stor/');
	return (re.test(key));
}
