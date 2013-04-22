/*
 * lib/schema.js: JSON schemas for various Marlin records
 */

/*
 * Primitive schema components
 */

var sBoolean = {
	'type': 'string',
	'enum': [ 'true', 'false' ]
};

/* MANTA-1065 workaround */
var sBooleanWorkaround = {
	'type': 'string',
	'enum': [ 'true', 'false', 'TRUE', 'FALSE' ]
};

var sObject = { 'type': 'object' };

var sString = { 'type': 'string' };

var sStringRequired = {
	'type': 'string',
	'required': true
};

var sStringRequiredNonEmpty = {
	'type': 'string',
	'required': true,
	'minLength': 1
};

var sDateTime = {
	'type': 'string',
	'format': 'date-time'
};

var sDateTimeRequired = {
	'type': 'string',
	'format': 'date-time',
	'required': true
};

var sStringArray = {
	'type': 'array',
	'items': sString
};

var sStringArrayRequired = {
	'type': 'array',
	'required': true,
	'items': sString
};

var sNonNegativeInteger = {
	'type': 'integer',
	'minimum': 0
};

var sNonNegativeIntegerRequired = {
	'type': 'integer',
	'required': true,
	'minimum': 0
};

var sReducerCount = {
	'type': 'integer',
	'minimum': 1,
	'maximum': 128
};

var sReducerIndex = {
	'type': 'integer',
	'minimum': 0,
	'maximum': 127
};

var sTcpPortRequired = {
	'required': true,
	'type': 'integer',
	'minimum': 1,
	'maximum': 65535
};

var sIntervalRequired = {
	'required': true,
	'type': 'integer',
	'minimum': 0
};

var sPercentRequired = {
	'required': true,
	'type': 'integer',
	'minimum': 0,
	'maximum': 100
};

var sMemory = {
	'type': 'number',
	'enum': [ 128, 256, 512, 1024, 2048, 4096, 8192 ]
};

var sDisk = {
	'type': 'number',
	/* BEGIN JSSTYLED */
	'enum': [
	       2,  /* 2 GB */
	       4,
	       8,
	      16,
	      32,
	      64,
	     128,
	     256,
	     512,
	    1024   /* 1TB */
	]
	/* END JSSTYLED */
};

/*
 * Marlin-specific reusable schema components
 */

var sJobPhase = {
	'type': 'object',
	'additionalProperties': false,
	'properties': {
		'type': {
			'type': 'string',
			'enum': [ 'generic', 'storage-map', 'reduce' ]
		},
		'assets': sStringArray,
		'exec': sStringRequiredNonEmpty,
		'init': sString,
		'count': sReducerCount,
		'memory': sMemory,
		'disk': sDisk,
		'uarg': sObject,
		'image': sString
	}
};

var sJobPhases = {
	'type': 'array',
	'required': true,
	'minItems': 1,
	'items': sJobPhase
};


/*
 * User input records
 */

var sHttpJobInput = {
	'type': 'object',
	'additionalProperties': false,
	'properties': {
		'jobName': sStringRequired,
		'phases': sJobPhases
	}
};

var sHttpJobInputPrivileged = {
	'type': 'object',
	'additionalProperties': false,
	'properties': {
		'jobName': sStringRequired,
		'phases': sJobPhases,
		'options': {
			'type': 'object'
		}
	}
};

/*
 * Moray records
 */

var sMorayError = {
	'type': 'object',
	'properties': {
		'errorId': sStringRequiredNonEmpty,
		'jobId': sStringRequiredNonEmpty,
		'phaseNum': sNonNegativeIntegerRequired,

		/* error details */
		'errorCode': sStringRequired,		/* programmatic code */
		'errorMessage': sStringRequired,	/* public message */
		'errorMessageInternal': sString,	/* internal message */

		/* only for dispatch failures or map task failures */
		'input': sString,			/* input object */
		'p0input': sString,			/* phase 0 input */
							/* (if known) */

		/* only for failures during execution */
		'taskId': sString,			/* parent task */
		'server': sString,			/* compute node */
		'mantaComputeId': sString,		/* compute node */
		'machine': sString,			/* lackey  */
		'stderr': sString,			/* stderr object name */
		'core': sString,			/* core object name */

		'prevRecordType': sString,		/* previous record */
		'prevRecordId': sString,		/* (for debugging) */

		/* describes whether the error was retried */
		'retried': sBoolean,
		'timeCommitted': sDateTime
	}
};

var sMorayHealth = {
	'type': 'object',
	'properties': {
		'component': {
		    'type': sStringRequiredNonEmpty,
		    'enum': [ 'agent' ]
		},
		'instance': sStringRequiredNonEmpty,
		'serverUuid': sStringRequiredNonEmpty,
		'generation': sStringRequiredNonEmpty
	}
};

var sJobStates = [ 'queued', 'running', 'done' ];

var sMorayJob = {
	'type': 'object',
	'properties': {
		/* immutable job definition */
		'jobId': sStringRequiredNonEmpty,
		'jobName': sStringRequired,
		'owner': sStringRequiredNonEmpty,
		'authToken': sStringRequiredNonEmpty,
		'phases': sJobPhases,
		'auth': {
			'type': 'object',
			'properties': {
				'uuid': sStringRequiredNonEmpty,
				'login': sStringRequiredNonEmpty,
				'groups': sStringArrayRequired,
				'token': sStringRequiredNonEmpty
			}
		},

		/* internal Marlin state */
		'worker': sString,

		/* public state (mediated by the web tier) */
		'state': {
			'type': 'string',
			'required': true,
			'enum': sJobStates
		},
		'timeCreated': sDateTimeRequired,
		'timeAssigned': sDateTime,
		'timeInputDone': sDateTime,
		'timeInputDoneRead': sDateTime,
		'timeCancelled': sDateTime,
		'timeDone': sDateTime,

		/* internal wrasse state */
		'archiver': sString,
		'timeArchiveStarted': sDateTime,
		'timeArchiveDone': sDateTime,

		'stats': {
			'type': 'object',
			'properties': {
				'nAssigns': sNonNegativeInteger,
				'nErrors': sNonNegativeInteger,
				'nInputsRead': sNonNegativeInteger,
				'nJobOutputs': sNonNegativeInteger,
				'nTasksDispatched': sNonNegativeInteger,
				'nTasksCommittedOk': sNonNegativeInteger,
				'nTasksCommittedFail': sNonNegativeInteger
			}
		}
	}
};

var sMorayJobInput = {
	'type': 'object',
	'properties': {
		'jobId': sStringRequiredNonEmpty,
		'input': sStringRequiredNonEmpty,	/* input object name */

		'timeCreated': sDateTime,
		'timePropagated': sDateTime,
		'nextRecordType': sString,	/* "task" or "taskinput" */
		'nextRecordId': sString		/* subsequent record id */
	}
};

var sMorayMantaStorage = {
	'type': 'object',
	'properties': {
		'server_uuid': sString,
		'zone_uuid': sString,
		'manta_compute_id': sString,
		'manta_storage_id': sString
	}
};

var sMorayTask = {
	'type': 'object',
	'properties': {
		/* immutable definition */
		'taskId': sStringRequiredNonEmpty,
		'jobId': sStringRequiredNonEmpty,
		'phaseNum': sNonNegativeIntegerRequired,
		'mantaComputeId': sStringRequiredNonEmpty,
		'agentGeneration': sStringRequiredNonEmpty,
		'nattempts': sNonNegativeInteger,

		/* map tasks only */
		'input': sString,		/* input object name */
		'p0input': sString,		/* phase 0 input object name */
		'zonename': sString,		/* mako zone with the object */
		'account': sString,		/* object's owner's uuid */
		'objectid': sString,		/* object's internal objectid */
		'prevRecordType': sString,	/* previous record */
		'prevRecordId': sString,	/* (for debugging) */

		/* reduce tasks only */
		'nInputs': sNonNegativeInteger,	/* total # of inputs */
		'rIdx': sNonNegativeInteger,	/* reducer index */

		/* current state */
		'state': {
			'type': 'string',
			'required': true,
			'enum': [
			    'dispatched',	/* awaiting agent pick-up */
			    'accepted',		/* awaiting execution */
			    'done'		/* finished or aborted */
			]
		},

		'machine': sString,		/* lackey (for debugging) */
		'nOutputs': sNonNegativeInteger,

		/* reduce only -- see note on timestamps below */
		'timeInputDone': sDateTime,	/* time input finished */
		'timeInputDoneRead': sDateTime,	/* agent saw timeInputDone */

		'result':  {
			'type': 'string',
			'enum': [ 'ok', 'fail' ]
		},

		/*
		 * The following timestamps are used as *booleans* (i.e. set or
		 * not).  We use a timestamp value rather than a proper boolean
		 * just to provide more debugging information.  The actual
		 * values should not be used programmatically for correctness
		 * because of clock drift.
		 *
		 * Note that not all tasks go through all of these steps.
		 *
		 * All tasks are dispatched: this is the time when the worker
		 * initially wrote out the record.
		 */
		'timeDispatched': sDateTimeRequired,	/* time created */

		/*
		 * The agent records when it accepts the task, which just
		 * indicates that it has read it.  Cancelled and aborted tasks
		 * may never become accepted.
		 */
		'timeAccepted': sDateTime,	/* time agent picked up */

		/*
		 * After executing the task, the agent records the time that
		 * execution started and finished.  Cancelled and aborted tasks
		 * may never be started/done.
		 */
		'timeStarted': sDateTime,	/* time execution started */
		'timeDone': sDateTime,		/* time execution ended */

		/*
		 * After the task becomes "done", the worker "commits" it,
		 * meaning that it has either processed the results of the task
		 * already or at least marked them to be processed.  This
		 * process itself involves several state changes on each
		 * "taskoutput" record.  It's described in lib/worker/worker.js.
		 */
		'timeCommitted': sDateTime,	/* time worker committed */

		/*
		 * There are two cases where task processing may stop without
		 * going through the above path: tasks may be taken over by the
		 * worker with an error, or they may be cancelled.
		 *
		 * The worker takes over a task when it gives up on the agent
		 * responsible for executing the task.  This happens if the
		 * agent fails to heartbeat for too long, and also happens today
		 * if the agent restarts because we don't support resuming
		 * accepted tasks after an agent restart.  When taking over a
		 * task, the worker updates all the fields of the task that the
		 * agent would except those pertaining to the actual execution
		 * of the task.  It also sets timeAbandoned.
		 */
		'timeAbandoned': sDateTime,	/* time worker abandoned */

		/*
		 *
		 * Cancellation occurs when the job itself is cancelled.  The
		 * worker cancels all non-done tasks.  Agents operating on these
		 * tasks stop execution immediately.
		 */
		'timeCancelled': sDateTime,	/* time task was cancelled */

		/*
		 * Tasks may be retried by the worker.  wantRetry is set when
		 * the task is committed, and a separate lap will read such
		 * records and set timeRetried when the retry task is
		 * dispatched.
		 */
		'wantRetry': sBooleanWorkaround,
		'timeRetried': sDateTime,

		/*
		 * To handle cleaning up intermediate data, once a map task is
		 * completed, if the input identifies an intermediate object
		 * created by this job, we set wantInputRemoved to true.  We set
		 * timeInputRemoved once we've actually removed it.
		 */
		'wantInputRemoved': sBooleanWorkaround,
		'timeInputRemoved': sDateTime
	}
};

var sMorayTaskInput = {
	'type': 'object',
	'properties': {
		'taskInputId': sStringRequiredNonEmpty,
		'jobId': sStringRequiredNonEmpty,
		'taskId': sStringRequiredNonEmpty,
		'mantaComputeId': sStringRequiredNonEmpty, /* assigned cn */
		'agentGeneration': sStringRequiredNonEmpty,
		'input': sStringRequiredNonEmpty,	/* input object name */
		'p0input': sString,			/* p0 object, if any */
		'account': sStringRequiredNonEmpty,	/* object's owner */
		'objectid': sDateTimeRequired,		/* object uuid */
		'servers': {				/* object's locations */
			'required': true,
			'type': 'array',
			'items': {
				'type': 'object',
				'properties': {
					'mantaComputeId':
						sStringRequiredNonEmpty,
					'zonename': sStringRequiredNonEmpty
				}
			}
		},

		'timeDispatched': sDateTimeRequired,	/* time created */
		'timeRead': sDateTime,			/* time read by agent */

		'prevRecordType': sString,		/* previous record */
		'prevRecordId': sString,		/* (for debugging) */

		/*
		 * Retry information: these fields are set when the
		 * corresponding task has been retried so that we can redispatch
		 * this taskinput.
		 */
		'retryTaskId': sString,
		'retryMantaComputeId': sString,
		'retryAgentGeneration': sString,

		/*
		 * timeRetried is set when we actually do dispatch the retry.
		 */
		'timeRetried': sDateTime,

		/*
		 * To handle cleaning up intermediate data, once a reduce task
		 * is completed, wantInputRemoved is set on the input objects.
		 * The worker scans for these, and if the input identifies an
		 * intermediate object created by this job, we remove it and set
		 * timeInputRemoved.
		 */
		'wantInputRemoved': sBooleanWorkaround,
		'timeInputRemoved': sDateTime
	}
};

var sMorayTaskOutput = {
	'type': 'object',
	'properties': {
		'jobId': sStringRequiredNonEmpty,
		'taskId': sStringRequiredNonEmpty,
		'phaseNum': sNonNegativeIntegerRequired,
		'output': sStringRequiredNonEmpty,	/* output object name */
		'rIdx': sReducerIndex,			/* assigned reducer */

		/*
		 * "valid" indicates whether this taskoutput should be
		 * considered for propagation or as a last-phase output object.
		 * This corresponds with whether the task that generated this
		 * output completed successfully.
		 */
		'valid': sBoolean,

		'timeCreated': sDateTimeRequired,	/* time created */
		'timeCommitted': sDateTime,		/* time committed */
		'timePropagated': sDateTime,		/* time propagated */

		/*
		 * Task outputs may result in subsequent tasks or taskinputs
		 * dispatched.  We include references here for debugging and for
		 * determining whether they've been written out.
		 */
		'nextRecordType': sString,	/* "task" or "taskinput" */
		'nextRecordId': sString		/* subsequent record id */
	}
};

var sBktJsonSchemas =  {
    'error': sMorayError,
    'health': sMorayHealth,
    'job': sMorayJob,
    'jobinput': sMorayJobInput,
    'storage': sMorayMantaStorage,
    'task': sMorayTask,
    'taskinput': sMorayTaskInput,
    'taskoutput': sMorayTaskOutput
};


/*
 * Moray bucket schemas.  These schemas only tell Moray what indices to create.
 * Objects in these buckets must conform to the more constrained schemas above.
 *
 * Any changes to these schemas, even backwards-compatible ones, require
 * updating the version number.  On startup, each component (workers and muskie)
 * tries to create or update schemas for the buckets that it uses, so the
 * version number is required to ensure that components with older versions of
 * the schema don't inadvertently replace newer versions (installed by newer
 * components) when they restart.
 *
 * Backwards-incompatible changes are more complicated: the impact on existing
 * deployments should be carefully assessed before making such a change.
 */
var sBktConfigs = {};

/*
 * IMPORTANT: see the note above about schema versioning before making ANY
 * changes to these schemas.
 */
sBktConfigs['error'] = {
    'options': {
	'version': 2
    },
    'index': {
	'errorId':		{ 'type': 'string' },
	'jobId':		{ 'type': 'string' },
	'phaseNum':		{ 'type': 'string' },
	'timeCommitted':	{ 'type': 'string' },
	'retried':		{ 'type': 'string' },

	/* indexes for debugging only */
	'errorCode': 		{ 'type': 'string' },
	'taskId':		{ 'type': 'string' },
	'server': 		{ 'type': 'string' },
	'mantaComputeId':	{ 'type': 'string' },
	'machine': 		{ 'type': 'string' }
    }
};

/*
 * IMPORTANT: see the note above about schema versioning before making ANY
 * changes to these schemas.
 */
sBktConfigs['health'] = {
    'options': {
	'version': 1
    },
    'index': {
    	'component':	{ 'type': 'string' },

	/* indexes for debugging only */
    	'instance':	{ 'type': 'string' },
	'generation':	{ 'type': 'string' }
    }
};

/*
 * IMPORTANT: see the note above about schema versioning before making ANY
 * changes to these schemas.
 */
sBktConfigs['job'] = {
    'options': {
	'version': 1
    },
    'index': {
	'jobId':		{ 'type': 'string', 'unique': true },
	'jobName':		{ 'type': 'string' },
	'owner':		{ 'type': 'string' },
	'state':		{ 'type': 'string' },
	'worker':		{ 'type': 'string' },
	'timeCancelled':	{ 'type': 'string' },
	'timeInputDone':	{ 'type': 'string' },
	'timeInputDoneRead':	{ 'type': 'string' },
	'timeDone':		{ 'type': 'string' }
    }
};

/*
 * IMPORTANT: see the note above about schema versioning before making ANY
 * changes to these schemas.
 */
sBktConfigs['jobinput'] = {
    'options': {
	'version': 1
    },
    'index': {
	'jobId':		{ 'type': 'string' },
	'input':		{ 'type': 'string' },
	'timePropagated':	{ 'type': 'string' }
    }
};

/*
 * IMPORTANT: see the note above about schema versioning before making ANY
 * changes to these schemas.
 */
sBktConfigs['taskinput'] = {
    'options': {
	'version': 3
    },
    'index': {
	'taskInputId':			{ 'type': 'string', 'unique': true },
	'jobId':			{ 'type': 'string' },
	'taskId':			{ 'type': 'string' },
	'mantaComputeId':		{ 'type': 'string' },
	'agentGeneration':		{ 'type': 'string' },
	'timeRead':			{ 'type': 'string' },
	'retryTaskId':			{ 'type': 'string' },
	'retryMantaComputeId':		{ 'type': 'string' },
	'retryAgentGeneration':		{ 'type': 'string' },
	'wantInputRemoved':		{ 'type': 'string' },
	'timeInputRemoved':		{ 'type': 'string' }
    }
};

/*
 * IMPORTANT: see the note above about schema versioning before making ANY
 * changes to these schemas.
 *
 * This bucket is created by the manta frontend.
 */
sBktConfigs['storage'] = {
    'nocreate': true
};

/*
 * IMPORTANT: see the note above about schema versioning before making ANY
 * changes to these schemas.
 */
sBktConfigs['taskoutput'] = {
    'options': {
	'version': 1
    },
    'index': {
	'jobId':		{ 'type': 'string' },
	'taskId':		{ 'type': 'string' },
	'phaseNum':		{ 'type': 'number' },
	'rIdx':			{ 'type': 'number' },
	'timeCommitted':	{ 'type': 'string' },
	'timePropagated':	{ 'type': 'string' },
	'valid':		{ 'type': 'string' }
    }
};

/*
 * IMPORTANT: see the note above about schema versioning before making ANY
 * changes to these schemas.
 */
sBktConfigs['task'] = {
    'options': {
	'version': 3
    },
    'index': {
	'taskId':		{ 'type': 'string', 'unique': true },
	'jobId':		{ 'type': 'string' },
	'phaseNum':		{ 'type': 'number' },
	'mantaComputeId':	{ 'type': 'string' },
	'agentGeneration':	{ 'type': 'string' },
	'rIdx':			{ 'type': 'number' },

	'result':		{ 'type': 'string' },
	'state':		{ 'type': 'string' },
	'wantRetry':		{ 'type': 'string' },
	'wantInputRemoved':	{ 'type': 'string' },
	'timeAbandoned':	{ 'type': 'string' },
	'timeCancelled':	{ 'type': 'string' },
	'timeInputDone':	{ 'type': 'string' },
	'timeInputDoneRead':	{ 'type': 'string' },
	'timeCommitted':	{ 'type': 'string' },
	'timeDone':		{ 'type': 'string' },
	'timeRetried':		{ 'type': 'string' },
	'timeInputRemoved':	{ 'type': 'string' },

	/* for debugging only */
	'nattempts':		{ 'type': 'number' },
	'nOutputs':		{ 'type': 'number' },
	'result':		{ 'type': 'string' },
	'machine':		{ 'type': 'string' }
    }
};


/* Public interface */
exports.sHttpJobInput = sHttpJobInput;
exports.sMorayHealth = sMorayHealth;
exports.sMorayJob = sMorayJob;
exports.sMorayJobInput = sMorayJobInput;
exports.sMorayMantaStorage = sMorayMantaStorage;
exports.sMorayTask = sMorayTask;
exports.sMorayTaskInput = sMorayTaskInput;
exports.sMorayTaskOutput = sMorayTaskOutput;
exports.sBktJsonSchemas = sBktJsonSchemas;
exports.sBktConfigs = sBktConfigs;
exports.sJobStates = sJobStates;

exports.sIntervalRequired = sIntervalRequired;
exports.sNonNegativeInteger = sNonNegativeInteger;
exports.sNonNegativeIntegerRequired = sNonNegativeIntegerRequired;
exports.sPercentRequired = sPercentRequired;
exports.sStringRequired = sStringRequired;
exports.sStringRequiredNonEmpty = sStringRequiredNonEmpty;
exports.sTcpPortRequired = sTcpPortRequired;
