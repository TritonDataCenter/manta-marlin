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
	'maximum': 1024
};

var sReducerIndex = {
	'type': 'integer',
	'minimum': 0,
	'maximum': 1023
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
	'enum': [ 128, 256, 512, 1024, 2048, 4096, 8192, 16384 ]
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
		'count': sReducerCount,
		'memory': sMemory,
		'uarg': sObject
	}
};

var sJobPhases = {
	'type': 'array',
	'required': true,
	'minItems': 1,
	'items': sJobPhase
};

var sInputKeys = {
	'type': 'array',
	'required': true,
	'minItems': 1,
	'items': sString
};

var sError = {
	'type': 'object',
	'properties': {
		'code': sStringRequiredNonEmpty,
		'message': sStringRequired
	}
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
		'machine': sString,			/* lackey  */
		'stderr': sString,			/* stderr object name */

		'prevRecordType': sString,		/* previous record */
		'prevRecordId': sString			/* (for debugging) */
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
		'key': sStringRequiredNonEmpty,	/* input object name */

		'timeCreated': sDateTime,
		'timePropagated': sDateTime,
		'nextRecordType': sString,	/* "task" or "taskinput" */
		'nextRecordId': sString		/* subsequent record id */
	}
};

var sMorayTask = {
	'type': 'object',
	'properties': {
		/* immutable definition */
		'taskId': sStringRequiredNonEmpty,
		'jobId': sStringRequiredNonEmpty,
		'phaseNum': sNonNegativeIntegerRequired,
		'server': sStringRequiredNonEmpty,
		'serverGeneration': sStringRequiredNonEmpty,

		/* map tasks only */
		'key': sString,			/* input object name */
		'p0key': sString,		/* phase 0 input object name */
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
		'timeCancelled': sDateTime	/* time task was cancelled */
	}
};

var sMorayTaskInput = {
	'type': 'object',
	'properties': {
		'taskInputId': sStringRequiredNonEmpty,
		'jobId': sStringRequiredNonEmpty,
		'taskId': sStringRequiredNonEmpty,
		'server': sStringRequiredNonEmpty,	/* assigned server */
		'key': sStringRequiredNonEmpty,		/* input object name */
		'p0key': sString,			/* p0 object, if any */
		'account': sStringRequiredNonEmpty,	/* object's owner */
		'objectid': sDateTimeRequired,		/* object uuid */
		'servers': {				/* object's locations */
			'required': true,
			'type': 'array',
			'items': {
				'type': 'object',
				'properties': {
					'server': sStringRequiredNonEmpty,
					'zonename': sStringRequiredNonEmpty
				}
			}
		},

		'timeDispatched': sDateTimeRequired,	/* time created */
		'timeRead': sDateTime,			/* time read by agent */

		'prevRecordType': sString,		/* previous record */
		'prevRecordId': sString			/* (for debugging) */
	}
};

var sMorayTaskOutput = {
	'type': 'object',
	'properties': {
		'jobId': sStringRequiredNonEmpty,
		'taskId': sStringRequiredNonEmpty,
		'phaseNum': sNonNegativeIntegerRequired,
		'key': sStringRequiredNonEmpty,		/* output object name */
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
    'job': sMorayJob,
    'jobinput': sMorayJobInput,
    'task': sMorayTask,
    'taskinput': sMorayTaskInput,
    'taskoutput': sMorayTaskOutput
};


/*
 * Moray bucket schemas.  These schemas only tell Moray what indices to create.
 * Objects in these buckets must conform to the more constrained schemas above.
 */
var sBktConfigs = {};

sBktConfigs['error'] = {
    'options': {},
    'index': {
	'errorId':	{ 'type': 'string' },
	'jobId':	{ 'type': 'string' },
	'phaseNum':	{ 'type': 'string' },

	/* indexes for debugging only */
	'errorCode': 	{ 'type': 'string' },
	'taskId':	{ 'type': 'string' },
	'server': 	{ 'type': 'string' },
	'machine': 	{ 'type': 'string' }
    }
};

sBktConfigs['health'] = {
    'options': {},
    'index': {
    	'component':	{ 'type': 'string' },

	/* indexes for debugging only */
    	'instance':	{ 'type': 'string' },
	'generation':	{ 'type': 'string' }
    }
};

sBktConfigs['job'] = {
    'options': {},
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

sBktConfigs['jobinput'] = {
    'options': {},
    'index': {
	'jobId':		{ 'type': 'string' },
	'key':			{ 'type': 'string' },
	'timePropagated':	{ 'type': 'string' }
    }
};

sBktConfigs['taskinput'] = {
    'options': {},
    'index': {
	'taskInputId':	{ 'type': 'string', 'unique': true },
	'jobId':	{ 'type': 'string' },
	'taskId':	{ 'type': 'string' },
	'server':	{ 'type': 'string' },
	'timeRead':	{ 'type': 'string' }
    }
};

sBktConfigs['taskoutput'] = {
    'options': {},
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

sBktConfigs['task'] = {
    'options': {},
    'index': {
	'taskId':		{ 'type': 'string', 'unique': true },
	'jobId':		{ 'type': 'string' },
	'phaseNum':		{ 'type': 'number' },
	'server':		{ 'type': 'string' },
	'serverGeneration':	{ 'type': 'string' },
	'rIdx':			{ 'type': 'number' },

	'result':		{ 'type': 'string' },
	'state':		{ 'type': 'string' },
	'timeAbandoned':	{ 'type': 'string' },
	'timeCancelled':	{ 'type': 'string' },
	'timeInputDone':	{ 'type': 'string' },
	'timeInputDoneRead':	{ 'type': 'string' },
	'timeCommitted':	{ 'type': 'string' },
	'timeDone':		{ 'type': 'string' },

	/* for debugging only */
	'nOutputs':		{ 'type': 'number' },
	'result':		{ 'type': 'string' },
	'machine':		{ 'type': 'string' }
    }
};


/* Public interface */
exports.sHttpJobInput = sHttpJobInput;
exports.sMorayJob = sMorayJob;
exports.sMorayJobInput = sMorayJobInput;
exports.sMorayTask = sMorayTask;
exports.sMorayTaskInput = sMorayTaskInput;
exports.sMorayTaskOutput = sMorayTaskOutput;
exports.sBktJsonSchemas = sBktJsonSchemas;
exports.sBktConfigs = sBktConfigs;
exports.sJobStates = sJobStates;

exports.sTcpPortRequired = sTcpPortRequired;
exports.sStringRequired = sStringRequired;
exports.sStringRequiredNonEmpty = sStringRequiredNonEmpty;
exports.sNonNegativeInteger = sNonNegativeInteger;
exports.sIntervalRequired = sIntervalRequired;
