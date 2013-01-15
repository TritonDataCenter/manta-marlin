/*
 * lib/schema.js: JSON schemas for various Marlin records
 */

/*
 * Primitive schema components
 */

var sString = { 'type': 'string' };
var sObject = { 'type': 'object' };

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
		'input': sString,
		'phases': sJobPhases
	}
};


/*
 * Moray records
 */

var sMorayError = {
	'type': 'object',
	'properties': {
		'jobId': sStringRequiredNonEmpty,
		'phaseNum': sNonNegativeIntegerRequired,

		/* error details */
		'errorCode': sStringRequired,		/* programmatic code */
		'errorMessage': sStringRequired,	/* public message */
		'errorMessageInternal': sString,	/* internal message */

		/* only for dispatch failures or map task failures */
		'input': sString,	/* input object */
		'p0input': sString,	/* phase 0 input (if known) */

		/* only for failures during execution */
		'taskId': sString,	/* parent task */
		'server': sString,	/* CN where error happened */
		'machine': sString	/* lackey  */
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
		'input': sString,
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

		/*
		 * XXX MANTA-928 making server required since we have separate
		 * "errors" now.  Make sure I always set it.
		 */
		'server': sStringRequiredNonEmpty,	/* assigned agent */

		/* map tasks only */
		/* XXX s/key/input/g ? */
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
		 * Note that not all tasks go through all of these steps: a task
		 * for an object that does not exist may be written out in the
		 * "done" state, having never actually executed and thus with no
		 * timeStarted field.
		 */
		'timeDispatched': sDateTimeRequired,	/* time created */
		'timeAccepted': sDateTime,	/* time agent picked up */
		'timeStarted': sDateTime,	/* time execution started */
		'timeDone': sDateTime,		/* time execution ended */
		'timeCommitted': sDateTime,	/* time worker committed */
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
		'p0key': sStringRequiredNonEmpty,	/* p0 object, if any */
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
		/*
		 * XXX MANTA-928: make sure "phaseNum" and
		 * "key" are always specified
		 */
		'key': sStringRequiredNonEmpty,		/* output object name */
		'rIdx': sReducerIndex,			/* assigned reducer */

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
	'jobId':	{ 'type': 'string' },
	'phaseNum':	{ 'type': 'string' },

	/* indexes for debugging only */
	'errorCode': 	{ 'type': 'string' },
	'taskId':	{ 'type': 'string' },
	'server': 	{ 'type': 'string' },
	'machine': 	{ 'type': 'string' }
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
	'timePropagated':	{ 'type': 'string' }
    }
};

sBktConfigs['task'] = {
    'options': {},
    'index': {
	'taskId':		{ 'type': 'string', 'unique': true },
	'jobId':		{ 'type': 'string' },
	'phaseNum':		{ 'type': 'number' },
	'server':		{ 'type': 'string' },
	'rIdx':			{ 'type': 'number' },

	'state':		{ 'type': 'string' },
	'timeCancelled':	{ 'type': 'string' },
	'timeInputDone':	{ 'type': 'string' },
	'timeInputDoneRead':	{ 'type': 'string' },
	'timeCommitted':	{ 'type': 'string' },

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
