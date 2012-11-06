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
		'timeCancelled': sDateTime,
		'timeDone': sDateTime
	}
};

var sMorayTask = {
	'type': 'object',
	'properties': {
		'jobId': sStringRequiredNonEmpty,
		'taskId': sStringRequiredNonEmpty,
		'phaseNum': sNonNegativeIntegerRequired,
		'server': sString,
		'state': {
			'type': 'string',
			'required': true,
			'enum': [
			    'dispatched',
			    'running',
			    'done',
			    'cancelled',
			    'aborted'
			]
		},
		'machine': sString,
		'result':  {
			'type': 'string',
			'enum': [ 'ok', 'fail' ]
		},
		'error': sError,
		'timeDispatched': sDateTimeRequired,
		'timeQueued': sDateTime,
		'timeStarted': sDateTime,
		'timeDone': sDateTime,
		'timeCommitted': sDateTime,
		'stderr': sString,
		'nOutputs': sNonNegativeInteger,
		'firstOutputs': {
			'type': 'array',
			'items': {
				'type': 'object',
				'properties': {
					'key': sStringRequiredNonEmpty,
					'timeCreated': sDateTimeRequired,
					'rIdx': sReducerIndex
				}
			}
		},

		/* map tasks only */
		'key': sString,
		'p0key': sString,
		'account': sString,
		'objectid': sString,
		'zonename': sString,

		/* reduce tasks only */
		'timeInputDone': sDateTime
	}
};

var sMorayJobInput = {
	'type': 'object',
	'properties': {
		'jobId': sStringRequiredNonEmpty,
		'key': sString
	}
};

var sMorayTaskInput = {
	'type': 'object',
	'properties': {
		'jobId': sStringRequiredNonEmpty,
		'taskId': sStringRequiredNonEmpty,
		'key': sStringRequiredNonEmpty,
		'account': sStringRequiredNonEmpty,
		'objectid': sDateTimeRequired,
		'servers': {
			'required': true,
			'type': 'array',
			'minItems': 1,
			'items': {
				'type': 'object',
				'properties': {
					'server': sStringRequiredNonEmpty,
					'zonename': sStringRequiredNonEmpty
				}
			}
		}
	}
};

var sMorayTaskOutput = {
	'type': 'object',
	'properties': {
		'jobId': sStringRequiredNonEmpty,
		'taskId': sStringRequiredNonEmpty,
		'error': sError,
		'key': sString,
		'timeCreated': sDateTimeRequired,
		'rIdx': sReducerIndex
	}
};

var sBktJsonSchemas =  {
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

sBktConfigs['job'] = {
    'options': {
	'guaranteeOrder': true,
	'trackModification': true
    },
    'index': {
	'jobId':	{ 'type': 'string', 'unique': true },
	'jobName':	{ 'type': 'string' },
	'owner':	{ 'type': 'string' },
	'state':	{ 'type': 'string' },
	'worker':	{ 'type': 'string' },
	'timeDone':	{ 'type': 'string' }
    }
};

sBktConfigs['jobinput'] = {
    'index': {
	'jobId':	{ 'type': 'string' }
    },
    'options': {
	'guaranteeOrder': true
    }
};

sBktConfigs['taskinput'] = {
    'index': {
	'jobId':	{ 'type': 'string' },
	'taskId':	{ 'type': 'string' }
    },
    'options': {
	'guaranteeOrder': true
    }
};

sBktConfigs['taskoutput'] = {
    'index': {
	'jobId':	{ 'type': 'string' },
	'taskId':	{ 'type': 'string' }
    },
    'options': {
	'guaranteeOrder': true
    }
};

sBktConfigs['task'] = {
    'options': {
	'guaranteeOrder': true,
	'trackModification': true
    },
    'index': {
	'taskId':		{ 'type': 'string', 'unique': true },
	'jobId':		{ 'type': 'string' },
	'phaseNum':		{ 'type': 'number' },
	'nOutputs':		{ 'type': 'number' },
	'server':		{ 'type': 'string' },
	'state':		{ 'type': 'string' },
	'result':		{ 'type': 'string' },
	'timeCommitted':	{ 'type': 'string' }
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
exports.sIntervalRequired = sIntervalRequired;
