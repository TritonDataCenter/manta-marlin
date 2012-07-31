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

var sNonNegativeIntegerRequired = {
	'type': 'integer',
	'required': true,
	'minimum': 0
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

var sInputKeyRecords = {
	'type': 'array',
	'required': true,
	'minItems': 1,
	'items': {
		'type': 'object',
		'properties': {
			'key': sStringRequiredNonEmpty,
			'objectid': sString,
			'zonename': sString
		}
	}
};

var sOutputKeys = sStringArrayRequired;

var sError = {
	'type': 'object',
	'properties': {
		'code': sStringRequiredNonEmpty,
		'message': sStringRequired
	}
};

/*
 * Job record in Moray
 */
var sMorayJob = {
	'type': 'object',
	'properties': {
		/* immutable job definition */
		'jobId': sStringRequiredNonEmpty,
		'jobName': sStringRequired,
		'phases': sJobPhases,
		'inputKeys': sInputKeys,
		'createTime': sDateTimeRequired,
		'owner': sStringRequiredNonEmpty,

		/* public state (mediated by the web tier) */
		'state': {
			'type': 'string',
			'required': true,
			'enum': [ 'queued', 'running', 'done' ]
		},
		'outputKeys': sOutputKeys,
		'discardedKeys': sOutputKeys,
		'finishTime': sDateTime,

		/* internal Marlin state */
		'worker': sString
	}
};

/*
 * Job record submitted by user
 */
var sHttpJobInput = {
	'type': 'object',
	'additionalProperties': false,
	'properties': {
		'jobName': sStringRequired,
		'phases': sJobPhases,
		'inputKeys': sInputKeys
	}
};

/*
 * Task group record in Moray
 */
var sMorayTaskGroup = {
	'type': 'object',
	'properties': {
		'jobId': sStringRequiredNonEmpty,
		'taskGroupId': sStringRequiredNonEmpty,
		'host': sStringRequiredNonEmpty,
		'inputKeys': sInputKeyRecords,
		'phase': sJobPhase,
		'phaseNum': sNonNegativeIntegerRequired,
		'state': {
			'type': 'string',
			'required': true,
			'enum': [ 'dispatched', 'running', 'cancelled', 'done' ]
		},
 		'results': {
 			'type': 'array',
 			'required': true,
 			'items': {
 				'type': 'object',
 				'properties': {
 					'input': sStringRequiredNonEmpty,
 					'machine': sString,
 					'result': {
 						'type': 'string',
 						'required': true,
 						'enum': [ 'ok', 'fail' ]
 					},
 					'outputs': sOutputKeys,
 					'partials': sStringArray,
 					'discarded': sStringArray,
 					'startTime': sDateTime,
 					'doneTime': sDateTime,
 					'error': sError
 				}
 			}
 		}
	}
};

/*
 * Moray bucket schemas.  These schemas only tell Moray what indices to create.
 * Objects in these buckets must conform to the more constrained schemas above.
 */
var sBktSchemas = {};

sBktSchemas['job'] = {
	'jobId': {
		'type': 'string',
		'unique': true
	},
	'jobName': {
		'type': 'string'
	},
	'owner': {
		'type': 'string'
	},
	'state': {
		'type': 'string'
	}
};

sBktSchemas['taskgroup'] = {
    'taskGroupId': {
    	'type': 'string',
    	'unique': true
    },
    'jobId': {
    	'type': 'string',
    	'unique': false
    },
    'host': {
	'type': 'string',
	'unique': false
    },
    'phaseNum': {
    	'type': 'number',
    	'unique': false
    },
    'state': {
	'type': 'string',
	'unique': false
    }
};

/* Public interface */
exports.sMorayJob = sMorayJob;
exports.sHttpJobInput = sHttpJobInput;
exports.sMorayTaskGroup = sMorayTaskGroup;
exports.sBktSchemas = sBktSchemas;
exports.sTcpPortRequired = sTcpPortRequired;
exports.sStringRequired = sStringRequired;
exports.sStringRequiredNonEmpty = sStringRequiredNonEmpty;
exports.sIntervalRequired = sIntervalRequired;
