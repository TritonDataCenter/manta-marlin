/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * lib/worker/schema.js: supervisor configuration schema
 */

var mod_schema = require('../schema');

module.exports = {
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
		'index': {
		    'required': true,
		    'type': 'object'
		},
		'storage': {
		    'required': true,
		    'type': 'object'
		}
	    }
	},
	'auth': {
	    'required': true,
	    'type': 'object',
	    'properties': {
		'url': mod_schema.sStringRequiredNonEmpty,
		'maxAuthCacheSize': mod_schema.sNonNegativeInteger,
		'maxAuthCacheAgeMs': mod_schema.sNonNegativeInteger,
		'maxTranslationCacheSize': mod_schema.sNonNegativeInteger,
		'maxTranslationCacheAgeMs': mod_schema.sNonNegativeInteger
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
		'domain': mod_schema.sStringRequiredNonEmpty,
		'error': mod_schema.sStringRequiredNonEmpty,
		'health': mod_schema.sStringRequiredNonEmpty,
		'job': mod_schema.sStringRequiredNonEmpty,
		'jobinput': mod_schema.sStringRequiredNonEmpty,
		'task': mod_schema.sStringRequiredNonEmpty,
		'taskinput': mod_schema.sStringRequiredNonEmpty,
		'taskoutput': mod_schema.sStringRequiredNonEmpty
	    }
	},
	'tunables': {
	    'required': true,
	    'type': 'object',
	    'properties': {
		'maxPendingAuths': mod_schema.sIntervalRequired,
		'maxPendingDeletes': mod_schema.sIntervalRequired,
		'maxPendingLocates': mod_schema.sIntervalRequired,
		'maxPendingPuts': mod_schema.sIntervalRequired,
		'maxRecordsPerQuery': mod_schema.sIntervalRequired,
		'maxTaskRetries': mod_schema.sIntervalRequired,
		'timeAgentPoll': mod_schema.sIntervalRequired,
		'timeAgentTimeout': mod_schema.sIntervalRequired,
		'timeHeartbeat': mod_schema.sIntervalRequired,
		'timeJobIdleClose': mod_schema.sIntervalRequired,
		'timeJobSave': mod_schema.sIntervalRequired,
		'timeMarkInputs': mod_schema.sIntervalRequired,
		'timePoll': mod_schema.sIntervalRequired,
		'timeTick': mod_schema.sIntervalRequired,
		'timeWorkerAbandon': mod_schema.sIntervalRequired,
		'timeWorkerPoll': mod_schema.sIntervalRequired
	    }
	},
	'images': {
	    'required': true,
	    'type': 'array',
	    'items': mod_schema.sStringRequiredNonEmpty
	}
    }
};
