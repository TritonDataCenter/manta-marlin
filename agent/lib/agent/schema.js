/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * lib/agent/schema.js: agent configuration schema
 */

var mod_schema = require('../schema');

/*
 * Properties that have been around since early versions of the agent are
 * required.  Properties added since then are optional so that it's easier for
 * administrators to upgrade and rollback agents without having to muck with
 * configuration files in the common cases.
 */
module.exports = {
    'type': 'object',
    'properties': {
	'instanceUuid': mod_schema.sStringRequiredNonEmpty,
	'mantaComputeId': mod_schema.sStringRequiredNonEmpty,
	'port': mod_schema.sTcpPortRequired,

	'manta': {
	    'type': 'object',
	    'required': true,
	    'properties': {
		'url': mod_schema.sStringRequiredNonEmpty
	    }
	},

	'moray': {
	    'type': 'object',
	    'required': true,
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
	},

	'dns': {
	    'type': 'object',
	    'required': true,
	    'properties': {
		'nameservers': {
		    'type': 'array',
		    'items': mod_schema.sStringRequiredNonEmpty,
		    'minItems': 1
		},

		/*
		 * These two properties are no longer used, but they're still
		 * allowed in the configuration file so that the file itself can
		 * be backwards-compatible.  (They don't need to be here in the
		 * schema, but it's easier to keep this schema in sync with the
		 * template, and affords us a place to document this fact, which
		 * we cannot do in the JSON file.)
		 */
		'triggerInterval': mod_schema.sInterval,
		'graceInterval':   mod_schema.sInterval
	    }
	},

	'buckets': {
	    'type': 'object',
	    'required': true,
	    'properties': {
		'error': mod_schema.sStringRequiredNonEmpty,
		'health': mod_schema.sStringRequiredNonEmpty,
		'job': mod_schema.sStringRequiredNonEmpty,
		'task': mod_schema.sStringRequiredNonEmpty,
		'taskinput': mod_schema.sStringRequiredNonEmpty,
		'taskoutput': mod_schema.sStringRequiredNonEmpty
	    }
	},

	'tunables': {
	    'type': 'object',
	    'required': true,
	    'properties': {
		'cueballDefaultTimeout': mod_schema.sNonNegativeInteger,
		'cueballDefaultMaxTimeout': mod_schema.sNonNegativeInteger,
		'cueballDefaultRetries': mod_schema.sNonNegativeInteger,
		'cueballDefaultDelay': mod_schema.sNonNegativeInteger,
		'cueballDefaultMaxDelay': mod_schema.sNonNegativeInteger,
		'httpMaxSockets': mod_schema.sNonNegativeInteger,
		'httpSpareSockets': mod_schema.sNonNegativeInteger,
		'httpTcpKeepAliveDelay': mod_schema.sNonNegativeInteger,
		'maxPendingOutputsPerTask': mod_schema.sIntervalRequired,
		'maxPendingPuts': mod_schema.sIntervalRequired,
		'timeHeartbeat': mod_schema.sIntervalRequired,
		'timeHogGrace': mod_schema.sIntervalRequired,
		'timeHogKill': mod_schema.sIntervalRequired,
		'timePoll': mod_schema.sIntervalRequired,
		'timeTasksCheckpoint': mod_schema.sIntervalRequired,
		'timeTick': mod_schema.sIntervalRequired,
		'timeZoneIdleMin': mod_schema.sIntervalRequired,
		'timeZoneIdleMax': mod_schema.sIntervalRequired,
		'zoneDisabledMaxPercent': mod_schema.sPercentRequired,
		'zoneDiskSlopPercent': mod_schema.sPercentRequired,
		'zoneLivenessCheck': mod_schema.sNonNegativeIntegerRequired,
		'zoneMemorySlopPercent': mod_schema.sPercentRequired,
		'zoneReserveMin': mod_schema.sIntervalRequired,
		'zoneReservePercent': mod_schema.sPercentRequired
	    }
	},

	'zoneDefaultImage': mod_schema.sStringRequiredNonEmpty,

	'zoneDefaults': {
	    'type': 'object',
	    'required': true
	}
    }
};
