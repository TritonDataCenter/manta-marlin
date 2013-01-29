/*
 * lib/agent/schema.js: agent configuration schema
 */

var mod_schema = require('../schema');

module.exports = {
    'type': 'object',
    'properties': {
	'instanceUuid': mod_schema.sStringRequiredNonEmpty,
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
		'triggerInterval': mod_schema.sIntervalRequired,
		'graceInterval':   mod_schema.sIntervalRequired
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
		'httpMaxSockets': mod_schema.sNonNegativeInteger,
		'maxPendingPuts': mod_schema.sIntervalRequired,
		'timeHeartbeat': mod_schema.sIntervalRequired,
		'timePoll': mod_schema.sIntervalRequired,
		'timeTick': mod_schema.sIntervalRequired,
		'zoneReserveMin': mod_schema.sIntervalRequired,
		'zoneReservePercent': mod_schema.sPercentRequired
	    }
	},

	'zoneDefaults': {
	    'type': 'object',
	    'required': true
	}
    }
};
