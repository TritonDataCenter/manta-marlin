/*
 * lib/worker/schema.js: worker configuration schema
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
