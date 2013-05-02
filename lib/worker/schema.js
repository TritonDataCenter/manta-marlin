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
		'index': {
		    'required': true,
		    'type': 'object',
		    'properties': {
			'host': mod_schema.sStringRequiredNonEmpty,
			'port': mod_schema.sTcpPortRequired,
			'retry': {
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
		'redis_options': {
		    'type': 'object'
		},

		'connectTimeout': mod_schema.sInterval,
		'maxTimeout': mod_schema.sInterval,
		'cache': {
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
