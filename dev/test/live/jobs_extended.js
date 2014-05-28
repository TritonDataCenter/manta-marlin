/*
 * jobs_extended.js: test cases that are part of the test suite, but which tend
 * to take a long time.  These are not used during typical crash test runs,
 * since most of the supervisor and agent time is usually spent waiting.
 */

var mod_fs = require('fs');
var mod_path = require('path');

/* jsl:import ../../../common/lib/errors.js */
require('../../lib/errors');

module.exports = registerTestCases;

var testcases = {
    'jobMerrorOom': {
	'job': {
	    'assets': {
		'/%user%/stor/mallocbomb':
		    mod_fs.readFileSync(mod_path.join(
			__dirname, '../mallocbomb/mallocbomb'))
	    },
	    'phases': [ {
		'assets': [ '/%user%/stor/mallocbomb' ],
		'type': 'reduce',
		'exec': '/assets/%user%/stor/mallocbomb; false'
	    } ]
	},
	'inputs': [],
	'timeout': 180 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: reduce',
	    'code': EM_USERTASK,
	    'message': 'user command exited with code 1 (WARNING: ran out of ' +
		'memory during execution)'
	} ]
    },

    'jobMerrorDisk': {
	'job': {
	    'phases': [ {
		'type': 'reduce',
		'disk': 2,
		'exec': 'mkfile 2g /var/tmp/junk'
	    } ]
	},
	'inputs': [],
	'timeout': 450 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: reduce',
	    'code': EM_USERTASK,
	    'message': 'user task ran out of local disk space'
	} ]
    },

    'jobMerrorLackeyOom': {
	/*
	 * It would be better if we could actually run the lackey out of memory,
	 * but this seems difficult to do in practice.  But when it happens, the
	 * lackey tends to dump core, resulting in a timeout.
	 */
	'job': {
	    'assets': {
		'/%user%/stor/mallocbomb':
		    mod_fs.readFileSync(mod_path.join(
			__dirname, '../mallocbomb/mallocbomb'))
	    },
	    'phases': [ {
		'assets': [ '/%user%/stor/mallocbomb' ],
		'type': 'reduce',
		'exec': '/assets/%user%/stor/mallocbomb & ' +
		    'pstop $(pgrep -c $(svcs -Hoctid lackey))'
	    } ]
	},
	'inputs': [],
	'timeout': 180 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: reduce',
	    'code': EM_USERTASK,
	    'message': 'user task ran out of memory'
	} ]
    },

    'jobMerrorMuskieRetry': {
	/*
	 * This test validates that we get the expected number of errors.  It's
	 * probabilistic, though: we set the probability of an upstream muskie
	 * failure to 0.5, and we know Marlin retries 3 times, so there should
	 * be close to 12 failures per 100 keys.
	 */
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'curl -i -X POST localhost/my/jobs/task/perturb?p=0.5'
	    } ]
	},
	'inputs': [],
	'timeout': 360 * 1000,
	'error_count': [ 1, 25 ],
	'errors': [ {
	    'phaseNum': '0',
	    'code': EM_INTERNAL,
	    'message': 'internal error'
	} ]
    },

    'jobMerrorMuskieRetryMpipe': {
	/*
	 * Like jobMerrorMuskieRetry, but with mpipe.
	 */
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'curl -i -X POST ' +
		    'localhost/my/jobs/task/perturb?p=0.5 | mpipe'
	    } ]
	},
	'inputs': [],
	'timeout': 360 * 1000,
	'error_count': [ 1, 25 ],
	'errors': [ {
	    'phaseNum': '0',
	    'code': EM_USERTASK,
	    'message': 'user command exited with code 1'
	} ]
    },

    'jobRmuskieRetry': {
	/*
	 * Tests that reducers that experience transient errors fetching data
	 * continue to retry the requests.
	 */
	'job': {
	    'phases': [ {
		'type': 'reduce',
		'init': 'curl -i -X POST localhost/my/jobs/task/perturb?p=0.1',
		'exec': 'wc'
	    } ]
	},
	'inputs': [],
	'timeout': 60 * 1000,
	'errors': [],
	'expected_outputs': [ /%user%\/jobs\/.*\/stor\/reduce\.0\./ ],
	'expected_output_content': [ '      0     401    5090\n' ]
    },

    'jobRerrorMuskieRetry': {
	/*
	 * Tests that reducers that experience large numbers of transient errors
	 * eventually blow up with an error.
	 */
	'job': {
	    'phases': [ {
		'type': 'reduce',
		'init': 'curl -i -X POST localhost/my/jobs/task/perturb?p=1',
		'exec': 'wc'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 60 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: reduce',
	    'code': EM_SERVICEUNAVAILABLE,
	    'message': /error fetching inputs/
	} ]
    }
};

function registerTestCases()
{
	var job, i, key;

	job = testcases.jobMerrorMuskieRetry;
	for (i = 0; i < 100; i++) {
		key = '/%user%/stor/obj' + i;
		job['inputs'].push(key);
	}

	testcases.jobMerrorMuskieRetryMpipe['inputs'] =
	    testcases.jobMerrorMuskieRetry['inputs'];
	testcases.jobRmuskieRetry['inputs'] =
	    testcases.jobMerrorMuskieRetry['inputs'];
	return (testcases);
}
