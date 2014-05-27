/*
 * test/live/jobs.js: This file defines a number of independent test cases.
 * These are defined (mostly) declaratively in terms of inputs, expected
 * outputs, expected errors, and the like.  The idea is to keep these as
 * self-contained as possible so that they can be tested in a number of
 * different configurations: one-at-a-time or all at once; in series or in
 * parallel; using the marlin client to submit jobs or using the front door; or
 * in a stress mode that simulates failed Moray requests.  The only parts that
 * are implemented today are the one-at-a-time, all-at-once in series, and
 * all-at-once in parallel.
 */

var mod_assert = require('assert');
var mod_fs = require('fs');
var mod_jsprim = require('jsprim');
var mod_path = require('path');
var mod_extsprintf = require('extsprintf');
var mod_vasync = require('vasync');

var mod_maufds = require('../../lib/ufds');
var mod_schema = require('../../lib/schema');
var mod_testcommon = require('../common');
var sprintf = mod_extsprintf.sprintf;
var log = mod_testcommon.log;
var StringInputStream = mod_testcommon.StringInputStream;
/* jsl:import ../../../common/lib/errors.js */
require('../../lib/errors');

/*
 * MANTA_USER should be set to an operator (e.g., "poseidon") in order to run
 * the tests, but most of the tests will run as DEFAULT_USER.
 */
var DEFAULT_USER = 'marlin_test';

/*
 * These inputs and errors are used by a few dispatch error tests and so are
 * defined here for consistency between tests.
 */
var inputs_disperrors = [
    '/notavalidusername/stor/obj1',
    '/%user%/stor/notavalidfilename',
    '/%user%/stor/mydir',
    '/%user%/stor/obj1'
];

var extrainputs_disperrors = [
    '/',
    '/%user%',
    '/%user%/',
    '/%user%/stor',
    '/%user%/stor/',
    '/%user%/jobs',
    '/%user%/jobs/'
];

var errors_disperrors0 = [ {
    'phaseNum': '0',
    'what': 'phase 0: input "/notavalidusername/stor/obj1"',
    'input': '/notavalidusername/stor/obj1',
    'p0input': '/notavalidusername/stor/obj1',
    'code': EM_RESOURCENOTFOUND,
    'message': 'no such object: "/notavalidusername/stor/obj1"'
}, {
    'phaseNum': '0',
    'what': 'phase 0: input "/%user%/stor/notavalidfilename"',
    'input': '/%user%/stor/notavalidfilename',
    'p0input': '/%user%/stor/notavalidfilename',
    'code': EM_RESOURCENOTFOUND,
    'message': 'no such object: "/%user%/stor/notavalidfilename"'
}, {
    'phaseNum': '0',
    'what': 'phase 0: input "/%user%/stor/mydir"',
    'input': '/%user%/stor/mydir',
    'p0input': '/%user%/stor/mydir',
    'code': EM_INVALIDARGUMENT,
    'message': 'objects of type "directory" are not supported: ' +
	'"/%user%/stor/mydir"'
}, {
    'phaseNum': '0',
    'what': 'phase 0: input "/"',
    'input': '/',
    'p0input': '/',
    'code': EM_RESOURCENOTFOUND,
    'message': 'malformed object name: "/"'
}, {
    'phaseNum': '0',
    'what': 'phase 0: input "/%user%"',
    'input': '/%user%',
    'p0input': '/%user%',
    'code': EM_RESOURCENOTFOUND,
    'message': 'malformed object name: "/%user%"'
}, {
    'phaseNum': '0',
    'what': 'phase 0: input "/%user%/"',
    'input': '/%user%/',
    'p0input': '/%user%/',
    'code': EM_RESOURCENOTFOUND,
    'message': 'no such object: "/%user%/"'
}, {
    'phaseNum': '0',
    'what': 'phase 0: input "/%user%/stor"',
    'input': '/%user%/stor',
    'p0input': '/%user%/stor',
    'code': EM_RESOURCENOTFOUND,
    'message': 'no such object: "/%user%/stor"'
}, {
    'phaseNum': '0',
    'what': 'phase 0: input "/%user%/stor/"',
    'input': '/%user%/stor/',
    'p0input': '/%user%/stor/',
    'code': EM_RESOURCENOTFOUND,
    'message': 'no such object: "/%user%/stor/"'
}, {
    'phaseNum': '0',
    'what': 'phase 0: input "/%user%/jobs"',
    'input': '/%user%/jobs',
    'p0input': '/%user%/jobs',
    'code': EM_RESOURCENOTFOUND,
    'message': 'no such object: "/%user%/jobs"'
}, {
    'phaseNum': '0',
    'what': 'phase 0: input "/%user%/jobs/"',
    'input': '/%user%/jobs/',
    'p0input': '/%user%/jobs/',
    'code': EM_RESOURCENOTFOUND,
    'message': 'no such object: "/%user%/jobs/"'
} ];

/*
 * These parameters are used to dynamically generate the authn/authz test cases.
 * For more information, see initAuthzTestCases().
 */
var authzAccountA = 'marlin_test_authzA';
var authzAccountB = 'marlin_test_authzB';
var authzUserAnon = 'anonymous';
var authzUserA1 = 'authzA1';
var authzUserA2 = 'authzA2';
var authzUserB1 = 'authzB1';
var authzUfdsConfig;
var authzMantaObjects = [ {
    'label': '/A/public/X: any object under /public',
    'account': authzAccountA,
    'public': true,
    'name': 'X'
}, {
    'label': '/A/stor/A: A creates object under /A/stor',
    'account': authzAccountA,
    'name': 'A'
}, {
    'label': '/A/stor/pub',
    'account': authzAccountA,
    'name': 'pub',
    'roles': [ authzUserAnon + '-readall' ]
}, {
    'label': '/A/stor/U1: object readable only by U1',
    'account': authzAccountA,
    'roles': [ authzUserA1 + '-readall' ],
    'name': 'U1'
} ];

/*
 * Test cases
 *
 * The keys in this object are the names of test cases.  These names can be
 * passed to the basic testrunner (tst.basic.js) to run individual tests.  The
 * test suite asserts a number of correctness conditions for each job,
 * including:
 *
 *     o The job completed within the allowed timeout.
 *
 *     o The job completed with the expected errors and outputs (optionally
 *       including output contents).
 *
 *     o The job completed without any job reassignments (which would indicate a
 *       worker crash) or retries (which would indicate an agent crash), unless
 *       being run in "stress" mode.
 *
 *     o The various job counters (inputs, tasks, errors, and outputs) match
 *       what we expect.
 *
 *     o If the job wasn't cancelled, then there should be no objects under the
 *       job's directory that aren't accounted for as output objects.
 *
 * Each test specifies the following fields:
 *
 *	job
 *
 *         Job definition, including "phases" (with at least "type" and "exec",
 *         and optionally "memory", "disk", and "image) and "assets" (which
 *         should be an object mapping paths to asset content).
 *
 *	inputs
 *
 *         Names of job input objects.  These will be created automatically
 *         before submitting the job and they will be submitted as inputs to the
 *         job.  By default, the objects are populated with a basic, unique
 *         string ("auto-generated content for key ..."), but a few special
 *         patterns trigger different behavior:
 *
 *		.*dir		a directory is created instead of a key
 *
 *		.*0bytes	object is created with no content
 *
 *	timeout
 *
 *         Milliseconds to wait for a job to complete, after which the test case
 *         is considered failed.  This should describe how long the test should
 *         take when run by itself (i.e., on an uncontended system), but should
 *         include time taken to reset zones.  The "concurrent" tests will
 *         automatically bump this timeout appropriately.
 *
 * Each test must also specify either "errors" and "expected_outputs" OR
 * "error_count":
 *
 *	errors
 *
 *         Array of objects describing the errors expected from the job.  Each
 *         property of the error may be a string or a regular expression that
 *         will be compared with the actual errors emitted.  It's not a failure
 *         if the actual errors contain properties not described here.  There
 *         must be a 1:1 mapping between the job's errors and these expected
 *         errors.
 *
 *	expected_outputs
 *
 *         Array of strings or regular expressions describing the *names* of
 *         expected output objects.  There must be a 1:1 mapping between
 *         the job's outputs and these expected outputs, but they may appear in
 *         any order.
 *
 *	error_count
 *
 *         Two-element array denoting the minimum and maximum number of errors
 *         allowed, used for probabilistic tests.
 *
 * Tests may also specify a few other fields, but these are less common:
 *
 *      account
 *
 *         The SDC account login name under which to run the job.  Most jobs run
 *         under DEFAULT_USER.  This is expanded for '%jobuser%' in object names
 *         and error messages, and for '%user%' as well unless "account_objects"
 *         is also specified.
 *
 *      account_objects
 *
 *         The SDC account login name to replace inside object names for the
 *         string '%user%'.  This is only useful for tests that use multiple
 *         accounts, which are pretty much just the authn/authz tests.
 *
 *	expected_output_content
 *
 *         Array of strings or regular expressions describing the contents of
 *         the expected output objects.  If specified, there must be a 1:1
 *         mapping between the job's output contents and these expected outputs,
 *         but they may appear in any order.
 *
 *	extra_inputs
 *
 *         Array of names of job input objects that should be submitted as
 *         inputs, but should not go through the creation process described for
 *         "inputs" above.
 *
 *	extra_objects
 *
 *         Array of strings or regular expressions describing objects that may
 *         be present under the job's directory after the job has completed.
 *         The test suite automatically checks that there are no extra objects
 *         under the job directory for most jobs, but some jobs create objects
 *         as side effects, and these need to be whitelisted here.
 *
 *      legacy_auth
 *
 *         Submit the job in legacy-authz/authn mode, in which only legacy
 *         credentials are supplied with the job and only legacy authorization
 *         checks can be applied by Marlin.
 *
 *	metering_includes_checkpoints
 *
 *         If true, the test suite (when run in the appropriate mode) will
 *         verify that metering data includes at least one checkpoint record.
 *
 *	pre_submit(api, callback)
 *
 *         Callback to be invoked before the job is submitted.  "callback"
 *         should be invoked when the job is ready to be submitted.
 *
 *	post_submit(api, jobid)
 *
 *         Callback to be invoked once the job has been submitted.
 *
 *	verify(verify)
 *
 *         Callback to be invoked once the job has completed to run additional
 *         checks.
 *
 *	skip_input_end
 *
 *         If true, then the job's input stream will not be closed after the job
 *         is submitted.  You'll probably want to use post_submit to do
 *         something or else the job will likely hit its timeout.
 *
 *      user
 *
 *         The SDC account subuser under which to run the job.  Most jobs run
 *         under the DEFAULT_USER account (not a subuser).
 */

var testcases = {
    'jobM': {
	'job': {
	    'phases': [ { 'type': 'map', 'exec': 'wc' } ]
	},
	'inputs': [
	    '/%user%/stor/obj1',
	    '/%user%/stor/obj1',
	    '/%user%/stor/obj2',
	    '/%user%/stor/obj3'
	],
	'timeout': 60 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj2\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj3\.0\./
	],
	'errors': []
    },

    'jobMcrossAccountLink': {
	'pre_submit': function (api, callback) {
	    var user1 = 'marlin_test_user_xacct';
	    var user2 = DEFAULT_USER;
	    var srckey = sprintf('/%s/public/xacctobj', user1);
	    var dstdir = sprintf('/%s/stor/subdir', user2);
	    var dstkey = sprintf('%s/obj_link', dstdir);

	    log.info('setting up cross-account link test');
	    mod_vasync.pipeline({
		'funcs': [
		    function ensureAccount1(_, subcb) {
			    mod_testcommon.ensureAccount(user1, subcb);
		    },
		    function ensureAccount2(_, subcb) {
			    mod_testcommon.ensureAccount(user2, subcb);
		    },
		    function populateSource(_, subcb) {
			    var data = 'auto-generated snaplink source content';
			    var stream = new StringInputStream(data);
			    log.info('PUT key "%s"', srckey);
			    api.manta.put(srckey, stream,
			        { 'size': data.length }, subcb);
		    },
		    function mkdirDst(_, subcb) {
			    log.info('creating destination directory');
			    api.manta.mkdirp(dstdir, subcb);
		    },
		    function mklinkDist(_, subcb) {
			    log.info('creating snaplink');
			    api.manta.ln(srckey, dstkey, subcb);
		    }
		]
	    }, function (err) {
		    if (err)
			    log.error(err, 'failed to set up test');
		    else
			    log.info('test is ready');
		    callback(err);
	    });
	},
	'job': {
	    'phases': [ { 'type': 'map', 'exec': 'wc' } ]
	},
	'inputs': [],
	'extra_inputs': [ '/%user%/stor/subdir/obj_link' ],
	'timeout': 30 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/subdir\/obj_link\.0\./
	],
	'errors': []
    },

    'jobMimage': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'image': '13.3.5',
		'exec': 'grep 13.3.5 /etc/motd'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 60 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'errors': []
    },

    'jobMX': {
	/* Like jobM, but makes use of several task output objects */
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'cat > /var/tmp/tmpfile; ' +
		    'for i in 1 2 3 4 5 6 7 8; do ' +
		    '    wc < /var/tmp/tmpfile | mpipe; ' +
		    'done'
	    } ]
	},
	'inputs': [
	    '/%user%/stor/obj1',
	    '/%user%/stor/obj2',
	    '/%user%/stor/obj3'
	],
	'timeout': 60 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj2\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj2\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj2\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj2\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj2\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj2\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj2\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj2\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj3\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj3\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj3\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj3\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj3\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj3\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj3\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj3\.0\./
	],
	'errors': []
    },

    'jobMqparams': {
	'job': {
	    'assets': {
		'/%user%/stor/queryparams.sh':
		    mod_fs.readFileSync(mod_path.join(
			__dirname, 'queryparams.sh'))
	    },
	    'phases': [ {
		'assets': [ '/%user%/stor/queryparams.sh' ],
		'type': 'reduce',
		'exec': '/assets/%user%/stor/queryparams.sh'
	    } ]
	},
	'inputs': [],
	'timeout': 30 * 1000,
	'expected_outputs': [ /\/%user%\/jobs\/.*\/reduce.0./ ],
	'errors': []
    },

    'jobMmpipeAnon': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'echo foo; echo bar | mpipe'
	    } ]
	},
	'inputs': [
	    '/%user%/stor/obj1',
	    '/%user%/stor/obj2',
	    '/%user%/stor/obj3'
	],
	'timeout': 30 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj2\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj3\.0\./
	],
	'expected_output_content': [ 'bar\n', 'bar\n', 'bar\n' ],
	'errors': []
    },

    'jobMmpipeNamed': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'echo foo; echo bar | mpipe -p /%user%/stor/extra/out1'
	    } ]
	},
	'inputs': [
	    '/%user%/stor/obj1'
	],
	'timeout': 15 * 1000,
	'expected_outputs': [
	    '/%user%/stor/extra/out1'
	],
	'expected_output_content': [ 'bar\n' ],
	'errors': []
    },

    'jobMmcat': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'echo foo; mcat /%user%/stor/obj1'
	    } ]
	},
	'inputs': [
	    '/%user%/stor/obj1',
	    '/%user%/stor/obj2',
	    '/%user%/stor/obj3'
	],
	'timeout': 15 * 1000,
	'expected_outputs': [
	    '/%user%/stor/obj1',
	    '/%user%/stor/obj1',
	    '/%user%/stor/obj1'
	],
	'expected_output_content': [
	    'auto-generated content for key /someuser/stor/obj1',
	    'auto-generated content for key /someuser/stor/obj1',
	    'auto-generated content for key /someuser/stor/obj1'
	],
	'errors': []
    },

    'jobM0bi': {
	'job': {
	    'phases': [ { 'type': 'map', 'exec': 'wc' } ]
	},
	'inputs': [ '/%user%/stor/0bytes' ],
	'timeout': 15 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/0bytes\.0\./
	],
	'expected_output_content': [ '0 0 0\n' ],
	'errors': []
    },

    'jobR0bi': {
	/*
	 * It's surprising that this output is different than the analogous
	 * 1-phase map job, but it is, because GNU wc's output is different when
	 * you "wc < 0-byte-file" than when you "emit_zero_byte_stream | wc".
	 */
	'job': {
	    'phases': [ { 'type': 'reduce', 'exec': 'wc' } ]
	},
	'inputs': [ '/%user%/stor/0bytes' ],
	'timeout': 15 * 1000,
	'expected_outputs': [ /\/%user%\/jobs\/.*\/stor\/reduce\.0\./ ],
	'expected_output_content': [ '      0       0       0\n' ],
	'errors': []
    },

    'jobM0bo': {
	'job': {
	    'phases': [ { 'type': 'map', 'exec': 'true' } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 15 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'expected_output_content': [ '' ],
	'errors': []
    },

    'jobR': {
	'job': {
	    'phases': [ { 'type': 'reduce', 'exec': 'wc' } ]
	},
	'inputs': [
	    '/%user%/stor/obj1',
	    '/%user%/stor/obj2',
	    '/%user%/stor/obj3'
	],
	'timeout': 90 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/reduce\.0\./
	],
	'errors': []
    },

    'jobM0inputs': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'wc'
	    } ]
	},
	'inputs': [],
	'timeout': 15 * 1000,
	'expected_outputs': [],
	'errors': []
    },

    'jobR0inputs': {
	'job': {
	    'phases': [ {
		'type': 'reduce',
		'exec': 'wc'
	    } ]
	},
	'inputs': [],
	'timeout': 15 * 1000,
	'expected_outputs': [ /\/%user%\/jobs\/.*\/stor\/reduce\.0\./ ],
	'errors': [],
	'expected_output_content': [ '      0       0       0\n' ]
    },

    'jobRcatbin': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'mpipe -f /bin/bash'
	    }, {
		'type': 'reduce',
		'exec': 'cat /bin/bash /bin/bash /bin/bash > /var/tmp/exp && ' +
		    'cat > /var/tmp/actual && ' +
		    'diff /var/tmp/exp /var/tmp/actual && echo okay'
	    } ]
	},
	'inputs': [
	    '/%user%/stor/obj1',
	    '/%user%/stor/obj2',
	    '/%user%/stor/obj3'
	],
	'timeout': 60 * 1000,
	'expected_outputs': [ /\/%user%\/jobs\/.*\/stor\/reduce\.1\./ ],
	'errors': [],
	'expected_output_content': [ 'okay\n' ]
    },

    'jobMM': {
	'job': {
	    'phases': [
		{ 'type': 'map', 'exec': 'wc' },
		{ 'type': 'map', 'exec': 'wc' }
	    ]
	},
	'inputs': [
	    '/%user%/stor/obj1',
	    '/%user%/stor/obj2',
	    '/%user%/stor/obj3'
	],
	'timeout': 60 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.1\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj2\.1\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj3\.1\./
	],
	'errors': []
    },

    'jobMR': {
	'job': {
	    'phases': [
		{ 'type': 'map', 'exec': 'wc' },
		{ 'type': 'reduce', 'exec': 'wc' }
	    ]
	},
	'inputs': [
	    '/%user%/stor/obj1',
	    '/%user%/stor/obj2',
	    '/%user%/stor/obj3'
	],
	'timeout': 60 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/reduce\.1\./
	],
	'errors': []
    },

    'jobMMRR': {
	'job': {
	    'phases': [
		{ 'type': 'map', 'exec': 'wc' },
		{ 'type': 'map', 'exec': 'wc' },
		{ 'type': 'reduce', 'exec': 'wc' },
		{ 'type': 'reduce', 'exec': 'wc' }
	    ]
	},
	'inputs': [
	    '/%user%/stor/obj1',
	    '/%user%/stor/obj2',
	    '/%user%/stor/obj3'
	],
	'timeout': 90 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/reduce\.3\./
	],
	'errors': []
    },

    'jobMRRoutput': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'for i in {1..10}; do echo $i; done | msplit -n 3'
	    }, {
		'type': 'reduce',
		'count': 3,
		'exec': 'awk \'{sum+=$1} END {print sum}\''
	    }, {
		'type': 'reduce',
		'exec': 'awk \'{sum+=$1} END {print sum}\''
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 90 * 1000,
	'expected_outputs': [ /\/%user%\/jobs\/.*\/stor\/reduce\.2\./ ],
	'expected_output_content': [ '55\n' ],
	'errors': []
    },

    'jobMMRIntermedRm': {
	/*
	 * The following tests exercise some corner cases in deleting
	 * intermediate objects.  First, verify that everything works normally
	 * even if the user removes an intermediate output object before we get
	 * to it.
	 */
	'job': {
	    'phases': [
		{ 'type': 'map', 'exec': 'wc' },
		{ 'type': 'map',
		    'exec': 'mrm $MANTA_INPUT_OBJECT && echo hello' },
		{ 'type': 'reduce', 'exec': 'cat' }
	    ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 90 * 1000,
	'expected_outputs': [ /\/%user%\/jobs\/.*\/stor\/reduce\.2\./ ],
	'errors': [],
	'expected_output_content': [ 'hello\n' ]
    },

    'jobMMRIntermedDir': {
	/*
	 * Next, make sure that everything works even if the user turns the
	 * intermediate object into a directory.
	 * The "mrm" isn't currently necessary because of MANTA-1852, but we use
	 * it here to avoid breaking when that bug is fixed.
	 */
	'job': {
	    'phases': [
		{ 'type': 'map', 'exec': 'wc' },
		{ 'type': 'map', 'exec': 'mrm $MANTA_INPUT_OBJECT && ' +
		    'mmkdir $MANTA_INPUT_OBJECT && echo hello' },
		{ 'type': 'reduce', 'exec': 'cat' }
	    ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 90 * 1000,
	'expected_outputs': [ /\/%user%\/jobs\/.*\/stor\/reduce\.2\./ ],
	'errors': [],
	'expected_output_content': [ 'hello\n' ]
    },

    'jobMMRIntermedDirNonEmpty': {
	/*
	 * Finally, make sure that things continue to work even if the user
	 * turns the intermediate object into a non-empty directory.
	 * The "mrm" isn't currently necessary because of MANTA-1852, but we use
	 * it here to avoid breaking when that bug is fixed.
	 */
	'job': {
	    'phases': [
		{ 'type': 'map', 'exec': 'wc' },
		{ 'type': 'map', 'exec': 'mrm $MANTA_INPUT_OBJECT && ' +
		    'mmkdir $MANTA_INPUT_OBJECT && ' +
		    'mput $MANTA_INPUT_OBJECT/urd && echo hello' },
		{ 'type': 'reduce', 'exec': 'cat' }
	    ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 90 * 1000,
	'expected_outputs': [ /\/%user%\/jobs\/.*\/stor\/reduce\.2\./ ],
	'errors': [],
	'expected_output_content': [ 'hello\n' ],
	'extra_objects': [ /\/%user%\/jobs\/.*stor\/.*urd$/ ]
    },

    'jobMcancel': {
	'job': {
	    'phases': [
		{ 'type': 'map', 'exec': 'wc && sleep 3600' }
	    ]
	},
	'inputs': [
	    '/%user%/stor/obj1',
	    '/%user%/stor/obj2',
	    '/%user%/stor/obj3'
	],
	'timeout': 30 * 1000,
	'expected_outputs': [],
	'post_submit': function (api, jobid) {
	    setTimeout(function () {
		    log.info('cancelling job');
		    api.jobCancel(jobid, function (err) {
			    if (err) {
				log.fatal('failed to cancel job');
				throw (err);
			    }

			    log.info('job cancelled');
		    });
	    }, 10 * 1000);
	},
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'input': '/%user%/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_JOBCANCELLED,
	    'message': 'job was cancelled'
	}, {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj2"',
	    'input': '/%user%/stor/obj2',
	    'p0input': '/%user%/stor/obj2',
	    'code': EM_JOBCANCELLED,
	    'message': 'job was cancelled'
	}, {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj3"',
	    'input': '/%user%/stor/obj3',
	    'p0input': '/%user%/stor/obj3',
	    'code': EM_JOBCANCELLED,
	    'message': 'job was cancelled'
	} ]
    },

    'jobMtmpfs': {
	/*
	 * Tests that there's no tmpfs mounted at /tmp.  Using tmpfs there
	 * results in annoying issues related to memory management for no
	 * measurable performance improvements over ZFS in many cases.  This
	 * test exists because we've inadvertently reintroduced tmpfs a few
	 * times.
	 */
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'set -o pipefail; set -o errexit; ' +
		    'df /tmp | awk \'NR == 2{ print $NF }\'; ' +
		    'mount | awk \'$3 == "swap"{ print $1 }\' | ' +
		        'grep ^/tmp || echo "okay"'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 30 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./ ],
	'errors': [],
	'expected_output_content': [ '/\nokay\n' ]
    },

    'jobMasset': {
	'job': {
	    'assets': {
		'/%user%/stor/test_asset.sh':
		    '#!/bin/bash\n' +
		    'echo "sarabi" "$*"\n'
	    },
	    'phases': [ {
		'assets': [ '/%user%/stor/test_asset.sh' ],
		'type': 'map',
		'exec': '/assets/%user%/stor/test_asset.sh 17'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 30 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'expected_output_content': [ 'sarabi 17\n' ],
	'errors': []
    },

    'jobMcore': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'node -e "process.abort();"'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 60 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'input': '/%user%/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_USERTASK,
	    'message': 'user command or child process dumped core',
	    'core': /\/%user%\/jobs\/.*\/stor\/cores\/0\/core.node./
	} ]
    },

    'jobMdiskDefault': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'df --block-size=M / | awk \'{print $4}\' | tail -1'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 15 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'expected_output_content': [ /^81\d\dM\n$/ ],
	'errors': []
    },

    'jobMdiskExtended': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'df --block-size=M / | awk \'{print $4}\' | tail -1',
		'disk': 16
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 15 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'expected_output_content': [ /^163\d\dM\n$/ ],
	'errors': []
    },

    'jobMmemoryDefault': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'prtconf | grep -i memory'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 15 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'expected_output_content': [ 'Memory size: 1024 Megabytes\n' ],
	'errors': []
    },

    'jobMmemoryExtended': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'prtconf | grep -i memory',
		'memory': 512
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 15 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'expected_output_content': [ 'Memory size: 512 Megabytes\n' ],
	'errors': []
    },

    'jobMmget': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'mget $MANTA_INPUT_OBJECT > /var/tmp/tmpfile; ' +
		    'diff $MANTA_INPUT_FILE /var/tmp/tmpfile && echo okay'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 30 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'expected_output_content': [ 'okay\n' ],
	'errors': []
    },

    'jobMmls': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'mls $MANTA_INPUT_OBJECT'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 30 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'expected_output_content': [ 'obj1\n' ],
	'errors': []
    },

    'jobMmjob': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'mjob inputs $MANTA_JOB_ID'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 30 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'expected_output_content': [ '/%user%/stor/obj1\n' ],
	'errors': []
    },

    'jobMRnormalize': {
	'job': {
	    'phases': [ {
		'type': 'reduce',
		'exec': 'mcat /%user%//stor/obj1'
	    }, {
		'type': 'map',
		'exec': 'wc -w'
	    } ]
	},
	'inputs': [ '/%user%/stor///obj1' ],
	'timeout': 30 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.1\./
	],
	'expected_output_content': [ '5\n' ],
	'errors': []
    },

/*
 * The following several tests validate that we properly deal with object names
 * with characters that require encoding.  The first job exercises their use in:
 * - job input API
 * - jobinput, task, taskinput, and taskoutput records
 * - mcat, mpipe, and default stdout capture
 * - job output API
 */
    'jobMMRenc': {
	'job': {
	    'phases': [ {
		/* Covers input API, jobinput, task, taskoutput, and mcat. */
		'type': 'map',
		'exec': 'mcat "$MANTA_INPUT_OBJECT"'
	    }, {
		/* Covers use in default stdout capture. */
		'type': 'map',
		'exec': 'cat && echo' /* append newline so we can sort */
	    }, {
		/* Covers use in reduce, taskinput, mpipe, and output API. */
		'type': 'reduce',
		'exec': 'sort | mpipe "${MANTA_OUTPUT_BASE} with spaces"'
	    } ]
	},
	'inputs': [
	    '/%user%/stor/my dir',
	    '/%user%/stor/my obj1', /* normal case (should be encoded) */
	    '/%user%/stor/my dir/my obj',	/* ditto, in dir with spaces */
	    '/%user%/stor/M%41RK'	/* should be encoded, and we should */
					/* never see "MARK" */
	],
	'timeout': 45 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/reduce\.2\.* with spaces/
	],
	'expected_output_content': [
	    'auto-generated content for key /someuser/stor/M%41RK\n' +
	    'auto-generated content for key /someuser/stor/my dir/my obj\n' +
	    'auto-generated content for key /someuser/stor/my obj1\n'
	],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/my dir"',
	    'input': '/%user%/stor/my dir',
	    'p0input': '/%user%/stor/my dir',
	    'code': EM_INVALIDARGUMENT,
	    'message': 'objects of type "directory" are not supported: ' +
		'"/%user%/stor/my dir"'
	} ]
    },

    'jobRassetEncoding': {
	/*
	 * Tests that assets with names that require encoding work.
	 */
	'job': {
	    'assets': {
		'/%user%/stor/hello 1': '1234'
	    },
	    'phases': [ {
		'type': 'reduce',
		'assets': [ '/%user%/stor/hello 1' ],
		'exec': 'find /assets -type f'
	    } ]
	},
	'inputs': [],
	'timeout': 30 * 1000,
	'errors': [],
	'expected_outputs': [ /\/%user%\/jobs\/.*\/reduce.0./ ],
	'expected_output_content': [ '/assets/%user%/stor/hello 1\n' ]
    },

    'jobMkill': {
	'job': {
	    'phases': [ { 'type': 'map', 'exec': 'sleep 86400 &' } ]
	},
	'inputs': [
	    '/%user%/stor/obj1'
	],
	'timeout': 15 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'errors': []
    },

    'jobMerrorEnc': {
	/*
	 * This job does something similar, but exercises stderr capture and the
	 * job error API.
	 */
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'grep nothing_here "$MANTA_INPUT_OBJECT"'
	    } ]
	},
	'inputs': [
	    '/%user%/stor/my obj1',	/* see above */
	    '/%user%/stor/M%41RK'	/* see above */
	],
	'timeout': 15 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/my obj1"',
	    'input': '/%user%/stor/my obj1',
	    'p0input': '/%user%/stor/my obj1',
	    'code': EM_USERTASK,
	    'stderr': /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/my obj1.0.err/
	}, {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/M%41RK"',
	    'input': '/%user%/stor/M%41RK',
	    'p0input': '/%user%/stor/M%41RK',
	    'code': EM_USERTASK,
	    'stderr': /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/M%41RK.0.err/
	} ]
    },

    'jobMerrorMemoryTooBig': {
	/*
	 * This test relies on the fact that the systems where we run the test
	 * suite don't support even a single task with this much memory.
	 */
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'prtconf | grep -i memory',
		'memory': 8192
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 15 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'input': '/%user%/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_TASKINIT,
	    'message': 'failed to dispatch task: not enough memory available'
	} ]
    },

    'jobMerrorDiskTooBig': {
	/*
	 * Ditto, for disk.
	 */
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'df --block-size=M / | awk \'{print $4}\' | tail -1',
		'disk': 1024
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 15 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'input': '/%user%/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_TASKINIT,
	    'message': 'failed to dispatch task: ' +
	        'not enough disk space available'
	} ]
    },

    'jobMerrorsDispatch0': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'wc'
	    } ]
	},
	'inputs': inputs_disperrors,
	'extra_inputs': extrainputs_disperrors,
	'timeout': 20 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'expected_output_content': [ ' 0  5 50\n' ],
	'errors': errors_disperrors0
    },

    'jobMerrorsDispatch1': {
	/*
	 * This job behaves just like jobMerrorsDispatch0, but makes sure that
	 * this also works when these same errors occur in phases > 0, which
	 * goes through a slightly different code path.
	 *
	 * The job inputs themselves aren't used by the job, but those same
	 * inputs are referenced by the "mcat" phase, so the test suite has to
	 * make sure they're present.
	 *
	 * The job will include errors that match the ones in
	 * jobMerrorsDispatch0, for the same reason as in that test case, but
	 * here we're mostly interested in checking the other errors.
	 */
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'mcat ' + inputs_disperrors.concat(
		    extrainputs_disperrors).join(' ')
	    }, {
		'type': 'map',
		'exec': 'wc'
	    } ]
	},
	'inputs': inputs_disperrors,
	'extra_inputs': extrainputs_disperrors,
	'timeout': 30 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.1\./
	],
	'expected_output_content': [ ' 0  5 50\n' ],
	/*
	 * Unlike the phase-0 errors above, many of these errors are duplicated
	 * because we test both /foo and /foo/, and these both get normalized to
	 * /foo by the "mcat" mechanism.
	 */
	'errors': errors_disperrors0.concat([ {
	    'phaseNum': '1',
	    'what': 'phase 1: input "/notavalidusername/stor/obj1" ' +
		'(from job input "/%user%/stor/obj1")',
	    'input': '/notavalidusername/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_RESOURCENOTFOUND,
	    'message': 'no such object: "/notavalidusername/stor/obj1"'
	}, {
	    'phaseNum': '1',
	    'what': 'phase 1: input "/%user%/stor/notavalidfilename" ' +
		'(from job input "/%user%/stor/obj1")',
	    'input': '/%user%/stor/notavalidfilename',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_RESOURCENOTFOUND,
	    'message': 'no such object: "/%user%/stor/notavalidfilename"'
	}, {
	    'phaseNum': '1',
	    'what': 'phase 1: input "/%user%/stor/mydir" ' +
		'(from job input "/%user%/stor/obj1")',
	    'input': '/%user%/stor/mydir',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_INVALIDARGUMENT,
	    'message': 'objects of type "directory" are not supported: ' +
		'"/%user%/stor/mydir"'
	}, {
	    'phaseNum': '1',
	    'what': 'phase 1: input "/" ' +
	        '(from job input "/%user%/stor/obj1")',
	    'input': '/',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_RESOURCENOTFOUND,
	    'message': 'malformed object name: "/"'
	}, {
	    'phaseNum': '1',
	    'what': 'phase 1: input "/%user%" ' +
	        '(from job input "/%user%/stor/obj1")',
	    'input': '/%user%',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_RESOURCENOTFOUND,
	    'message': 'malformed object name: "/%user%"'
	}, {
	    'phaseNum': '1',
	    'what': 'phase 1: input "/%user%" ' +
	        '(from job input "/%user%/stor/obj1")',
	    'input': '/%user%',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_RESOURCENOTFOUND,
	    'message': 'malformed object name: "/%user%"'
	}, {
	    'phaseNum': '1',
	    'what': 'phase 1: input "/%user%/stor" ' +
	        '(from job input "/%user%/stor/obj1")',
	    'input': '/%user%/stor',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_RESOURCENOTFOUND,
	    'message': 'no such object: "/%user%/stor"'
	}, {
	    'phaseNum': '1',
	    'what': 'phase 1: input "/%user%/stor" ' +
	        '(from job input "/%user%/stor/obj1")',
	    'input': '/%user%/stor',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_RESOURCENOTFOUND,
	    'message': 'no such object: "/%user%/stor"'
	}, {
	    'phaseNum': '1',
	    'what': 'phase 1: input "/%user%/jobs" ' +
	        '(from job input "/%user%/stor/obj1")',
	    'input': '/%user%/jobs',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_RESOURCENOTFOUND,
	    'message': 'no such object: "/%user%/jobs"'
	}, {
	    'phaseNum': '1',
	    'what': 'phase 1: input "/%user%/jobs" ' +
	        '(from job input "/%user%/stor/obj1")',
	    'input': '/%user%/jobs',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_RESOURCENOTFOUND,
	    'message': 'no such object: "/%user%/jobs"'
	} ])
    },

    'jobRerrorReboot': {
	/*
	 * It's important that users aren't able to reboot zones.  If they
	 * could, our metering data would get blown away, and users could steal
	 * compute.
	 */
	'job': {
	    'phases': [ {
		'type': 'reduce',
		'exec': 'uadmin 2 1; uadmin 1 1'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 15 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: reduce',
	    'code': 'UserTaskError',
	    'message': 'user command exited with code 1'
	} ]
    },

    'jobMerrorRebootOutput': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'uadmin 2 1 2>&1; uadmin 1 1 2>&1; true'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 15 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./ ],
	'expected_output_content': [ 'uadmin: Not owner\nuadmin: Not owner\n' ],
	'errors': []
    },

    'jobMerrorAssetMissing': {
	'job': {
	    'phases': [ {
		'assets': [ '/%user%/stor/notavalidasset' ],
		'type': 'map',
		'exec': 'echo "should not ever get here"'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 15 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'input': '/%user%/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_TASKINIT,
	    'message': 'failed to dispatch task: first of 1 error: error ' +
		'retrieving asset "/%user%/stor/notavalidasset" ' +
		'(status code 404)'
	} ]
    },

    'jobMerrorBadReducer': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'mpipe -r1'
	    }, {
		'type': 'reduce',
		'exec': 'wc'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 60 * 1000,
	'expected_outputs': [ /\/%user%\/jobs\/.*\/reduce.1./ ],
	'errors': [ {
	    'phaseNum': '1',
	    'what': new RegExp('phase 1: input ' +
		'"/%user%/jobs/.*/stor/%user%/stor/obj1.0..*" ' +
		'\\(from job input "/%user%/stor/obj1"\\)'),
	    'input': new RegExp('/%user%/jobs/.*/stor/%user%/stor/obj1.0..*'),
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_INVALIDARGUMENT,
	    'message': 'reducer "1" specified, but only 1 reducers exist'
	} ]
    },

    'jobMerrorVeryBadReducer': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'mpipe -r' + mod_schema.sMaxReducers
	    }, {
		'type': 'reduce',
		'exec': 'wc'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 30 * 1000,
	'expected_outputs': [ /\/%user%\/jobs\/.*\/reduce.1./ ],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'input': '/%user%/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_USERTASK,
	    'message': 'user command exited with code 1'
	} ]
    },

    'jobMerrorLackeyCrash': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'pkill -c $(svcs -Hoctid lackey)'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 30 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'code': EM_INTERNAL,
	    'message': 'internal error'
	} ]
    },

    'jobMerrorCmd': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'grep professor_frink'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 20 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'input': '/%user%/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_USERTASK,
	    'message': 'user command exited with code 1'
	} ]
    },

    'jobMerrorMuskie': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'curl -i -X POST localhost/my/jobs/task/perturb?p=1'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 20 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'input': '/%user%/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_INTERNAL,
	    'message': 'internal error'
	} ]
    },

    'jobMerrorMuskieMpipe': {
	/*
	 * Like jobMerrorMuskie, but with mpipe.  Such errors get translated as
	 * UserTaskErrors in this case.
	 */
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'curl -i -X POST localhost/my/jobs/task/perturb?p=1 ' +
		    '| mpipe'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 20 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'input': '/%user%/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_USERTASK,
	    'message': 'user command exited with code 1'
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

    'jobMerrorMpipeMkdirp': {
	/*
	 * mpipe should not auto-create directories by default, and it should
	 * fail if the directory does not exist.  The case where it creates
	 * directories is tested by jobMpipeNamed.
	 */
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'echo hello | mpipe /%user%/stor/marlin_tests/1/2/3/4'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 20 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'input': '/%user%/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_USERTASK,
	    'message': 'user command exited with code 1'
	} ]
    },

    'jobMerrorBadImage': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'wc',
		'image': '0.0.1'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 20 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'input': '/%user%/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_INVALIDARGUMENT,
	    'message': 'failed to dispatch task: ' +
	        'requested image is not available'
	} ]
    },

    'jobMerrorHsfs': {
	/*
	 * We set fs_allowed=- to disallow users from mounting HSFS, NFS, and
	 * other filesystems because we believe it may be possible to do bad
	 * things to the system if one is allowed to mount these filesystems.
	 * (At least, we haven't proved to ourselves that it's safe.)
	 */
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'mkisofs -o /my.iso /manta && ' +
		    'mkdir -p /mnt2 && ' +
		    'mount -F hsfs /my.iso /mnt2 2>&1; echo $?; find /mnt2'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 20 * 1000,
	'errors': [],
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'expected_output_content': [
	    'mount: insufficient privileges\n33\n/mnt2\n'
	]
    },

    'jobMenv': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'env | egrep "^(MANTA_|HOME)" | sort'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 30 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'errors': [],
	'expected_output_content': [
	    'HOME=/root\n' +
	    'MANTA_INPUT_FILE=/manta/%user%/stor/obj1\n' +
	    'MANTA_INPUT_OBJECT=/%user%/stor/obj1\n' +
	    'MANTA_JOB_ID=$jobid\n' +
	    'MANTA_NO_AUTH=true\n' +
	    'MANTA_OUTPUT_BASE=/%user%/jobs/$jobid/stor/' +
		'%user%/stor/obj1.0.\n' +
	    'MANTA_URL=http://localhost:80/\n' +
	    'MANTA_USER=%user%\n'
	]
    },

    'jobRenv': {
	'job': {
	    'phases': [ {
		'type': 'reduce',
		'count': 3,
		/* Workaround MANTA-992 */
		'exec': 'cat > /dev/null; env | egrep "^(MANTA_|HOME)" | sort'
	    } ]
	},
	'inputs': [],
	'timeout': 30 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/reduce\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/reduce\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/reduce\.0\./
	],
	'errors': [],
	'expected_output_content': [
	    'HOME=/root\n' +
	    'MANTA_JOB_ID=$jobid\n' +
	    'MANTA_NO_AUTH=true\n' +
	    'MANTA_OUTPUT_BASE=/%user%/jobs/$jobid/stor/reduce.0.\n' +
	    'MANTA_REDUCER=0\n' +
	    'MANTA_URL=http://localhost:80/\n' +
	    'MANTA_USER=%user%\n',

	    'HOME=/root\n' +
	    'MANTA_JOB_ID=$jobid\n' +
	    'MANTA_NO_AUTH=true\n' +
	    'MANTA_OUTPUT_BASE=/%user%/jobs/$jobid/stor/reduce.0.\n' +
	    'MANTA_REDUCER=1\n' +
	    'MANTA_URL=http://localhost:80/\n' +
	    'MANTA_USER=%user%\n',

	    'HOME=/root\n' +
	    'MANTA_JOB_ID=$jobid\n' +
	    'MANTA_NO_AUTH=true\n' +
	    'MANTA_OUTPUT_BASE=/%user%/jobs/$jobid/stor/reduce.0.\n' +
	    'MANTA_REDUCER=2\n' +
	    'MANTA_URL=http://localhost:80/\n' +
	    'MANTA_USER=%user%\n'
	]
    },

    'jobMmeterCheckpoints': {
	'job': {
	    'phases': [ { 'type': 'map', 'exec': 'sleep 7' } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 30 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./ ],
	'metering_includes_checkpoints': true,
	'errors': []
    },

    'jobMmeterExitsEarly': {
	/*
	 * This tests that metering data accounts for the whole time a zone is
	 * used, even if that's much longer than a task actually ran for, as in
	 * the case where the reduce task bails out early.
	 */
	'job': {
	    'phases': [ {
		'type': 'map',
		'exec': 'wc'
	    }, {
		'type': 'reduce',
		'exec': 'awk +' /* (awk syntax error exits immediately) */
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 60 * 1000,
	'expected_outputs': [],
	'metering_includes_checkpoints': true,
	'errors': [ { 'code': EM_USERTASK } ],
	'skip_input_end': true,
	'post_submit': function (api, jobid) {
	    setTimeout(function () {
		    log.info('job "%s": ending input late', jobid);
		    api.jobEndInput(jobid, { 'retry': { 'retries': 3 } },
			function (err) {
				if (err) {
					log.fatal('failed to end input');
					throw (err);
				}

				log.info('job input ended');
			});
	    }, 15 * 1000);
	},
	'verify': function (verify) {
		if (verify['metering'] === null) {
			log.warn('job "%s": skipping metering check',
			    verify['jobid']);
			return;
		}

		var meter, taskid, elapsed, ns;
		meter = verify['metering']['cumulative'];
		log.info('job "%s": checking total time elapsed',
		    verify['jobid']);
		for (taskid in meter) {
			elapsed = meter[taskid]['time'];
			ns = elapsed[0] * 1e9 + elapsed[1];

			/*
			 * When running this test concurrently with other jobs,
			 * it's hard to know whether it actually did the right
			 * thing, since it may be some time before the reduce
			 * task gets on-CPU.  We leave it running for 15
			 * seconds, but we can only really assume it will be
			 * running for a few seconds (and obviously that's still
			 * racy).  (We could actually address this race by
			 * opening up a local HTTP server and having the reduce
			 * task hit it when it starts, and only ending input 10s
			 * after that.  If this becomes a problem, we should
			 * just do that.)
			 */
			if (ns >= 5 * 1e9 && ns < 20 * 1e9)
				return;
		}

		log.error('job "%s": no task took 5 < N < 20 seconds',
		    verify['jobid'], meter);
		throw (new Error('no task took 5 < N < 20 seconds'));
	}
    },

    'jobMinit': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'init': 'echo >> /var/tmp/test_temp',
		'exec': 'wc < /var/tmp/test_temp'
	    } ]
	},
	'inputs': [
	    '/%user%/stor/obj1',
	    '/%user%/stor/obj2',
	    '/%user%/stor/obj3',
	    '/%user%/stor/obj4',
	    '/%user%/stor/obj5',
	    '/%user%/stor/obj6',
	    '/%user%/stor/obj7',
	    '/%user%/stor/obj8',
	    '/%user%/stor/obj9'
	],
	'timeout': 60 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj2\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj3\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj4\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj5\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj6\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj7\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj8\.0\./,
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj9\.0\./
	],
	'expected_output_content': [
	    '1 0 1\n',
	    '1 0 1\n',
	    '1 0 1\n',
	    '1 0 1\n',
	    '1 0 1\n',
	    '1 0 1\n',
	    '1 0 1\n',
	    '1 0 1\n',
	    '1 0 1\n'
	],
	'errors': []
    },

    'jobMinitEnv': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'init': 'env | egrep ^MANTA_ | sort > /var/tmp/test_temp',
		'exec': 'cat /var/tmp/test_temp'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 60 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./ ],
	'expected_output_content': [
	    'MANTA_JOB_ID=$jobid\n' +
	    'MANTA_NO_AUTH=true\n' +
	    'MANTA_OUTPUT_BASE=/%user%/jobs/$jobid/stor/' +
		'%user%/stor/obj1.0.\n' +
	    'MANTA_URL=http://localhost:80/\n' +
	    'MANTA_USER=%user%\n'
	],
	'errors': []
    },

    'jobMinitFail': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'init': 'false',
		'exec': 'wc'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 60 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'input': '/%user%/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_TASKINIT,
	    'message': 'user command exited with code 1'
	} ]
    },

    'jobMinitCore': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'init': 'node -e "process.abort();"',
		'exec': 'wc'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 60 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'input': '/%user%/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_TASKINIT,
	    'message': 'user command or child process dumped core',
	    'core': /\/%user%\/jobs\/.*\/stor\/cores\/0\/core.node./
	} ]
    },

    'jobMinitKill': {
	/*
	 * This is a poorly handled (but extremely unlikely) error case, and all
	 * we're really checking is that we do at least report an error rather
	 * than doing the wrong thing.
	 */
	'job': {
	    'phases': [ {
		'type': 'map',
		'init': 'pkill -c $(svcs -Hoctid lackey)',
		'exec': 'wc'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 60 * 1000,
	'expected_outputs': [],
	'errors': [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/obj1"',
	    'input': '/%user%/stor/obj1',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_INTERNAL,
	    'message': 'internal error'
	} ]
    },

    'jobMinitKillAfter': {
	'job': {
	    'phases': [ {
		'type': 'map',
		'init': 'echo >> /var/tmp/test_temp',
		/*
		 * We have to hack around the error-on-lackey-crash behavior in
		 * order to test that we do the right thing with respect to
		 * "init".
		 */
		'exec': 'if [[ -f /var/tmp/ranonce ]]; then\n' +
		    'wc < /var/tmp/test_temp\n' +
		    'else\n' +
		    'echo > /var/tmp/ranonce\n' +
		    'rm -f /var/tmp/.marlin_task_started\n' +
		    'pkill -c $(svcs -Hoctid lackey)\n' +
		    'fi'
	    } ]
	},
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 60 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./ ],
	'expected_output_content': [ '1 0 1\n' ],
	'errors': []
    },

/*
 * "Corner case" tests.  See jobsCornerCases below.
 */

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
    },

/*
 * "Stress" tests: these tests operate on larger numbers of inputs.
 */

    'jobM500': {
	'job': {
	    'phases': [ { 'type': 'map', 'exec': 'wc' } ]
	},
	'inputs': [],
	'timeout': 90 * 1000,
	'expected_outputs': [],
	'errors': []
    },

    'jobMR1000': {
	'job': {
	    'phases': [
		{ 'type': 'map', 'exec': 'wc' },
		{ 'type': 'reduce', 'exec': 'wc' }
	    ]
	},
	'inputs': [],
	'timeout': 180 * 1000,
	'expected_outputs': [ /%user%\/jobs\/.*\/stor\/reduce\.1\./ ],
	'expected_output_content': [ '   1000    3000    9000\n' ],
	'errors': []
    },

    'jobM4RR1000': {
	'job': {
	    'phases': [
		{ 'type': 'map', 'exec': 'wc' },
		{ 'type': 'reduce', 'count': 4, 'exec': 'wc' },
		{ 'type': 'reduce',
		    'exec': 'awk \'{sum+=$1} END { print sum }\'' }
	    ]
	},
	'inputs': [],
	'timeout': 300 * 1000,
	'expected_outputs': [ /%user%\/jobs\/.*\/stor\/reduce\.2\./ ],
	'expected_output_content': [ '1000\n' ],
	'errors': []
    }

/*
 * Authentication/authorization tests are generated dynamically by
 * initAuthzTestCases() below.
 */
};

/*
 * Initialize a few of the larger-scale test cases for which it's easier to
 * generate the inputs and expected outputs programmatically.
 */
function initTestCases()
{
	var job = testcases.jobM500;

	for (var i = 0; i < 500; i++) {
		var key = '/%user%/stor/obj' + i;
		var okey = '/%user%/jobs/.*/stor' + key;

		job['inputs'].push(key);
		job['expected_outputs'].push(new RegExp(okey));
	}

	job = testcases.jobMR1000;
	for (i = 0; i < 1000; i++) {
		key = '/%user%/stor/obj' + i;
		job['inputs'].push(key);
	}

	job = testcases.jobM4RR1000;
	for (i = 0; i < 1000; i++) {
		key = '/%user%/stor/obj' + i;
		job['inputs'].push(key);
	}

	job = testcases.jobMerrorMuskieRetry;
	for (i = 0; i < 100; i++) {
		key = '/%user%/stor/obj' + i;
		job['inputs'].push(key);
	}

	testcases.jobMerrorMuskieRetryMpipe['inputs'] =
	    testcases.jobMerrorMuskieRetry['inputs'];
	testcases.jobRmuskieRetry['inputs'] =
	    testcases.jobMerrorMuskieRetry['inputs'];

	for (var k in testcases)
		testcases[k]['label'] = k;

	initAuthzUfdsConfig();
	initAuthzTestCases();
}

/*
 * In order to exhaustively test access control with jobs, we have to cover
 * several cases.  We'll define these accounts and users:
 *
 *     Account A: users A1, A2
 *     Account B: user B1
 *     Operator account O
 *
 * and we'll test this matrix:
 *
 *    PATH               A   A1   A2  B/B1   O
 *    /A/public/X        *    *    *    *    *    (any object under /public)
 *    /A/stor/A          *                   *    (A creates object under /stor)
 *    /A/stor/pub        *    *    *    *    *    (public object under /stor)
 *    /A/stor/A1         *    *              *    (object readable only by A1)
 *  * /A/stor/B1         *              *    *    (object readable by B1)
 *  * /A/stor/B          *    *         *    *    (object readable by B)
 *
 * XXX The starred test cases are blocked on MANTA-2171.
 * XXX Test cases for the "conditions" that Marlin supplies (e.g., fromjob=true)
 *     are blocked on joyent/node-aperture#1
 */
function initAuthzUfdsConfig()
{
	/*
	 * Generate the UFDS configuration that we're going to use for all of
	 * the authz/authn test cases.  It would be nice if this were totally
	 * statically declared, but in order to use variables as the account
	 * names so that we can change them easily, we have to create the
	 * configuration programmatically.
	 */
	authzUfdsConfig = {};
	authzUfdsConfig[authzAccountA] = {
	    'subusers': [ authzUserA1, authzUserA2, authzUserAnon ],
	    'template': 'poseidon',
	    'keyid': process.env['MANTA_KEY_ID'],
	    'policies': {
	        'readall': 'can getobject',
	        'readjob': 'can getobject when fromjob = true',
	        'readhttp': 'can getobject when fromjob = false'
	    },
	    'roles': {}
	};
	authzUfdsConfig[authzAccountB] = {
	    'subusers': [ authzUserB1 ],
	    'template': 'poseidon',
	    'keyid': process.env['MANTA_KEY_ID']
	};

	for (var pk in authzUfdsConfig[authzAccountA].policies) {
		authzUfdsConfig[authzAccountA].roles[
		    authzUserA1 + '-' + pk] = {
		    'user': authzUserA1,
		    'policy': pk
		};
		authzUfdsConfig[authzAccountA].roles[
		    authzUserAnon + '-' + pk] = {
		    'user': authzUserAnon,
		    'policy': pk
		};
	}
}

function initAuthzTestCases()
{
	var template, tcconfig, expected_outputs, expected_errors;

	/*
	 * Generate the actual authz/authn test cases, starting with a template
	 * that we'll use for all of these test cases.
	 */
	template = {
	    'pre_submit': setupAuthzTestCase,
	    'job': {
		'phases': [ { 'exec': 'cat' } ]
	    },
	    'inputs': [],
	    'extra_inputs': [
		'/%user%/public/X',
		'/%user%/stor/A',
		'/%user%/stor/pub',
		'/%user%/stor/U1'
	    ],
	    'timeout': 30 * 1000,
	    'errors': [],
	    'expected_outputs': []
	};

	/*
	 * Generate the names of the expected outputs for each input, assuming
	 * authorization succeeds.
	 */
	expected_outputs = [
	    /\/%jobuser%\/jobs\/.*\/stor\/%user%\/public\/X/,
	    /\/%jobuser%\/jobs\/.*\/stor\/%user%\/stor\/A/,
	    /\/%jobuser%\/jobs\/.*\/stor\/%user%\/stor\/pub/,
	    /\/%jobuser%\/jobs\/.*\/stor\/%user%\/stor\/U1/
	];

	/*
	 * Generate the names of the expected errors for each input, assuming
	 * authorization fails.
	 */
	expected_errors = [ {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/public/X"',
	    'code': EM_AUTHORIZATION,
	    'message': 'permission denied: "/%user%/public/X"',
	    'input': '/%user%/public/X',
	    'p0input': '/%user%/public/X'
	}, {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/A"',
	    'code': EM_AUTHORIZATION,
	    'message': 'permission denied: "/%user%/stor/A"',
	    'input': '/%user%/stor/A',
	    'p0input': '/%user%/stor/A'
	}, {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/pub"',
	    'code': EM_AUTHORIZATION,
	    'message': 'permission denied: "/%user%/stor/pub"',
	    'input': '/%user%/stor/pub',
	    'p0input': '/%user%/stor/pub'
	}, {
	    'phaseNum': '0',
	    'what': 'phase 0: input "/%user%/stor/U1"',
	    'code': EM_AUTHORIZATION,
	    'message': 'permission denied: "/%user%/stor/U1"',
	    'input': '/%user%/stor/U1',
	    'p0input': '/%user%/stor/U1'
	} ];

	/*
	 * Now, enumerate the test cases we want to build.  This is basically a
	 * straight translation of the above ASCII table.  The definition of
	 * each test case here describes only those properties that differ from
	 * the template, and instead of repeating each of the regular
	 * expressions and objects for the expected outputs and errors
	 * (respectively), we just enumerate the inputs that are expected to
	 * succeed.  The code below constructs actual test cases with the
	 * appropriate set of expected outputs and errors.
	 */
	tcconfig = [ {
	    'label': 'authzA',		/* jobMauthzA, jobRauthzA */
	    'account': authzAccountA,
	    'ok': [ 0, 1, 2, 3 ]
	}, {
	    'label': 'authzA1',		/* jobMauthzA1, jobRauthzA2 */
	    'account': authzAccountA,
	    'user': authzUserA1,
	    'ok': [ 0, 2, 3 ]
	}, {
	    'label': 'authzA2',		/* jobMauthzA2, jobRauthzA2 */
	    'account': authzAccountA,
	    'user': authzUserA2,
	    'ok': [ 0, 2 ]
	}, {
	    'label': 'authzB',		/* jobMauthzB, jobRauthzB */
	    'account_objects': authzAccountA,
	    'account': authzAccountB,
	    'ok': [ 0, 2 ]
	}, {
	    'label': 'authzB1',		/* jobMauthzB1, jobRauthzB1 */
	    'account_objects': authzAccountA,
	    'account': authzAccountB,
	    'ok': [ 0, 2 ]
	}, {
	    'label': 'authzOp',		/* jobMauthzOp, jobRauthzOp */
	    'account_objects': authzAccountA,
	    'account': process.env['MANTA_USER'],
	    'ok': [ 0, 1, 2, 3 ]
	}, {
	/*
	 * The legacy mechanism doesn't affect account A's ability to access
	 * its own objects.
	 */
	    'label': 'authzLegacyA', /* jobMauthzLegacyA, jobRauthzLegacyA */
	    'legacy_auth': true,
	    'account': authzAccountA,
	    'ok': [ 0, 1, 2, 3 ]
	}, {
	/*
	 * Using legacy authorization, account B cannot access public
	 * objects under account A's private directory because the
	 * credentials aren't available to allow that.
	 */
	    'label': 'authzLegacyB', /* jobMauthzLegacyB, jobRauthzLegacyB */
	    'legacy_auth': true,
	    'account_objects': authzAccountA,
	    'account': authzAccountB,
	    'ok': [ 0 ]
	}, {
	    'label': 'authzLegacyOp', /* jobMauthzLegacyOp, jobRauthzLegacyOp */
	    'account_objects': authzAccountA,
	    'account': process.env['MANTA_USER'],
	    'ok': [ 0, 1, 2, 3 ]
	} ];

	/*
	 * Generate proper test cases from the above descriptions.  We do this
	 * by copying the test case template, overriding properties from the
	 * description, and using the "ok" array to select the right set of
	 * expected outputs and errors.  We create both "map" and "reduce" test
	 * cases for the non-legacy tests, though the reduce case really tests
	 * token pass-through and muskie semantics (which is why we can't really
	 * do legacy tests for these).
	 */
	tcconfig.forEach(function (tccfg) {
		var testcase;

		testcase = authzMakeOneTestCase(template, tccfg,
		    expected_errors, 'jobM');
		testcase['job']['phases'][0]['type'] = 'map';
		tccfg['ok'].forEach(function (i) {
			testcase['expected_outputs'].push(expected_outputs[i]);
		});

		mod_assert.equal(testcase['errors'].length +
		    testcase['expected_outputs'].length,
		    expected_outputs.length);
		mod_assert.ok(!testcases.hasOwnProperty(testcase['label']));
		testcases[testcase['label']] = testcase;
		exports.jobsMain.push(testcase);

		if (tccfg['legacy_auth'])
			return;

		testcase = authzMakeOneTestCase(template, tccfg,
		    expected_errors, 'jobR');
		testcase['job']['phases'][0]['type'] = 'reduce';
		testcase['expected_outputs'].push(new RegExp(''));
		mod_assert.ok(!testcases.hasOwnProperty(testcase['label']));
		testcases[testcase['label']] = testcase;
		exports.jobsMain.push(testcase);
	});
}

function authzMakeOneTestCase(template, tccfg, expected_errors, labelprefix)
{
	var testcase, p;

	testcase = mod_jsprim.deepCopy(template);
	for (p in tccfg) {
		if (p == 'ok')
			continue;

		if (p == 'label') {
			testcase[p] = labelprefix + tccfg[p];
			continue;
		}

		testcase[p] = tccfg[p];
	}

	mod_assert.ok(testcase['label']);
	expected_errors.forEach(function (_, i) {
		if (tccfg['ok'].indexOf(i) == -1)
			testcase['errors'].push(expected_errors[i]);
	});

	return (testcase);
}

/*
 * pre_submit() function for each of the authz/authn test cases.  This is
 * responsible for applying the UFDS configuration that we use for all of these
 * cases, as well as making sure the corresponding Manta objects exist.
 */
var setupAuthzQueue = mod_vasync.queue(function (f, cb) { f(cb); }, 1);
function setupAuthzTestCase(api, callback)
{
	mod_vasync.pipeline({
	    'funcs': [
		function applyUfdsConfig(_, next) {
			/*
			 * Because ufdsMakeAccounts() is not atomic or
			 * concurrent-safe (but is idempotent), we run all calls
			 * through a queue.
			 */
			setupAuthzQueue.push(function (queuecb) {
				mod_maufds.ufdsMakeAccounts({
				    'log': mod_testcommon.log,
				    'manta': api.manta,
				    'ufds': mod_testcommon.ufdsClient(),
				    'config': authzUfdsConfig
				}, function (err) {
					queuecb();
					next(err);
				});
			});
		},

		function createMantaObjects(_, next) {
			var manta = mod_testcommon.manta;
			var content = 'expected content\n';
			var size = content.length;

			mod_vasync.forEachParallel({
			    'inputs': authzMantaObjects,
			    'func': function (objcfg, putcb) {
				var account, roles, path, stream;
				var headers, options;

				mod_assert.equal('string',
				    typeof (objcfg.account));
				mod_assert.equal('string',
				    typeof (objcfg.name));
				mod_assert.equal('string',
				    typeof (objcfg.label));

				account = objcfg.account;
				roles = objcfg.roles || [];
				mod_assert.ok(Array.isArray(roles));
				path = sprintf('/%s/%s/%s',
				    account, objcfg.public ? 'public' : 'stor',
				    objcfg.name);

				headers = {};
				if (roles.length > 0)
					headers['role-tag'] = roles.join(', ');

				options = {
				    'size': size,
				    'headers': headers
				};

				stream = new StringInputStream(content);
				log.info({
				    'path': path
				}, 'creating object', { 'headers': headers });
				manta.put(path, stream, options, putcb);
			    }
			}, next);
		}
	    ]
	}, callback);
}

exports.DEFAULT_USER = DEFAULT_USER;
exports.testcases = testcases;

/*
 * There are four groups of tests:
 *
 *	"main"
 *
 *		The main body of quick, functional tests.  Most tests go here.
 *
 *      "corner case"
 *
 *		A few particularly expensive cases that are important to run
 *		before integrating, but which shouldn't be part of an automated
 *		stress-test loop.  These often trigger multi-minute timeouts,
 *		during which not much is happening, so they don't make good
 *		candidates for testing crash-safety.
 *
 *	"all"
 *
 *		The "main" and "corner case" tests combined, which make up the
 *		standard automated regression suite.
 *
 *	"stress"
 *
 *		A few additional jobs that exercise larger numbers of inputs,
 *		outputs, tasks, and task inputs.  These are useful to run by
 *		hand, but are not part of the automated suite because they would
 *		be too expensive to run on smaller systems.
 */
exports.jobsMain = [
    testcases.jobM,
    testcases.jobMcrossAccountLink,
    testcases.jobMimage,
    testcases.jobMX,
    testcases.jobMqparams,
    testcases.jobMmpipeAnon,
    testcases.jobMmpipeNamed,
    testcases.jobMmcat,
    testcases.jobM0bi,
    testcases.jobR0bi,
    testcases.jobM0bo,
    testcases.jobR,
    testcases.jobR0inputs,
    testcases.jobRcatbin,
    testcases.jobMM,
    testcases.jobMR,
    testcases.jobMMRR,
    testcases.jobMRRoutput,
    testcases.jobMMRIntermedRm,
    testcases.jobMMRIntermedDir,
    testcases.jobMMRIntermedDirNonEmpty,
    testcases.jobMcancel,
    testcases.jobMtmpfs,
    testcases.jobMasset,
    testcases.jobMcore,
    testcases.jobMdiskDefault,
    testcases.jobMdiskExtended,
    testcases.jobMmemoryDefault,
    testcases.jobMmemoryExtended,
    testcases.jobMmget,
    testcases.jobMmls,
    testcases.jobMmjob,
    testcases.jobMRnormalize,
    testcases.jobMMRenc,
    testcases.jobRassetEncoding,
    testcases.jobMkill,
    testcases.jobMerrorEnc,
    testcases.jobMerrorMemoryTooBig,
    testcases.jobMerrorDiskTooBig,
    testcases.jobMerrorsDispatch0,
    testcases.jobMerrorsDispatch1,
    testcases.jobRerrorReboot,
    testcases.jobMerrorRebootOutput,
    testcases.jobMerrorAssetMissing,
    testcases.jobMerrorBadReducer,
    testcases.jobMerrorVeryBadReducer,
    testcases.jobMerrorLackeyCrash,
    testcases.jobMerrorCmd,
    testcases.jobMerrorMuskie,
    testcases.jobMerrorMuskieMpipe,
    testcases.jobMerrorMuskieRetryMpipe,
    testcases.jobMerrorMpipeMkdirp,
    testcases.jobMerrorBadImage,
    testcases.jobMerrorHsfs,
    testcases.jobMenv,
    testcases.jobRenv,
    testcases.jobMmeterCheckpoints,
    testcases.jobMmeterExitsEarly,
    testcases.jobMinit,
    testcases.jobMinitEnv,
    testcases.jobMinitFail,
    testcases.jobMinitCore,
    testcases.jobMinitKill,
    testcases.jobMinitKillAfter
];

/* See comment above. */
exports.jobsCornerCases = [
    testcases.jobMerrorOom,
    testcases.jobMerrorDisk,
    testcases.jobMerrorLackeyOom,
    testcases.jobMerrorMuskieRetry,
    testcases.jobRmuskieRetry,
    testcases.jobRerrorMuskieRetry
];

/* See comment above. */
exports.jobsAll = exports.jobsCornerCases.concat(exports.jobsMain);

/* See comment above. */
exports.jobsStress = exports.jobsMain.concat([
    testcases.jobM500,
    testcases.jobMR1000,
    testcases.jobM4RR1000
]);

initTestCases();
