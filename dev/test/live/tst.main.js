/*
 * tst.main.js: The main set of quick, functional test cases in the Marlin test
 * suite.
 */

var mod_fs = require('fs');
var mod_path = require('path');
var mod_extsprintf = require('extsprintf');
var mod_schema = require('../../lib/schema');
var mod_testcommon = require('../common');
var mod_livetests = require('./common');
var mod_vasync = require('vasync');

/* jsl:import ../../../common/lib/errors.js */
require('../../lib/errors');

var sprintf = mod_extsprintf.sprintf;
var log = mod_testcommon.log;
var StringInputStream = mod_testcommon.StringInputStream;

var concurrency = 5;	/* how many tests to run at once */

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
    'code': EM_INVALIDARGUMENT,
    'message': 'objects of type "directory" are not supported: "/%user%/stor"'
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
	    var user2 = mod_livetests.DEFAULT_USER;
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
		'image': '13.3.6',
		'exec': 'grep 13.3.6 /etc/motd'
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
	    'code': EM_INVALIDARGUMENT,
	    'message': 'objects of type "directory" are not supported: ' +
	        '"/%user%/stor"'
	}, {
	    'phaseNum': '1',
	    'what': 'phase 1: input "/%user%/stor" ' +
	        '(from job input "/%user%/stor/obj1")',
	    'input': '/%user%/stor',
	    'p0input': '/%user%/stor/obj1',
	    'code': EM_INVALIDARGUMENT,
	    'message': 'objects of type "directory" are not supported: ' +
	        '"/%user%/stor"'
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
    }
};

mod_livetests.jobTestRunner(testcases, process.argv, concurrency);
