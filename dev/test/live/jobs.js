/*
 * test/live/jobs.js: test job definitions used by multiple tests
 */

var mod_assert = require('assert');
var mod_child = require('child_process');
var mod_fs = require('fs');
var mod_http = require('http');
var mod_path = require('path');
var mod_stream = require('stream');
var mod_url = require('url');
var mod_util = require('util');

var mod_extsprintf = require('extsprintf');
var mod_jsprim = require('jsprim');
var mod_manta = require('manta');
var mod_memorystream = require('memorystream');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_marlin = require('../../lib/marlin');
var mod_testcommon = require('../common');
var mod_schema = require('../../lib/schema');

/* jsl:import ../../lib/errors.js */
require('../../lib/errors');

var sprintf = mod_extsprintf.sprintf;
var VError = mod_verror.VError;
var exnAsync = mod_testcommon.exnAsync;
var log = mod_testcommon.log;

exports.jobSubmit = jobSubmit;
exports.jobTestRun = jobTestRun;
exports.jobTestVerifyTimeout = jobTestVerifyTimeout;
exports.populateData = populateData;

exports.jobM = {
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
};

exports.jobMimage = {
    'job': {
	'phases': [ {
	    'type': 'map',
	    'image': '13.1.0',
	    'exec': 'grep 13.1.0 /etc/motd'
	} ]
    },
    'inputs': [ '/%user%/stor/obj1' ],
    'timeout': 60 * 1000,
    'expected_outputs': [ /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./ ],
    'errors': []
};

/* Like jobM, but makes use of separate external task output objects */
exports.jobMX = {
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
};

exports.jobMqparams = {
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
};

exports.jobMmpipeAnon = {
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
};

exports.jobMmpipeNamed = {
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
};

exports.jobMmcat = {
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
};

exports.jobMM = {
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
};

exports.jobR = {
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
};

exports.jobMR = {
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
};

exports.jobMMRR = {
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
};

exports.jobM500 = {
    'job': {
	'phases': [ { 'type': 'map', 'exec': 'wc' } ]
    },
    'inputs': [],
    'timeout': 90 * 1000,
    'expected_outputs': [],
    'errors': []
};

exports.jobMR1000 = {
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
};

exports.jobM4RR1000 = {
    'job': {
	'phases': [
	    { 'type': 'map', 'exec': 'wc' },
	    { 'type': 'reduce', 'count': 4, 'exec': 'wc' },
	    { 'type': 'reduce', 'exec': 'awk \'{sum+=$1} END { print sum }\'' }
	]
    },
    'inputs': [],
    'timeout': 300 * 1000,
    'expected_outputs': [ /%user%\/jobs\/.*\/stor\/reduce\.2\./ ],
    'expected_output_content': [ '1000\n' ],
    'errors': []
};

exports.jobMRRoutput = {
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
};

var asset_body = [
    '#!/bin/bash\n',
    '\n',
    'echo "sarabi" "$*"\n'
].join('\n');

exports.jobMasset = {
    'job': {
	'assets': {
	    '/%user%/stor/test_asset.sh': asset_body
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
};

/*
 * It's important that users aren't able to reboot zones.  If they could, our
 * metering data would get blown away, and users could steal compute.
 */
exports.jobRerrorReboot = {
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
};

exports.jobMerrorRebootOutput = {
    'job': {
	'phases': [ {
	    'type': 'map',
	    'exec': 'uadmin 2 1 2>&1; uadmin 1 1 2>&1; true'
	} ]
    },
    'inputs': [ '/%user%/stor/obj1' ],
    'timeout': 15 * 1000,
    'expected_outputs': [ /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./ ],
    'expected_output_content': [ 'uadmin: Not owner\nuadmin: Not owner\n' ],
    'errors': []
};

exports.jobMerrorAssetMissing = {
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
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'input': '/%user%/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_TASKINIT,
	'message': 'failed to dispatch task: first of 1 error: error ' +
	    'retrieving asset "/%user%/stor/notavalidasset" ' +
	    '(status code 404)'
    } ]
};

exports.jobM0bi = {
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
};

/*
 * It's surprising that this output is different than the analogous 1-phase map
 * job, but it is, because GNU wc's output is different when you "wc <
 * 0-byte-file" than when you "emit_zero_byte_stream | wc".
 */
exports.jobR0bi = {
    'job': {
	'phases': [ { 'type': 'reduce', 'exec': 'wc' } ]
    },
    'inputs': [ '/%user%/stor/0bytes' ],
    'timeout': 15 * 1000,
    'expected_outputs': [ /\/%user%\/jobs\/.*\/stor\/reduce\.0\./ ],
    'expected_output_content': [ '      0       0       0\n' ],
    'errors': []
};

exports.jobM0bo = {
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
};

exports.jobMcore = {
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
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'input': '/%user%/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_USERTASK,
	'message': 'user command or child process dumped core',
	'core': /\/%user%\/jobs\/.*\/stor\/cores\/0\/core.node./
    } ]
};

exports.jobMdiskDefault = {
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
};

exports.jobMdiskExtended = {
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
};

exports.jobMmemoryDefault = {
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
};

exports.jobMmemoryExtended = {
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
};

exports.jobMmget = {
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
};

exports.jobMmls = {
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
};

exports.jobMmjob = {
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
};

exports.jobMRnormalize = {
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
};

/*
 * The following several tests validate that we properly deal with object names
 * with characters that require encoding.  The first job exercises their use in:
 * - job input API
 * - jobinput, task, taskinput, and taskoutput records
 * - mcat, mpipe, and default stdout capture
 * - job output API
 */
exports.jobMMRenc = {
    'job': {
	'phases': [ {
	    /* Covers job input API, jobinput, task, taskoutput, and mcat. */
	    'type': 'map',
	    'exec': 'mcat "$MANTA_INPUT_OBJECT"'
	}, {
	    /* Covers use in default stdout capture. */
	    'type': 'map',
	    'exec': 'cat && echo' /* append newline so we can sort */
	}, {
	    /* Covers use in reduce, taskinput, mpipe, and job output API. */
	    'type': 'reduce',
	    'exec': 'sort | mpipe "${MANTA_OUTPUT_BASE} with spaces"'
	} ]
    },
    'inputs': [
	'/%user%/stor/my dir',
	'/%user%/stor/my obj1',	/* covers normal case that should be encoded */
	'/%user%/stor/my dir/my obj',	/* ditto, in dir with spaces */
	'/%user%/stor/M%41RK'	/* this should be encoded, and we should */
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
	'what': 'phase 0: map input "/%user%/stor/my dir"',
	'input': '/%user%/stor/my dir',
	'p0input': '/%user%/stor/my dir',
	'code': EM_INVALIDARGUMENT,
	'message': 'objects of type "directory" are not supported: ' +
	    '"/%user%/stor/my dir"'
    } ]
};

exports.jobMkill = {
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
};

/*
 * This job does something similar, but exercises stderr capture and the job
 * error API.
 */
exports.jobMerrorEnc = {
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
	'what': 'phase 0: map input "/%user%/stor/my obj1"',
	'input': '/%user%/stor/my obj1',
	'p0input': '/%user%/stor/my obj1',
	'code': EM_USERTASK,
	'stderr': /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/my obj1.0.err/
    }, {
	'phaseNum': '0',
	'what': 'phase 0: map input "/%user%/stor/M%41RK"',
	'input': '/%user%/stor/M%41RK',
	'p0input': '/%user%/stor/M%41RK',
	'code': EM_USERTASK,
	'stderr': /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/M%41RK.0.err/
    } ]
};

/*
 * This test relies on the fact that the systems where we run the test suite
 * don't support even a single task with this much memory.
 */
exports.jobMerrorMemoryTooBig = {
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
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'input': '/%user%/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_TASKINIT,
	'message': 'failed to dispatch task: not enough memory available'
    } ]
};

/*
 * Ditto, for disk.
 */
exports.jobMerrorDiskTooBig = {
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
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'input': '/%user%/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_TASKINIT,
	'message': 'failed to dispatch task: not enough disk space available'
    } ]
};

exports.jobMerrorsDispatch0 = {
    'job': {
	'phases': [ {
	    'type': 'map',
	    'exec': 'wc'
	} ]
    },
    'inputs': [
	/*
	 * XXX We should also have tests for "/", "/%user%", "/%user%/stor",
	 * "/%user%/jobs", and other special paths, but at the moment these
	 * don't do the right thing.  See MANTA-401.
	 */
	'/notavalidusername/stor/obj1',
	'/%user%/stor/notavalidfilename',
	'/%user%/stor/mydir',
	'/%user%/stor/obj1'
    ],
    'timeout': 20 * 1000,
    'expected_outputs': [
	/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
    ],
    'expected_output_content': [ ' 0  5 50\n' ],
    'errors': [ {
	'phaseNum': '0',
	'what': 'phase 0: map input "/notavalidusername/stor/obj1"',
	'input': '/notavalidusername/stor/obj1',
	'p0input': '/notavalidusername/stor/obj1',
	'code': EM_RESOURCENOTFOUND,
	'message': 'no such object: "/notavalidusername/stor/obj1"'
    }, {
	'phaseNum': '0',
	'what': 'phase 0: map input "/%user%/stor/notavalidfilename"',
	'input': '/%user%/stor/notavalidfilename',
	'p0input': '/%user%/stor/notavalidfilename',
	'code': EM_RESOURCENOTFOUND,
	'message': 'no such object: "/%user%/stor/notavalidfilename"'
    }, {
	'phaseNum': '0',
	'what': 'phase 0: map input "/%user%/stor/mydir"',
	'input': '/%user%/stor/mydir',
	'p0input': '/%user%/stor/mydir',
	'code': EM_INVALIDARGUMENT,
	'message': 'objects of type "directory" are not supported: ' +
	    '"/%user%/stor/mydir"'
    } ]
};

/*
 * This job behaves just like jobMerrorsDispatch0, but makes sure that this also
 * works when these same errors occur in phases > 0, which goes through a
 * slightly different code path.
 *
 * The job inputs themselves aren't used by the job, but those same inputs are
 * referenced by the "mcat" phase, so the test suite has to make sure they're
 * present.
 *
 * The job will include errors that match the ones in jobMerrorsDispatch0, for
 * the same reason as in that test case, but here we're mostly interested in
 * checking the other errors.
 */
exports.jobMerrorsDispatch1 = {
    'job': {
	'phases': [ {
	    'type': 'map',
	    'exec': 'mcat ' + exports.jobMerrorsDispatch0['inputs'].join(' ')
	}, {
	    'type': 'map',
	    'exec': 'wc'
	} ]
    },
    'inputs': exports.jobMerrorsDispatch0['inputs'],
    'timeout': 30 * 1000,
    'expected_outputs': [
	/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.1\./
    ],
    'expected_output_content': [ ' 0  5 50\n' ],
    'errors': exports.jobMerrorsDispatch0['errors'].concat([ {
	'phaseNum': '1',
	'what': 'phase 1: map input "/notavalidusername/stor/obj1" ' +
	    '(from job input "/%user%/stor/obj1")',
	'input': '/notavalidusername/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_RESOURCENOTFOUND,
	'message': 'no such object: "/notavalidusername/stor/obj1"'
    }, {
	'phaseNum': '1',
	'what': 'phase 1: map input "/%user%/stor/notavalidfilename" ' +
	    '(from job input "/%user%/stor/obj1")',
	'input': '/%user%/stor/notavalidfilename',
	'p0input': '/%user%/stor/obj1',
	'code': EM_RESOURCENOTFOUND,
	'message': 'no such object: "/%user%/stor/notavalidfilename"'
    }, {
	'phaseNum': '1',
	'what': 'phase 1: map input "/%user%/stor/mydir" ' +
	    '(from job input "/%user%/stor/obj1")',
	'input': '/%user%/stor/mydir',
	'p0input': '/%user%/stor/obj1',
	'code': EM_INVALIDARGUMENT,
	'message': 'objects of type "directory" are not supported: ' +
	    '"/%user%/stor/mydir"'
    } ])
};

exports.jobMerrorBadReducer = {
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
	'what': new RegExp('phase 1: map input ' +
	    '"/%user%/jobs/.*/stor/%user%/stor/obj1.0..*" ' +
	    '\\(from job input "/%user%/stor/obj1"\\)'),
	'input': new RegExp('/%user%/jobs/.*/stor/%user%/stor/obj1.0..*'),
	'p0input': '/%user%/stor/obj1',
	'code': EM_INVALIDARGUMENT,
	'message': 'reducer "1" specified, but only 1 reducers exist'
    } ]
};

exports.jobMerrorVeryBadReducer = {
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
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'input': '/%user%/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_USERTASK,
	'message': 'user command exited with code 1'
    } ]
};

exports.jobMerrorOom = {
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
};

exports.jobMerrorDisk = {
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
};

exports.jobMerrorLackeyCrash = {
    'job': {
	'phases': [ {
	    'type': 'map',
	    'exec': 'pkill -f lackey'
	} ]
    },
    'inputs': [ '/%user%/stor/obj1' ],
    'timeout': 30 * 1000,
    'expected_outputs': [],
    'errors': [ {
	'phaseNum': '0',
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'code': EM_INTERNAL,
	'message': 'internal error'
    } ]
};

/*
 * It would be better if we could actually run the lackey out of memory, but
 * this seems difficult to do in practice.  But when it happens, the lackey
 * tends to dump core, resulting in a timeout.
 */
exports.jobMerrorLackeyOom = {
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
};

exports.jobMerrorCmd = {
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
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'input': '/%user%/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_USERTASK,
	'message': 'user command exited with code 1'
    } ]
};

exports.jobMerrorMuskie = {
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
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'input': '/%user%/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_INTERNAL,
	'message': 'internal error'
    } ]
};

/*
 * Like jobMerrorMuskie, but with mpipe.  Such errors get translated as
 * UserTaskErrors in this case.
 */
exports.jobMerrorMuskieMpipe = {
    'job': {
	'phases': [ {
	    'type': 'map',
	    'exec': 'curl -i -X POST localhost/my/jobs/task/perturb?p=1 | mpipe'
	} ]
    },
    'inputs': exports.jobMerrorMuskie['inputs'],
    'timeout': exports.jobMerrorMuskie['timeout'],
    'expected_outputs': exports.jobMerrorMuskie['expected_outputs'],
    'errors': [ {
	'phaseNum': '0',
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'input': '/%user%/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_USERTASK,
	'message': 'user command exited with code 1'
    } ]
};

/*
 * This test validates that we get the expected number of errors.  It's
 * probabilistic, though: we set the probability of an upstream muskie failure
 * to 0.5, and we know Marlin retries 3 times, so there should be close to 12
 * failures per 100 keys.
 */
exports.jobMerrorMuskieRetry = {
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
};

/*
 * Like jobMerrorMuskieRetry, but with mpipe.
 */
exports.jobMerrorMuskieRetryMpipe = {
    'job': {
	'phases': [ {
	    'type': 'map',
	    'exec': 'curl -i -X POST localhost/my/jobs/task/perturb?p=0.5 |' +
	        'mpipe'
	} ]
    },
    'inputs': exports.jobMerrorMuskieRetry['inputs'],
    'timeout': exports.jobMerrorMuskieRetry['timeout'],
    'error_count': exports.jobMerrorMuskieRetry['error_count'],
    'errors': [ {
	'phaseNum': '0',
	'code': EM_USERTASK,
	'message': 'user command exited with code 1'
    } ]
};

/*
 * Tests that reducers that experience transient errors fetching data continue
 * to retry the requests.
 */
exports.jobRmuskieRetry = {
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
};

/*
 * Tests that reducers that experience large numbers of transient errors
 * eventually blow up with an error.
 */
exports.jobRerrorMuskieRetry = {
    'job': {
	'phases': [ {
	    'type': 'reduce',
	    'init': 'curl -i -X POST localhost/my/jobs/task/perturb?p=1',
	    'exec': 'wc'
	} ]
    },
    'inputs': [ '/poseidon/stor/obj1' ],
    'timeout': 60 * 1000,
    'expected_outputs': [],
    'errors': [ {
	'phaseNum': '0',
	'what': 'phase 0: reduce',
	'code': EM_SERVICEUNAVAILABLE,
	'message': /error fetching inputs/
    } ]
};

/*
 * mpipe should not auto-create directories by default, and it should fail if
 * the directory does not exist.  The case where it creates directories is
 * tested by jobMpipeNamed.
 */
exports.jobMerrorMpipeMkdirp = {
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
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'input': '/%user%/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_USERTASK,
	'message': 'user command exited with code 1'
    } ]
};

exports.jobMerrorBadImage = {
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
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'input': '/%user%/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_INVALIDARGUMENT,
	'message': 'failed to dispatch task: requested image is not available'
    } ]
};

exports.jobR0inputs = {
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
};

exports.jobRcatbin = {
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
	'/poseidon/stor/obj1',
	'/poseidon/stor/obj2',
	'/poseidon/stor/obj3'
    ],
    'timeout': 60 * 1000,
    'expected_outputs': [ /\/%user%\/jobs\/.*\/stor\/reduce\.1\./ ],
    'errors': [],
    'expected_output_content': [ 'okay\n' ]
};

exports.jobMenv = {
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
};

exports.jobRenv = {
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
};

exports.jobMcancel = {
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
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'input': '/%user%/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_JOBCANCELLED,
	'message': 'job was cancelled'
    }, {
	'phaseNum': '0',
	'what': 'phase 0: map input "/%user%/stor/obj2"',
	'input': '/%user%/stor/obj2',
	'p0input': '/%user%/stor/obj2',
	'code': EM_JOBCANCELLED,
	'message': 'job was cancelled'
    }, {
	'phaseNum': '0',
	'what': 'phase 0: map input "/%user%/stor/obj3"',
	'input': '/%user%/stor/obj3',
	'p0input': '/%user%/stor/obj3',
	'code': EM_JOBCANCELLED,
	'message': 'job was cancelled'
    } ]
};

exports.jobMmeterCheckpoints = {
    'job': {
	'phases': [ { 'type': 'map', 'exec': 'sleep 7' } ]
    },
    'inputs': [ '/%user%/stor/obj1' ],
    'timeout': 30 * 1000,
    'expected_outputs': [ /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./ ],
    'meteringIncludesCheckpoints': true,
    'errors': []
};

/*
 * This tests that metering data accounts for the whole time a zone is used,
 * even if that's much longer than a task actually ran for, as in the case where
 * the reduce task bails out early.
 */
exports.jobMmeterExitsEarly = {
    'job': {
	'phases': [ {
	    'type': 'map',
	    'exec': 'wc'
	}, {
	    'type': 'reduce',
	    'exec': 'awk {'
	} ]
    },
    'inputs': [ '/%user%/stor/obj1' ],
    'timeout': 60 * 1000,
    'expected_outputs': [],
    'meteringIncludesCheckpoints': true,
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
		log.warn('job "%s": skipping metering check', verify['jobid']);
		return;
	}

	var meter, taskid, elapsed, ns;
	meter = verify['metering']['cumulative'];
	log.info('job "%s": checking total time elapsed', verify['jobid']);
	for (taskid in meter) {
		elapsed = meter[taskid]['time'];
		ns = elapsed[0] * 1e9 + elapsed[1];

		/*
		 * When running this test concurrently with other jobs, it's
		 * hard to know whether it actually did the right thing, since
		 * it may be some time before the reduce task gets on-CPU.  We
		 * leave it running for 15 seconds, but we can only really
		 * assume it will be running for a few seconds (and obviously
		 * that's still racy).  (We could actually address this race by
		 * opening up a local HTTP server and having the reduce task hit
		 * it when it starts, and only ending input 10s after that.  If
		 * this becomes a problem, we should just do that.)
		 */
		if (ns >= 5 * 1e9 && ns < 20 * 1e9)
			return;
	}

	log.error('job "%s": no task took 5 < N < 20 seconds',
	    verify['jobid'], meter);
	throw (new Error('no task took 5 < N < 20 seconds'));
    }
};

exports.jobMinit = {
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
};

exports.jobMinitEnv = {
    'job': {
	'phases': [ {
	    'type': 'map',
	    'init': 'env | egrep ^MANTA_ | sort > /var/tmp/test_temp',
	    'exec': 'cat /var/tmp/test_temp'
	} ]
    },
    'inputs': [ '/%user%/stor/obj1' ],
    'timeout': 60 * 1000,
    'expected_outputs': [ /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./ ],
    'expected_output_content': [
	'MANTA_JOB_ID=$jobid\n' +
	'MANTA_NO_AUTH=true\n' +
	'MANTA_OUTPUT_BASE=/%user%/jobs/$jobid/stor/' +
	    '%user%/stor/obj1.0.\n' +
	'MANTA_URL=http://localhost:80/\n' +
	'MANTA_USER=%user%\n'
    ],
    'errors': []
};

exports.jobMinitFail = {
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
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'input': '/%user%/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_TASKINIT,
	'message': 'user command exited with code 1'
    } ]
};

exports.jobMinitCore = {
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
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'input': '/%user%/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_TASKINIT,
	'message': 'user command or child process dumped core',
	'core': /\/%user%\/jobs\/.*\/stor\/cores\/0\/core.node./
    } ]
};

/*
 * This is a poorly handled (but extremely unlikely) error case, and all we're
 * really checking is that we do at least report an error rather than doing the
 * wrong thing.
 */
exports.jobMinitKill = {
    'job': {
	'phases': [ {
	    'type': 'map',
	    'init': 'pkill -f lackey',
	    'exec': 'wc'
	} ]
    },
    'inputs': [ '/%user%/stor/obj1' ],
    'timeout': 60 * 1000,
    'expected_outputs': [],
    'errors': [ {
	'phaseNum': '0',
	'what': 'phase 0: map input "/%user%/stor/obj1"',
	'input': '/%user%/stor/obj1',
	'p0input': '/%user%/stor/obj1',
	'code': EM_INTERNAL,
	'message': 'internal error'
    } ]
};

exports.jobMinitKillAfter = {
    'job': {
	'phases': [ {
	    'type': 'map',
	    'init': 'echo >> /var/tmp/test_temp',
	    'exec': 'if [[ -f /var/tmp/ranonce ]]; then\n' +
	        'wc < /var/tmp/test_temp\n' +
		'else\n' +
		'echo > /var/tmp/ranonce\n' +
		/*
		 * We have to hack around the error-on-lackey-crash behavior in
		 * order to test that we do the right thing with respect to
		 * "init".
		 */
		'rm -f /var/tmp/.marlin_task_started\n' +
		'pkill -f lackey\n' +
		'fi'
	} ]
    },
    'inputs': [ '/%user%/stor/obj1' ],
    'timeout': 60 * 1000,
    'expected_outputs': [ /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./ ],
    'expected_output_content': [ '1 0 1\n' ],
    'errors': []
};

exports.jobsMain = [
    exports.jobM,
    exports.jobMimage,
    exports.jobMX,
    exports.jobMqparams,
    exports.jobMmpipeAnon,
    exports.jobMmpipeNamed,
    exports.jobMmcat,
    exports.jobM0bi,
    exports.jobR0bi,
    exports.jobM0bo,
    exports.jobR,
    exports.jobR0inputs,
    exports.jobRcatbin,
    exports.jobMM,
    exports.jobMR,
    exports.jobMMRR,
    exports.jobMRRoutput,
    exports.jobMcancel,
    exports.jobMasset,
    exports.jobMcore,
    exports.jobMmemoryDefault,
    exports.jobMmemoryExtended,
    exports.jobMdiskDefault,
    exports.jobMdiskExtended,
    exports.jobMmget,
    exports.jobMmls,
    exports.jobMmjob,
    exports.jobMRnormalize,
    exports.jobMMRenc,
    exports.jobMkill,
    exports.jobMerrorEnc,
    exports.jobMerrorMemoryTooBig,
    exports.jobMerrorDiskTooBig,
    exports.jobMerrorsDispatch0,
    exports.jobMerrorsDispatch1,
    exports.jobRerrorReboot,
    exports.jobMerrorRebootOutput,
    exports.jobMerrorAssetMissing,
    exports.jobMerrorBadReducer,
    exports.jobMerrorVeryBadReducer,
    exports.jobMerrorLackeyCrash,
    exports.jobMerrorCmd,
    exports.jobMerrorMuskie,
    exports.jobMerrorMuskieMpipe,
    exports.jobMerrorMuskieRetryMpipe,
    exports.jobMerrorMpipeMkdirp,
    exports.jobMerrorBadImage,
    exports.jobMenv,
    exports.jobRenv,
    exports.jobMmeterCheckpoints,
    exports.jobMmeterExitsEarly,
    exports.jobMinit,
    exports.jobMinitEnv,
    exports.jobMinitFail,
    exports.jobMinitCore,
    exports.jobMinitKill,
    exports.jobMinitKillAfter
];

/*
 * The "corner cases" are really just a bunch of expensive jobs that we don't
 * necessariliy want to be running frequently during stress tests and where
 * basically any success validates the correct behavior.
 */
exports.jobsCornerCases = [
    exports.jobMerrorOom,
    exports.jobMerrorDisk,
    exports.jobMerrorLackeyOom,
    exports.jobMerrorMuskieRetry,
    exports.jobRmuskieRetry,
    exports.jobRerrorMuskieRetry,
];

exports.jobsAll = exports.jobsCornerCases.concat(exports.jobsMain);
exports.jobsStress = exports.jobsMain.concat([
    exports.jobM500,
    exports.jobMR1000,
    exports.jobM4RR1000
]);

function initJobs()
{
	var job = exports.jobM500;

	for (var i = 0; i < 500; i++) {
		var key = '/%user%/stor/obj' + i;
		var okey = '/%user%/jobs/.*/stor' + key;

		job['inputs'].push(key);
		job['expected_outputs'].push(new RegExp(okey));
	}

	job = exports.jobMR1000;
	for (i = 0; i < 1000; i++) {
		key = '/%user%/stor/obj' + i;
		job['inputs'].push(key);
	}

	job = exports.jobM4RR1000;
	for (i = 0; i < 1000; i++) {
		key = '/%user%/stor/obj' + i;
		job['inputs'].push(key);
	}

	job = exports.jobMerrorMuskieRetry;
	for (i = 0; i < 100; i++) {
		key = '/%user%/stor/obj' + i;
		job['inputs'].push(key);
	}

	exports.jobMerrorMuskieRetryMpipe['inputs'] =
	    exports.jobMerrorMuskieRetry['inputs'];
	exports.jobRmuskieRetry['inputs'] =
	    exports.jobMerrorMuskieRetry['inputs'];
}

function replaceParams(str)
{
	var user = process.env['MANTA_USER'];

	if (typeof (str) == 'string')
		return (str.replace(/%user%/g, user));

	mod_assert.equal(str.constructor.name, 'RegExp');
	return (new RegExp(str.source.replace(/%user%/g, user)));
}

initJobs();

function jobTestRun(api, testspec, options, callback)
{
	jobSubmit(api, testspec, function (err, jobid) {
		if (err) {
			callback(err);
			return;
		}

		jobTestVerifyTimeout(api, testspec, jobid, options, callback);
	});
}

function jobSubmit(api, testspec, callback)
{
	var jobdef, login, url, funcs, private_key, signed_path, jobid;

	login = process.env['MANTA_USER'];
	url = mod_url.parse(process.env['MANTA_URL']);

	if (!login) {
		process.nextTick(function () {
			callback(new VError(
			    'MANTA_USER must be specified in the environment'));
		});
		return;
	}

	jobdef = {
	    'auth': {
		'login': login,
		'groups': [ 'operators' ] /* XXX */
	    },
	    'phases': testspec['job']['phases'],
	    'name': 'marlin test suite job',
	    'options': {
		'frequentCheckpoint': true
	    }
	};

	jobdef['phases'].forEach(function (p) {
		p['exec'] = replaceParams(p['exec']);
		if (p['assets'])
			p['assets'] = p['assets'].map(replaceParams);
	});

	funcs = [
	    function (_, stepcb) {
		log.info('looking up user "%s"', login);
		mod_testcommon.loginLookup(login, function (err, owner) {
			jobdef['auth']['uuid'] = owner;
			jobdef['owner'] = owner;
			stepcb(err);
		});
	    },
	    function (_, stepcb) {
		var path = process.env['MANTA_KEY'] ||
			mod_path.join(process.env['HOME'], '.ssh/id_rsa');
		log.info('reading private key from %s', path);
		mod_fs.readFile(path, function (err, contents) {
			private_key = contents.toString('utf8');
			stepcb(err);
		});
	    },
	    function (_, stepcb) {
		log.info('creating signed URL');

		mod_manta.signUrl({
		    'algorithm': 'rsa-sha256',
		    'expires': Date.now() + 86400 * 1000,
		    'host': url['host'],
		    'keyId': process.env['MANTA_KEY_ID'],
		    'method': 'POST',
		    'path': sprintf('/%s/tokens', login),
		    'user': login,
		    'sign': mod_manta.privateKeySigner({
			'algorithm': 'rsa-sha256',
			'key': private_key,
			'keyId': process.env['MANTA_KEY_ID'],
			'log': log,
			'user': login
		    })
		}, function (err, path) {
			signed_path = path;
			stepcb(err);
		});
	    },
	    function (_, stepcb) {
		log.info('creating auth token', signed_path);

		var req = mod_http.request({
		    'method': 'POST',
		    'path': signed_path,
		    'host': url['hostname'],
		    'port': parseInt(url['port'], 10) || 80
		});

		req.end();

		req.on('response', function (response) {
			log.info('auth token response: %d',
			    response.statusCode);

			if (response.statusCode != 201) {
				stepcb(new VError(
				    'wrong status code for auth token'));
				return;
			}

			var body = '';
			response.on('data', function (chunk) {
				body += chunk;
			});
			response.on('end', function () {
				var token = JSON.parse(body)['token'];
				jobdef['auth']['token'] = token;
				jobdef['authToken'] = token;
				stepcb();
			});
		});
	    },
	    function (_, stepcb) {
		log.info('submitting job', jobdef);
		api.jobCreate(jobdef, function (err, result) {
			jobid = result;
			stepcb(err);
		});
	    }
	];

	if (testspec['job']['assets']) {
		mod_jsprim.forEachKey(testspec['job']['assets'],
		    function (key, content) {
			key = replaceParams(key);
			funcs.push(function (_, stepcb) {
				log.info('submitting asset "%s"', key);
				var stream = new mod_memorystream(content,
				    { 'writable': false });
				api.manta.put(key, stream,
				    { 'size': content.length }, stepcb);
			});
		    });
	}

	funcs.push(function (_, stepcb) {
		var final_err;

		if (testspec['inputs'].length === 0) {
			stepcb();
			return;
		}

		var queue = mod_vasync.queuev({
			'concurrency': 15,
			'worker': function (key, subcallback) {
				if (final_err) {
					subcallback();
					return;
				}

				log.info('job "%s": adding key %s',
				    jobid, key);
				api.jobAddKey(jobid, key,
				    function (err) {
					if (err)
						final_err = err;
					subcallback();
				    });
			}
		});

		testspec['inputs'].forEach(function (key) {
			queue.push(replaceParams(key));
		});

		queue.drain = function () { stepcb(final_err); };
	});

	if (!testspec['skip_input_end']) {
		funcs.push(function (_, stepcb) {
			log.info('job "%s": ending input', jobid);
			api.jobEndInput(jobid, { 'retry': { 'retries': 3 } },
			    stepcb);
		});
	}

	mod_vasync.pipeline({ 'funcs': funcs }, function (err) {
		if (!err)
			log.info('job "%s": job submission complete', jobid);
		callback(err, jobid);

		if (testspec['post_submit'])
			testspec['post_submit'](api, jobid);
	});
}

function jobTestVerifyTimeout(api, testspec, jobid, options, callback)
{
	var interval = testspec['timeout'];

	mod_testcommon.timedCheck(Math.ceil(interval / 1000), 1000,
	    function (subcallback) {
		jobTestVerify(api, testspec, jobid, options, function (err) {
			if (err)
				err.noRetry = !err.retry;
			subcallback(err);
		});
	    }, callback);
}

function jobTestVerify(api, testspec, jobid, options, callback)
{
	mod_vasync.pipeline({
	    'arg': {
		'strict': options['strict'],
		'api': api,
		'testspec': testspec,
		'jobid': jobid,
		'job': undefined,
		'objects_found': [],
		'inputs': [],
		'outputs': [],
		'errors': [],
		'content': [],
		'metering': null
	    },
	    'funcs': [
		jobTestVerifyFetchJob,
		jobTestVerifyFetchInputs,
		jobTestVerifyFetchErrors,
		jobTestVerifyFetchOutputs,
		jobTestVerifyFetchOutputContent,
		jobTestVerifyFetchObjectsFound,
		jobTestVerifyFetchMeteringRecords,
		jobTestVerifyResult
	    ]
	}, callback);
}

function jobTestVerifyFetchJob(verify, callback)
{
	verify['api'].jobFetch(verify['jobid'], function (err, job) {
		if (!err && job['value']['state'] != 'done') {
			err = new VError('job is not finished');
			err.retry = true;
		}

		if (!err)
			verify['job'] = job['value'];

		callback(err);
	});
}

function jobTestVerifyFetchInputs(verify, callback)
{
	var req = verify['api'].jobFetchInputs(verify['jobid']);
	req.on('key', function (key) { verify['inputs'].push(key); });
	req.on('end', callback);
	req.on('error', callback);
}

function jobTestVerifyFetchErrors(verify, callback)
{
	var req = verify['api'].jobFetchErrors(verify['jobid']);
	req.on('err', function (error) { verify['errors'].push(error); });
	req.on('end', callback);
	req.on('error', callback);
}

function jobTestVerifyFetchOutputs(verify, callback)
{
	var pi = verify['job']['phases'].length - 1;
	var req = verify['api'].jobFetchOutputs(verify['jobid'], pi,
	    { 'limit': 1000 });
	req.on('key', function (key) { verify['outputs'].push(key); });
	req.on('end', callback);
	req.on('error', callback);
}

function jobTestVerifyFetchOutputContent(verify, callback)
{
	var manta = verify['api'].manta;

	mod_vasync.forEachParallel({
	    'inputs': verify['outputs'],
	    'func': function (objectpath, subcallback) {
		log.info('fetching output "%s"', objectpath);
		manta.get(objectpath, function (err, mantastream) {
			if (err) {
				subcallback(err);
				return;
			}

			var data = '';
			mantastream.on('data', function (chunk) {
				data += chunk.toString('utf8');
			});

			mantastream.on('end', function () {
				verify['content'].push(data);
				subcallback();
			});
		});
	    }
	}, callback);
}

/*
 * Fetch the full list of objects under /%user/jobs/%jobid/stor to make sure
 * we've cleaned up intermediate objects.
 */
function jobTestVerifyFetchObjectsFound(verify, callback)
{
	/*
	 * It would be good if this were in node-manta as a library function,
	 * but until then, we use the command-line version to avoid reinventing
	 * the wheel.
	 */
	var cmd = mod_path.join(__dirname,
	    '../../node_modules/manta/bin/mfind');

	mod_child.execFile(process.execPath, [ cmd, '-i', '-t', 'o',
	    replaceParams('/%user%/jobs/' + verify['jobid'] + '/stor') ],
	    function (err, stdout, stderr) {
		if (err) {
			callback(new VError(err, 'failed to list job ' +
			    'objects: stderr = %s', JSON.stringify(stderr)));
			return;
		}

		var lines = stdout.split('\n');
		mod_assert.equal(lines[lines.length - 1], '');
		lines = lines.slice(0, lines.length - 1);

		verify['objects_found'] = verify['objects_found'].concat(lines);
		verify['objects_found'].sort();
		callback();
	    });
}

function jobTestVerifyFetchMeteringRecords(verify, callback)
{
	var agentlog = process.env['MARLIN_METERING_LOG'];

	if (!agentlog) {
		log.warn('not checking metering records ' +
		    '(MARLIN_METERING_LOG not set)');
		callback();
		return;
	}

	var barrier = mod_vasync.barrier();
	verify['metering'] = {};
	[ 'deltas', 'cumulative' ].forEach(function (type) {
		barrier.start(type);

		var reader = new (mod_marlin.MarlinMeterReader)({
		    'stream': mod_fs.createReadStream(agentlog),
		    'summaryType': type,
		    'aggrKey': [ 'jobid', 'taskid' ],
		    'startTime': verify['job']['timeCreated'],
		    'resources': [ 'time', 'nrecords', 'cpu', 'memory',
		        'vfs', 'zfs', 'vnic0' ]
		});

		reader.on('end', function () {
			verify['metering'][type] =
			    reader.reportHierarchical()[verify['jobid']];
			barrier.done(type);
		});
	});

	barrier.on('drain', callback);
}

function jobTestVerifyResult(verify, callback)
{
	try {
		jobTestVerifyResultSync(verify);
	} catch (ex) {
		if (ex.name == 'AssertionError')
			ex.message = ex.toString();
		var err = new VError(ex, 'verifying job "%s"', verify['jobid']);

		/*
		 * Cancelled jobs may take a few extra seconds for the metering
		 * records to be written out.  Retry this whole fetch process
		 * for up to 10 seconds to deal with that case.
		 */
		if (verify['job']['timeCancelled'] !== undefined &&
		    Date.now() - Date.parse(
		    verify['job']['timeDone']) < 10000) {
			err.retry = true;
			log.error(err,
			    'retrying due to error on cancelled job');
		} else {
			log.fatal(err);
		}

		callback(err);
		return;
	}

	callback();
}

function jobTestVerifyResultSync(verify)
{
	var job = verify['job'];
	var inputs = verify['inputs'];
	var outputs = verify['outputs'];
	var joberrors = verify['errors'];
	var testspec = verify['testspec'];
	var strict = verify['strict'];
	var foundobj = verify['objects_found'];
	var expected_inputs;

	/* verify jobSubmit, jobFetch, jobFetchInputs */
	mod_assert.deepEqual(testspec['job']['phases'], job['phases']);
	expected_inputs = testspec['inputs'].map(replaceParams);

	/*
	 * We don't really need to verify this for very large jobs, and it's
	 * non-trivial to do so.
	 */
	if (expected_inputs.length <= 1000)
		mod_assert.deepEqual(expected_inputs.sort(), inputs.sort());

	/* Wait for the job to be completed. */
	mod_assert.equal(job['state'], 'done');

	/* Sanity-check the rest of the job record. */
	mod_assert.ok(job['worker']);
	mod_assert.ok(job['timeInputDone'] >= job['timeCreated']);
	mod_assert.ok(job['timeDone'] >= job['timeCreated']);

	/*
	 * Check job execution and jobFetchOutputs.  Only probabilistic tests
	 * don't set expected_outputs.
	 */
	if (testspec['expected_outputs']) {
		var expected_outputs = testspec['expected_outputs'].map(
		    replaceParams).sort();
		outputs.sort();
		mod_assert.equal(outputs.length, expected_outputs.length);

		for (var i = 0; i < outputs.length; i++) {
			if (typeof (expected_outputs[i]) == 'string')
				mod_assert.equal(expected_outputs[i],
				    outputs[i],
				    'output ' + i + ' doesn\'t match');
			else
				mod_assert.ok(
				    expected_outputs[i].test(outputs[i]),
				    'output ' + i + ' doesn\'t match');
		}
	} else {
		mod_assert.ok(testspec['error_count']);
	}

	/* Check job execution and jobFetchErrors */
	if (!strict)
		/* Allow retried errors in non-strict mode. */
		joberrors = joberrors.filter(
		    function (error) { return (!error['retried']); });

	if (!testspec['error_count']) {
		mod_assert.equal(joberrors.length, testspec['errors'].length);
	} else {
		mod_assert.ok(joberrors.length >= testspec['error_count'][0]);
		mod_assert.ok(joberrors.length <= testspec['error_count'][1]);
	}

	testspec['errors'].forEach(function (expected_error, idx) {
		var okay = false;

		joberrors.forEach(function (actual_error) {
			var match = true;
			var exp;

			for (var k in expected_error) {
				exp = replaceParams(expected_error[k]);
				if (typeof (expected_error[k]) == 'string') {
					/*
					 * In non-strict modes, cancelled jobs
					 * are allowed to fail with EM_INTERNAL
					 * instead of EM_JOBCANCELLED.
					 */
					if (!strict &&
					    exp['code'] == EM_JOBCANCELLED &&
					    actual_error['code'] ==
					    EM_INTERNAL &&
					    (k == 'code' || k == 'message')) {
						log.warn('expected %s, ' +
						    'but got %s, which is ' +
						    'okay in non-strict mode',
						    exp[k], actual_error[k]);
					    	continue;
					}
					if (actual_error[k] !== exp) {
						match = false;
						break;
					}
				} else {
					if (!exp.test(actual_error[k])) {
						match = false;
						break;
					}
				}
			}

			if (match)
				okay = true;
		});

		if (!okay)
			throw (new VError('no match for error %d: %j %j',
			    idx, expected_error, joberrors));
	});

	/* Check stat counters. */
	var stats = job['stats'];

	if (strict) {
		mod_assert.equal(stats['nAssigns'], 1);
		mod_assert.equal(stats['nRetries'], 0);
	}

	mod_assert.equal(stats['nErrors'], verify['errors'].length);
	mod_assert.equal(stats['nInputsRead'], verify['inputs'].length);
	mod_assert.equal(stats['nJobOutputs'], verify['outputs'].length);
	mod_assert.equal(stats['nTasksDispatched'],
	    stats['nTasksCommittedOk'] + stats['nTasksCommittedFail']);

	/*
	 * For every failed task, there should be either a retry or an error.
	 * But there may be errors for inputs that were never dispatched as
	 * tasks (e.g., because the object didn't exist).
	 */
	mod_assert.ok(stats['nTasksCommittedFail'] <=
	    stats['nRetries'] + stats['nErrors']);

	/*
	 * Check metering records.  This is not part of the usual tests, but can
	 * be run on-demand by running the test suite in the global zone (where
	 * we have access to the metering records).  This also assumes that all
	 * tasks executed on this server.
	 */
	if (verify['metering'] !== null) {
		var sum, taskid;

		if (testspec['meteringIncludesCheckpoints']) {
			sum = 0;
			for (taskid in verify['metering']['deltas']) {
				if (verify['metering']['deltas'][taskid][
				    'nrecords'] > 1)
					sum++;
			}

			if (sum === 0) {
				log.error('expected metering checkpoints',
				    verify['metering']['deltas']);
				throw (new Error(
				    'expected metering checkpoints'));
			}
		}

		/*
		 * The "deltas" and "cumulative" reports should be identical
		 * except for the "nrecords" column.
		 */
		for (taskid in verify['metering']['deltas']) {
			if (verify['metering']['deltas'][
			    taskid]['nrecords'] != 1)
				log.info('checkpoint records found');
			delete (verify['metering']['deltas'][
			    taskid]['nrecords']);
		}

		var cumul = verify['metering']['cumulative'];
		for (taskid in cumul) {
			mod_assert.equal(1, cumul[taskid]['nrecords']);
			delete (cumul[taskid]['nrecords']);
		}

		mod_assert.deepEqual(cumul, verify['metering']['deltas']);

		/*
		 * For cancelled jobs, it's possible that some of the tasks
		 * never even ran.  Ditto jobs with agent dispatch errors.
		 */
		var expected_count = verify['job']['stats']['nTasksDispatched'];
		for (i = 0; i < joberrors.length; i++) {
			if (joberrors[i]['message'].indexOf(
			    'failed to dispatch task') != -1)
				--expected_count;
		}
		if (verify['job']['timeCancelled'] === undefined &&
		    expected_count > 0)
			mod_assert.equal(Object.keys(cumul).length,
			    expected_count);
	}

	/*
	 * Check for intermediate objects.  We don't do this in non-strict mode
	 * because agent restarts can result in extra copies of stderr and core
	 * files.
	 */
	if (strict && job['timeCancelled'] === undefined) {
		/*
		 * "foundobj" may not contain all output objects, since some of
		 * those may have been given names somewhere else.  But it must
		 * not contain any objects that aren't final outputs or stderr
		 * objects.
		 */
		var dedup = {};
		outputs.forEach(function (o) { dedup[o] = true; });
		joberrors.forEach(function (e) {
			if (e['stderr'])
				dedup[e['stderr']] = true;
			if (e['core'])
				dedup[e['core']] = true;
		});
		foundobj = foundobj.filter(function (o) {
			if (dedup[o])
				return (false);

			/*
			 * It's possible to see last-phase output objects that
			 * are not final outputs because the corresponding task
			 * actually failed.  These are legit: no sense removing
			 * them.
			 */
			var match = /.*\.(\d+)\.[0-9a-z-]{36}$/.exec(o);
			if (!match)
				return (true);

			if (parseInt(match[1], 10) ==
			    testspec['job']['phases'].length - 1)
				return (false);

			/*
			 * There's one other corner case, which is when we
			 * successfully emit an intermediate object, but fail to
			 * actually forward it onto the next phase.  The only
			 * way this can happen today is when the object is
			 * designated for a non-existent reducer.  We ignore
			 * this case, effectively saying that the semantics of
			 * this are that the intermediate object is made
			 * available to the user in this error case.
			 */
			var j, e;
			for (j = 0; j < joberrors.length; j++) {
				e = joberrors[j];
				if (e['input'] == o &&
				    e['code'] == EM_INVALIDARGUMENT &&
				    /* JSSTYLED */
				    /reducer "\d+" specified, but only/.test(
				    e['message']))
					return (false);
			}

			return (true);
		});

		if (foundobj.length !== 0) {
			log.error('extra objects found', foundobj);
			throw (new Error('extra objects found'));
		}
	}

	/* Check custom verifiers. */
	if (testspec['verify'])
		testspec['verify'](verify);

	/* Check output content */
	if (!testspec['expected_output_content'])
		return;

	var bodies = verify['content'];
	var expected = testspec['expected_output_content'].map(function (o) {
		var n;
		if (typeof (o) == 'string')
			n = o.replace(/\$jobid/g, verify['jobid']);
		else
			n = new RegExp(o.source.replace(
			    /\$jobid/g, verify['jobid']));
	        return (replaceParams(n));
	});
	bodies.sort();
	expected.sort();

	mod_assert.equal(bodies.length, expected.length,
	    'wrong number of output files');

	for (i = 0; i < expected.length; i++) {
		if (typeof (expected[i]) == 'string')
			mod_assert.equal(bodies[i], expected[i],
			    sprintf('output "%d" didn\'t match ' +
			        '(expected "%s", got "%s")', i, expected[i],
				bodies[i]));
		else
			mod_assert.ok(expected[i].test(bodies[i]),
			    sprintf('output "%d" didn\'t match ' +
			        '(expected "%s", got "%s")', i, expected[i],
				bodies[i]));
	}
}

function populateData(manta, keys, callback)
{
	log.info('populating keys', keys);

	if (keys.length === 0) {
		callback();
		return;
	}

	var final_err;
	var dirs = keys.filter(
	    function (key) { return (mod_jsprim.endsWith(key, 'dir')); });
	keys = keys.filter(
	    function (key) { return (!mod_jsprim.endsWith(key, 'dir')); });
	var done = {};

	var queue = mod_vasync.queuev({
	    'concurrency': 15,
	    'worker': function (key, subcallback) {
		    if (final_err) {
			    subcallback();
			    return;
		    }

		    if (done[key]) {
			    subcallback();
			    return;
		    }

		    done[key] = true;
		    key = replaceParams(key);

		    if (mod_jsprim.endsWith(key, 'dir')) {
			manta.mkdir(key, function (err) {
				/* Work around node-manta#24. */
				if (err && err.name == 'ConcurrentRequestError')
					log.warn(
					    'ignoring ConcurrentRequestError');
				else if (err)
					final_err = err;

				subcallback();
			});
			return;
		    }

		    var data;
		    if (mod_jsprim.endsWith(key, '0bytes'))
			data = '';
		    else
			data = 'auto-generated content for key /someuser' +
			    mod_path.normalize(key.substr(key.indexOf('/', 1)));

		    var stream = new StringInputStream(data);

		    log.info('PUT key "%s"', key);

		    manta.put(key, stream, { 'size': data.length },
		        function (err) {
				/* Work around node-manta#24. */
				if (err && err.name == 'ConcurrentRequestError')
					log.warn(
					    'ignoring ConcurrentRequestError');
				else if (err)
					final_err = err;

				subcallback();
			});
	    }
	});

	var putKeys = function (keylist) {
		keylist.forEach(function (key) {
			if (key.indexOf('notavalid') == -1)
				queue.push(key);
		});
	};

	if (dirs.length > 0) {
		putKeys(dirs);
	} else {
		putKeys(keys);
		keys = [];
	}

	queue.drain = function () {
		if (keys.length > 0) {
			putKeys(keys);
			keys = [];
		} else {
			callback(final_err);
		}
	};
}

function StringInputStream(contents)
{
	mod_stream.Stream();

	this.s_data = contents;
	this.s_paused = false;
	this.s_done = false;

	this.scheduleEmit();
}

mod_util.inherits(StringInputStream, mod_stream.Stream);

StringInputStream.prototype.pause = function ()
{
	this.s_paused = true;
};

StringInputStream.prototype.resume = function ()
{
	this.s_paused = false;
	this.scheduleEmit();
};

StringInputStream.prototype.scheduleEmit = function ()
{
	var stream = this;

	process.nextTick(function () {
		if (stream.s_paused || stream.s_done)
			return;

		stream.emit('data', stream.s_data);
		stream.emit('end');

		stream.s_data = null;
		stream.s_done = true;
	});
};

/*
 * TODO more test cases:
 * - Input variations:
 *   - 0 input keys
 *   - non-existent key
 *   - directory
 * - Other features
 *   - user code fails on some inputs (e.g., "grep" job)
 *   - uses assets for both M and R phases
 *   - phase emits more than 5 keys (using taskoutput records)
 *   - cancellation
 */
