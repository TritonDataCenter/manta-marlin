/*
 * test/live/tst.authz.js: tests both modern and legacy authorization mechanisms
 * in Marlin jobs.  These tests could in principle be folded into the main
 * testrunner, but since there's a bunch of code to set up and tear down the
 * tests, it's easier to separate them out here.
 */

var mod_assert = require('assert');
var mod_extsprintf = require('extsprintf');
var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var VError = require('verror');

/* jsl:import ../../../common/lib/errors.js */
require('../../lib/errors');

var sprintf = mod_extsprintf.sprintf;

var mod_maufds = require('../../lib/ufds');
var testcommon = require('../common');
var jobcommon = require('./common');
var testapi;

/*
 * In order to exhaustively test access control with jobs, we have to cover
 * several cases.  We'll define these accounts and users:
 *
 *     Account A: users U1, U2
 *     Account B: user U3
 *     Operator:  user U4
 *
 * and we'll test this matrix:
 *
 *    PATH               A   U1   U2  B/U3   U4
 *    /A/public/X        *    *    *    *    *    (any object under /public)
 *    /A/stor/A          *                   *    (A creates object under /stor)
 *    /A/stor/public     *    *    *    *    *    (public object under /stor)
 *    /A/stor/U1         *    *              *    (object readable only by U1)
 *  * /A/stor/U3         *              *    *    (object readable by U3)
 *  * /A/stor/B          *    *         *    *    (object readable by B)
 *
 *  *: These last two cases are pending MANTA-2171.
 *  XXX The starred test cases are blocked on MANTA-2171.
 *  XXX Test cases for the "conditions" that Marlin supplies (e.g.,
 *      fromjob=true) are blocked on joyent/node-aperture#1
 */
var tcAccountA = 'marlin_test_authzA';
var tcAccountB = 'marlin_test_authzB';
var tcUserAnon = 'anonymous';
var tcUserA1 = 'authzA1';
var tcUserA2 = 'authzA2';
var tcUserB1 = 'authzB1';
var tcTestCaseOptions = { 'strict': true };
var tcConfig = {};

testcommon.pipeline({
    'funcs': [
	function setupCommon(_, next) {
		testcommon.setup(function (c) {
			testapi = c;
			next();
		});
	},

	function generateUfdsConfig(_, next) {
		/*
		 * It would be nice if this were totally statically declared,
		 * but in order to use variables as the account names so that we
		 * can change them easily, we have to create the configuration
		 * programmatically.
		 */
		tcConfig = {};
		tcConfig[tcAccountA] = {
		    'subusers': [ tcUserA1, tcUserA2, tcUserAnon ],
		    'template': 'poseidon',
		    'keyid': process.env['MANTA_KEY_ID'],
		    'policies': {
		        'readall': 'can getobject',
		        'readjob': 'can getobject when fromjob = true',
		        'readhttp': 'can getobject when fromjob = false'
		    },
		    'roles': {}
		};
		tcConfig[tcAccountB] = {
		    'subusers': [ tcUserB1 ],
		    'template': 'poseidon',
		    'keyid': process.env['MANTA_KEY_ID']
		};

		for (var pk in tcConfig[tcAccountA].policies) {
			tcConfig[tcAccountA].roles[tcUserA1 + '-' + pk] = {
			    'user': tcUserA1,
			    'policy': pk
			};
			tcConfig[tcAccountA].roles[tcUserAnon + '-' + pk] = {
			    'user': tcUserAnon,
			    'policy': pk
			};
		}
		next();
	},

	function applyUfdsConfig(_, next) {
		mod_maufds.ufdsMakeAccounts({
		    'log': testcommon.log,
		    'ufds': testcommon.ufdsClient(),
		    'config': tcConfig
		}, next);
	},

	function createMantaObjects(_, next) {
		var manta = testcommon.manta;
		var log = testcommon.log;
		var content = 'expected content\n';
		var size = content.length;
		var objects = [];

		mod_vasync.forEachParallel({
		    'inputs': objects,
		    'func': function (objcfg, callback) {
			var account, roles, path, stream;
			var headers, options;

			mod_assert.equal('string', typeof (objcfg.account));
			mod_assert.equal('string', typeof (objcfg.name));
			mod_assert.equal('string', typeof (objcfg.label));

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

			stream = new testcommon.StringInputStream(content);
			log.info({
			    'path': path
			}, 'creating object', { 'headers': headers });
			manta.put(path, stream, options, callback);
		    }
		}, next);
	},

	function runMapJobAsA(_, next) {
		jobcommon.jobTestCaseRun(testapi, {
		    'label': 'jobMauthzA',
		    'account': tcAccountA,
		    'job': {
		        'phases': [ { 'type': 'map', 'exec': 'cat' } ]
		    },
		    'inputs': [],
		    'extra_inputs': [
		        '/%user%/public/X',
		        '/%user%/stor/A',
		        '/%user%/stor/public',
		        '/%user%/stor/U1'
		    ],
		    'timeout': 30 * 1000,
		    'errors': [],
		    'expected_outputs': [
			/\/%user%\/jobs\/.*\/stor\/%user%\/public\/X/,
			/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/A/,
			/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/public/,
			/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/U1/
		    ]
		}, tcTestCaseOptions, next);
	},

	function runMapJobAsA1(_, next) {
		jobcommon.jobTestCaseRun(testapi, {
		    'label': 'jobMauthzA1',
		    'account': tcAccountA,
		    'user': tcUserA1,
		    'job': {
		        'phases': [ { 'type': 'map', 'exec': 'cat' } ]
		    },
		    'inputs': [],
		    'extra_inputs': [
		        '/%user%/public/X',
		        '/%user%/stor/A',
		        '/%user%/stor/public',
		        '/%user%/stor/U1'
		    ],
		    'timeout': 30 * 1000,
		    'errors': [ {
			'phaseNum': '0',
			'what': 'phase 0: map input "/%user%/stor/A"',
			'code': EM_AUTHORIZATION,
			'message': 'permission denied: "/%user%/stor/A"',
			'input': '/%user%/stor/A',
			'p0input': '/%user%/stor/A'
		    } ],
		    'expected_outputs': [
			/\/%user%\/jobs\/.*\/stor\/%user%\/public\/X/,
			/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/public/,
			/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/U1/
		    ]
		}, tcTestCaseOptions, next);
	},

	function runMapJobAsA2(_, next) {
		jobcommon.jobTestCaseRun(testapi, {
		    'label': 'jobMauthzA2',
		    'account': tcAccountA,
		    'user': tcUserA2,
		    'job': {
		        'phases': [ { 'type': 'map', 'exec': 'cat' } ]
		    },
		    'inputs': [],
		    'extra_inputs': [
		        '/%user%/public/X',
		        '/%user%/stor/A',
		        '/%user%/stor/public',
		        '/%user%/stor/U1'
		    ],
		    'timeout': 30 * 1000,
		    'errors': [ {
			'phaseNum': '0',
			'what': 'phase 0: map input "/%user%/stor/A"',
			'code': EM_AUTHORIZATION,
			'message': 'permission denied: "/%user%/stor/A"',
			'input': '/%user%/stor/A',
			'p0input': '/%user%/stor/A'
		    }, {
			'phaseNum': '0',
			'what': 'phase 0: map input "/%user%/stor/U1"',
			'code': EM_AUTHORIZATION,
			'message': 'permission denied: "/%user%/stor/U1"',
			'input': '/%user%/stor/U1',
			'p0input': '/%user%/stor/U1'
		    } ],
		    'expected_outputs': [
			/\/%user%\/jobs\/.*\/stor\/%user%\/public\/X/,
			/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/public/
		    ]
		}, tcTestCaseOptions, next);
	},

	function runMapJobAsB(_, next) {
		jobcommon.jobTestCaseRun(testapi, {
		    'label': 'jobMauthzB',
		    'account_objects': tcAccountA,
		    'account': tcAccountB,
		    'job': {
		        'phases': [ { 'type': 'map', 'exec': 'cat' } ]
		    },
		    'inputs': [],
		    'extra_inputs': [
		        '/%user%/public/X',
		        '/%user%/stor/A',
		        '/%user%/stor/public',
		        '/%user%/stor/U1'
		    ],
		    'timeout': 30 * 1000,
		    'errors': [ {
			'phaseNum': '0',
			'what': 'phase 0: map input "/%user%/stor/A"',
			'code': EM_AUTHORIZATION,
			'message': 'permission denied: "/%user%/stor/A"',
			'input': '/%user%/stor/A',
			'p0input': '/%user%/stor/A'
		    }, {
			'phaseNum': '0',
			'what': 'phase 0: map input "/%user%/stor/U1"',
			'code': EM_AUTHORIZATION,
			'message': 'permission denied: "/%user%/stor/U1"',
			'input': '/%user%/stor/U1',
			'p0input': '/%user%/stor/U1'
		    } ],
		    'expected_outputs': [
			/\/%jobuser%\/jobs\/.*\/stor\/%user%\/public\/X/,
			/\/%jobuser%\/jobs\/.*\/stor\/%user%\/stor\/public/
		    ]
		}, tcTestCaseOptions, next);
	},

	function runMapJobAsB1(_, next) {
		jobcommon.jobTestCaseRun(testapi, {
		    'label': 'jobMauthzB1',
		    'account_objects': tcAccountA,
		    'account': tcAccountB,
		    'user': tcUserB1,
		    'job': {
		        'phases': [ { 'type': 'map', 'exec': 'cat' } ]
		    },
		    'inputs': [],
		    'extra_inputs': [
		        '/%user%/public/X',
		        '/%user%/stor/A',
		        '/%user%/stor/public',
		        '/%user%/stor/U1'
		    ],
		    'timeout': 30 * 1000,
		    'errors': [ {
			'phaseNum': '0',
			'what': 'phase 0: map input "/%user%/stor/A"',
			'code': EM_AUTHORIZATION,
			'message': 'permission denied: "/%user%/stor/A"',
			'input': '/%user%/stor/A',
			'p0input': '/%user%/stor/A'
		    }, {
			'phaseNum': '0',
			'what': 'phase 0: map input "/%user%/stor/U1"',
			'code': EM_AUTHORIZATION,
			'message': 'permission denied: "/%user%/stor/U1"',
			'input': '/%user%/stor/U1',
			'p0input': '/%user%/stor/U1'
		    } ],
		    'expected_outputs': [
			/\/%jobuser%\/jobs\/.*\/stor\/%user%\/public\/X/,
			/\/%jobuser%\/jobs\/.*\/stor\/%user%\/stor\/public/
		    ]
		}, tcTestCaseOptions, next);
	},

	function runLegacyMapJobAsA(_, next) {
		jobcommon.jobTestCaseRun(testapi, {
		    'label': 'jobMauthzLegacyA',
		    'account': tcAccountA,
		    'legacy_auth': true,
		    'job': {
		        'phases': [ { 'type': 'map', 'exec': 'cat' } ]
		    },
		    'inputs': [],
		    'extra_inputs': [
		        '/%user%/public/X',
		        '/%user%/stor/A',
		        '/%user%/stor/public',
		        '/%user%/stor/U1'
		    ],
		    'timeout': 30 * 1000,
		    'errors': [],
		    'expected_outputs': [
			/\/%user%\/jobs\/.*\/stor\/%user%\/public\/X/,
			/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/A/,
			/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/public/,
			/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/U1/
		    ]
		}, tcTestCaseOptions, next);
	},

	function runLegacyMapJobAsB(_, next) {
		jobcommon.jobTestCaseRun(testapi, {
		    'label': 'jobMauthzLegacyB',
		    'account_objects': tcAccountA,
		    'account': tcAccountB,
		    'legacy_auth': true,
		    'job': {
		        'phases': [ { 'type': 'map', 'exec': 'cat' } ]
		    },
		    'inputs': [],
		    'extra_inputs': [
		        '/%user%/public/X',
		        '/%user%/stor/A',
		        '/%user%/stor/public',
		        '/%user%/stor/U1'
		    ],
		    'timeout': 30 * 1000,
		    'errors': [ {
			'phaseNum': '0',
			'what': 'phase 0: map input "/%user%/stor/A"',
			'code': EM_AUTHORIZATION,
			'message': 'permission denied: "/%user%/stor/A"',
			'input': '/%user%/stor/A',
			'p0input': '/%user%/stor/A'
		    }, {
			'phaseNum': '0',
			'what': 'phase 0: map input "/%user%/stor/U1"',
			'code': EM_AUTHORIZATION,
			'message': 'permission denied: "/%user%/stor/U1"',
			'input': '/%user%/stor/U1',
			'p0input': '/%user%/stor/U1'
		    }, {
			/*
			 * Using legacy authorization, account B cannot access
			 * public objects under account A's private directory
			 * because the credentials aren't available to allow
			 * that.
			 */
			'phaseNum': '0',
			'what': 'phase 0: map input "/%user%/stor/public"',
			'code': EM_AUTHORIZATION,
			'message': 'permission denied: "/%user%/stor/public"',
			'input': '/%user%/stor/public',
			'p0input': '/%user%/stor/public'
		    } ],
		    'expected_outputs': [
			/\/%jobuser%\/jobs\/.*\/stor\/%user%\/public\/X/
		    ]
		}, tcTestCaseOptions, next);
	},

	function teardown(_, next) {
		testcommon.teardown(testapi, next);
	}
    ]
});
