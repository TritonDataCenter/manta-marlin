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
 *
 * XXX also want to test legacy cases
 * XXX also want to test cases where an object is accessible in a job, but not
 * via Manta, and vice versa (to check "conditions").
 */
var tcAccountA = 'marlin_test_authzA';
var tcAccountB = 'marlin_test_authzB';
var tcUserAnon = 'anonymous';
var tcUserA1 = 'authzA1';
var tcUserA2 = 'authzA2';
var tcUserB1 = 'authzB1';
var tcAccountOperator = process.env['MANTA_USER'];
var tcTestCaseOptions = { 'strict': true };
var tcConfig = {};
var tcAuthzLegacy = {
	/* XXX demo */
	'label': 'jobMAuthzLegacy',
	'job': { 'phases': [ { 'type': 'map', 'exec': 'wc' } ] },
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 15 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'errors': []
};
var tcAuthzModern = {
	/* XXX demo */
	'label': 'jobMAuthzModern',
	'job': { 'phases': [ { 'type': 'map', 'exec': 'wc' } ] },
	'inputs': [ '/%user%/stor/obj1' ],
	'timeout': 15 * 1000,
	'expected_outputs': [
	    /\/%user%\/jobs\/.*\/stor\/%user%\/stor\/obj1\.0\./
	],
	'errors': []
};

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

		objects.push({
		    'label': '/A/public/X: any object under /public',
		    'account': tcAccountA,
		    'public': true,
		    'name': 'X'
		});

		objects.push({
		    'label': '/A/stor/A: A creates object under /A/stor',
		    'account': tcAccountA,
		    'name': 'A'
		});

		objects.push({
		    'label': '/A/stor/public',
		    'account': tcAccountA,
		    'name': 'public',
		    'roles': [ tcUserAnon + '-readall' ]
		});

		objects.push({
		    'label': '/A/stor/U1: object readable only by U1',
		    'account': tcAccountA,
		    'roles': [ tcUserA1 + '-readall' ],
		    'name': 'U1'
		});

		/*
		 * XXX want /A/stor/B and /A/stor/U3, which are blocked on
		 * MANTA-2171.
		 */

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
		    'timeout': 15 * 1000,
		    'errors': [],
		    'expected_outputs': [
			/\/%user%\/jobs\/.*\/stor\/%user%\/public\/X/,
			/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/A/,
			/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/public/,
			/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/U1/
		    ]
		}, tcTestCaseOptions, next);
	},

	function rumMapJobAsA1(_, next) {
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
		    'timeout': 15 * 1000,
		    'errors': [],
		    'expected_outputs': [
			/\/%user%\/jobs\/.*\/stor\/%user%\/public\/X/,
			/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/public/,
			/\/%user%\/jobs\/.*\/stor\/%user%\/stor\/U1/
		    ]
		}, tcTestCaseOptions, next);
	},

//	function runLegacy(_, next) {
//		jobcommon.jobTestCaseRun(testapi, tcAuthzLegacy,
//		    tcTestCaseOptions, next);
//	},
//
//	function runModern(_, next) {
//		jobcommon.jobTestCaseRun(testapi, tcAuthzModern,
//		    tcTestCaseOptions, next);
//	},

	function teardown(_, next) {
		testcommon.teardown(testapi, next);
	}
    ]
});
