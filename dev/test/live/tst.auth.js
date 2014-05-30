/*
 * tst.auth.js: Authorization/authentication-related test cases.
 */

var mod_assert = require('assert');
var mod_extsprintf = require('extsprintf');
var mod_jsprim = require('jsprim');
var mod_vasync = require('vasync');

var mod_livetests = require('./common');
var mod_maufds = require('../../lib/ufds');
var mod_testcommon = require('../common');
var sprintf = mod_extsprintf.sprintf;
var StringInputStream = mod_testcommon.StringInputStream;

/* jsl:import ../../../common/lib/errors.js */
require('../../lib/errors');

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
var authzTestCases = {};
var authzUfdsConfig;

function main()
{
	initAuthzUfdsConfig();
	initAuthzTestCases();
	mod_livetests.jobTestRunner(authzTestCases, process.argv, 5);
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
	        'readall': 'can getobject'
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
		mod_assert.ok(!authzTestCases.hasOwnProperty(
		    testcase['label']));
		authzTestCases[testcase['label']] = testcase;

		if (tccfg['legacy_auth'])
			return;

		testcase = authzMakeOneTestCase(template, tccfg,
		    expected_errors, 'jobR');
		testcase['job']['phases'][0]['type'] = 'reduce';
		testcase['expected_outputs'].push(new RegExp(''));
		mod_assert.ok(!authzTestCases.hasOwnProperty(
		    testcase['label']));
		authzTestCases[testcase['label']] = testcase;
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
				mod_testcommon.log.info({
				    'path': path
				}, 'creating object', { 'headers': headers });
				manta.put(path, stream, options, putcb);
			    }
			}, next);
		}
	    ]
	}, callback);
}

main();
