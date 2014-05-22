/*
 * test/live/tst.authz.js: tests both modern and legacy authorization mechanisms
 * in Marlin jobs.  These tests could in principle be folded into the main
 * testrunner, but since there's a bunch of code to set up and tear down the
 * tests, it's easier to separate them out here.
 */

var mod_assert = require('assert');
var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');
var VError = require('verror');

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
 *    /A/stor/X          *    *    *         *    (object readable by U1, U2)
 *    /A/stor/public     *    *    *    *    *    (public object under /stor)
 *    /A/stor/U1_only    *    *              *    (object readable only by U1)
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
var tcUserA1 = 'authzA1';
var tcUserA2 = 'authzA2';
var tcUserB1 = 'authzB1';
var tcAccountOperator = process.env['MANTA_USER'];
var tcOptions = { 'strict': true };
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

	setupAuthz,

//	function runLegacy(_, next) {
//		jobcommon.jobTestCaseRun(testapi, tcAuthzLegacy,
//		    tcOptions, next);
//	},
//
//	function runModern(_, next) {
//		jobcommon.jobTestCaseRun(testapi, tcAuthzModern,
//		    tcOptions, next);
//	},

	function teardown(_, next) {
		testcommon.teardown(testapi, next);
	}
    ]
});

/*
 * Create any accounts, roles, and objects needed to execute these tests and
 * make sure permissions are set appropriately.
 */
function setupAuthz(_0, callback)
{
	var log, pk, config;
	var pipelineconf;

	log = testcommon.log;
	config = {};
	config[tcAccountA] = {
	    'subusers': [ tcUserA1, tcUserA2 ],
	    'template': 'poseidon',
	    'keyid': process.env['MANTA_KEY_ID'],
	    'policies': {
	        'readall': 'can readobject',
	        'readjob': 'can readobject when fromjob=true',
	        'readhttp': 'can readobject when fromjob=false'
	    },
	    'roles': {}
	};
	config[tcAccountB] = {
	    'subusers': [ tcUserB1 ],
	    'template': 'poseidon',
	    'keyid': process.env['MANTA_KEY_ID']
	};

	for (pk in config[tcAccountA].policies) {
		config[tcAccountA].roles[tcUserA1 + '-' + pk] = {
		    'user': tcUserA1,
		    'policy': pk
		};
	}

	pipelineconf = configCompile({
	    'log': log,
	    'ufds': testcommon.ufdsClient(),
	    'config': config
	});

	mod_vasync.pipeline(pipelineconf, callback);
}

/*
 * Given a configuration defining accounts, subusers, policies, and roles,
 * return a vasync 'pipeline' configuration that will idempotently generate the
 * given configuration.  The configuration looks like this:
 *
 *    {
 *        ACCOUNT1: {
 *            'template': TEMPLATE_USER,
 *            'keyid': KEY_ID,
 *            'subusers': [ USER1, ... ],
 *            'policies': {
 *                POLICY_NAME1: RULE,
 *                ...
 *            },
 *            'roles': {
 *                ROLE_NAME1: {
 *                    'user': USERN,
 *                    'policy': POLICY_NAMEN
 *                }, ...
 *            }
 *        }, ...
 *    }
 *
 * When the pipeline completes successfully, all accounts and users have been
 * created based on the template user, and all policies and roles have been
 * created.  If any of these already exist, they're assumed to be complete and
 * correct.
 *
 * This mechanism is not fully generic since in general, rules can have more
 * than policy and roles can have members other than a single user.
 */
function configCompile(arg)
{
	var log, ufds, cfg, ctx;
	var funcs;

	log = arg.log;
	ufds = arg.ufds;
	cfg = mod_jsprim.deepCopy(arg.config);
	ctx = {
	    'cc_ufds': ufds,	 /* ufds handle */
	    'cc_cfg': cfg,	 /* original, unmodified configuration */
	    'cc_accountids': {}, /* account login -> account uuid */
	    'cc_userids': {},	 /* account login -> user login -> user uuid */
	    'cc_users': {},	 /* account/user uuid -> record */
	    'cc_haskey': {},	 /* account/user uuid -> boolean */
	    'cc_keys': {},	 /* keyid -> key */
	    'cc_policies': {},	 /* account login -> policy name -> policy */
	    'cc_roles': {}	 /* account login -> role name -> role */
	};

	funcs = [];

	/*
	 * Invoke a UFDS client function that fetches something from UFDS.  If
	 * the object is present, invoke "func".  Always invoke "callback",
	 * propagating any non-404 errors.
	 */
	function ufdsFetch(client, clientfunc, args, func, callback) {
		var actual_args = args.slice(0);
		var logkey = { 'func': clientfunc.name, 'args': args };
		log.info(logkey);

		actual_args.push(function (err) {
			var sargs;
			if (err && err.statusCode != 404) {
				log.error(err, logkey);
				callback(new VError(err, '%s(%s)',
				    clientfunc.name, args.join(', ')));
			} else if (err) {
				log.info(logkey, 'object missing');
				callback();
			} else {
				log.info(logkey, 'object found');
				sargs = Array.prototype.slice.call(
				    arguments, 1);
				func.apply(null, sargs);
				callback();
			}
		});

		clientfunc.apply(client, actual_args);
	}

	var ops = [];
	mod_jsprim.forEachKey(cfg, function (account, accountcfg) {
		ops.push({
		    'kind': 'fetch_account',
		    'account': account
		});

		ops.push({
		    'kind': 'fetch_account_key',
		    'account': account,
		    'keyid': accountcfg.keyid
		});

		accountcfg.subusers.forEach(function (userlogin) {
			ops.push({
			    'kind': 'fetch_user',
			    'account': account,
			    'user': userlogin
			});

			ops.push({
			    'kind': 'fetch_user_key',
			    'account': account,
			    'user': userlogin,
			    'keyid': accountcfg.keyid
			});
		});

		ops.push({
		    'kind': 'fetch_account',
		    'account': accountcfg.template
		});

		ops.push({
		    'kind': 'fetch_account_key',
		    'account': accountcfg.template,
		    'keyid': accountcfg.keyid
		});

		ops.push({
		    'kind': 'create_account',
		    'account': account,
		    'template': accountcfg.template
		});

		ops.push({
		    'kind': 'create_account_key',
		    'account': account,
		    'template': accountcfg.template,
		    'keyid': accountcfg.keyid
		});

		accountcfg.subusers.forEach(function (userlogin) {
			ops.push({
			    'kind': 'create_user',
			    'account': account,
			    'user': userlogin,
			    'template': accountcfg.template
			});

			ops.push({
			    'kind': 'create_user_key',
			    'account': account,
			    'user': userlogin,
			    'keyid': accountcfg.keyid
			});
		});

		mod_jsprim.forEachKey(accountcfg.policies,
		    function (policyname, rule) {
			/*
			 * We could just always try to create the policy and
			 * let that fail if it exists but that currently doesn't
			 * work due to CAPI-411.
			 */
			ops.push({
			    'kind': 'fetch_policy',
			    'account': account,
			    'name': policyname
			});

			ops.push({
			    'kind': 'create_policy',
			    'account': account,
			    'name': policyname,
			    'rule': rule
			});
		    });

		mod_jsprim.forEachKey(accountcfg.roles,
		    function (rolename, rolecfg) {
			/*
			 * We could just always try to create the role and let
			 * that fail if it exists but that currently doesn't
			 * work due to CAPI-411.
			 */
			ops.push({
			    'kind': 'fetch_role',
			    'account': account,
			    'name': rolename
			});

			ops.push({
			    'kind': 'create_role',
			    'account': account,
			    'name': rolename,
			    'user': rolecfg.user,
			    'policy': rolecfg.policy
			});
		    });
	});

	ops.forEach(function (op) {
		if (op.kind == 'fetch_account') {
			funcs.push(function (_, callback) {
				log.info(op, 'starting op');
				ufdsFetch(ufds, ufds.getUser, [ op.account ],
				    function (ret) {
					ctx.cc_accountids[op.account] =
					    ret['uuid'];
					ctx.cc_users[ret['uuid']] = ret;
					ctx.cc_userids[op.account] = {};
					ctx.cc_policies[op.account] = {};
					ctx.cc_roles[op.account] = {};
				    }, callback);
			});
		} else if (op.kind == 'fetch_user') {
			funcs.push(function (_, callback) {
				log.info(op, 'starting op');
				if (!ctx.cc_accountids.hasOwnProperty(
				    op.account)) {
					setImmediate(callback);
					return;
				}

				var accountid = ctx.cc_accountids[op.account];
				ufdsFetch(ufds, ufds.getUser, [ op.user,
				    accountid ], function (ret) {
					ctx.cc_userids[op.account][op.user] =
					    ret['uuid'];
					ctx.cc_users[ret['uuid']] = ret;
				    }, callback);
			});
		} else if (op.kind == 'fetch_account_key') {
			funcs.push(function (_, callback) {
				log.info(op, 'starting op');
				if (!ctx.cc_accountids.hasOwnProperty(
				    op.account)) {
					setImmediate(callback);
					return;
				}

				var accountid = ctx.cc_accountids[op.account];
				var user = ctx.cc_users[accountid];
				ufdsFetch(user, user.getKey, [ op.keyid ],
				    function (ret) {
					ctx.cc_keys[op.keyid] = ret;
					ctx.cc_haskey[accountid] = true;
				    }, callback);
			});
		} else if (op.kind == 'fetch_user_key') {
			funcs.push(function (_, callback) {
				log.info(op, 'starting op');
				if (!ctx.cc_userids.hasOwnProperty(
				    op.account) ||
				    !ctx.cc_userids[op.account].hasOwnProperty(
				    op.user)) {
					setImmediate(callback);
					return;
				}

				var userid = ctx.cc_userids[op.account][
				    op.user];
				var user = ctx.cc_users[userid];
				ufdsFetch(user, user.getKey, [ op.keyid ],
				    function (ret) {
					ctx.cc_keys[op.keyid] = ret;
					ctx.cc_haskey[userid] = true;
				    }, callback);
			});
		} else if (op.kind == 'create_account' ||
		    op.kind == 'create_user') {
			funcs.push(function (_, callback) {
				log.info(op, 'starting op');
				if (op.kind == 'create_account' &&
				    ctx.cc_accountids.hasOwnProperty(
				    op.account)) {
					setImmediate(callback);
					return;
				}

				if (op.kind == 'create_user') {
					/* We must have created the account. */
					mod_assert.ok(ctx.cc_accountids.
					    hasOwnProperty(op.account));

					if (ctx.cc_userids[op.account].
					    hasOwnProperty(op.user)) {
						setImmediate(callback);
						return;
					}
				}

				var tplaccount, tplaccountid, template;
				var newemail, newuser;

				tplaccount = op.template;
				if (!ctx.cc_accountids.hasOwnProperty(
				    tplaccount)) {
					setImmediate(callback, new VError(
					    'failed to create account "%s"%s:' +
					    ' template user "%s" was not found',
					    op.account,
					    op.user ?
					    ' user "' + op.user + '"' : '',
					    tplaccount));
					return;
				}

				tplaccountid = ctx.cc_accountids[tplaccount];
				template = ctx.cc_users[tplaccountid];
				newemail = template['email'].replace('@',
				    '+' + mod_uuid.v4().substr(0, 8) + '@');
				newuser = {
				    'login': op.user || op.account,
				    'userpassword': 'secret123',
				    'email': newemail,
				    'approved_for_provisioning': true
				};

				if (op.kind == 'create_user') {
					newuser['account'] = ctx.cc_accountids[
					    op.account];
				}

				log.info(newuser, 'creating user');
				ufds.addUser(newuser,
				    function (err, ret) {
					if (err) {
						log.error(err, 'creating user',
						    newuser);
						callback(new VError(err,
						    'creating user "%s"',
						    op.account));
						return;
					}

					log.info(newuser, 'created user');
					if (op.kind == 'create_account') {
						ctx.cc_accountids[op.account] =
						    ret['uuid'];
						ctx.cc_userids[op.account] = {};
						ctx.cc_policies[
						    op.account] = {};
						ctx.cc_roles[op.account] = {};
					} else {
						ctx.cc_userids[op.account][
						    op.user] = ret['uuid'];
					}
					ctx.cc_users[ret['uuid']] = ret;
					callback();
				    });

			});
		} else if (op.kind == 'create_account_key' ||
		    op.kind == 'create_user_key') {
			funcs.push(function (_, callback) {
				log.info(op, 'starting op');
				var account, accountid, userlogin, userid, user;
				var key;

				account = op.account;

				if (!ctx.cc_keys.hasOwnProperty(op.keyid)) {
					setImmediate(callback,
					    new VError('key "%s" for ' +
					        'account "%s"%s hasn\'t been ' +
						'fetched', op.keyid, account,
						op.user ?
						' user "' + op.user + '"' :
						''));
					return;
				}

				/* We must have created the account. */
				mod_assert.ok(ctx.cc_accountids.hasOwnProperty(
				    op.account));
				accountid = ctx.cc_accountids[account];

				if (op.kind == 'create_account_key') {
					user = ctx.cc_users[accountid];
					if (ctx.cc_haskey[accountid]) {
						setImmediate(callback);
						return;
					}
				} else {
					userlogin = op.user;

					/* We must have created the user. */
					mod_assert.ok(ctx.cc_userids[account].
					    hasOwnProperty(userlogin));
					userid = ctx.cc_userids[account][
					    userlogin];
					user = ctx.cc_users[userid];
					if (ctx.cc_haskey[userid]) {
						setImmediate(callback);
						return;
					}
				}

				log.info(op, 'adding key');
				key = ctx.cc_keys[op.keyid];
				user.addKey(key['openssh'], function (err) {
					if (err) {
						log.error(err,
						    'adding key', op);
						callback(new VError(err,
						    'adding key'));
					} else {
						log.info('added key');
						callback();
					}
				});
			});
		} else if (op.kind == 'fetch_policy') {
			funcs.push(function (_, callback) {
				log.info(op, 'starting op');

				var accountid = ctx.cc_accountids[op.account];
				ufdsFetch(ufds, ufds.getPolicy, [ accountid,
				    op.name ], function (ret) {
					ctx.cc_policies[op.account][op.name] =
					    ret;
				    }, callback);
			});
		} else if (op.kind == 'create_policy') {
			funcs.push(function (_, callback) {
				log.info(op, 'starting op');

				if (ctx.cc_policies[op.account].
				    hasOwnProperty(op.name)) {
					setImmediate(callback);
					return;
				}

				var accountid = ctx.cc_accountids[op.account];
				var policy = {
				    'name': op.name,
				    'account': accountid,
				    'rule': op.rule
				};
				log.info({
				    'account': op.account,
				    'accountid': accountid,
				    'policy': policy
				}, 'creating policy');
				ufds.addPolicy(accountid, policy,
				    function (err, ret) {
					if (err) {
						log.error(err,
						    'policy create failed');
						callback(new VError(err,
						    'policy created failed'));
					} else {
						log.info(ret, 'created policy');
						ctx.cc_policies[op.account][
						    op.name] = ret;
						callback();
					}
				    });
			});
		} else if (op.kind == 'fetch_role') {
			funcs.push(function (_, callback) {
				log.info(op, 'starting op');

				var accountid = ctx.cc_accountids[op.account];
				ufdsFetch(ufds, ufds.getRole, [ accountid,
				    op.name ], function (ret) {
					ctx.cc_roles[op.account][op.name] = ret;
				    }, callback);
			});
		} else if (op.kind == 'create_role') {
			funcs.push(function (_, callback) {
				log.info(op, 'starting op');

				if (ctx.cc_roles[op.account].
				    hasOwnProperty(op.name)) {
					setImmediate(callback);
					return;
				}

				mod_assert.ok(ctx.cc_policies[op.account].
				    hasOwnProperty(op.policy));
				var policy = ctx.cc_policies[op.account][
				    op.policy];

				mod_assert.ok(ctx.cc_userids[op.account][
				    op.user]);
				var userid = ctx.cc_userids[op.account][
				    op.user];
				var user = ctx.cc_users[userid];

				var accountid = ctx.cc_accountids[op.account];
				var role = {
				    'name': op.name,
				    'account': accountid,
				    'memberpolicy': policy.dn,
				    'uniquemember': user.dn
				};
				log.info(role, 'creating role');
				ufds.addRole(accountid, role,
				    function (err, ret) {
					if (err) {
						log.error(err, 'creating role');
						callback(new VError(err,
						    'creating role'));
					} else {
						log.info('created role', ret);
						ctx.cc_roles[op.account][
						    op.name] = ret;
						callback();
					}
				    });
			});
		} else {
			throw (new VError('unexpected operation type: "%s"',
			    op.kind));
		}
	});

	return ({
	    'input': ctx,
	    'funcs': funcs
	});
}
