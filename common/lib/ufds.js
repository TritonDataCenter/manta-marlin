/*
 * common/lib/ufds.js: UFDS utility functions
 */

var mod_assert = require('assert');

var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');

var VError = require('verror');

/* public interface */
exports.ufdsMakeAccounts = ufdsMakeAccounts;

/*
 * Given a configuration that defines accounts, subusers, policies, and roles,
 * asynchronously and idempotently ensure that the given configuration exists.
 * Arguments:
 *
 *     ufds		UFDS client object (from node-sdc-clients.git)
 *
 *     log		Bunyan-style logger
 *
 *     config		A configuration object, which has this general form:
 *
 *              {
 *                  ACCOUNT1: {
 *                      'template': TEMPLATE_USER,
 *                      'keyid': KEY_ID,
 *                      'subusers': [ USER1, ... ],
 *                      'policies': {
 *                          POLICY_NAME1: RULE,
 *                          ...
 *                      },
 *                      'roles': {
 *                          ROLE_NAME1: {
 *                              'user': USERN,
 *                              'policy': POLICY_NAMEN
 *                          }, ...
 *                      }
 *                  }, ...
 *              }
 *
 * When the pipeline completes successfully, all accounts and users have been
 * created based on the corresponding account's template user, and all policies
 * and roles have been created.  If any of these already exist, they're assumed
 * to be correct.
 *
 * This mechanism is not fully generic since in general, rules can have more
 * than policy and roles can have members other than a single user.
 *
 * Here's an example configuration that creates two accounts (acctA and acctB)
 * with subusers and with policies and roles on the first account:
 *
 *    {
 *        "acctA": {
 *            "subusers": [ "authzA1", "authzA2" ]
 *            "template": "poseidon",
 *            "keyid": "35:28:81:b7:c6:d9:b1:b3:0f:63:8d:7c:1d:d8:45:fe",
 *            "policies": {
 *                "readall": "can readobject",
 *                "readjob": "can readobject when fromjob=true",
 *                "readhttp": "can readobject when fromjob=false"
 *            },
 *            "roles": {
 *                "authzA1-readall": {
 *                    "user": "authzA1",
 *                    "policy": "readall"
 *                },
 *                "authzA1-readjob": {
 *                    "user": "authzA1",
 *                    "policy": "readjob"
 *                },
 *                "authzA1-readhttp": {
 *                    "user": "authzA1",
 *                    "policy": "readhttp"
 *                }
 *            }
 *        },
 *        "acctB": {
 *            "subusers": [ "authzB1" ],
 *            "template": "poseidon",
 *            "keyid": "35:28:81:b7:c6:d9:b1:b3:0f:63:8d:7c:1d:d8:45:fe"
 *        }
 *    }
 */
function ufdsMakeAccounts(arg, usercallback)
{
	var log, ufds, cfg, ctx;
	var ops, funcs;

	mod_assert.equal('object', typeof (arg));
	mod_assert.ok(arg !== null);
	mod_assert.equal('object', typeof (arg.ufds));
	mod_assert.ok(arg.ufds !== null);
	mod_assert.equal('object', typeof (arg.log));
	mod_assert.ok(arg.log !== null);
	mod_assert.equal('object', typeof (arg.config));
	mod_assert.ok(arg.config !== null);

	log = arg.log;
	ufds = arg.ufds;
	cfg = mod_jsprim.deepCopy(arg.config);

	/*
	 * To make a complicated operation like this debuggable, it's critical
	 * that we can access all of the asynchronous state from a debugger.  As
	 * a result, we don't use local variable, but rather store everything
	 * off this "context" object, which we'll use as the "input" argument to
	 * the vasync pipeline.  That way, the caller can expose this over kang
	 * or a REPL or whatever and engineers can always find it with MDB and
	 * see exactly what's happened, what state was retrieved from UFDS, and
	 * what operation is outstanding.
	 *
	 * Do NOT add local variable to keep track of state here.
	 */
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

	/*
	 * What we're trying to do is deceptively complicated because we want to
	 * avoid tearing down and recreating the world if we can, but we want to
	 * make sure every user, subuser, ssh key, policy, and role exists, and
	 * there are dependencies between them.  Besides that, many of these
	 * operations must be phrased in terms of identifiers like account uuids
	 * and LDAP distinguished names that we don't have yet (and won't have
	 * until we start executing the pipeline).  Finally, many of these
	 * operations are similar to each other (e.g., adding a key to a user
	 * vs. adding one to an account), so it's useful to be able to refer to
	 * these chunks of code multiple times, but a function isn't necessarily
	 * the appropriate abstraction here, since there's a lot of context as
	 * well.
	 *
	 * So we go through two phases: first we compile a list of operations,
	 * which are objects describing what we'd logically like to do (e.g.,
	 * create key "foo" under account "bar").  There will be dependencies
	 * between these (e.g., creating a key requires having previously
	 * fetched the account so that we have the account's uuid), but they
	 * will be automatically satisfied by the order in which we generate the
	 * operations.  In the second pass, we generate a function for each
	 * operation and append it to the list of functions that will make up
	 * the pipeline.
	 *
	 * The general pattern here is that we generate "fetch" operations
	 * before "create" operations, and the "create" operation is a noop if
	 * the fetch operation fetched something.  In some cases, we may be able
	 * to do better by trying the create and and seeing if it fails, but
	 * that doesn't work for some objects (see CAPI-411), and we need to
	 * fetch accounts and users in order to check and add ssh keys, anyway.
	 */
	ops = [];
	mod_jsprim.forEachKey(cfg, function (account, accountcfg) {
		/* Fetch the account. */
		ops.push({
		    'kind': 'fetch_account',
		    'account': account
		});

		ops.push({
		    'kind': 'fetch_account_key',
		    'account': account,
		    'keyid': accountcfg.keyid
		});

		/* Fetch all subusers on the account. */
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

		/*
		 * Fetch the template account.  We won't know if we actually
		 * need to do this until after we try fetching the main account
		 * and users, but we want to generate the logic ahead of time so
		 * it's easiest to just always fetch it.
		 */
		ops.push({
		    'kind': 'fetch_account',
		    'account': accountcfg.template
		});

		ops.push({
		    'kind': 'fetch_account_key',
		    'account': accountcfg.template,
		    'keyid': accountcfg.keyid
		});

		/* Next, create the account. */
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

		/* Now create the subusers. */
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

		/* Fetch policies and create any that don't exist. */
		mod_jsprim.forEachKey(accountcfg.policies,
		    function (policyname, rule) {
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

		/* Fetch roles and create any that don't exist. */
		mod_jsprim.forEachKey(accountcfg.roles,
		    function (rolename, rolecfg) {
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

	/*
	 * Now convert the operations into separate functions.
	 */
	funcs = [];
	ops.forEach(function (op) {
		if (op.kind == 'fetch_account') {
			funcs.push(function (_, callback) {
				log.info(op, 'starting op');
				ufdsFetch(log, ufds, ufds.getUser,
				    [ op.account ], function (ret) {
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
				ufdsFetch(log, ufds, ufds.getUser, [ op.user,
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
				ufdsFetch(log, user, user.getKey, [ op.keyid ],
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
				ufdsFetch(log, user, user.getKey, [ op.keyid ],
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
				ufdsFetch(log, ufds, ufds.getPolicy,
				    [ accountid, op.name ], function (ret) {
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
				ufdsFetch(log, ufds, ufds.getRole, [ accountid,
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
				    'uniquemember': user.dn,
				    'uniquememberdefault': user.dn
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

	/*
	 * XXX add a "flush" operation
	 */

	return (mod_vasync.pipeline({
	    'input': ctx,
	    'funcs': funcs
	}, usercallback));
}

/*
 * Invoke a UFDS client function that fetches something from UFDS.  If
 * the object is present, invoke "func".  Always invoke "callback",
 * propagating any non-404 errors.
 */
function ufdsFetch(log, client, clientfunc, args, func, callback) {
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
			sargs = Array.prototype.slice.call(arguments, 1);
			func.apply(null, sargs);
			callback();
		}
	});

	clientfunc.apply(client, actual_args);
}
