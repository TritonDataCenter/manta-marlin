/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * common/lib/ufds.js: UFDS utility functions
 */

var mod_assert = require('assert');

var mod_jsprim = require('jsprim');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');

var sprintf = require('extsprintf').sprintf;
var VError = require('verror');

/* public interface */
exports.ufdsMakeAccounts = ufdsMakeAccounts;

/*
 * Given a configuration that defines accounts, subusers, policies, and roles,
 * asynchronously and idempotently ensure that the given configuration exists.
 * This function is not concurrent-safe; it assumes nothing else is modifying
 * these objects at the same time.
 *
 * Arguments:
 *
 *     log		Bunyan-style logger
 *
 *     manta		Manta client.  This is currently used with a heuristic
 *     			approach to determine when the UFDS changes have been
 *     			propagated to Mahi.  If this function proves useful
 *     			outside a context where a Manta client is available (or
 *     			if we develop a better approach to waiting on
 *     			replication), this dependency could be removed.
 *
 *     ufds		UFDS client object (from node-ufds.git)
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
 * and roles have been created.  If any of these objects already exist, they're
 * assumed to be correct.
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
 *                "readall": "can getobject",
 *                "readjob": "can getobject when fromjob=true",
 *                "readhttp": "can getobject when fromjob=false"
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
	var ctx;

	mod_assert.equal('object', typeof (arg));
	mod_assert.ok(arg !== null);
	mod_assert.equal('object', typeof (arg.log));
	mod_assert.ok(arg.log !== null);
	mod_assert.equal('object', typeof (arg.manta));
	mod_assert.ok(arg.manta !== null);
	mod_assert.equal('object', typeof (arg.ufds));
	mod_assert.ok(arg.ufds !== null);
	mod_assert.equal('object', typeof (arg.config));
	mod_assert.ok(arg.config !== null);

	/*
	 * See UfdsApplier below.
	 */
	ctx = new UfdsApplier(arg);
	return (ctx.run(usercallback));
}

/*
 * To make this complex operation debuggable, we must be able to access all of
 * the asynchronous state from a debugger.  We don't use local variables, but
 * instead store everything off this context object, which we'll use as the
 * "arg" argument to a vasync pipeline.  That way, the caller can expose this
 * over kang or a REPL or whatever and engineers can always find it with MDB and
 * see exactly what's already happened, what state was retrieved from UFDS, and
 * what operation is outstanding.  Do NOT add local variables to keep track of
 * state here.
 *
 * We also split this into two phases: compiling the list of steps to
 * produce a vasync pipeline, and then executing the vasync pipeline.
 */
function UfdsApplier(arg)
{
	mod_assert.equal('object', typeof (arg));
	mod_assert.ok(arg !== null);
	mod_assert.equal('object', typeof (arg.log));
	mod_assert.ok(arg.log !== null);
	mod_assert.equal('object', typeof (arg.manta));
	mod_assert.ok(arg.manta !== null);
	mod_assert.equal('object', typeof (arg.ufds));
	mod_assert.ok(arg.ufds !== null);
	mod_assert.equal('object', typeof (arg.config));
	mod_assert.ok(arg.config !== null);

	this.cc_log = arg.log;		/* bunyan logger */
	this.cc_manta = arg.manta;	/* manta client */
	this.cc_ufds = arg.ufds; 	/* ufds handle */

	/* original, unmodified configuration */
	this.cc_cfg = mod_jsprim.deepCopy(arg.config);

	this.cc_accountids = {}; /* account login -> account uuid */
	this.cc_userids = {};	 /* account login -> user login -> user uuid */
	this.cc_users = {};	 /* account/user uuid -> record */
	this.cc_haskey = {};	 /* account/user uuid -> boolean */
	this.cc_keys = {};	 /* keyid -> key */
	this.cc_policies = {};	 /* account login -> policy name -> policy */
	this.cc_roles = {};	 /* account login -> role name -> role */
	this.cc_flushes = {};	 /* set of account logins needing flush */

	this.cc_steps = null;	 /* list of pipeline functions */
}

/*
 * Apply the UFDS configuration specified in the constructor.  This is the only
 * public function.
 */
UfdsApplier.prototype.run = function (callback)
{
	mod_assert.ok(this.cc_steps === null,
	    'cannot run() more than once');
	this.compile();
	return (this.pipeline(callback));
};

/*
 * [private] Execute the pipeline constructed by compile().
 */
UfdsApplier.prototype.pipeline = function (callback)
{
	var self, log;

	mod_assert.ok(this.cc_steps !== null,
	    'cannot invoke pipeline() before compile()');

	self = this;
	log = this.cc_log;

	this.cc_pipeline = mod_vasync.forEachPipeline({
	    'inputs': this.cc_steps,
	    'func': function ufdsApplierPipeline(input, stepcb) {
		log.debug(input.op, 'starting op');
		input.func.call(self, input.op, stepcb);
	    }
	}, callback);
	return (this.cc_pipeline);
};

/*
 * [private] Helper function to append a new "operation" to the vasync pipeline.
 */
UfdsApplier.prototype.op = function (func, op)
{
	mod_assert.equal('function', typeof (func));
	mod_assert.equal('object', typeof (op));
	mod_assert.ok(op !== null);
	mod_assert.equal('string', typeof (func.name),
	    'function\'s name must be defined');

	op.opname = func.name;
	this.cc_steps.push({ 'op': op, 'func': func });
};

/*
 * [private] Construct a vasync pipeline to apply the configuration.
 *
 * What we're trying to do is deceptively complicated because we want to avoid
 * tearing down and recreating the world if we can, but we want to make sure
 * every user, subuser, ssh key, policy, and role exists, and there are
 * dependencies between them.  Besides that, many of these operations must be
 * phrased in terms of identifiers like account uuids and LDAP distinguished
 * names that we don't have yet (and won't have until we start executing the
 * pipeline).  Finally, many of these operations are similar to each other
 * (e.g., adding a key to a user vs. adding one to an account), so it's useful
 * to be able to refer to these chunks of code multiple times, but a function
 * isn't necessarily the appropriate abstraction here, since there's a lot of
 * context as well.
 *
 * This pass constructs a list of vasync pipeline functions.  There will be
 * dependencies between these (e.g., creating a key requires having previously
 * fetched the account so that we have the account's uuid), but they will be
 * automatically satisfied by the order in which we generate them.  For more
 * about these operations, see the comment above opFetchAccount() below.
 */
UfdsApplier.prototype.compile = function ()
{
	var ctx = this;
	var cfg = this.cc_cfg;

	this.cc_steps = [];

	mod_jsprim.forEachKey(cfg, function (account, accountcfg) {
		/* Fetch the account. */
		ctx.op(ctx.opFetchAccount, { 'account': account });
		ctx.op(ctx.opFetchAccountKey, {
		    'account': account,
		    'keyid': accountcfg.keyid
		});

		/* Fetch all subusers on the account. */
		accountcfg.subusers.forEach(function (userlogin) {
			ctx.op(ctx.opFetchUser, {
			    'account': account,
			    'user': userlogin
			});
			ctx.op(ctx.opFetchUserKey, {
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
		ctx.op(ctx.opFetchAccount, { 'account': accountcfg.template });
		ctx.op(ctx.opFetchAccountKey, {
		    'account': accountcfg.template,
		    'keyid': accountcfg.keyid
		});

		/* Next, create the account. */
		ctx.op(ctx.opCreateUser, {
		    'account': account,
		    'template': accountcfg.template
		});
		ctx.op(ctx.opCreateUserKey, {
		    'account': account,
		    'template': accountcfg.template,
		    'keyid': accountcfg.keyid
		});

		/* Now create the subusers. */
		accountcfg.subusers.forEach(function (userlogin) {
			ctx.op(ctx.opCreateUser, {
			    'account': account,
			    'user': userlogin,
			    'template': accountcfg.template
			});
			ctx.op(ctx.opCreateUserKey, {
			    'account': account,
			    'user': userlogin,
			    'keyid': accountcfg.keyid
			});
		});

		/* Fetch policies and create any that don't exist. */
		mod_jsprim.forEachKey(accountcfg.policies,
		    function (policyname, rule) {
			ctx.op(ctx.opFetchPolicy, {
			    'account': account,
			    'name': policyname
			});
			ctx.op(ctx.opCreatePolicy, {
			    'account': account,
			    'name': policyname,
			    'rule': rule
			});
		    });

		/* Fetch roles and create any that don't exist. */
		mod_jsprim.forEachKey(accountcfg.roles,
		    function (rolename, rolecfg) {
			ctx.op(ctx.opFetchRole, {
			    'account': account,
			    'name': rolename
			});
			ctx.op(ctx.opCreateRole, {
			    'account': account,
			    'name': rolename,
			    'user': rolecfg.user,
			    'policy': rolecfg.policy
			});
		    });
	});

	/*
	 * Generate flush operations for each account that wait for it to be
	 * replicated.
	 */
	mod_jsprim.forEachKey(cfg, function (account) {
		ctx.op(ctx.opFlushAccount, { 'account': account });
	});
};

/*
 * Invoke a UFDS client function that fetches something from UFDS.  If the
 * object is present, invoke "func".  Always invoke "callback", propagating any
 * non-404 errors.
 */
UfdsApplier.prototype.ufdsFetch = function (client, clientfunc, args, func,
    callback) {
	var log = this.cc_log;
	var actual_args = args.slice(0);
	var logkey = { 'func': clientfunc.name, 'args': args };
	log.debug(logkey);

	actual_args.push(function (err) {
		var sargs;
		if (err && err.statusCode != 404) {
			log.error(err, logkey);
			callback(new VError(err, '%s(%s)',
			    clientfunc.name, args.join(', ')));
		} else if (err) {
			log.debug(logkey, 'object missing');
			callback();
		} else {
			log.debug(logkey, 'object found');
			sargs = Array.prototype.slice.call(arguments, 1);
			func.apply(null, sargs);
			callback();
		}
	});

	clientfunc.apply(client, actual_args);
};

/*
 * The rest of this class defines the possible operations generated by
 * compile().  They all have the same signature (since they're vasync pipeline
 * functions), and they follow a few patterns:
 *
 *    o A "fetch" function does not fail if the object doesn't exist.
 *
 *    o A "fetch" function with dependencies can assume that we've already tried
 *      to satisfy its dependencies.  So if we try to fetch a subuser and the
 *      account doesn't exist locally, then we can skip the fetch, knowing that
 *      it cannot exist.
 *
 *    o A "create" function is a noop if the object was successfully fetched.
 *
 *    o A "create" function can assume that its dependencies have already been
 *      created.  So when we create a user, we can assume that the parent
 *      account has already been created.
 *
 *    o Any function can assume there has been no error up to this point (since
 *      we're using vasync.pipeline()).
 *
 * compile() uses these properties to generate an idempotent pipeline that works
 * from any initial state without knowing what that initial state actually is
 * (since fetching that is part of the pipeline).
 */

UfdsApplier.prototype.opFetchAccount = function opFetchAccount(op, callback)
{
	var ufds = this.cc_ufds;
	var ctx = this;

	this.ufdsFetch(ufds, ufds.getUser, [ op.account ], function (ret) {
		ctx.cc_accountids[op.account] = ret['uuid'];
		ctx.cc_users[ret['uuid']] = ret;
		ctx.cc_userids[op.account] = {};
		ctx.cc_policies[op.account] = {};
		ctx.cc_roles[op.account] = {};
	}, callback);
};

UfdsApplier.prototype.opFetchUser = function opFetchUser(op, callback)
{
	var ufds = this.cc_ufds;
	var ctx = this;
	var accountid;

	if (!this.cc_accountids.hasOwnProperty(op.account)) {
		setImmediate(callback);
		return;
	}

	accountid = this.cc_accountids[op.account];
	this.ufdsFetch(ufds, ufds.getUser, [ op.user, accountid ],
	    function (ret) {
		ctx.cc_userids[op.account][op.user] = ret['uuid'];
		ctx.cc_users[ret['uuid']] = ret;
	    }, callback);
};

UfdsApplier.prototype.opFetchAccountKey = function opFetchAccountKey(op,
    callback)
{
	var ctx = this;
	var accountid, user;

	if (!this.cc_accountids.hasOwnProperty(op.account)) {
		setImmediate(callback);
		return;
	}

	accountid = this.cc_accountids[op.account];
	user = this.cc_users[accountid];
	this.ufdsFetch(user, user.getKey, [ op.keyid ], function (ret) {
		ctx.cc_keys[op.keyid] = ret;
		ctx.cc_haskey[accountid] = true;
	}, callback);
};

UfdsApplier.prototype.opFetchUserKey = function opFetchUserKey(op, callback)
{
	var ctx = this;
	var userid, user;

	if (!this.cc_userids.hasOwnProperty(op.account) ||
	    !this.cc_userids[op.account].hasOwnProperty(op.user)) {
		setImmediate(callback);
		return;
	}

	userid = this.cc_userids[op.account][op.user];
	user = this.cc_users[userid];
	this.ufdsFetch(user, user.getKey, [ op.keyid ], function (ret) {
		ctx.cc_keys[op.keyid] = ret;
		ctx.cc_haskey[userid] = true;
	}, callback);
};

UfdsApplier.prototype.opFetchPolicy = function opFetchPolicy(op, callback)
{
	var ufds = this.cc_ufds;
	var ctx = this;
	var accountid = this.cc_accountids[op.account];

	this.ufdsFetch(ufds, ufds.getPolicy, [ accountid, op.name ],
	    function (ret) {
		ctx.cc_policies[op.account][op.name] = ret;
	    }, callback);
};

UfdsApplier.prototype.opFetchRole = function opFetchRole(op, callback)
{
	var ufds = this.cc_ufds;
	var ctx = this;
	var accountid = this.cc_accountids[op.account];

	this.ufdsFetch(ufds, ufds.getRole, [ accountid, op.name ],
	    function (ret) {
		ctx.cc_roles[op.account][op.name] = ret;
	    }, callback);
};

UfdsApplier.prototype.opCreateUser = function opCreateUser(op, callback)
{
	var log = this.cc_log;
	var ufds = this.cc_ufds;
	var ctx = this;
	var isaccount = op.user === undefined;
	var loglabel = sprintf('account "%s"%s', op.account,
	    isaccount ? '' : ' user "' + op.user + '"');
	var tplaccount, tplaccountid, template, newuser;

	if (isaccount && this.cc_accountids.hasOwnProperty(op.account)) {
		setImmediate(callback);
		return;
	}

	if (!isaccount) {
		/* We must have created the account. */
		mod_assert.ok(this.cc_accountids.hasOwnProperty(op.account));
		if (this.cc_userids[op.account].hasOwnProperty(op.user)) {
			setImmediate(callback);
			return;
		}
	}

	tplaccount = op.template;
	if (!this.cc_accountids.hasOwnProperty(tplaccount)) {
		setImmediate(callback, new VError(
		    'failed to create %s: template account "%s" was not found',
		    op.account, loglabel, tplaccount));
		return;
	}

	tplaccountid = this.cc_accountids[tplaccount];
	template = this.cc_users[tplaccountid];
	newuser = ufdsMakeUserFromTemplate(template, op.user || op.account);
	if (!isaccount)
		newuser['account'] = this.cc_accountids[op.account];

	log.info(newuser, 'creating user');
	ufds.addUser(newuser, function (err, ret) {
		if (err) {
			err = new VError(err, 'creating %s', loglabel);
			log.error(err);
			callback(err);
			return;
		}

		log.debug(newuser, 'created user');
		ctx.cc_flushes[op.account] = true;
		if (isaccount) {
			ctx.cc_accountids[op.account] = ret['uuid'];
			ctx.cc_userids[op.account] = {};
			ctx.cc_policies[op.account] = {};
			ctx.cc_roles[op.account] = {};
		} else {
			ctx.cc_userids[op.account][op.user] = ret['uuid'];
		}

		ctx.cc_users[ret['uuid']] = ret;
		callback();
	});
};

UfdsApplier.prototype.opCreateUserKey = function opCreateUserKey(op, callback)
{
	var log = this.cc_log;
	var account, accountid;
	var userlogin, userid;
	var user, key;
	var isaccount, loglabel;

	isaccount = op.user === undefined;
	loglabel = sprintf('account "%s"%s', op.account,
	    isaccount ? '' : ' user "' + op.user + '"');
	account = op.account;

	if (!this.cc_keys.hasOwnProperty(op.keyid)) {
		setImmediate(callback, new VError(
		    'key "%s" for %s has not been fetched',
		    op.keyid, loglabel));
		return;
	}

	/* We must have created the account. */
	mod_assert.ok(this.cc_accountids.hasOwnProperty(op.account));
	accountid = this.cc_accountids[account];

	if (isaccount) {
		user = this.cc_users[accountid];
		if (this.cc_haskey[accountid]) {
			setImmediate(callback);
			return;
		}
	} else {
		userlogin = op.user;

		/* We must have created the user. */
		mod_assert.ok(this.cc_userids[account].hasOwnProperty(
		    userlogin));
		userid = this.cc_userids[account][userlogin];
		user = this.cc_users[userid];
		if (this.cc_haskey[userid]) {
			setImmediate(callback);
			return;
		}
	}

	log.info(op, 'adding key');
	key = this.cc_keys[op.keyid];
	user.addKey(key['openssh'], function (err) {
		if (err) {
			log.error(err, 'adding key', op);
			callback(new VError(err, 'adding key'));
		} else {
			log.debug('added key');
			callback();
		}
	});
};

UfdsApplier.prototype.opCreatePolicy = function opCreatePolicy(op, callback)
{
	var log = this.cc_log;
	var ufds = this.cc_ufds;
	var ctx = this;
	var accountid, policy;

	if (this.cc_policies[op.account].hasOwnProperty(op.name)) {
		setImmediate(callback);
		return;
	}

	accountid = this.cc_accountids[op.account];
	policy = {
	    'name': op.name,
	    'account': accountid,
	    'rule': op.rule
	};
	log.info({
	    'account': op.account,
	    'accountid': accountid,
	    'policy': policy
	}, 'creating policy');
	ufds.addPolicy(accountid, policy, function (err, ret) {
		if (err) {
			err = new VError(err, 'policy create failed');
			log.error(err);
			callback(err);
		} else {
			log.debug(ret, 'created policy');
			ctx.cc_policies[op.account][op.name] = ret;
			callback();
		}
	});
};

UfdsApplier.prototype.opCreateRole = function opCreateRole(op, callback)
{
	var log = this.cc_log;
	var ufds = this.cc_ufds;
	var ctx = this;
	var policy, userid, user;
	var accountid, role;

	if (this.cc_roles[op.account].hasOwnProperty(op.name)) {
		setImmediate(callback);
		return;
	}

	mod_assert.ok(this.cc_policies[op.account].hasOwnProperty(op.policy));
	policy = this.cc_policies[op.account][op.policy];
	mod_assert.ok(this.cc_userids[op.account][op.user]);
	userid = this.cc_userids[op.account][op.user];
	user = this.cc_users[userid];
	accountid = this.cc_accountids[op.account];
	role = {
	    'name': op.name,
	    'account': accountid,
	    'memberpolicy': policy.dn,
	    'uniquemember': user.dn,
	    'uniquememberdefault': user.dn
	};
	log.info(role, 'creating role');
	ufds.addRole(accountid, role, function (err, ret) {
		if (err) {
			err = new VError(err, 'creating role');
			log.error(err);
			callback(err);
		} else {
			log.debug('created role', ret);
			ctx.cc_roles[op.account][op.name] = ret;
			callback();
		}
	});
};

/*
 * For any accounts that we created, wait for these to be replicated to Mahi.
 * There may be multiple Mahi servers, and we have to wait for all of them to
 * get the objects.  As a crude attempt to wait on this condition, we wait until
 * at least 20 Manta requests complete.  This is obviously cheesy, and you might
 * think that fewer than 20 would reliably work, but it doesn't.  We should also
 * wait for replication of users, policies, and roles, too, but that's trickier,
 * and this is generally good enough.
 */
UfdsApplier.prototype.opFlushAccount = function opFlushAccount(op, callback)
{
	var log = this.cc_log;
	var manta = this.cc_manta;
	var login, path, timeout, errmsg;
	var count, start, iter;

	login = op.account;
	path = sprintf('/%s/stor', login);
	timeout = 30; /* seconds */
	errmsg = sprintf('replication timed out after %ds', timeout);

	if (!this.cc_flushes[login]) {
		setImmediate(callback);
		return;
	}

	log.info(login, 'waiting for replication');
	count = 0;
	start = Date.now();
	iter = function () {
		manta.info(path, function (err, rv) {
			if (err) {
				count = 0;
				if (Date.now() - start > timeout * 1000) {
					callback(new VError(errmsg));
				} else {
					setTimeout(iter, 100);
				}

				return;
			}

			if (++count == 20) {
				log.info('completed %d requests', count);
				callback();
			} else {
				setTimeout(iter, 10);
			}
		});
	};

	iter();
};

function ufdsMakeUserFromTemplate(template, login)
{
	var emailsuffix = '+' + mod_uuid.v4().substr(0, 8) + '@';
	var newemail = template['email'].replace('@', emailsuffix);

	return ({
	    'login': login,
	    'userpassword': 'secret123',
	    'email': newemail,
	    'approved_for_provisioning': true
	});
}
