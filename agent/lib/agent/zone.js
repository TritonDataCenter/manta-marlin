/*
 * lib/agent/zone.js: compute zone management
 *
 * Agents manage the execution of user code inside compute zones.  This module
 * exports the maZone class that manages an individual compute zone, including
 * setting it up for a new task and tearing it down after the task has
 * completed.
 */

/*
 * Compute Zone Lifecycle
 *
 * At any given time, a zone is in one of three states:
 *
 *    uninitialized	We've never touched this zone, or it's been arbitrarily
 *    			modified by a user task.  We assume that it's in an
 *    			arbitrary state, but that we can rollback to its origin
 *    			snapshot to get a known-good state.
 *
 *    ready		We've put this zone into a known-good state that's ready
 *    			to run new user tasks.
 *
 *    busy		The zone has been assigned to run a user task.
 *
 *    disabled		The zone has been taken out of service.
 *
 * Upon startup, any zone we're configured to use that we've never seen before
 * is assumed to be uninitialized.  We take several steps to initialize it
 * before putting it into the "ready" state:
 *
 *    o Halt the zone and wait for the zone to become halted so we can safely
 *      modify the zone's filesystem.
 *
 *    o "zfs rollback" the zone's dataset to its origin snapshot.  This puts the
 *      zone into a known-good state, removing anything left by previous tasks.
 *
 *    o Boot the zone and wait for the zone to finish booting.
 *
 *    o Set up a zsock server inside the zone to forward localhost HTTP requests
 *      back to this agent, automatically namespaced and authenticated.
 *
 * Once a zone is "ready", our consumer can dispatch tasks to it quickly by just
 * responding to the HTTP request.
 *
 *
 * Crash Recovery
 *
 * Today, we keep no persistent state about zones, so when this program crashes,
 * we have to assume all zones are uninitialized and reset them.  Whatever task
 * was running at the time is failed upstream.  In the future, we could keep
 * persistent state about zone state so that we could avoid resetting zones
 * unnecessarily, and potentially even avoid any impact to running tasks when we
 * crash, but this requires careful consideration of the zone-agent
 * interactions, the failure modes to be improved, and how this would work in an
 * upgrade situation.
 */

var mod_assert = require('assert');
var mod_child = require('child_process');
var mod_extsprintf = require('extsprintf');
var mod_fs = require('fs');
var mod_hyprlofs = require('hyprlofs');
var mod_path = require('path');

var mod_jsprim = require('jsprim');
var mod_restify = require('restify');
var mod_vasync = require('vasync');
var mod_verror = require('verror');
var mod_zoneid = require('zoneid');
var mod_zsockasync = require('zsock-async');

var mod_mautil = require('../util');

var sprintf = mod_extsprintf.sprintf;
var VError = mod_verror.VError;

/*
 * Public interface
 */
exports.maZone = maZone;
exports.maZoneAgentLog = maZoneAgentLog;
exports.maZoneAdd = maZoneAdd;
exports.maZoneRemove = maZoneRemove;
exports.maZoneMakeReady = maZoneMakeReady;
exports.maZoneApiCallback = maZoneApiCallback;
exports.maZoneSet = maZoneSet;

var maZoneSetupApi;

function maZoneAdd(zonename, spawner, log)
{
	return (new maZone(zonename, spawner, log));
}

function maZoneApiCallback(setup_callback)
{
	maZoneSetupApi = setup_callback;
}

/*
 * maZone is pretty much a plain old object.  All of the interesting logic
 * happens in maZoneManager and the maZone* functions.
 */
function maZone(zonename, spawner, log)
{
	this.z_zonename = zonename;
	this.z_spawner = spawner;
	this.z_log = log;

	this.z_state = maZone.ZONE_S_UNINIT;
	this.z_failed = undefined;
	this.z_quiesced = undefined;
	this.z_options = {};
	this.z_options_pending = undefined;
	this.z_image = undefined;
	this.z_disabled_error = undefined;
	this.z_disabled_time = undefined;

	/* XXX these really should be dynamic. */
	this.z_dataset = 'zones/' + this.z_zonename;
	this.z_origin = this.z_dataset + '@marlin_init';
	this.z_root = '/' + this.z_dataset + '/root';
	this.z_agentroot = '/opt/marlin';

	/* file descriptor and server for in-zone Unix Domain Socket */
	this.z_sockfd = undefined;
	this.z_listening = false;
	this.z_server = undefined;

	/* clients long polling on new task information */
	this.z_waiters = [];
	this.z_last_contact = undefined;

	/* hyprlofs mount */
	this.z_manta_root = '/manta';
	this.z_hyprlofs_root = mod_path.join(this.z_root, this.z_manta_root);
	this.z_hyprlofs = new mod_hyprlofs.Filesystem(this.z_hyprlofs_root);

	this.z_zoneid = undefined;

	/* debugging state for multi-stage operations */
	this.z_pipeline = undefined;

	/* debugging state for retries within pipelines */
	this.z_timeout = undefined;

	/* debugging state for pending shell command */
	this.z_pending_command = undefined;
}

maZone.prototype.httpState = function ()
{
	var obj = {
		'zonename': this.z_zonename,
		'state': this.z_state
	};

	if (this.z_sockfd)
		obj['sockfd'] = this.z_sockfd;
	if (this.z_pipeline)
		obj['pipeline'] = this.z_pipeline;
	if (this.z_timeout)
		obj['timeout'] = true;
	if (this.z_pending_command)
		obj['pendingCommand'] = this.z_pending_command;

	obj['nwaiters'] = this.z_waiters.length;

	if (this.z_disabled_time !== undefined) {
		obj['disableTime'] = mod_jsprim.iso8601(this.z_disabled_time);
		obj['disableErrorMessage'] = this.z_disabled_error.message;
	}

	return (obj);
};

maZone.prototype.zoneid = function ()
{
	mod_assert.ok(this.z_zoneid !== undefined,
	    'request to get zoneid while zone not ready');
	return (this.z_zoneid);
};

/* Zone states (see block comment above) */
maZone.ZONE_S_UNINIT	= 'uninit';
maZone.ZONE_S_READY	= 'ready';
maZone.ZONE_S_BUSY	= 'busy';
maZone.ZONE_S_DISABLED  = 'disabled';

function maZoneAgentLog(zone)
{
	var filename = mod_path.join(zone.z_root,
	    '/var/svc/log/smartdc-marlin-lackey:default.log');
	return (mod_fs.createReadStream(filename));
}

/*
 * Clean up any references we have on this zone that may prevent its removal
 * from the system.
 */
function maZoneRemove(zone, callback)
{
	mod_assert.equal(zone.z_state, maZone.ZONE_S_DISABLED);
	zone.z_log.info('cleaning up in preparation to remove');

	zone.z_pipeline = mod_vasync.pipeline({
	    'arg': zone,
	    'funcs': maZoneStagesCleanup
	}, function (err) {
		zone.z_pipeline = undefined;
		callback(err);
	});
}

/*
 * Begin transitioning an uninitialized zone to the "ready" state.
 */
function maZoneMakeReady(zone, callback)
{
	mod_assert.ok(zone.z_state == maZone.ZONE_S_UNINIT ||
	    zone.z_state == maZone.ZONE_S_DISABLED);

	zone.z_failed = undefined;
	zone.z_disabled_error = undefined;
	zone.z_disabled_time = undefined;

	zone.z_state = maZone.ZONE_S_UNINIT;
	zone.z_log.info('beginning transition to "ready" state');
	zone.z_pipeline = mod_vasync.pipeline({
	    'arg': zone,
	    'funcs': maZoneStagesReady
	}, function (err) {
		zone.z_pipeline = undefined;

		if (!err && zone.z_state == maZone.ZONE_S_DISABLED)
			err = new VError('zone disabled during reset');

		if (err) {
			/* XXX close fd */
			zone.z_state = maZone.ZONE_S_DISABLED;
			zone.z_log.error(err, 'failed to make zone ready');
			callback(zone, err);
			return;
		}

		mod_assert.equal(zone.z_state, maZone.ZONE_S_READY);
		zone.z_log.info('zone is now "ready"');
		callback(zone);
	});
}

/*
 * Wrapper around spawn-async.
 */
function maZoneExec(zone, argv, callback)
{
	mod_assert.ok(zone.z_pending_command === undefined);
	zone.z_pending_command = argv;

	zone.z_spawner.aspawn(argv, function (err, stdout, stderr) {
		zone.z_pending_command = undefined;

		if (err) {
			if (stderr !== undefined)
				err = new VError(err, 'command "%s" ' +
				    'failed with stderr: %s', argv.join(' '),
				    stderr);
			else
				err = new VError(err, 'command "%s" failed',
				    argv.join(' '));
			callback(err);
		} else {
			callback(null, stdout);
		}
	    });
}

/*
 * Waits for a zone to reach the given end_state.
 */
function maZoneWaitState(zone, end_state, callback)
{
	maZoneExec(zone, [ 'zoneadm', '-z', zone.z_zonename, 'list', '-p' ],
	    function (err, stdout) {
		var state;

		if (!err) {
			state = stdout.split(':')[2];
			/* XXX check for unexpected states. */

			if (state == end_state) {
				callback();
				return;
			}

			zone.z_log.debug('found zone state "%s"; ' +
			    'will retry later', state);
		} else {
			zone.z_log.debug('failed to retrieve zone state; ' +
			    'will retry later');
		}

		/*
		 * If we failed to check for whatever reason, or if the state is
		 * something other than "installed", retry in a few seconds.
		 */
		mod_assert.ok(zone.z_timeout === undefined);
		zone.z_timeout = setTimeout(function () {
			zone.z_timeout = undefined;
			maZoneWaitState(zone, end_state, callback);
		}, 1000);
	    });
}

/*
 * Waits for the given zone's given SMF service to reach "online".  It's assumed
 * that whatever would trigger that state change has already happened, so that
 * if we find, for example, that the state is "maintenance", then we can safely
 * abort.
 * XXX timeout
 */
function maServiceWaitOnline(zone, fmri, callback)
{
	maZoneExec(zone, [ 'svcs', '-H', '-z', zone.z_zonename, '-ostate',
	    fmri ], function (err, stdout) {
		if (!err) {
			var state = stdout.substr(0, stdout.length - 1);

			if (state == 'online') {
				callback();
				return;
			}

			if (state == 'maintenance') {
				callback(new VError(null,
				    'waiting for "%s" to reach "online", ' +
				    'but current state is "maintenance"',
				    state));
				return;
			}

			zone.z_log.debug('found svc %s in state "%s"; ' +
			    'will retry later', fmri, state);
		} else {
			zone.z_log.debug('failed to retrieve svc %s state; ' +
			    'will retry later', fmri);
		}

		/*
		 * If we failed to check for whatever reason, or if the state is
		 * something other than "installed", retry in a few seconds.
		 */
		mod_assert.ok(zone.z_timeout === undefined);
		zone.z_timeout = setTimeout(function () {
			zone.z_timeout = undefined;
			maServiceWaitOnline(zone, fmri, callback);
		}, 1000);
	    });
}

/*
 * The following stages clean up any remaining references we have on a zone in
 * preparation for removing it.
 */
var maZoneStagesCleanup = [
    maZoneCleanupServer,
    maZoneCleanupHyprlofs
];

/*
 * The following stages move an "uninitialized" zone into the "ready" state.
 */
var maZoneStagesReady = [
    maZoneCleanupServer,
    maZoneCleanupHyprlofs,
    maZoneReadyHalt,
    maZoneReadyWaitHalted,
    maZoneReadyRollback,
    maZoneReadyBoot,
    maZoneReadyWaitBooted,
    maZoneReadyKstat,
    maZoneReadyZsock,
    maZoneReadyHyprlofs,
    maZoneReadyDropAgent,
    maZoneReadyStartAgent
];

/*
 * The first stage cleans up server and socket state left over from a previous
 * run.  In the common (success) case, the previous run created a zsock fd and
 * created a server that's listening on it.  We must close the server so that
 * libuv knows to stop calling accept(2) on it, and this operation also closes
 * the underlying socket fd.  In the case where the last run failed to set up
 * the server after having successfully created the socket, it should have
 * cleaned up the socket.  So this cleanup stage can always assume that either
 * the server has been initialized, or none of the server/socket fields are
 * initialized.
 */
function maZoneCleanupServer(zone, callback)
{
	zone.z_zoneid = undefined;

	if (zone.z_server === undefined) {
		mod_assert.ok(zone.z_sockfd === undefined);
		mod_assert.ok(!zone.z_listening);
		callback();
		return;
	}

	zone.z_log.debug('closing server');

	mod_assert.ok(zone.z_sockfd !== undefined);
	mod_assert.ok(zone.z_listening);

	zone.z_server.on('close', function () {
		zone.z_server = undefined;
		zone.z_sockfd = undefined;
		zone.z_listening = false;
		callback();
	});

	zone.z_server.close();

	/*
	 * Terminate the connections associated with any pending requests.
	 */
	var waiters = zone.z_waiters;
	zone.z_waiters = [];
	waiters.forEach(function (w) {
		zone.z_log.debug('forcibly terminating client connection');
		w.wakeup(true);
	});
}

function maZoneCleanupHyprlofs(zone, callback)
{
	zone.z_log.debug('cleaning up hyprlofs mount');
	zone.z_hyprlofs.removeAll(function (err) {
		if (err) {
			if (err['code'] == 'ENOENT' || err['code'] == 'ENOTTY')
				err = undefined;
			callback(err);
			return;
		}

		/*
		 * We attempt to unmount the filesystem, if for no other reason
		 * than to make sure we have no references to it, since those
		 * would cause the joyent brand to SIGKILL us during halt.  We
		 * don't care if this fails, though, since we're halting the
		 * zone.
		 */
		zone.z_hyprlofs.unmount(function (suberr) {
			if (suberr)
				zone.z_log.debug('failed to unmount ' +
				    'hyprlofs before zone halt');
			callback();
		});
	});
}

function maZoneReadyHalt(zone, callback)
{
	mod_assert.equal(zone.z_state, maZone.ZONE_S_UNINIT);
	maZoneExec(zone, [ 'zoneadm', '-z', zone.z_zonename, 'halt' ],
	    callback);
}

function maZoneReadyWaitHalted(zone, callback)
{
	mod_assert.equal(zone.z_state, maZone.ZONE_S_UNINIT);
	maZoneWaitState(zone, 'installed', callback);
}

function maZoneReadyRollback(zone, callback)
{
	mod_assert.equal(zone.z_state, maZone.ZONE_S_UNINIT);
	maZoneExec(zone, [ 'zfs', 'rollback', zone.z_origin ], callback);
}

function maZoneReadyBoot(zone, callback)
{
	mod_assert.equal(zone.z_state, maZone.ZONE_S_UNINIT);
	maZoneExec(zone, [ 'zoneadm', '-z', zone.z_zonename, 'boot' ],
	    callback);
}

function maZoneReadyWaitBooted(zone, callback)
{
	mod_assert.equal(zone.z_state, maZone.ZONE_S_UNINIT);
	maZoneWaitState(zone, 'running', function () {
		maServiceWaitOnline(zone, 'milestone/multi-user:default',
		    callback);
	});
}

function maZoneReadyKstat(zone, callback)
{
	zone.z_zoneid = mod_zoneid.getzoneidbyname(zone.z_zonename);
	if (zone.z_zoneid < 0) {
		callback(new VError('failed to get zoneid for zone "%s"',
		    zone.z_zonename));
		return;
	}

	callback();
}

function maZoneReadyZsock(zone, callback)
{
	mod_assert.equal(zone.z_state, maZone.ZONE_S_UNINIT);
	mod_assert.ok(zone.z_sockfd === undefined);
	mod_assert.ok(zone.z_server === undefined);

	zone.z_log.debug('creating zsock');

	mod_zsockasync.createZoneSocket({
	    'zone': zone.z_zonename,
	    'path': sprintf('/var/run/.marlin.%s.sock', zone.z_zonename),
	    'spawner': zone.z_spawner
	}, function (err, fd) {
		if (err) {
			callback(err);
			return;
		}

		zone.z_log.debug('zsock fd = %d', fd);
		zone.z_sockfd = fd;

		var s = zone.z_server = mod_restify.createServer({
		    'name': zone.z_zonename + ' agent server',
		    'noWriteContinue': true,
		    'log': zone.z_log.child({
			'component': 'ZoneServer',
			'zone': zone.z_zonename,
			'serializers': mod_restify.bunyan.serializers
		    }),
		    'version': ['1.0.0']
		});

		maZoneSetupApi(zone, s);

		s.on('error', function (suberr) {
			if (zone.z_listening) {
				zone.z_log.warn(suberr, 'unexpected error ' +
				    'on zone socket');
				return;
			}

			zone.z_log.error(suberr,
			    'failed to start per-zone server');
			zone.z_sockfd = undefined;
			mod_fs.close(fd, function () { callback(suberr); });
		});

		mod_zsockasync.listenFD(s.server, zone.z_sockfd, function () {
			mod_assert.ok(!zone.z_listening);
			zone.z_listening = true;
			zone.z_log.debug('custom zone http server listening ' +
			    'on fd %s', zone.z_sockfd);
			callback();
		});
	});
}

function maZoneReadyHyprlofs(zone, callback)
{
	mod_assert.equal(zone.z_state, maZone.ZONE_S_UNINIT);

	zone.z_log.debug('creating hyprlofs mount');

	mod_fs.mkdir(zone.z_hyprlofs_root, function (err) {
		if (err) {
			callback(err);
			return;
		}

		zone.z_hyprlofs.mount(callback);
	});
}

function maZoneReadyDropAgent(zone, callback)
{
	mod_assert.equal(zone.z_state, maZone.ZONE_S_UNINIT);
	var src = mod_path.normalize(__dirname + '/../../');
	var dst = zone.z_root + zone.z_agentroot;

	zone.z_log.debug('mounting %s into zone', src);

	mod_fs.mkdir(dst, function (err) {
		if (err) {
			callback(new VError(err, '"mkdir" failed'));
			return;
		}

		maZoneExec(zone, [ 'mount', '-F', 'lofs', '-oro', src, dst ],
		    function (suberr, stdout, stderr) {
			if (suberr) {
				callback(new VError(suberr,
				    '"mount" failed: %s', stderr));
				return;
			}

			callback();
		    });
	});
}

function maZoneReadyStartAgent(zone, callback)
{
	mod_assert.equal(zone.z_state, maZone.ZONE_S_UNINIT);
	zone.z_log.debug('starting zone agent');

	/*
	 * We set READY here because as soon as we start this step, the agent
	 * may see requests from the zone, at which point the zone really is
	 * ready.
	 */
	zone.z_state = maZone.ZONE_S_READY;

	maZoneExec(zone, [ 'zlogin', zone.z_zonename, 'svccfg', 'import',
	    zone.z_agentroot + '/smf/manifests/marlin-lackey.xml' ],
	    function (err) {
		if (err) {
			callback(err);
			return;
		}

		maServiceWaitOnline(zone, 'marlin/lackey', callback);
	    });
}

/*
 * Sets a vmadm(1M) property of this zone, typically max_swap and
 * max_physical_memory.  At most one "set" operation may be going on at once,
 * and this interfaces avoids setting properties when the current values are
 * known to match.
 */
function maZoneSet(zone, options, callback)
{
	var args = [ 'vmadm', 'update', zone.z_zonename ];
	var used = [];
	var key, setoptions;

	mod_assert.ok(zone.z_state == maZone.ZONE_S_READY ||
	    zone.z_state == maZone.ZONE_S_BUSY);
	mod_assert.ok(!zone.z_options_pending);

	setoptions = {};
	for (key in options) {
		if (zone.z_options[key] === options[key])
			continue;

		used.push(key);
		args.push(key + '=' + options[key]);
		setoptions[key] = options[key];
	}

	if (used.length === 0) {
		zone.z_log.info('skipping property set (no changes)', options);
		process.nextTick(callback);
		return;
	}

	/*
	 * If we set anything, we also set "tmpfs", since setting other
	 * properties can cause tmpfs to be set to a different value.
	 */
	if (!setoptions.hasOwnProperty('tmpfs') &&
	    (options.hasOwnProperty('tmpfs') ||
	    zone.z_options.hasOwnProperty('tmpfs'))) {
		used.push('tmpfs');
		setoptions['tmpfs'] = options.hasOwnProperty('tmpfs') ?
		    options['tmpfs'] : zone.z_options['tmpfs'];
		args.push('tmpfs=' + setoptions['tmpfs']);
	}

	zone.z_log.info('setting zone properties', setoptions);
	zone.z_options_pending = true;
	maZoneExec(zone, args, function (err) {
		zone.z_options_pending = false;

		if (err) {
			callback(err);
			return;
		}

		used.forEach(function (ukey) {
			zone.z_options[ukey] = setoptions[ukey];
		});

		callback();
	});
}
