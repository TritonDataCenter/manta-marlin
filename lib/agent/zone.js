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
 * This module manages a fixed-size pool of compute zones.  At any given time, a
 * zone is in one of three states:
 *
 *    uninitialized	We've never touched this zone, or it's been arbitrarily
 *    			modified by a user task.  We assume that it's in an
 *    			arbitrary state, but that we can rollback to its origin
 *    			snapshot to get a known-good state.
 *
 *    ready		We've put this zone into a known-good state that's ready
 *    			to run new user tasks, but it has not yet been assigned
 *    			to any user task.
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
 * Once a zone is "ready", we can dispatch tasks to it by marking the zone
 * "loading" and then:
 *
 *    o For map tasks: create a hyprlofs mount inside the zone, and populate it
 *      with the appropriate input keys.
 *
 *    o Download the task's assets into the zone.
 *
 *    o Invoke the task's "exec" script to start it running.  Since we need to
 *      monitor the user code, this step actually pokes an agent of ours running
 *      inside the zone to start the script.
 *
 * Importantly, the time required to transition from "ready" to "loading" is
 * determined mostly by the input.  In many cases, it should be possible to do
 * this in hundreds of milliseconds, allowing users to run complete jobs in just
 * a few seconds.
 *
 * When the task completes, fails, or times out, we mark the state
 * "uninitialized" again (since the internal state is arbitrarily changed by the
 * user's task) and then transition it to "ready" so that it can run another
 * task.
 *
 *
 * Crash Recovery
 *
 * An important design constraint is that a task must not fail as a result of
 * transient failures of this agent.  To recover from a crash, we persistently
 * track the state of each zone.  When we come up, we iterate known zones and
 * deal with them appropriately:
 *
 *    "uninitialized" zones are transitioned to "ready" (see above).
 *
 *    "ready" zones are placed into the pool of available zones.
 *
 *    "busy" zones are more complex.  We need to know whether we crashed during
 *    setup for this zone or while running user code.  There's no way to
 *    reliably do this ourselves, so we ask our agent inside the zone if user
 *    code has started running.  If so, we do nothing, since the task is running
 *    successfully.  If not, or if we cannot contact the agent, then we crashed
 *    sometime during setup.  Rather than trying to recover, we just move the
 *    zone to "uninitialized" and move the assigned task to the front of the
 *    dispatch list.
 *
 * Relative to agent failure, one obvious question is: what happens if the user
 * makes an HTTP request to us while we're down?  In this case, the in-zone HTTP
 * server retries the request indefinitely until it gets a response.  For
 * transient agent failures, this should only result in small performance blips.
 * If somehow the agent becomes permanently disabled, this would be pretty
 * serious, since the node is no longer available to Marlin at all.  The task
 * itself may stall until an administrator can correct the problem.  But to the
 * rest of Marlin, this looks just like total compute node failure, so it will
 * eventually reassign the task to other nodes.  When the agent does come back,
 * it must be disallowed from checkpointing, since the task is no longer valid.
 */

var mod_assert = require('assert');
var mod_child = require('child_process');
var mod_fs = require('fs');
var mod_path = require('path');

var mod_restify = require('restify');
var mod_vasync = require('vasync');
var mod_verror = require('verror');
var mod_zsock = require('zsock');

var mod_mautil = require('../util');

/*
 * External interface
 */
exports.maZoneInit = maZoneInit;

function maZoneInit(log)
{
	return (new maZoneManager(log));
}

/*
 * maZone is pretty much a plain old object.  All of the interesting logic
 * happens in maZoneManager and the maZone* functions.
 */
function maZone(zonename, log)
{
	this.z_zonename = zonename;
	this.z_task = undefined;
	this.z_log = log.child({ 'zone': zonename });
	this.z_state = maZone.ZONE_S_UNINIT;

	/* XXX these really should be dynamic. */
	this.z_dataset = 'zones/' + this.z_zonename;
	this.z_origin = this.z_dataset + '@marlin_init';
	this.z_root = '/' + this.z_dataset + '/root';
	this.z_agentroot = '/opt/marlin';

	/* file descriptor and server for in-zone Unix Domain Socket */
	this.z_sockfd = undefined;
	this.z_server = undefined;

	/* clients long polling on new task information */
	this.z_waiters = [];

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

	if (this.z_task)
		obj['taskId'] = this.z_task.t_id;
	if (this.z_sockfd)
		obj['sockfd'] = this.z_sockfd;
	if (this.z_pipeline)
		obj['pipeline'] = this.z_pipeline;
	if (this.z_timeout)
		obj['timeout'] = true;
	if (this.z_pending_command)
		obj['pendingCommand'] = this.z_pending_command;

	return (obj);
};

/* Zone states (see block comment above) */
maZone.ZONE_S_UNINIT	= 'uninit';
maZone.ZONE_S_READY	= 'ready';
maZone.ZONE_S_BUSY	= 'busy';
maZone.ZONE_S_DISABLED  = 'disabled';

/*
 * The zone manager keeps track of all available zones as well as which ones are
 * "ready".
 */
function maZoneManager(log)
{
	this.mz_zones = {};	/* all zones, by zonename */
	this.mz_ready = [];	/* list of "ready" zones */
	this.mz_log = log.child({ 'component': 'zonemgr' });
}

/*
 * [public] Add a newly-found zone.
 */
maZoneManager.prototype.addZone = function (zonename)
{
	this.zoneInsert(zonename);
};

maZoneManager.prototype.avail = function ()
{
	if (this.mz_ready.length === 0)
		return (null);

	return (this.mz_ready.shift());
};

/*
 * Load zone configuration from disk.  For now, this hardcodes a dummy zone.
 */
maZoneManager.prototype.load = function ()
{
	var fakename = '4e6ac104-8357-11e1-b9b7-7b3733640d06';
	this.mz_log.info('adding fake zone "%s"', fakename);
	this.zoneInsert(fakename);
};

/*
 * Add a newly-found zone.
 */
maZoneManager.prototype.zoneInsert = function (zonename)
{
	var zone;

	mod_assert.ok(!this.mz_zones.hasOwnProperty(zonename),
	    'attempted to add duplicate zone ' + zonename);

	zone = new maZone(zonename, this.mz_log);
	this.mz_zones[zone.z_zonename] = zone;
	zone.z_log.info('zone added');
	this.zoneMakeReady(zone);
};

/*
 * Begin transitioning an uninitialized zone to the "ready" state.
 */
maZoneManager.prototype.zoneMakeReady = function (zone)
{
	var mgr = this;

	mod_assert.equal(zone.z_state, maZone.ZONE_S_UNINIT);

	zone.z_log.info('beginning transition to "ready" state');
	zone.z_pipeline = mod_vasync.pipeline({
	    'arg': zone,
	    'funcs': maZoneStagesReady
	}, function (err) {
		zone.z_pipeline = undefined;

		if (err) {
			/* XXX close fd */
			zone.z_log.error(err, 'failed to make zone ready');
			mgr.zoneDisable(zone);
			return;
		}

		zone.z_state = maZone.ZONE_S_READY;
		mgr.mz_ready.push(zone);
		zone.z_log.info('zone is now "ready"');

		/* XXX dispatch tasks as necessary */
	});
};

/*
 * Take zone "zone" out of service.
 */
maZoneManager.prototype.zoneDisable = function (zone)
{
	/*
	 * We don't really need to do anything special here besides update the
	 * state.  We never reference disabled zones again.
	 */
	zone.z_log.error('taking zone out of service');
	zone.z_state = maZone.ZONE_S_DISABLED;
};

/*
 * Begin running the given task inside the given ready zone.
 */
maZoneManager.prototype.zoneRunTask = function (zone, task)
{
	var waiters;

	mod_assert.equal(zone.z_state, maZone.ZONE_S_READY);
	mod_assert.ok(zone.z_task === undefined);

	zone.z_task = task;

	waiters = zone.z_waiters;
	zone.z_waiters = [];

	waiters.forEach(function (w) {
		maTaskApiTask(zone, w['request'], w['response'], w['next']);
	});
};

/*
 * Wrapper around Node's child_process.execFile() that logs all commands.
 */
function maZoneExec(zone, cmd, args, callback)
{
	var cmdstr = cmd + ' ' + args.join(' ');

	mod_assert.ok(zone.z_pending_command === undefined);

	zone.z_log.info('invoking "%s"', cmdstr);
	zone.z_pending_command = cmdstr;

	mod_child.execFile(cmd, args, {}, function (err, stdout, stderr) {
		zone.z_pending_command = undefined;

		if (err) {
			zone.z_log.warn(err, 'command "%s" failed with ' +
			    'stderr: %s', cmdstr, stderr);
			callback(err);
			return;
		}

		zone.z_log.info('command "%s" ok', cmdstr);
		callback(null, stdout);
	});
}

/*
 * Waits for a zone to reach the given end_state.
 */
function maZoneWaitState(zone, end_state, callback)
{
	maZoneExec(zone, 'zoneadm', [ '-z', zone.z_zonename, 'list', '-p' ],
	    function (err, stdout) {
		var state;

		if (!err) {
			state = stdout.split(':')[2];
			/* XXX check for unexpected states. */

			if (state == end_state) {
				callback();
				return;
			}

			zone.z_log.info('found zone state "%s"; ' +
			    'will retry later', state);
		} else {
			zone.z_log.info('failed to retrieve zone state; ' +
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
	maZoneExec(zone, 'svcs', [ '-H', '-z', zone.z_zonename, '-ostate',
	    fmri ], function (err, stdout) {
		if (!err) {
			var state = stdout.substr(0, stdout.length - 1);

			if (state == 'online') {
				callback();
				return;
			}

			if (state == 'maintenance') {
				callback(new mod_verror.VError(null,
				    'waiting for "%s" to reach "online", ' +
				    'but current state is "maintenance"',
				    state));
				return;
			}

			zone.z_log.info('found svc %s in state "%s"; ' +
			    'will retry later', fmri, state);
		} else {
			zone.z_log.info('failed to retrieve svc %s state; ' +
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
 * The following stages move an "uninitialized" zone into the "ready" state.
 */
var maZoneStagesReady = [
    maZoneReadyHalt,
    maZoneReadyWaitHalted,
    maZoneReadyRollback,
    maZoneReadyBoot,
    maZoneReadyWaitBooted,
    maZoneReadyZsock,
    maZoneReadyDropAgent,
    maZoneReadyStartAgent
];

function maZoneReadyHalt(zone, callback)
{
	mod_assert.equal(zone.z_state, maZone.ZONE_S_UNINIT);
	maZoneExec(zone, 'zoneadm', [ '-z', zone.z_zonename, 'halt' ],
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
	maZoneExec(zone, 'zfs', [ 'rollback', zone.z_origin ], callback);
}

function maZoneReadyBoot(zone, callback)
{
	mod_assert.equal(zone.z_state, maZone.ZONE_S_UNINIT);
	maZoneExec(zone, 'zoneadm', [ '-z', zone.z_zonename, 'boot' ],
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

function maZoneReadyZsock(zone, callback)
{
	mod_assert.equal(zone.z_state, maZone.ZONE_S_UNINIT);
	mod_assert.ok(zone.z_sockfd === undefined);
	mod_assert.ok(zone.z_server === undefined);

	zone.z_log.info('creating zsock');

	mod_zsock.createZoneSocket({
	    'zone': zone.z_zonename,
	    'path': '/tmp/.marlin.sock'
	}, function (err, fd) {
		if (err) {
			callback(err);
			return;
		}

		zone.z_log.info('zsock fd = %d', fd);
		zone.z_sockfd = fd;

		var s = zone.z_server = mod_restify.createServer({
		    'name': zone.z_zonename + ' agent server',
		    'log': zone.z_log
		});

		s.use(mod_restify.acceptParser(s.acceptable));
		s.use(mod_restify.queryParser());
		s.use(mod_restify.bodyParser({ 'mapParams': false }));

		s.on('uncaughtException', mod_mautil.maRestifyPanic);
		s.on('after', mod_restify.auditLogger({ 'log': zone.z_log }));
		s.on('error', function (suberr) {
			zone.z_log.error(suberr,
			    'failed to start per-zone server');
			zone.z_sockfd = undefined;
			mod_fs.close(fd, function () { callback(suberr); });
		});

		s.get('/task', maTaskApiTask.bind(null, zone));
		s.post('/commit', maTaskApiCommit.bind(null, zone));

		s.server.listenFD(zone.z_sockfd, function () {
			zone.z_log.info('custom zone http server listening ' +
			    'on fd %s', zone.z_sockfd);
			callback();
		});
	});
}

function maZoneReadyDropAgent(zone, callback)
{
	var src = mod_path.normalize(__dirname + '/../../');
	var dst = zone.z_root + zone.z_agentroot;

	zone.z_log.info('mounting %s into zone', src);

	mod_fs.mkdir(dst, function (err) {
		if (err) {
			callback(new mod_verror.VError(err, '"mkdir" failed'));
			return;
		}

		mod_child.execFile('mount', [ '-F', 'lofs', '-oro', src, dst ],
		    function (suberr, stdout, stderr) {
			if (suberr) {
				callback(new mod_verror.VError(suberr,
				    '"mount" failed: %s', stderr));
				return;
			}

			callback();
		    });
	});
}

function maZoneReadyStartAgent(zone, callback)
{
	zone.z_log.info('starting zone agent');

	maZoneExec(zone, 'zlogin', [ zone.z_zonename, 'svccfg', 'import',
	    zone.z_agentroot + '/smf/manifests/marlin-zone-agent.xml' ],
	    function (err) {
		if (err) {
			callback(err);
			return;
		}

		maServiceWaitOnline(zone, 'marlin/zone-agent', callback);
	    });
}

/*
 * Task Control API entry points
 */

/*
 * Fetch the task currently assigned to this zone.  By default, this entry point
 * always returns immediately, either with 200 plus the task or 204 if no task
 * is assigned right now.  The in-zone agent invokes this with wait=true, which
 * means to block until a task is available.
 */
function maTaskApiTask(zone, request, response, next)
{
	if (zone.z_task) {
		response.send(zone.z_task.httpState());
		next();
		return;
	}

	if (request.query['wait'] != 'true') {
		response.send(204);
		next();
		return;
	}

	zone.z_waiters.push({
	    'time': Date.now(),
	    'request': request,
	    'response': response,
	    'next': next
	});
}

/*
 * Indicate that the given key has been successfully processed.
 */
function maTaskApiCommit(zone, request, response, next)
{
	var body = request.body || {};

	if (!body.hasOwnProperty('key')) {
		response.send(new mod_restify.InvalidArgumentError(
		    '"key" is required'));
		next();
		return;
	}

	/* XXX layering. also, this should take the accumulated output keys? */
	zone.z_task.commitInputKey(body['key'], function (err) {
		response.send(err ? err : 204);
		next();
	});
}
