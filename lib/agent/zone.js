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
var mod_zsock = require('zsock');

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

	/* file descriptor for in-zone Unix Domain Socket */
	this.z_sockfd = undefined;

	/* debugging state for multi-stage operations */
	this.z_pipeline = undefined;

	/* debugging state for retries within pipelines */
	this.z_timeout = undefined;

	/* debugging state for pending shell command */
	this.z_pending_command = undefined;
}

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
	zone.z_pipeline = maZoneRunStages(zone, maZoneStagesReady,
	    function (err) {
		zone.z_pipeline = undefined;

		if (err) {
			mgr.zoneDisable(zone);
			return;
		}

		zone.z_state = maZone.ZONE_S_READY;
		mgr.mz_ready.push(zone);
		zone.z_log.info('zone is now "ready"');
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
	mod_assert.equal(zone.z_state, maZone.ZONE_S_READY);
	zone.z_task = task;
	/* XXX */
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
			zone.z_log.error(err, 'command "%s" failed with ' +
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
		}, 5000);
	    });
}

/*
 * Waits for the given zone's given SMF service to reach the given state.
 * XXX timeout
 */
function maServiceWaitState(zone, fmri, end_state, callback)
{
	maZoneExec(zone, 'svcs', [ '-H', '-z', zone.z_zonename, '-ostate',
	    fmri ], function (err, stdout) {
		var state = stdout.substr(0, stdout.length - 1);

		if (!err) {
			if (state == end_state) {
				callback();
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
			maServiceWaitState(zone, fmri, end_state, callback);
		}, 5000);
	    });
}

/*
 * Runs the given set of stages on the zone.  Returns a "state" argument that
 * can be used to keep track of progress (mostly for debugging).
 */
function maZoneRunStages(zone, stages, callback)
{
	var state = {
	    'stages': stages,
	    'current_stage': -1,
	    'fatal_error': undefined
	};

	var next = function (err) {
		if (err) {
			state['fatal_error'] = err;
			return (callback(err));
		}

		if (++state['current_stage'] >= state['stages'].length)
			return (callback());

		return (process.nextTick(function () {
			state['stages'][state['current_stage']](zone, next);
		}));
	};

	next();
	return (state);
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
    maZoneReadyDropAgent
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
	maZoneExec(zone, 'zfs', [ 'rollback', 'zones/' + zone.z_zonename +
	    '@marlin_init' ], callback);
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
		maServiceWaitState(zone, 'milestone/multi-user:default',
		    'online', callback);
	});
}

function maZoneReadyZsock(zone, callback)
{
	mod_assert.equal(zone.z_state, maZone.ZONE_S_UNINIT);
	mod_assert.ok(zone.z_sockfd === undefined);

	zone.z_log.info('creating zsock');

	mod_zsock.createZoneSocket({
	    'zone': zone.z_zonename,
	    'path': '/tmp/marlin.sock'
	}, function (err, fd) {
		if (!err) {
			zone.z_log.info('zsock fd = %d', fd);
			zone.z_sockfd = fd;
		}

		callback(err);
	});
}

function maZoneReadyDropAgent(zone, callback)
{
	/* XXX */
	callback();
}
