/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * zone-pool.js: manages a pool of zones
 *
 * Each compute node is based on a particular image version, and the Marlin
 * agent maintains separate pools of zones based the image version.  The logic
 * for keeping track of such zones is implemented here.  The zone scheduler
 * also operates on a per-pool basis.
 */

var mod_child = require('child_process');

var mod_assertplus = require('assert-plus');
var mod_strsplit = require('strsplit');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var VError = mod_verror.VError;

/* Public interface */
exports.discoverZones = discoverZones;
exports.listImages = listImages;
exports.ZonePool = ZonePool;

/*
 * Discovers zones on this system that are available for use by Marlin.
 */
function discoverZones(log, callback)
{
	var cmd = 'vmadm';
	var args = [ 'list', '-H', '-o',
	    'uuid,image_uuid,tags.manta_role,tags.manta_compute' ];
	runTableCommand(log, cmd, args, function (err, records) {
		if (err) {
			callback(err);
			return;
		}

		var rv = [];
		records.forEach(function (rec) {
			var parts = mod_strsplit.strsplit(rec, /\s+/, 4);
			if (parts.length != 4) {
				log.warn('listing images: line garbled: ', rec);
				return;
			}

			if (parts[3] == 'removed') {
				log.info('ignoring removed zone', parts[0]);
				return;
			}

			/*
			 * Marlin zones may be tagged with either "compute" or
			 * "marlincompute", depending on whether they were
			 * deployed before MANTA-1157 was integrated.
			 */
			if (parts[2] != 'compute' &&
			    parts[2] != 'marlincompute')
				return;

			rv.push({
			    'zonename': parts[0],
			    'image': parts[1]
			});
		});

		log.info('found %d zones', rv.length);
		callback(null, rv);
	});
}

function listImages(log, callback)
{
	var cmd = 'imgadm';
	var args = [ 'list', '-H', '-o', 'uuid,name,version' ];
	runTableCommand(log, cmd, args, function (err, records) {
		if (err) {
			callback(err);
			return;
		}

		var rv = [];
		records.forEach(function (rec) {
			var parts = mod_strsplit.strsplit(rec, /\s+/, 3);
			if (parts.length != 3) {
				log.warn('listing images: line garbled: ', rec);
				return;
			}

			rv.push({
			    'uuid': parts[0],
			    'name': parts[1],
			    /* JSSTYLED */
			    'version': parts[2].replace(/^[^\/]*\//, '')
			});
		});

		log.info('found %d images', rv.length);
		callback(null, rv);
	});
}

function runTableCommand(log, cmd, args, callback)
{
	var cmdstr = cmd + ' ' + args.join(' ');
	log.info('invoking "%s"', cmdstr);
	mod_child.execFile(cmd, args, function (err, stdout, stderr) {
		if (err) {
			callback(new VError(err, '"%s" failed (stderr = %s)',
			    cmdstr, stderr));
			return;
		}

		var rv = [];
		stdout.split('\n').forEach(function (line) {
			line = line.trim();
			if (line.length === 0)
				return;

			rv.push(line);
		});
		callback(null, rv);
	});
}

function ZonePool(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args['log'], 'args.log');
	mod_assertplus.object(args['tunables'], 'args.tunables');

	this.zp_log = args['log'];
	this.zp_tun_reservemin = args['tunables']['zoneReserveMin'];
	this.zp_tun_reservepct = args['tunables']['zoneReservePercent'];

	/* Zones and their states */
	this.zp_zones = {};		/* set of all zonenames */
	this.zp_nzones = 0;		/* count of all zones */
	this.zp_ready = {};		/* set of ready zonenames */
	this.zp_readyq = [];		/* queue of ready zones */
	this.zp_disabled = {};		/* set of disabled zones */
	this.zp_ndisabled = 0;		/* count of disabled zones */

	/* Scheduler state */
	this.zp_nreserved = 0;
}

ZonePool.prototype.zoneAdd = function (zonename)
{
	mod_assertplus.string(zonename, 'zonename');

	mod_assertplus.ok(!this.zp_zones.hasOwnProperty(zonename),
	    'attempted to add duplicate zone ' + zonename);
	this.zp_log.info('adding zone "%s"', zonename);
	this.zp_zones[zonename] = true;
	this.zp_nzones++;
	this.schedUpdateReserve();
};

ZonePool.prototype.zoneMarkReady = function (zonename)
{
	mod_assertplus.ok(this.zp_zones.hasOwnProperty(zonename));
	mod_assertplus.ok(!this.zp_ready.hasOwnProperty(zonename));
	mod_assertplus.ok(!this.zp_disabled.hasOwnProperty(zonename));
	this.zp_ready[zonename] = true;
	this.zp_readyq.push(zonename);
};

/*
 * Check whether the given zone is "ready".
 */
ZonePool.prototype.zoneIsReady = function (zonename)
{
	return (this.zp_ready.hasOwnProperty(zonename));
};

/*
 * This is an unusual operation, used only when a ready zone is being removed
 * from service.
 */
ZonePool.prototype.zoneUnready = function (zonename)
{
	var i;

	mod_assertplus.ok(this.zp_ready.hasOwnProperty(zonename));
	for (i = 0; i < this.zp_readyq.length; i++) {
		if (this.zp_readyq[i] == zonename)
			break;
	}

	/* XXX must this always be the case? */
	if (i < this.zp_readyq.length) {
		this.zp_readyq.splice(i, 1);
		delete (this.zp_ready[zonename]);
	}
};

/*
 * This is also an unsual operation, used only when a disabled zone is being
 * removed from the system entirely.
 */
ZonePool.prototype.zoneRemove = function (zonename)
{
	mod_assertplus.ok(this.zp_zones.hasOwnProperty(zonename));
	mod_assertplus.ok(this.zp_disabled.hasOwnProperty(zonename));

	delete (this.zp_zones[zonename]);
	this.zp_nzones--;

	delete (this.zp_disabled[zonename]);
	this.zp_ndisabled--;

	this.schedUpdateReserve();
};

ZonePool.prototype.zoneMarkDisabled = function (zonename)
{
	mod_assertplus.ok(this.zp_zones.hasOwnProperty(zonename));
	mod_assertplus.ok(!this.zp_disabled.hasOwnProperty(zonename));
	mod_assertplus.ok(!this.zp_ready.hasOwnProperty(zonename));
	this.zp_disabled[zonename] = true;
	this.zp_ndisabled++;
	this.schedUpdateReserve();
};

ZonePool.prototype.zoneMarkEnabled = function (zonename)
{
	mod_assertplus.ok(this.zp_zones.hasOwnProperty(zonename));
	mod_assertplus.ok(this.zp_disabled.hasOwnProperty(zonename));
	delete (this.zp_disabled[zonename]);
	this.zp_ndisabled--;
	this.schedUpdateReserve();
};

ZonePool.prototype.ndisabled = function ()
{
	return (this.zp_ndisabled);
};

ZonePool.prototype.nzones = function ()
{
	return (this.zp_nzones);
};

ZonePool.prototype.hasZoneReady = function ()
{
	return (this.zp_readyq.length > 0);
};

ZonePool.prototype.zonePick = function ()
{
	mod_assertplus.ok(this.hasZoneReady());
	mod_assertplus.ok(this.zp_ready[this.zp_readyq[0]]);
	var zonename = this.zp_readyq.shift();
	delete (this.zp_ready[zonename]);
	return (zonename);
};

ZonePool.prototype.unreservedCapacity = function ()
{
	return (this.zp_nzones - this.zp_nreserved);
};

ZonePool.prototype.saturated = function ()
{
	return (this.zp_readyq.length <= this.zp_nreserved);
};

ZonePool.prototype.schedUpdateReserve = function ()
{
	var pct = this.zp_tun_reservepct / 100;
	var nenabled = this.zp_nzones - this.zp_ndisabled;
	var nreserved = Math.floor(pct * nenabled);

	mod_assertplus.ok(nreserved <= this.zp_nzones);
	mod_assertplus.ok(nreserved >= 0);
	nreserved = Math.max(nreserved,
	    Math.min(this.zp_tun_reservemin, nenabled));
	this.zp_log.info('updating reserve to %d zones (%d zones, %d disabled)',
	    nreserved, this.zp_nzones, this.zp_ndisabled);
	this.zp_nreserved = nreserved;
};

ZonePool.prototype.kangState = function ()
{
	return ({
	    'nzones': this.zp_nzones,
	    'nreserved': this.zp_nreserved,
	    'ndisabled': this.zp_ndisabled
	});
};
