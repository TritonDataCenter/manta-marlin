/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * tst.zonepool.js: tests the agent's ZonePool class
 */

var mod_assert = require('assert');

var mod_bunyan = require('bunyan');

var mod_agent_zonepool = require('../../lib/agent/zone-pool');
var ZonePool = mod_agent_zonepool.ZonePool;

var log = new mod_bunyan({ 'name': 'tst.zonepool.js' });
var pool = new ZonePool({
    'log': log.child({ 'component': 'pool' }),
    'tunables': {
	'zoneReserveMin': 2,
	'zoneReservePercent': 30
    }
});

/* empty pool state */
mod_assert.ok(pool.ndisabled() === 0);
mod_assert.ok(pool.nzones() === 0);
mod_assert.ok(pool.unreservedCapacity() === 0);
mod_assert.ok(!pool.hasZoneReady());
mod_assert.ok(pool.saturated());

/* add a zone, which is not ready by default */
pool.zoneAdd('zone-1');
mod_assert.ok(pool.ndisabled() === 0);
mod_assert.ok(pool.nzones() == 1);
mod_assert.ok(pool.unreservedCapacity() === 0);
mod_assert.ok(!pool.hasZoneReady());
mod_assert.ok(pool.saturated());
mod_assert.ok(!pool.zoneIsReady('zone-1'));

/* mark the zone ready */
pool.zoneMarkReady('zone-1');
mod_assert.ok(pool.ndisabled() === 0);
mod_assert.ok(pool.nzones() == 1);
mod_assert.ok(pool.unreservedCapacity() === 0);
mod_assert.ok(pool.hasZoneReady());
mod_assert.ok(pool.saturated());
mod_assert.ok(pool.zoneIsReady('zone-1'));

/* now select it (marking it unready) */
var zone = pool.zonePick();
mod_assert.equal(zone, 'zone-1');
mod_assert.ok(pool.ndisabled() === 0);
mod_assert.ok(pool.nzones() == 1);
mod_assert.ok(pool.unreservedCapacity() === 0);
mod_assert.ok(!pool.hasZoneReady());
mod_assert.ok(pool.saturated());
mod_assert.ok(!pool.zoneIsReady('zone-1'));

pool.zoneMarkReady('zone-1');
mod_assert.ok(pool.ndisabled() === 0);
mod_assert.ok(pool.nzones() == 1);
mod_assert.ok(pool.unreservedCapacity() === 0);
mod_assert.ok(pool.hasZoneReady());
mod_assert.ok(pool.saturated());
mod_assert.ok(pool.zoneIsReady('zone-1'));

/* add a bunch more zones and check reserve state */
for (var i = 0; i < 9; i++) {
	pool.zoneAdd('zone-' + (i + 2));
	pool.zoneMarkReady('zone-' + (i + 2));
}
mod_assert.ok(pool.ndisabled() === 0);
mod_assert.ok(pool.nzones() == 10);
mod_assert.ok(pool.unreservedCapacity() === 7);
mod_assert.ok(pool.hasZoneReady());
mod_assert.ok(!pool.saturated());

/* now select the whole non-reserve set of the pool and check state */
var picked = {};
for (i = 0; i < 7; i++) {
	mod_assert.ok(pool.unreservedCapacity() === 7);
	mod_assert.ok(pool.hasZoneReady());
	mod_assert.ok(!pool.saturated());
	picked[pool.zonePick()] = true;
}
mod_assert.equal(7, Object.keys(picked).length);
mod_assert.ok(pool.hasZoneReady());
mod_assert.ok(pool.saturated());

/* select the last few and check state */
for (i = 0; i < 3; i++) {
	mod_assert.ok(pool.hasZoneReady());
	mod_assert.ok(pool.saturated());
	picked[pool.zonePick()] = true;
}

mod_assert.ok(!pool.hasZoneReady());


/* disable a zone */
pool.zoneMarkDisabled('zone-1');
mod_assert.ok(pool.nzones() == 10);
mod_assert.ok(pool.ndisabled() == 1);
mod_assert.ok(!pool.hasZoneReady());

pool.zoneMarkEnabled('zone-1');
mod_assert.ok(pool.ndisabled() === 0);
mod_assert.ok(!pool.hasZoneReady());

/* unready a zone (as happens when it's disabled from the ready state */
pool.zoneMarkReady('zone-1');
mod_assert.ok(pool.hasZoneReady());
pool.zoneUnready('zone-1');
mod_assert.ok(!pool.hasZoneReady());
mod_assert.ok(pool.ndisabled() === 0);

pool.zoneMarkDisabled('zone-1');
mod_assert.ok(pool.nzones() == 10);
mod_assert.ok(pool.ndisabled() == 1);
pool.zoneRemove('zone-1');
mod_assert.ok(pool.nzones() == 9);
mod_assert.ok(pool.ndisabled() === 0);
delete (picked['zone-1']);


/* invalid operations */
mod_assert.throws(function () { pool.zoneAdd('zone-2'); },
    /duplicate zone/);

/* enable a non-existent or enabled zone */
mod_assert.throws(function () { pool.zoneMarkEnabled('zone-XXX'); });
mod_assert.throws(function () { pool.zoneMarkEnabled('zone-2'); });

/* mark a non-existent or disabled zone disabled */
pool.zoneMarkDisabled('zone-2');
mod_assert.throws(function () { pool.zoneMarkDisabled('zone-XXX'); });
mod_assert.throws(function () { pool.zoneMarkDisabled('zone-2'); });

/* mark a disabled zone ready */
mod_assert.throws(function () { pool.zoneMarkReady('zone-2'); });

/* remove a non-existent or non-disabled zone */
pool.zoneMarkEnabled('zone-2');
mod_assert.throws(function () { pool.zoneRemove('zone-XXX'); });
mod_assert.throws(function () { pool.zoneRemove('zone-2'); });

/* disable a ready zone */
pool.zoneMarkReady('zone-2');
mod_assert.throws(function () { pool.zoneMarkDisabled('zone-2'); });

/* mark a ready zone ready */
mod_assert.throws(function () { pool.zoneMarkReady('zone-2'); });

mod_assert.equal('zone-2', pool.zonePick());
mod_assert.throws(function () { pool.zoneUnready('zone-2'); });
mod_assert.throws(function () { pool.zonePick(); });


/* remove zones and watch the reserve come down. */
picked = Object.keys(picked);
mod_assert.equal(9, picked.length);

for (i = 0; i < 7; i++) {
	mod_assert.equal(10 - i - 1, pool.nzones());
	mod_assert.equal(2, pool.nzones() - pool.unreservedCapacity());
	zone = picked.shift();
	pool.zoneMarkDisabled(zone);
	pool.zoneRemove(zone);
}

mod_assert.equal(2, pool.nzones());
mod_assert.equal(0, pool.unreservedCapacity());
zone = picked.shift();
pool.zoneMarkDisabled(zone);
pool.zoneRemove(zone);

mod_assert.equal(1, pool.nzones());
mod_assert.equal(0, pool.unreservedCapacity());
zone = picked.shift();
pool.zoneMarkDisabled(zone);
pool.zoneRemove(zone);

mod_assert.equal(0, pool.nzones());
mod_assert.equal(0, pool.unreservedCapacity());

log.info('TEST PASSED');
