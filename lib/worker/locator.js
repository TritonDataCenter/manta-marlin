/*
 * lib/worker/locator.js: interface for locating Manta objects.
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_fs = require('fs');
var mod_util = require('util');

var mod_extsprintf = require('extsprintf');
var mod_jsprim = require('jsprim');
var mod_libmanta = require('libmanta');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');

var mod_mautil = require('../util');

var sprintf = mod_extsprintf.sprintf;
var CVError = mod_mautil.CVError;

/* jsl:import ../errors.js */
require('../errors');

/* Public interface */
exports.createLocator = createLocator;

/*
 * Locators are objects with a single method for locating a set of keys in
 * Manta:
 *
 *	locate(keys, callback)
 *
 * "keys" is an array of Manta object keys.  "callback" is invoked later as
 * callback(err, results), where "results" is an object mapping the input keys
 * to an array of values describing one copy of the object on the storage tier:
 *
 *      objectid	Unique identifier for this object.  This value is the
 *      		same for all copies of this object.
 *
 *	host		Unique identifier for the physical server on which this
 *			copy resides.  This is not necessarily a hostname, but a
 *			static identifier for the server (e.g., server_uuid).
 *
 *	zonename	Unique identifier for the Mako shark on "host" which
 *			actually stores this copy.
 *
 * We currently only implement a Manta-based locator which is used for standard
 * deployments.  This locator takes a ring of Moray shards as input and uses
 * them to locate objects.  For future extension, createLocator is used to
 * create an appropriate locator for a given configuration, which is a subset of
 * the standard job worker configuration with the following properties:
 *
 *    locator	The default value is "manta".  See above.
 *
 *    moray	Moray configuration (see generic worker configuration)
 *
 *	moray.indexing
 *
 *	    moray.indexing.urls: list of Moray shards in the Manta indexing tier
 *
 * The locator will use the Moray instances specified by moray.indexing.urls to
 * find objects.
 */
function createLocator(conf, args)
{
	if (!conf['locator'] || conf['locator'] == 'manta') {
		mod_assert.ok(conf['moray']);
		mod_assert.ok(conf['moray']['indexing']);
		return (new MantaLocator(conf['moray']['indexing'],
					args['log'],
					args['storage_map']));
	}

	throw (new Error('unsupported value for property "locator": ' +
	    conf['locator']));
}

function MantaLocator(indexconf, log, storage_map)
{
	mod_assert.ok(indexconf);

	var conf = mod_jsprim.deepCopy(indexconf);
	conf['log'] = log;
	conf['retry'] = conf['reconnect'];
	delete (conf['reconnect']);

	this.ml_log = log;
	this.ml_ring = mod_libmanta.createIndexRing(conf);
	this.ml_ops = {};
	this.ml_storage_map = storage_map;
	mod_events.EventEmitter();

	var loc = this;
	this.ml_ring.once('ready', function () {
		loc.ml_log.info('locator ready');
		loc.emit('ready');
	});
}

mod_util.inherits(MantaLocator, mod_events.EventEmitter);

/*
 * Guarantees that the shark data always contains:
 *    objectid
 *    zonename
 *    mantaComputeId
 *    mantaStorageId
 *    host  (soon to be removed)
 */
function populateSharkData(locator, result, shark) {
	var smByStorage = locator.ml_storage_map['by_storage_id'];
	var smByZone = locator.ml_storage_map['by_zone_uuid'];
	var rec = smByStorage[shark['manta_storage_id']] ||
		smByZone[shark['zone_uuid']];
	return ({
		'objectid': result['objectId'],
		'mantaStorageId': rec['manta_storage_id'],
		'mantaComputeId': rec['manta_compute_id'],
		'zonename': rec['zone_uuid'],
		'host': rec['server_uuid']
	});
}

MantaLocator.prototype.locate = function (keys, callback)
{
	/*
	 * For now this operation uses mod_vasync to parallelize one request for
	 * each key.  In the future, this could be a single batch operation.
	 */
	var loc = this;
	var uuid = mod_uuid.v4();
	var ring = this.ml_ring;
	var ops = this.ml_ops;

	this.ml_ops[uuid] = mod_vasync.forEachParallel({
	    'inputs': keys,
	    'func': function (key, subcallback) {
		/* XXX requestid could be jobid, plus phase, plus index? */
		ring.getMetadata({ 'requestId': uuid, 'key': key },
		    function (err, result) {
			if ((err && err['name'] == 'ObjectNotFoundError')) {
				subcallback(new CVError(EM_RESOURCENOTFOUND,
				    'no such object'));
				return;
			}

			if (err) {
				subcallback(new CVError(EM_INTERNAL, err));
				return;
			}

			if (result['type'] != 'object') {
				subcallback(new CVError(EM_INVALIDARGUMENT,
				    'objects of type "%s" are not supported',
				    result['type']));
				return;
			}

			if (!result['sharks'] ||
			    !Array.isArray(result['sharks'])) {
				subcallback(new CVError(EM_INTERNAL,
				    'missing or invalid "sharks" property'));
				return;
			}

			if (result['sharks'].length === 0 &&
			    result['contentLength'] !== 0) {
				subcallback(new CVError(EM_INTERNAL,
				    'no sharks found for non-empty object'));
				return;
			}

			subcallback(null, result['sharks'].map(function (s) {
				return (populateSharkData(loc, result, s));
			}));
		    });
	    }
	}, function (err, result) {
		delete (ops[uuid]);

		var rv = {};

		keys.forEach(function (key, i) {
			if (result['operations'][i]['status'] != 'ok') {
				rv[key] = { 'error':
				    result['operations'][i]['err'] };
				return;
			}

			rv[key] = result['operations'][i]['result'];
		});

		callback(err, rv);
	});
};

MantaLocator.prototype.cleanup = function ()
{
	this.ml_ring.destroy(function () {});
};
