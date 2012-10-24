/*
 * lib/worker/locator.js: interface for locating Manta objects.
 */

var mod_assert = require('assert');
var mod_events = require('events');
var mod_fs = require('fs');
var mod_util = require('util');

var mod_extsprintf = require('extsprintf');
var mod_jsprim = require('jsprim');
var mod_libindex = require('libindex');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');

var mod_mautil = require('../util');

var sprintf = mod_extsprintf.sprintf;
var CVError = mod_mautil.CVError;

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
		    args['log']));
	}

	throw (new Error('unsupported value for property "locator": ' +
	    conf['locator']));
}

function MantaLocator(indexconf, log)
{
	mod_assert.ok(indexconf);

	var conf = mod_jsprim.deepCopy(indexconf);
	conf['log'] = log;
	conf['retry'] = conf['reconnect'];
	delete (conf['reconnect']);

	this.ml_ring = mod_libindex.createRing(conf);
	this.ml_ops = {};

	mod_events.EventEmitter();

	var loc = this;
	this.ml_ring.on('ready', function () { loc.emit('ready'); });
}

mod_util.inherits(MantaLocator, mod_events.EventEmitter);

MantaLocator.prototype.locate = function (keys, callback)
{
	/*
	 * For now this operation uses mod_vasync to parallelize one request for
	 * each key.  In the future, this could be a single batch operation.
	 */
	var uuid = mod_uuid.v4();
	var ring = this.ml_ring;
	var ops = this.ml_ops;

	this.ml_ops[uuid] = mod_vasync.forEachParallel({
	    'inputs': keys,
	    'func': function (key, subcallback) {
		/* XXX requestid could be jobid, plus phase, plus index? */
		ring.getMetadata({ 'requestId': uuid, 'key': key },
		    function (err, result) {
			if ((err && err['name'] == 'ObjectNotFoundError') ||
			    (result['sharks'] &&
			    result['sharks'].length === 0)) {
				subcallback(new CVError('EJ_NOENT',
				    'no such object'));
				return;
			}

			if (err) {
				subcallback(new CVError('EJ_INTERNAL', err));
				return;
			}

			if (result['type'] != 'object') {
				subcallback(new CVError('EJ_INVAL',
				    'objects of type "%s" are not supported',
				    result['type']));
				return;
			}

			if (!result['sharks'] ||
			    !Array.isArray(result['sharks'])) {
				subcallback(new CVError('EJ_INTERNAL',
				    'missing or invalid "sharks" property'));
				return;
			}

			subcallback(null, result['sharks'].map(function (s) {
				return ({
					'host': s['server_uuid'],
					'objectid': result['objectId'],
					'zonename': s['zone_uuid']
				});
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
