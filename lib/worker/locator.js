/*
 * lib/worker/locator.js: interface for locating Manta objects.
 */

var mod_assert = require('assert');
var mod_fs = require('fs');

var mod_libindex = require('libindex');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');

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
 * to an array of values descsribing one copy of the object on the storage tier:
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
 * We implement three types of locators:
 *
 *    o A Manta-based locator which is used for standard deployments.  This
 *      locator takes a ring of Moray shards as input and uses them to locate
 *      objects.
 *
 *    o A mock-up locator which is used by parts of the automated test suite.
 *      This locator assigns made-up objectids, hosts, and zonenames.
 *
 *    o A special locator for manual testing on COAL, where the mock-manta
 *      service is running in the dev zone alongside the job worker, and the
 *      agent is running in a global zone called "headnode".  This locator
 *      assigns "objectid" = key, "host" = "headnode", and "zonename" = the
 *      current zone's zonename.
 *
 * createLocator is used to create an appropriate locator for a given
 * configuration, which is a subset of the standard job worker configuration
 * with the following properties:
 *
 *    locator	One of "manta", "mock", and "test", describing the locator mode
 *    		to use.  The default value is "manta".  See above.
 *
 *    moray	Moray configuration (see generic worker configuration)
 *
 *	moray.indexing
 *
 *	    moray.indexing.urls: list of Moray shards in the Manta indexing tier
 *
 * If locator is unspecified or "manta", then the locator will use the Moray
 * instances specified by moray.indexing.urls to find objects.  This is used for
 * a standard deployment.  If locator is "mock" or "test", then the
 * corresponding mock-up or manual test locators will be returned (see above).
 */
function createLocator(conf)
{
	if (!conf['locator'] || conf['locator'] == 'manta') {
		mod_assert.ok(conf['moray']);
		mod_assert.ok(conf['moray']['indexing']);
		return (new MantaLocator(conf['moray']['indexing']));
	}

	if (conf['locator'] == 'test')
		return (new ManualTestLocator());

	mod_assert.equal(conf['locator'], 'mock',
	    'unsupported value for property "locator": ' + conf['locator']);
	return (new MockLocator());
}

function MantaLocator(indexconf)
{
	mod_assert.ok(indexconf);
	this.ml_ring = mod_libindex.createRing(indexconf);
	this.ml_ops = {};
}

MantaLocator.prototype.locate = function (keys, callback)
{
	/*
	 * For now this operation uses mod_vasync to parallelize one request for
	 * each key.  In the future, this will be a single batch operation.
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
			if (err) {
				if (err['code'] == 'ResourceNotFound')
					subcallback(null, []);
				else
					subcallback(err);

				return;
			}

			if (result['type'] != 'object') {
				/* XXX need to deal with directories */
				subcallback(new Error(
				    'directories not yet supported'));
				return;
			}

			if (!result['sharks'] ||
			    !Array.isArray(result['sharks'])) {
				subcallback(new Error(
				    'missing "sharks" property on result'));
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
	}, function (err, rv) {
		delete (ops[uuid]);

		/*
		 * If we had any success at all, we report those and ignore any
		 * errors.  The caller is smart enough to know that some keys
		 * may not have been located by this call, and will retry them.
		 * If there's an error at that point (where we made no forward
		 * progress), we'll report that.
		 */
		if (err && rv['successes'].length === 0) {
			callback(err);
			return;
		}

		callback(null, rv['successes']);
	});
};

MantaLocator.prototype.cleanup = function ()
{
	this.ml_ring.destroy(function () {});
};

function MockLocator() {}

MockLocator.prototype.locate = function (keys, callback)
{
	var rv = {};
	var i = 0;

	keys.forEach(function (key) {
		rv[key] = [ {
		    'host': 'node' + (i++ % 3),
		    'objectid': key,
		    'zonename': 'fake-zonename'
		} ];
	});

	setTimeout(function () { callback(null, rv); }, 10);

};


function ManualTestLocator() {}

ManualTestLocator.prototype.locate = function (keys, callback)
{
	var rv = {};

	/*
	 * This isn't pretty, but it's a synchronous way to extract our own
	 * zonename.
	 */
	var zonename = mod_fs.readFileSync('/etc/zones/index').toString(
	    'utf-8').split(':')[0];

	keys.forEach(function (key) {
		rv[key] = [ {
		    'host': 'headnode',
		    'objectid': key,
		    'zonename': zonename
		} ];
	});

	setTimeout(function () { callback(null, rv); }, 10);
};
