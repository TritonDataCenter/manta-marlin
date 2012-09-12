/*
 * lib/worker/server.js: worker executable
 */

var mod_bunyan = require('bunyan');
var mod_kang = require('kang');
var mod_panic = require('panic');

var mod_mautil = require('../util');
var mod_worker = require('./worker');

var mwLogLevel = process.env['LOG_LEVEL'] || 'debug';
var mwWorkerInstance;

function main()
{
	var log, conf, worker;

	if (process.argv.length < 3) {
		console.error('usage: %s %s config.json', process.argv[0],
		    process.argv[1]);
		process.exit(2);
	}

	if (!process.env['NO_ABORT_ON_CRASH']) {
		mod_panic.enablePanicOnCrash({
		    'skipDump': true,
		    'abortOnPanic': true
		});
	}

	log = new mod_bunyan({ 'name': 'jobworker', 'level': mwLogLevel });
	conf = mod_mautil.readConf(log, mod_worker.mwConfSchema,
	    process.argv[2]);

	worker = mwWorkerInstance = new mod_worker.mwWorker({
	    'conf': conf,
	    'log': log
	});

	mod_kang.knStartServer({
	    'port': conf['port'],
	    'uri_base': '/kang',
	    'service_name': 'jobworker',
	    'version': '0.0.1',
	    'ident': conf['instanceUuid'],
	    'list_types': mwKangListTypes,
	    'list_objects': mwKangListObjects,
	    'get': mwKangGetObject,
	    'stats': mwKangStats
	}, function (err, server) {
		if (err) {
			log.fatal(err, 'failed to initialize kang server');
			throw (err);
		}

		var addr = server.address();
		log.info('kang server listening at http://%s:%d',
		    addr['address'], addr['port']);
		worker.start();
	});
}


/*
 * Kang (introspection) entry points
 */

function mwKangListTypes()
{
	return ([ 'worker', 'jobs' ]);
}

function mwKangListObjects(type)
{
	if (type == 'worker')
		return ([ mwWorkerInstance.mw_uuid ]);

	return (Object.keys(mwWorkerInstance.w_jobs));
}

function mwKangGetObject(type, ident)
{
	if (type == 'worker')
		return (mwWorkerInstance.debugState());

	return (mwWorkerInstance.w_jobs[ident].debugState());
}

function mwKangStats()
{
	return (mwWorkerInstance.kangStats());
}

main();
