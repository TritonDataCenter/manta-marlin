/*
 * lib/worker/server.js: worker executable
 */

var mod_os = require('os');

var mod_bunyan = require('bunyan');
var mod_kang = require('kang');
var mod_restify = require('restify');
var mod_vasync = require('vasync');

var mod_moray = require('./moray');
var mod_worker = require('./worker');

var mwWorkerInstance;

function main()
{
	var log, moray_args, moray, worker, server;

	log = new mod_bunyan({ 'name': 'worker-demo' });

	moray_args = {
	    'log': log,
	    'findInterval': mod_worker.mwConf['findInterval'],
	    'taskGroupInterval': mod_worker.mwConf['taskGroupInterval'],
	    'jobsBucket': mod_worker.mwConf['jobsBucket'],
	    'taskGroupsBucket': mod_worker.mwConf['taskGroupsBucket']
	};

	if (process.argv.length > 2) {
		moray_args['url'] = process.argv[2];
		log.info('using RemoteMoray at url %s', moray_args['url']);
		moray = new mod_moray.RemoteMoray(moray_args);
	} else {
		log.info('using MockMoray');
		moray = new mod_moray.MockMoray(moray_args);
	}

	worker = mwWorkerInstance = new mod_worker.mwWorker({
	    'uuid': 'worker-001',
	    'moray': moray,
	    'log': log
	});

	server = mod_restify.createServer({
	    'name': 'worker-demo',
	    'log': log
	});

	server.use(mod_restify.acceptParser(server.acceptable));
	server.use(mod_restify.queryParser());
	server.use(mod_restify.bodyParser({ 'mapParams': false }));

	server.get('/kang/.*', mod_kang.knRestifyHandler({
	    'uri_base': '/kang',
	    'service_name': 'worker-demo',
	    'version': '0.0.1',
	    'ident': mod_os.hostname(),
	    'list_types': mwKangListTypes,
	    'list_objects': mwKangListObjects,
	    'get': mwKangGetObject
	}));

	moray.restify(server);

	server.on('after', mod_restify.auditLogger({ 'log': log }));

	mod_vasync.pipeline({
	    'funcs': [
		function (_, next) { moray.setup(next); },
		function (_, next) { server.listen(8083, next); },
		function (_, next) { worker.start(); next(); }
	    ]
	}, function (err) {
		if (err)
			throw (err);

		var addr = server.address();
		log.info('kang server listening at http://%s:%d',
		    addr['address'], addr['port']);
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
		return ([ 0 ]);

	return (Object.keys(mwWorkerInstance.mw_jobs));
}

function mwKangGetObject(type, ident)
{
	if (type == 'worker')
		return ({
		    'uuid': mwWorkerInstance.mw_uuid,
		    'interval': mwWorkerInstance.mw_interval,
		    'moray': mwWorkerInstance.mw_moray.debugState(),
		    'timeout': mwWorkerInstance.mw_timeout ? 'yes' : 'no',
		    'worker_start': mwWorkerInstance.mw_worker_start,
		    'tick_start': mwWorkerInstance.mw_tick_start,
		    'tick_done': mwWorkerInstance.mw_tick_done
		});

	var obj = mwWorkerInstance.mw_jobs[ident];

	return ({
	    'job': obj.js_job,
	    'state': obj.js_state,
	    'state_time': obj.js_state_time,
	    'pending_start': obj.js_pending_start,
	    'phasei': obj.js_phasei,
	    'phases': obj.js_phases
	});
}

main();
