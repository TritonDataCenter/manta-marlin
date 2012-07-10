/*
 * lib/worker/server.js: worker executable
 */

var mod_os = require('os');

var mod_bunyan = require('bunyan');
var mod_extsprintf = require('extsprintf');
var mod_kang = require('kang');
var mod_restify = require('restify');
var mod_vasync = require('vasync');

var mod_locator = require('./locator');
var mod_mautil = require('../util');
var mod_moray = require('./moray');
var mod_worker = require('./worker');

var mwWorkerInstance;

function main()
{
	var log, conf, port, moray_args, moray, locator, worker, server;

	if (process.argv.length < 3) {
		console.error('usage: %s %s config.json', process.argv[0],
		    process.argv[1]);
		process.exit(2);
	}

	log = new mod_bunyan({ 'name': 'jobworker', 'level': 'debug' });
	conf = mod_mautil.readConf(log, mod_worker.mwConfSchema,
	    process.argv[2]);
	port = conf['port'];

	moray_args = {
	    'log': log,
	    'url': conf['moray']['storage']['url'],
	    'findInterval': conf['findInterval'],
	    'taskGroupInterval': conf['taskGroupInterval'],
	    'jobsBucket': conf['jobsBucket'],
	    'taskGroupsBucket': conf['taskGroupsBucket']
	};

	log.info('configuration', conf);
	log.info('using RemoteMoray at url %s', moray_args['url']);
	moray = new mod_moray.RemoteMoray(moray_args);

	conf.log = log;
	locator = mod_locator.createLocator(conf);

	worker = mwWorkerInstance = new mod_worker.mwWorker({
	    'instanceUuid': conf['instanceUuid'],
	    'moray': moray,
	    'locator': locator,
	    'log': log,
	    'jobAbandonTime': conf['jobAbandonTime'],
	    'saveInterval': conf['saveInterval'],
	    'tickInterval': conf['tickInterval'],
	    'taskGroupIdleTime': conf['taskGroupIdleTime']
	});

	server = mod_restify.createServer({
	    'name': 'jobworker',
	    'log': log
	});

	server.use(mod_restify.acceptParser(server.acceptable));
	server.use(mod_restify.queryParser());
	server.use(mod_restify.bodyParser({ 'mapParams': false }));

	server.get('/kang/.*', mod_kang.knRestifyHandler({
	    'uri_base': '/kang',
	    'service_name': 'jobworker',
	    'version': '0.0.1',
	    'ident': conf['instanceUuid'],
	    'list_types': mwKangListTypes,
	    'list_objects': mwKangListObjects,
	    'get': mwKangGetObject
	}));

	server.on('after', mod_restify.auditLogger({ 'log': log }));

	mod_vasync.pipeline({
	    'funcs': [
		function (_, next) { moray.setup(next); },
		function (_, next) { server.listen(port, next); },
		function (_, next) { worker.start(); next(); }
	    ]
	}, function (err) {
		if (err) {
			log.fatal(mod_extsprintf.sprintf('%r', err));
			throw (err);
		}

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
