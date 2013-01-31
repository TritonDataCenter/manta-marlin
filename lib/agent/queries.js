/*
 * lib/agent/queries.js: defines the one-shot and periodic moray queries
 */

var sprintf = require('extsprintf').sprintf;

/*
 * Global periodic queries
 */

exports.aqTasksDispatched = {
    'name': 'tasks dispatched',
    'bucket': 'task',
    'query': function (conf) {
    	return (sprintf('(&(server=%s)(state=dispatched)(serverGeneration=%s)' +
	    '(!(timeAbandoned=*))(!(timeCancelled=*)))', conf['instanceUuid'],
	    conf['instanceGeneration']));
    }
};

exports.aqTasksCancelled = {
    'name': 'tasks cancelled',
    'bucket': 'task',
    'query': function (conf) {
    	return (sprintf('(&(server=%s)(!(state=done))(timeCancelled=*)' +
	    '(serverGeneration=%s))', conf['instanceUuid'],
	    conf['instanceGeneration']));
    }
};

exports.aqTasksInputDone = {
    'name': 'tasks input done',
    'bucket': 'task',
    'query': function (conf) {
    	return (sprintf('(&(server=%s)(state=accepted)(!(timeCancelled=*))' +
	    '(timeInputDone=*)(!(timeInputDoneRead=*)))',
	    conf['instanceUuid']));
    }
};

exports.aqTaskInputs = {
    'name': 'task inputs',
    'bucket': 'taskinput',
    'query': function (conf) {
    	return (sprintf('(&(server=%s)(serverGeneration=%s)(!(timeRead=*)))',
	    conf['instanceUuid'], conf['instanceGeneration']));
    }
};

exports.aqJob = {
    'name': 'job details',
    'bucket': 'job',
    'query': function (jobid) {
	return (sprintf('(jobId=%s)', jobid));
    }
};
