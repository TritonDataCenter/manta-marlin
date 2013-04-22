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
    	return (sprintf(
	    '(&(mantaComputeId=%s)(state=dispatched)(agentGeneration=%s)' +
	    '(!(timeAbandoned=*))(!(timeCancelled=*)))',
	    conf['mantaComputeId'], conf['agentGeneration']));
    }
};

exports.aqTasksCancelled = {
    'name': 'tasks cancelled',
    'bucket': 'task',
    'query': function (conf) {
    	return (sprintf('(&(mantaComputeId=%s)(!(state=done))' +
	    '(timeCancelled=*)(agentGeneration=%s))', conf['mantaComputeId'],
	    conf['agentGeneration']));
    }
};

exports.aqTasksInputDone = {
    'name': 'tasks input done',
    'bucket': 'task',
    'query': function (conf) {
    	return (sprintf(
	    '(&(mantaComputeId=%s)(state=accepted)(!(timeCancelled=*))' +
	    '(timeInputDone=*)(!(timeInputDoneRead=*)))',
	    conf['mantaComputeId']));
    }
};

exports.aqTaskInputs = {
    'name': 'task inputs',
    'bucket': 'taskinput',
    'query': function (conf) {
    	return (sprintf(
	    '(&(mantaComputeId=%s)(agentGeneration=%s)(!(timeRead=*)))',
	    conf['mantaComputeId'], conf['agentGeneration']));
    }
};

exports.aqJob = {
    'name': 'job details',
    'bucket': 'job',
    'query': function (jobid) {
	return (sprintf('(jobId=%s)', jobid));
    }
};
