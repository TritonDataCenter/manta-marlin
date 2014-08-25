/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

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
	    '(!(timeAbandoned=*)))',
	    conf['mantaComputeId'], conf['agentGeneration']));
    }
};

exports.aqTasksCancelled = {
    'name': 'tasks cancelled',
    'bucket': 'task',
    'query': function (conf) {
    	return (sprintf('(&(mantaComputeId=%s)(state=accepted)' +
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
	    '(agentGeneration=%s)(timeInputDone=*)(!(timeInputDoneRead=*)))',
	    conf['mantaComputeId'], conf['agentGeneration']));
    }
};

exports.aqTaskInputs = {
    'name': 'task inputs',
    'bucket': 'taskinput',
    'query': function (conf) {
    	return (sprintf(
	    '(&(mantaComputeId=%s)(agentGeneration=%s)(!(timeRead=*))' +
	    '(!(timeJobCancelled=*))(!(retryTaskId=*)))',
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
