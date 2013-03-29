/*
 * lib/worker/queries.js: defines the one-shot and periodic moray queries
 */

/*
 * We've got a few different types of Moray queries:
 *
 *   o a static set of one-shot queries run at startup only
 *
 *   o a static set of repeated queries run periodically
 *
 *   o a per-job set of one-shot queries run at assignment only
 *
 *   o a per-job set of one-shot queries that only count records
 *
 *   o a per-job set of repeated queries run periodically
 *
 * Note that the "name" fields in these queries are used programmatically in
 * lib/worker/worker.js.
 */

var sprintf = require('extsprintf').sprintf;

/*
 * Global one-shot queries (run once at startup)
 */
exports.wqJobsOwned = {
    'name': 'jobs owned',
    'bucket': 'job',
    'query': function (conf) {
	return (sprintf('(&(worker=%s)(!(state=done)))', conf['instanceUuid']));
    }
};

/*
 * Global periodic queries
 */
exports.wqAgentHealth = {
    'name': 'agent health',
    'bucket': 'health',
    'options': function (conf) {
	return ({
	    'timePoll': conf['tunables']['timeAgentPoll'],
	    'limit': 10000
	});
    },
    'query': function () {
	return ('(component=agent)');
    }
};

exports.wqJobsAbandoned = {
    'name': 'jobs abandoned',
    'bucket': 'job',
    'query': function (conf, now) {
	return (sprintf('(&(_mtime<=%s)(!(state=done)))',
	    now - conf['tunables']['timeJobAbandon'] - 1));
    }
};

exports.wqJobsCreated = {
    'name': 'jobs created',
    'bucket': 'job',
    'query': function () { return ('(&(!(worker=*))(!(state=done)))'); }
};

exports.wqJobsCancelled = {
    'name': 'jobs cancelled',
    'bucket': 'job',
    'query': function (conf, now) {
	return (sprintf(
	    '(&(worker=%s)(timeCancelled=*)(!(state=done)))',
	    conf['instanceUuid']));
    }
};

exports.wqJobsInputEnded = {
    'name': 'jobs endinput',
    'bucket': 'job',
    'query': function (conf) {
	return (sprintf(
	    '(&(worker=%s)(timeInputDone=*)(!(timeInputDoneRead=*))' +
	        '(!(state=done)))', conf['instanceUuid']));
    }
};

/*
 * Per-job one-shot queries (run once when the job is assigned)
 */

exports.wqCountJobTasksUncommitted = {
    'name': 'count uncommitted tasks',
    'bucket': 'task',
    'countonly': true,
    'query': function (phasei, jobid) {
	return (sprintf('(&(jobId=%s)(phaseNum=%d)(!(timeCommitted=*)))',
	    jobid, phasei));
    }
};

exports.wqCountJobTasksNeedingRetry = {
    'name': 'count tasks needing retry',
    'bucket': 'task',
    'countonly': true,
    'query': function (phasei, jobid) {
 	return (sprintf('(&(jobId=%s)(phaseNum=%d)' +
	    '(|(wantRetry=true)(wantRetry=TRUE))' + /* workaround MANTA-1065 */
	    '(!(timeRetried=*)))', jobid, phasei));
    }
};

exports.wqCountJobTaskOutputsUnpropagated = {
    'name': 'count unpropagated taskoutputs',
    'bucket': 'taskoutput',
    'countonly': true,
    'query': function (phasei, jobid) {
	return (sprintf('(&(jobId=%s)(phaseNum=%d)(timeCommitted=*)' +
	    '(!(timePropagated=*)))', jobid, phasei));
    }
};

exports.wqCountErrors = {
    'name': 'count nErrors',
    'bucket': 'error',
    'countonly': true,
    'query': function (jobid) {
	/* Work around MANTA-1065. */
	return (sprintf('(&(jobId=%s)(|(retried=false)(retried=FALSE)))',
	    jobid));
    }
};

exports.wqCountRetries = {
    'name': 'count nRetries',
    'bucket': 'error',
    'countonly': true,
    'query': function (jobid) {
	/* Work around MANTA-1065. */
	return (sprintf('(&(jobId=%s)(|(retried=true)(retried=TRUE)))',
	    jobid));
    }
};

exports.wqCountInputsRead = {
    'name': 'count nInputsRead',
    'bucket': 'jobinput',
    'countonly': true,
    'query': function (jobid) {
	return (sprintf('(&(jobId=%s)(timePropagated=*))', jobid));
    }
};

exports.wqCountOutputs = {
    'name': 'count nJobOutputs',
    'bucket': 'taskoutput',
    'countonly': true,
    'query': function (jobid, phasei) {
	return (sprintf('(&(jobId=%s)(phaseNum=%s)(valid=true)' +
	    '(timeCommitted=*))', jobid, phasei));
    }
};

exports.wqCountTasksDispatched = {
    'name': 'count nTasksDispatched',
    'bucket': 'task',
    'countonly': true,
    'query': function (jobid) {
	return (sprintf('(jobId=%s)', jobid));
    }
};

exports.wqCountTasksCommittedOk = {
    'name': 'count nTasksCommittedOk',
    'bucket': 'task',
    'countonly': true,
    'query': function (jobid) {
	return (sprintf('(&(jobId=%s)(timeCommitted=*)(result=ok))', jobid));
    }
};

exports.wqCountTasksCommittedFail = {
    'name': 'count nTasksCommittedFail',
    'bucket': 'task',
    'countonly': true,
    'query': function (jobid) {
	return (sprintf('(&(jobId=%s)(timeCommitted=*)(!(result=ok)))',
	    jobid));
    }
};

exports.wqJobTasksReduce = {
    'name': 'reduce tasks',
    'bucket': 'task',
    'query': function (jobid) {
	return (sprintf('(&(jobId=%s)(rIdx=*)(!(timeRetried=*)))', jobid));
    }
};

exports.wqCountReduceTaskInputs = {
    'name': 'count taskinputs',
    'bucket': 'taskinput',
    'countonly': true,
    'query': function (taskid) {
	return (sprintf('(taskId=%s)', taskid));
    }
};

exports.wqCountJobTasksNeedingDelete = {
    'name': 'count tasks needing delete',
    'bucket': 'task',
    'countonly': true,
    'query': function (jobid) {
	return (sprintf(
	    '(&(jobId=%s)(wantInputRemoved=true)(!(timeInputRemoved=*)))',
	    jobid));
    }
};

exports.wqCountJobTaskInputsNeedingDelete = {
    'name': 'count taskinputs needing delete',
    'bucket': 'taskinput',
    'countonly': true,
    'query': function (jobid) {
	return (sprintf(
	    '(&(jobId=%s)(wantInputRemoved=true)(!(timeInputRemoved=*)))',
	    jobid));
    }
};

/*
 * Per-job periodic queries
 */

exports.wqJobInputs = {
    'name': 'job inputs',
    'bucket': 'jobinput',
    'query': function (jobid) {
	return (sprintf('(&(jobId=%s)(!(timePropagated=*)))', jobid));
    }
};

exports.wqJobTasksDone = {
    'name': 'done tasks',
    'bucket': 'task',
    'query': function (jobid) {
	return (sprintf('(&(jobId=%s)(state=done)(!(timeCommitted=*)))',
	    jobid));
    }
};

exports.wqJobTasksNeedingDelete = {
    'name': 'tasks needing delete',
    'bucket': 'task',
    'query': function (jobid) {
	return (sprintf(
	    '(&(jobId=%s)(wantInputRemoved=true)(!(timeInputRemoved=*)))',
	    jobid));
    }
};

exports.wqJobTaskInputsNeedingDelete = {
    'name': 'taskinputs needing delete',
    'bucket': 'taskinput',
    'query': function (jobid) {
	return (sprintf(
	    '(&(jobId=%s)(wantInputRemoved=true)(!(timeInputRemoved=*)))',
	    jobid));
    }
};

exports.wqJobTasksNeedingRetry = {
    'name': 'tasks needing retry',
    'bucket': 'task',
    'query': function (jobid) {
	return (sprintf('(&(jobId=%s)(!(timeRetried=*))' +
	    '(|(wantRetry=true)(wantRetry=TRUE)))', /* workaround MANTA-1065 */
	    jobid));
    }
};

exports.wqJobTaskInputsNeedingRetry = {
    'name': 'taskinputs retry',
    'bucket': 'taskinput',
    'query': function (jobid) {
	return (sprintf('(&(jobId=%s)(retryTaskId=*)(!(timeRetried=*)))',
	    jobid));
    }
};

exports.wqJobTaskOutputsUnpropagated = {
    'name': 'taskoutputs',
    'bucket': 'taskoutput',
    'query': function (jobid, lastphase) {
	return (sprintf('(&(jobId=%s)(timeCommitted=*)' +
	    '(phaseNum<=%d)(!(timePropagated=*)))', jobid, lastphase - 1));
    }
};
