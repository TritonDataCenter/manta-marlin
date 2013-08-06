/*
 * marlin agent DTrace provider
 */
module.exports = {
    'name': 'marlin-agent',
    'probes': [
	/* agent lifecycle	  agent id */
	[ 'agent-started',	  'char *' ],

	/* zone lifecycle	  zoneid   [error message] */
	[ 'zone-reset-start',	  'char *' ],
	[ 'zone-reset-done',	  'char *', 'char *' ],

	/* stream created	  jobid	    phase     zone */
	[ 'sched-stream-created', 'char *', 'char *', 'char *' ],

	/* task lifecycle	  jobid	    taskid    task */
	[ 'task-enqueued',	  'char *', 'char *', 'json' ],
	[ 'task-dispatched',	  'char *', 'char *', 'json' ],
	[ 'task-done',		  'char *', 'char *', 'json' ],
	[ 'task-killed',	  'char *', 'char *', 'json' ],

	/* taskinputs		  jobid	    taskid    task    input */
	[ 'taskinput-enqueued',	  'char *', 'char *', 'json', 'json' ],

	/* taskoutputs		  jobid	    taskid    task    type     object */
	[ 'taskoutput-emitted',	  'char *', 'char *', 'json', 'char*', 'char*' ]
    ]
};
