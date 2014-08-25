/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * jobworker DTrace provider
 */

module.exports = {
    'name': 'marlin-supervisor',
    'probes': [
	/* job lifecycle		jobid,		job definition */
	[ 'job-assigned',		'char *',	'json' ],
	[ 'job-input-done',		'char *',	'json' ],
	[ 'job-done',			'char *',       'json' ],

	/* other job events		jobid */
	[ 'job-inputs-read',		'char *' ],
	[ 'job-mark-inputs-start',	'char *' ],
	[ 'job-mark-inputs-done',	'char *' ],

	/* task lifecycle	jobid		taskid		task def */
	[ 'task-dispatched',	'char *',	'char *',	'json' ],
	[ 'task-input-done',	'char *',	'char *',	'json' ],
	[ 'task-committed',	'char *',	'char *',	'json' ],

	/* taskinputs		jobid		taskid		definition */
	[ 'taskinput-dispatched', 'char *',	'char *',	'json' ],

	/* errors		jobid		value */
	[ 'error-dispatched',	'char *',	'json' ],

	/* health check events		component id */
	[ 'worker-startup',		'char *' ],
	[ 'worker-timeout',		'char *' ],
	[ 'agent-timeout',		'char *' ],

	/* asynchronous ops	arg		error code */
	[ 'auth-start',		'char *' ],
	[ 'auth-done',		'char *',	'char *' ],
	[ 'locate-start',	'char *' ],
	[ 'locate-done',	'char *',	'char *' ],
	[ 'delete-start',	'char *' ],
	[ 'delete-done',	'char *',	'char *' ]
    ]
};
