/*
 * lib/job.js: Marlin representation of individual compute jobs.
 * Copyright (c) 2012, Joyent, Inc. All rights reserved.
 */

var mod_apiutil = require('./apiutil');
var mod_uuid = require('node-uuid');
var mod_restify = require('restify');

var mlJobs = {};			/* all jobs, by jobId */

/*
 * The following JSON schema describes the input format for new jobs.  See the
 * Marlin API documentation for details.
 */
var mlJobSchema = {
	'type': 'object',
	'properties': {
		'jobName': { 'type': 'string', 'required': true },
		'phases': {
			'type': 'array',
			'required': true,
			'minItems': 1,
			'items': {
				'type': 'object',
				'properties': {
					'exec': {
						'type': 'string',
						'required': true
					},
					'args': {
						'type': 'object'
					},
					'assets': {
						'type': 'array',
						'items': {
							'type': 'string'
						}
					}
				}
			}
		},
		'inputKeys': {
			'type': 'array',
 			'required': true,
			'minItems': 1,
			'items': { 'type': 'string' }
		}
	}
};

function jobValidate(input)
{
	return (mod_apiutil.mlValidateSchema(mlJobSchema, input));
}

/*
 * POST /jobs: Create a new Job object from the given arguments.
 */
exports.jobSubmit = function jobSubmit(request, response, next)
{
	var args = request.body || {};
	var error = jobValidate(args);

	if (error)
		return (next(error));

	var uuid = mod_uuid.v4();
	var obj = {};

	for (var key in args)
		obj[key] = args[key];

	obj['jobId'] = uuid;
	var job = new mlJob(obj);
	mlJobs[uuid] = job;

	response.send(job.info());
	return (next());
};

/*
 * GET /jobs: List all job objects.
 */
exports.jobList = function jobList(request, response, next)
{
	var uuid, job, rv;

	rv = [];
	for (uuid in mlJobs) {
		job = mlJobs[uuid];
		rv.push({
			'jobId': job.j_jobid,
			'jobName': job.j_name,
			'state': job.state()
		});
	}

	response.send(rv);
	next();
};

/*
 * GET /jobs/:jobid: List basic job information
 */
exports.jobLookup = function jobLookup(request, response, next)
{
	if (!mlJobs.hasOwnProperty(request.params['jobid']))
		return (next(new mod_restify.NotFoundError('no such job')));

	var job = mlJobs[request.params['jobid']];
	response.send(job.info());
	return (next());
};

/*
 * Instantiates a new Job object based on the given args.  The args are
 * described in the Marlin API documentation and have already been validated at
 * this point.
 */
function mlJob(args)
{
	this.j_args = args;
	this.j_jobid = args['jobId'];
	this.j_name = args['jobName'];
}

mlJob.prototype.state = function ()
{
	return ('queued');
};

mlJob.prototype.info = function ()
{
	return (this.j_args);
};
