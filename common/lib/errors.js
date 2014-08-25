/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * lib/errors.js: global list of Marlin user-facing errors.
 *
 * These are user-facing error codes, not internal error codes nor full objects.
 * Any errors added here should be added to the public-facing Manta
 * documentation.
 *
 * We use EM_ codes rather than the strings directly mainly to enforce that all
 * uses go through this file.
 */

/*
 * Generic Manta error codes.
 */
exports.EM_AUTHORIZATION	=      'AuthorizationError';
exports.EM_INTERNAL		=           'InternalError';
exports.EM_INVALIDARGUMENT	=    'InvalidArgumentError';
exports.EM_RESOURCENOTFOUND	=   'ResourceNotFoundError';
exports.EM_SERVICEUNAVAILABLE	= 'ServiceUnavailableError';

/*
 * Marlin-specific codes.
 */
exports.EM_TASKINIT		=           'TaskInitError';
exports.EM_TASKKILLED		=	  'TaskKilledError';
exports.EM_USERTASK		=           'UserTaskError';
exports.EM_JOBCANCELLED		=	'JobCancelledError';

/*
 * We export these as global variables so that jslint can catch typos.
 */
for (var code in exports)
	global[code] = exports[code];

/*
 * We also define variables for jslint.
 */
var EM_AUTHORIZATION;
var EM_INTERNAL;
var EM_INVALIDARGUMENT;
var EM_RESOURCENOTFOUND;
var EM_SERVICEUNAVAILABLE;
var EM_TASKINIT;
var EM_TASKKILLED;
var EM_USERTASK;
var EM_JOBCANCELLED;
