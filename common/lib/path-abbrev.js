/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * lib/path-abbrev.js: abbreviate long filesystem paths
 */

var mod_assertplus = require('assert-plus');

/* Public interface */
exports.pathAbbreviate = pathAbbreviate;

/*
 * These values are obtained from pathconf(2) using the _PC_NAME_MAX and
 * _PC_PATH_MAX parameters.  They're hardcoded here because in practice they're
 * very unlikely to change and this lets us avoid having to run pathconf(2) as
 * part of zone reset.
 */
var NAME_MAX = 255;
var PATH_MAX = 1024;

/*
 * Given a path "path" (like a filesystem path or Manta object path), return a
 * path that's as close as reasonable to the given path, but where no component
 * of the path (i.e., an intermediate directory name or the final basename)
 * exceeds "name_max" characters and the total path is no more than "path_max"
 * characters.
 *
 * This is intentionally somewhat vague.  Heuristics are used to try to make the
 * returned path look like the original path.  If the given path is already
 * normalized (i.e., contains no more than one slash in a row at any point in
 * the path) and already matches these constraints, the returned path will
 * exactly match the original path.  If these constraints are violated, then a
 * modified path will be returned.  This function attempts to return paths that
 * contain as much of the original path as possible, but may in some cases
 * return paths that look very different.
 *
 * Arguments:
 *
 *     path		filesystem or Manta object path, starting with a "/".
 *     			The path should already be normalized so that it does
 *     			not contain "..", ".", or runs of more than one "/" in a
 *     			row.
 *
 *     name_max		the maximum length of any component of the path
 *     (optional)	If unspecified, uses a reasonable default value for the
 *     			POSIX NAME_MAX configuration variable.  This value must
 *     			be at least 10.
 *
 *     path_max		the maximum length of the returned path
 *     (optional)	If unspecified, uses a reasonable default value for the
 *     			POSIX PATH_MAX configuration variable.  This value must
 *     			be at least 10, and at least four times "name_max + 1".
 *
 * It's not recommended to override "name_max" and "path_max".  That's mainly
 * used for testing.
 *
 * Error handling: syntactically invalid arguments (missing path, invalid values
 * for "name_max" or "path_max") are programmer errors that should not be
 * handled.  Invalid paths (e.g., those not starting with a "/") are operational
 * errors and are returned to the caller (not thrown).  If there's no error, an
 * object is returned with two properties:
 *
 *     abbrpath		the abbreviated path
 *
 *     shortened	a boolean indicating whether the path was shortened
 *
 * This function does not use Node's "path.sep" separator because it's intended
 * for Manta paths, which always use "/", even on Windows.  However, it could
 * easily be generalized to support a different path separator.
 */
function pathAbbreviate(args)
{
	var input, namemax, pathmax;
	var newinput, components, shortened;
	var i, c;

	mod_assertplus.object(args, 'args');
	mod_assertplus.string(args.path, 'args.path');
	mod_assertplus.optionalNumber(args.name_max, 'args.name_max');
	mod_assertplus.optionalNumber(args.path_max, 'args.path_max');

	if (typeof (args.name_max) == 'number') {
		namemax = args.name_max;
		mod_assertplus.ok(namemax >= 10);
	} else {
		namemax = NAME_MAX;
	}

	if (typeof (args.path_max) == 'number') {
		pathmax = args.path_max;
		mod_assertplus.ok(pathmax >= 10);
		mod_assertplus.ok(pathmax >= 4 * (namemax + 1),
		    'path_max must be at least 4 times name_max');
	} else {
		pathmax = PATH_MAX;
	}

	input = args.path;
	if (input.charAt(0) != '/') {
		return (new Error('malformed path: no leading "/"'));
	}

	newinput = input.substr(1);
	components = newinput.split('/');
	shortened = false;

	/*
	 * Check for components exceeding namemax and shorten them.
	 */
	for (i = 0; i < components.length; i++) {
		c = components[i];
		if (c.length <= namemax)
			continue;

		shortened = true;
		/*
		 * The leading underscore works around OS-4170.
		 */
		components[i] = c.substr(0, namemax - '_...'.length) + '_...';
	}

	newinput = '/' + components.join('/');
	if (newinput.length <= pathmax) {
		return ({
		    'abbrpath': newinput,
		    'shortened': shortened
		});
	}

	/*
	 * If the path itself exceeds PATH_MAX, construct a shorter name.  For
	 * caller-specific reasons, we prefer to keep the first three components
	 * and the last two components intact, if we can.
	 */
	if (components.length > 5) {
		/* See above on the leading underscore. */
		newinput = '/' +
		    [ components[0], components[1], components[2],
		    '_...', components[components.length - 2],
		    components[components.length - 1] ].join('/');
		if (newinput.length <= pathmax) {
			return ({
			    'abbrpath': newinput,
			    'shortened': true
			});
		}
	}

	/*
	 * The path exceeds PATH_MAX with even those five components.
	 * Something's got to give.  We know there are at least four components
	 * and together any four components must work because path_max must be
	 * at least 4 times name_max+1.  But we're going to add an ellipsis to
	 * indicate that we messed with this, so we can only use three
	 * components.
	 *
	 * See above on the leading undercore.
	 */
	mod_assertplus.ok(components.length >= 4);
	newinput = '/' +
	    [ components[0], components[1], '_...',
	    components[components.length - 1] ].join('/');
	mod_assertplus.ok(newinput.length <= pathmax);
	return ({
	    'abbrpath': newinput,
	    'shortened': true
	});
}
