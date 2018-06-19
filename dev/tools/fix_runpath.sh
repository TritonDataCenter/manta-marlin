#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2018, Joyent, Inc.
#

#
# This program adjusts the ELF runpath of several libraries and Node binary
# add-ons to make the Marlin software relocatable -- that is, to be able to
# function correctly when installed at an arbitrary location in the file
# system.
#
# Usage:
#
#    ./dev/tools/fix_runpath.sh [-v] PREFIX
#
#        -v        print more diagnostic output
#
#        PREFIX    the top-level Marlin software directory; i.e., the directory
#                  in which both "build/node" and "node_modules" appear
#
# Example:
#
#    To correct the runpath for binaries in the built proto area:
#        ./dev/tools/fix_runpath.sh ./build/proto/root/opt/smartdc/marlin
#

verbose=false

function dprintf {
	if [[ $verbose != true ]]; then
		return 0;
	fi

	printf "$@"
}

while getopts 'v' opt; do
	case "$opt" in
	v)
		verbose=true
		;;
	?)
		printf 'Usage: %s [-v] PREFIX\n' >&2
		exit 2
		;;
	esac
done

shift $(( $OPTIND - 1 ))

#
# This program needs to know the location in the proto area where marlin
# is installed; e.g., "$(TOP)/build/proto/root/opt/smartdc/marlin".
#
prefix=$1
if [[ ! -d $prefix ]] || [[ ! -d $prefix/node_modules ]]; then
	printf 'ERROR: specify prefix directory\n' >&2
	exit 1
fi

if ! cd "$prefix"; then
	exit 1
fi

#
# Some sdcnode builds have been shipped with an $ORIGIN-relative runpath for
# the "node" binary, but _not_ for the dependencies shipped in lib/.  Amending
# this runpath here is a workaround for this issue.
#
if ! libs=( $(find build/node/lib -type f -name 'lib*.so*') ); then
	printf 'ERROR: could not list node libraries\n' >&2
	exit 1
fi

for (( i = 0; i < ${#libs[@]}; i++ )); do
	p="${libs[$i]}"
	printf 'lib: %s\n' "$p"

	#
	# Modify the runpath in the node add-on.
	#
	rp='$ORIGIN'
	if ! /usr/bin/elfedit -e "dyn:runpath -o simple $rp" "$p"; then
		printf 'ERROR: could not set runpath in file "%s"\n' "$p" >&2
		exit 1
	fi
done

#
# Locate any Node binary add-ons built and installed in the "node_modules"
# directory tree.
#
if ! libs=( $(find node_modules -type f -name '*.node') ); then
	printf 'ERROR: could not list node binary add-ons\n' >&2
	exit 1
fi

#
# Each binary add-on lives in a directory structure defined by the build
# process for the specific Node module.  In addition, the directory tree can be
# arbitrarily deep, depending on the dependency relationships between modules.
# No single $ORIGIN-relative runpath will be appropriate for all of the add-ons
# we ship, so for each add-on we will construct an appropriate relative path
# back out to the lib/ directory for the project-private Node binary.
#
for (( i = 0; i < ${#libs[@]}; i++ )); do
	p="${libs[$i]}"
	printf 'add-on: %s\n' "$p"

	d=$(dirname "$p")
	dprintf 'dir: %s\n' "$d"

	#
	# Determine path depth by removing path elements until we run out of
	# elements.
	#
	depth=0
	while [[ $d != '.' ]]; do
		(( depth++ ))
		d=$(dirname "$d")
	done

	dprintf 'depth: %d\n' "$depth"

	#
	# Construct an $ORIGIN-relative runpath so that this shared object
	# will find the libraries we ship with our node binary.
	#
	rp='$ORIGIN'
	for (( j = 0; j < $depth; j++ )); do
		rp+='/..'
	done
	rp+='/build/node/lib'

	dprintf 'runpath: %s\n' "$rp"

	#
	# Modify the runpath in the node add-on.
	#
	if ! /usr/bin/elfedit -e "dyn:runpath -o simple $rp" "$p"; then
		printf 'ERROR: could not set runpath in file "%s"\n' "$p" >&2
		exit 1
	fi

	#
	# After editing the runpath, we don't expect to see any references to a
	# library shipped in the pkgsrc tree in the zone (/opt/local).  Do a
	# basic check to try and prevent new problems from slipping through.
	#
	if ldd "$p" | grep '/opt/local' >/dev/null; then
		printf 'ERROR: /opt/local library found in "ldd" output\n' >&2
		ldd "$p"
		exit 1
	fi

	dprintf '\n'
done
