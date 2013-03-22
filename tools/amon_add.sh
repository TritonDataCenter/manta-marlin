#!/bin/bash

#
# amon_add.sh: given a stream on stdin representing an amon probegroup and
# probes, add them to amon, replacing any existing probegroup with the same name
# (and probes in that probegroup).  This operation is idempotent but not atomic.
#
# The stdin stream looks like this:
#
#    {
#    	"probegroup": {
#    		"name": "marlin agent on acdd1468",
#    		"contacts": [ "email" ]
#    	},
#    	"probes": [ {
#    		"name": "marlin-agent logscan-errors",
#    		"type": "bunyan-log-scan",
#    		...
#    	}, ... ]
#    }
#

#
# amon_create probe|probegroup: reads a probegroup or probe definition on stdin
# and creates the corresponding amon probe upstream.
#
function amon_create
{
	local out

	#
	# It's hard to tell whether curl succeeded or failed, so we look for a
	# valid "uuid" field in the output object.
	#
	out=$(sdc-amon /pub/poseidon/${1}s -X POST -T -) ||
	    fail "failed to create $1"
	out=$(echo "$out" | json -H uuid)
	[[ -n "$out" ]] || fail "didn't find newly-created $1"
}

#
# extract_probes probegroup_uuid: given an amon configuration on stdin, extract
# individual probes, fill in the given probegroup uuid, and emit them
# one-line-at-a-time.  (This is surprisingly non-trivial.)
#
function extract_probes
{
	json probes | \
	    json -ae "this.group = '$1'; x = JSON.stringify(this)" x
}


# Configuration
set -o xtrace
set -o pipefail
. $(dirname $0)/common.sh

mpi_tmpfile=/var/tmp/marlin-probes-$$.json

# Save off stdin so we can extract various pieces from it.
cat > $mpi_tmpfile

#
# Identify the probegroup name and remove any existing probegroup and probes
# with the same name.
#
mpi_groupname=$(json probegroup.name < $mpi_tmpfile) || fail "invalid JSON"
[[ -n "$mpi_groupname" ]] || fail "failed to extract probegroup name"
$m_root/tools/amon_remove.sh "$mpi_groupname"

# Create the new probegroup.
json probegroup < "$mpi_tmpfile" | amon_create probegroup
pguuid=$(amon_find probegroup "$mpi_groupname")
[[ -n "$pguuid" ]] || fail "new probegroup not found"

# Create the new probes.
out=$(extract_probes $pguuid < $mpi_tmpfile | while read probedef; do
	echo "$probedef" | amon_create probe || fail "failed"
    done 2>&1)
(echo "$out" | grep "failed") && fail "failed to create probes"

rm -f $mpi_tmpfile
