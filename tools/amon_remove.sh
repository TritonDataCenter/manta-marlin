#!/bin/bash

#
# amon_remove.sh probegroup_uuid: removes a probe group (by name) AND all of its
# probes.  Idempotent.
#

set -o xtrace
set -o pipefail
. $(dirname $0)/common.sh

#
# amon_remove probe|probegroup uuid: removes an amon probe or probegroup.  This
# function is NOT recursive for probegroups.
#
function amon_remove
{
	sdc-amon /pub/poseidon/${1}s/$2 -X DELETE || \
	    fail "failed to remove $1 \"$2\""
	found=$(sdc-amon /pub/poseidon/${1}s | json -Hac "uuid == '$2'" uuid)
	[[ -n "$found" ]] && fail "found $1 \"$2\" after removing it"
}

#
# amon_remove_probegroup uuid: removes a probegroup AND all of its probes
#
function amon_remove_probegroup
{
	for probe in $(sdc-amon /pub/poseidon/probes | \
	    json -Hac "group == '$1'" uuid); do
		amon_remove probe $probe
	done

	amon_remove probegroup $1
}

[[ -n "$1" ]] || fail "usage: $0 probegroup_name"
pguuid=$(amon_find probegroup "$1")
[[ -n "$pguuid" ]] && amon_remove_probegroup "$pguuid"
exit 0
