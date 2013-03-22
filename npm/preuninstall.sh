#!/bin/bash

#
# Marlin agent preuninstall script: this is invoked before the Marlin package is
# uninstalled to disable and remove the SMF service.
#

set -o xtrace
set -o pipefail
. $(dirname $0)/../tools/common.sh

mpi_probegroup=$(amon_config | json probegroup.name)
[[ -n "$mpi_probegroup" ]] || fail "bad amon probegroup name"

#
# If this fails because amon is not available, drive on.  We're likely doing an
# upgrade anyway.
#
$m_root/tools/amon_remove.sh "$mpi_probegroup"

set -o errexit
svcadm disable -s marlin-agent
svccfg delete marlin-agent 
rm -f $npm_config_smfdir/marlin-agent.xml
