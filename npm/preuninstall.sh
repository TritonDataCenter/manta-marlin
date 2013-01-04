#!/bin/bash

#
# Marlin agent preuninstall script: this is invoked before the Marlin package is
# uninstalled to disable and remove the SMF service.
#

set -o xtrace
set -o errexit

svcadm disable -s marlin-agent
svccfg delete marlin-agent 
rm -f $npm_config_smfdir/marlin-agent.xml
