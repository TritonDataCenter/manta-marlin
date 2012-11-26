#!/bin/bash

set -o xtrace
DIR=`dirname $0`

#
# If we're not in the global zone, exit right away.
#
if [[ -e /usr/bin/zonename ]]; then
	zone=$(/usr/bin/zonename)

	if [[ $zone != "global" ]]; then
		exit 0
	fi
else
	exit 0
fi

#
# These environment variables are set by apm.
#
export SMF_DIR=$npm_config_smfdir
export VERSION=$npm_package_version

export PREFIX=/opt/smartdc/agents/lib/node_modules/marlin

#
# Transform marlin-agent's SMF manifest
#
subfile () {
	IN=$1
	OUT=$2

	sed -e "s#@@PREFIX@@#$PREFIX#g" \
	    -e "s/@@VERSION@@/$VERSION/g" \
	    $IN > $OUT
}

subfile "$DIR/../smf/manifests/marlin-agent.xml" "$SMF_DIR/marlin-agent.xml"

svccfg import $SMF_DIR/marlin-agent.xml
