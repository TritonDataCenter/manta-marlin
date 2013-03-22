#
# Common routines used by tools
#

function fail
{
	echo "$(basename $0): $@" >&2
	exit 1
}

#
# amon_find probe|probegroup name: return the uuid for the probe or probegroup
# with name "name", or the empty string if there was none.
#
function amon_find
{
	sdc-amon /pub/poseidon/${1}s | json -a -H -c "name == '$2'" uuid
}

#
# amon_config: emit the amon probe configuration, with variables translated for
# this installation
#
function amon_config
{
	local server_uuid

	server_uuid=$(sysinfo | json UUID) || fail "failed to get server_uuid"
	sed -e "s#{{SERVER_UUID}}#$server_uuid#g" $m_root/amon/marlin-agent.json
}

cd $(dirname $0)/..
m_root=$(pwd)			# root of Marlin installation
cd - > /dev/null
