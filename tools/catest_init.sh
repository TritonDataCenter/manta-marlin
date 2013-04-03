#
# catest_init.sh: Marlin-specific catest initialization file
#

agentconfig="/opt/smartdc/marlin/etc/agentconfig.json"

#
# resolve resolver_ip url: Return $url with the hostname part resolved using
# $resolver_ip as a DNS resolver.  On error, returns $url unmodified.
#
function resolve
{
	local url=$2
	local protocol
	local portsuffix
	local host
	local rv

	protocol=${url%%://*}
	portsuffix=${url##*:}
	if [[ -z "$protocol" || "$protocol" == "$url" || \
	    -z "$portsuffix" || "$portsuffix" == "$url" ]]; then
		echo "$url"
		return
	fi

	# First trim the leading "protocol" plus "://"
	host=${url:$(( 3 + ${#protocol}))}
	# Next trim the trailing $portsuffix plus ":"
	host=${host:0: $(( ${#host} - ${#portsuffix} - 1))}

	if [[ -z "$host" || "$host" == "$url" ]]; then
		echo "$url"
		return
	fi

	rv=$(set -o pipefail; \
	    host -t A "$host" $1 | tail -1 | awk '{print $4}') || rv=
	if [[ -z "$rv" ]]; then
		echo "$url"
		return
	fi

	echo "$protocol://$rv:$portsuffix"
}

#
# If we're in the global zone and there's an agent configuration file present,
# we initialize our own configuration from that one.
#
function catest_init_global
{
	[[ -f $agentconfig ]] || return 1

	echo -n "Attempting to configure based on GZ agent configuration ... "

	local nameserver=$(json dns.nameservers.0 < $agentconfig)
	[[ -n "$nameserver" ]] || return 1

	[[ -z $MANTA_URL ]] &&
	    export MANTA_URL=$(resolve $nameserver \
	        $(json manta.url < $agentconfig) | sed -e s'#^http:#https:#' | \
		sed -Ee s'#:80/?$##')
	[[ -z $MORAY_URL ]] &&
	    export MORAY_URL=$(resolve $nameserver \
	        $(json moray.url < $agentconfig))
	[[ -z $MANTA_USER ]] && export MANTA_USER="poseidon"
	[[ -z $MANTA_KEY_ID ]] && export MANTA_KEY_ID="$(sdc-ldap search -b \
	    "$(sdc-ldap search login=poseidon | head -1 | cut -c5-)" \
	    objectclass=sdckey | awk '/^fingerprint:/{ print $2 }')"

	#
	# The mahi URL is trickier because it's not actually in the agent
	# configuration.  But we can guess it by replacing the first component
	# of the unresolved $MANTA_URL with "auth".
	#
	[[ -z $MAHI_URL ]] &&
	    export MAHI_URL=$(resolve $nameserver \
		$(json manta.url < $agentconfig | sed -Ee \
		s'#https?://[a-zA-Z0-9]+([^:/]*)([:/].*)?#tcp://auth\1:6379#'))

	echo "done."
}

function catest_init
{
	nfailed=0

	if [[ $(zonename) == "global" ]]; then
		catest_init_global || \
		    echo "WARN: failed to configure from $agentconfig" >&2
	else
		echo "NOTE: skipping metering tests because you're running" \
		    "in a non-global zone." >&2
	fi

	if [[ -z "$MAHI_URL" ]]; then
		((nfailed++))
		echo "MAHI_URL must be set in the environment." >&2
	fi

	if [[ -z "$MORAY_URL" ]]; then
		((nfailed++))
		echo "MORAY_URL must be set in the environment." >&2
	fi

	if [[ -z "$MANTA_USER" || -z "$MANTA_KEY_ID" || -z "$MANTA_URL" ]]; then
		((nfailed++))
		echo "MANTA_USER, MANTA_URL, and MANTA_KEY_ID must be set" \
		    "in the environment." >&2
	fi

	if [[ $nfailed -ne 0 ]]; then
		if [[ "$opt_a" == "true" ]]; then
			echo "ERROR: required environment variables not set." >&2
			return $nfailed
		else
			echo "WARNING: required environment variables not" \
			    "set. Some tests may fail."
		fi
	fi

	echo "MAHI_URL     = $MAHI_URL"
	echo "MANTA_KEY_ID = $MANTA_KEY_ID"
	echo "MANTA_URL    = $MANTA_URL"
	echo "MANTA_USER   = $MANTA_USER"
	echo "MORAY_URL    = $MORAY_URL"

	export PATH="$PWD/build/node/bin:$PATH"
}
