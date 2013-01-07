#
# catest_init.sh: Marlin-specific catest initialization file
#

function catest_init
{
	nfailed=0

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
}
