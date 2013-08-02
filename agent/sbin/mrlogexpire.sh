#!/bin/bash

#
# mrlogexpire.sh: invoked by cron to remove old per-stream debug logs
#

set -o pipefail
set -o errexit
set -o xtrace

echo "job expiration start: $(date)" >&2

mle_ndays=14
mle_logroot=/var/smartdc/marlin/log/zones

cd $mle_logroot
find . -mtime +$mle_ndays -exec rm -f "{}" \;

echo "job expiration done:  $(date)" >&2
