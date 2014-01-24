#!/bin/bash

if [[ -z "$1" ]]; then
	echo "usage: $0 [PGREP_ARGS]" >&2
	echo "Try using '-P PPID', where PPID is the shell you launched " >&2
	echo "the jobsupervisor from." >&2
	exit 2
fi

#
# test_sup_kill.sh: periodically kills the jobsupervisor (used for stress
# testing).
#
while sleep $(( RANDOM % 30 + 5 )); do
	date
	pkill "$@"
done
