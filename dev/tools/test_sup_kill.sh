#!/bin/bash

#
# test_sup_kill.sh: periodically kills the jobsupervisor (used for stress
# testing).
#
while sleep $(( RANDOM % 30 + 5 )); do
	date
	pkill -f worker/server.js
done
