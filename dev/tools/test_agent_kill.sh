#!/bin/bash

#
# test_agent_kill.sh: periodically kills the agent (used for stress testing).
#
while sleep $(( RANDOM % 30 + 270 )); do
	date
	svcadm restart marlin-agent
done
