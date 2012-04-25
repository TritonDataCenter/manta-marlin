#!/bin/bash

echo "hello there from inside a task: $1" | \
    curl -s -X PUT -T- localhost:8080/object/task$$
