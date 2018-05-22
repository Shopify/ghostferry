#!/bin/bash

set -xe

docker-compose up -d mysql-1 mysql-2

# We need a way to check if the mysql servers have booted or not before running
# the tests and this way is slightly faster than installing mysql-client

wait_for_mysql() {
  port=$1
  echo "Waiting for MySQL at port $port..."
  attempts=0
  while ! nc -w 1 localhost $port | grep -q "mysql"; do
    sleep 1
    attempts=$((attempts + 1))
    if (( attempts > 60 )); then
      echo "ERROR: mysql $port was not started." >&2
      exit 1
    fi
  done
  echo "MySQL at port $port has started!"
}

wait_for_mysql 29291
wait_for_mysql 29292
