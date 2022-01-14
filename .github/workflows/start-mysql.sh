#!/bin/bash
set -xe

DOCKER_COMPOSE_VERSION=1.29.2

sudo apt-get update
sudo apt-get install -y netcat-openbsd make gcc

sudo curl -o /usr/local/bin/docker-compose -L https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-`uname -s`-`uname -m`
sudo chmod +x /usr/local/bin/docker-compose

docker-compose up -d mysql-1 mysql-2

# We need a way to check if the mysql servers have booted or not before running
# the tests and this way is slightly faster than installing mysql-client

wait_for_mysql() {
  port=$1
  echo "Waiting for MySQL at port $port..."
  attempts=0
  while nc -vvw 1 localhost $port | grep -q "succeeded"; do
    sleep 1
    attempts=$((attempts + 1))
    if (( attempts > 60 )); then
      echo "ERROR: mysql $port was not started." >&2
      exit 1
    fi
  done
  echo "MySQL at port $port has started!"
}

wait_for_version () {
  instance=$1
  max_attempts=$2

  attempts=0
  until docker exec -t $instance mysql -u root -e "select @@version"; do
    sleep 1
    attempts=$((attempts + 1))
    if (( attempts > $max_attempts )); then
      echo "ERROR: $instance was not started." >&2
    exit 1
    fi
  done
}

wait_for_healthy () {
  instance=$1
  max_attempts=$2

  attempts=0
  while [ "`docker inspect $instance -f '{{json .State.Health.Status}}'`" != "\"healthy\"" ]; do
    sleep 1
    attempts=$((attempts + 1))
    if (( attempts > $max_attempts )); then
      echo "ERROR: $instance was not started." >&2
    exit 1
    fi
  done
}

wait_for_mysql 29291
wait_for_mysql 29292

wait_for_version "ghostferry_mysql-1_1" 60
wait_for_version "ghostferry_mysql-2_1" 60

wait_for_healthy "ghostferry_mysql-1_1" 60
wait_for_healthy "ghostferry_mysql-2_1" 60
