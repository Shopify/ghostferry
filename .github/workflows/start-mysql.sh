#!/bin/bash
set -xe

DOCKER_COMPOSE_VERSION=v2.2.3

sudo apt-get update
sudo apt-get install -y netcat-openbsd make gcc

sudo curl -o /usr/local/bin/docker-compose -L https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-`uname -s`-`uname -m`
sudo chmod +x /usr/local/bin/docker-compose

if [ "$MYSQL_VERSION" == "8.0" ]; then
  docker-compose -f docker-compose_8.0.yml up -d mysql-1 mysql-2
else
  docker-compose up -d mysql-1 mysql-2
fi

MAX_ATTEMPTS=60

function wait_for_version () {
  attempts=0
  until docker exec -t $1 mysql -N -s -u root -e "select @@version"; do
    sleep 1
    attempts=$((attempts + 1))
    if (( attempts > $MAX_ATTEMPTS )); then
      echo "ERROR: $1 was not started." >&2
    exit 1
    fi
  done
}

wait_for_configuration () {
  attempts=0
  # we do need to see the "root@%" user configured, so wait for that
  until mysql --port $1 --protocol tcp --skip-password -N -s -u root -e "select host from mysql.user where user = 'root';" 2>/dev/null | grep -q '%'; do
    sleep 1
    attempts=$((attempts + 1))
    if (( attempts > $MAX_ATTEMPTS )); then
      echo "ERROR: $1 was not started." >&2
    exit 1
    fi
  done
}

wait_for_version "ghostferry-mysql-1-1"
wait_for_version "ghostferry-mysql-2-1"

wait_for_configuration 29291
wait_for_configuration 29292
