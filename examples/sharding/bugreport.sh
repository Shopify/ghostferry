#!/bin/bash

set -e

docker-compose up -d

mysql -h 127.0.0.1 -u root -P 29291 -e "drop database abc"
mysql -h 127.0.0.1 -u root -P 29292 -e "drop database abc"

mysql -h 127.0.0.1 -u root -P 29291 -e "create database abc"
mysql -h 127.0.0.1 -u root -P 29292 -e "create database abc"

mysql -h 127.0.0.1 -u root -P 29291 -e 'CREATE TABLE `abc`.`t` (   `tenant_id` bigint(20) NOT NULL,   `col1` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,   `id` bigint(20) NOT NULL AUTO_INCREMENT,   `d` datetime(6) NOT NULL,   PRIMARY KEY (`tenant_id`,`col1`), KEY `id` (`id`) )'
mysql -h 127.0.0.1 -u root -P 29292 -e 'CREATE TABLE `abc`.`t` (   `tenant_id` bigint(20) NOT NULL,   `col1` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,   `id` bigint(20) NOT NULL AUTO_INCREMENT,   `d` datetime(6) NOT NULL,   PRIMARY KEY (`tenant_id`,`col1`), KEY `id` (`id`) )'

mysql -h 127.0.0.1 -u root -P 29291 -e "insert into abc.t values (1, 'z', null, now())"
mysql -h 127.0.0.1 -u root -P 29291 -e "insert into abc.t values (1, 'a', null, now())"

echo "Here's a composite PK table"
mysql -h 127.0.0.1 -u root -P 29291 -e "show create table abc.t\G"
read -p "Press enter to continue..."
echo

echo "It has two rows in it. We can SELECT * FROM abc.t:"
mysql -h 127.0.0.1 -u root -P 29291 -e "select * from abc.t"
echo "Notice how the id column are in reverse order in the above case, likely because the order is in PK order, which is composite on (tenant_id, col1)."

read -p "Press enter to run ghostferry..."
echo

set +e

ruby c.rb >/dev/null &
child_process=$!
ghostferry-sharding -config-path ./docker-compose-conf.json

kill $child_process
wait $child_process

echo "Ghostferry failed, with a verifier error, let's take a look at what the table looks like"
read -p "Press enter to continue"

set -e

echo "Source database:"
mysql -u root -h 127.0.0.1 -P 29291 -e "select * from abc.t"

echo "Target database:"
mysql -u root -h 127.0.0.1 -P 29292 -e "select * from abc.t"

echo "This is a major amount of corruption..."
