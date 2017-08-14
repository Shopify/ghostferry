#!/bin/bash

N1=gf.n1
N2=gf.n2

# TODO: at somepoint we can open source mysql-vending-machine so we don't
#       have to do this hack and just clone from https
rm -rf /tmp/mvm

git clone git@github.com:Shopify/mysql-vending-machine.git /tmp/mvm
pushd /tmp/mvm
  gem build mysql_vending_machine.gemspec
  sudo -E gem install mysql_vending_machine*.gem
popd

rm -rf /tmp/mvm

mvm create $N1 $N2
mvm adduser -u ghostferry -p ghostferry -h '%' $N1 $N2

mvm sql $N1 "CREATE DATABASE abc"
mvm sql $N1 "CREATE TABLE abc.schema_migrations (id bigint(20) AUTO_INCREMENT, data varchar(16), primary key(id))"
mvm sql $N1 "CREATE TABLE abc.table1 (id bigint(20) AUTO_INCREMENT, data varchar(16), primary key(id))"
mvm sql $N1 "CREATE TABLE abc.table2 (id bigint(20) AUTO_INCREMENT, data TEXT, primary key(id))"

rm -f /tmp/n1create.sql

for i in `seq 1 350`; do
  echo "INSERT INTO abc.schema_migrations (id, data) VALUES (${i}, '$(cat /dev/urandom | tr -cd 'a-z0-9' | head -c 16)');" >> /tmp/n1create.sql
  echo "INSERT INTO abc.table1 (id, data) VALUES (${i}, '$(cat /dev/urandom | tr -cd 'a-z0-9' | head -c 16)');" >> /tmp/n1create.sql
  echo "INSERT INTO abc.table2 (id, data) VALUES (${i}, '$(cat /dev/urandom | tr -cd 'a-z0-9' | head -c 16)');" >> /tmp/n1create.sql
done

cat /tmp/n1create.sql | mvm console $N1

rm -f /tmp/n1create.sql
