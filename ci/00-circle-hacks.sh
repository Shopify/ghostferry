#!/bin/bash

export DEBIAN_FRONTEND=noninteractive
export DEBIAN_PRIORITY=critical

# the key for this repo was expired at the time of this code
sudo -E rm -f /etc/apt/sources.list.d/mysql.list

sudo -E cp ci/files/circle-percona-apt-pin /etc/apt/preferences.d/percona-apt-pin-1001

# circle has some custom build of mysql that ends up conflicting with percona
sudo -E apt-get purge -y mysql-community-server libmysqld-dev libmysqlclient-dev
sudo -E apt-get --purge -y autoremove
