#!/bin/bash

set -e
export DEBIAN_FRONTEND=noninteractive
export DEBIAN_PRIORITY=critical

mkdir -p /tmp/percona
pushd /tmp/percona
  wget https://repo.percona.com/apt/percona-release_0.1-4.$(lsb_release -sc)_all.deb
  sudo -E dpkg -i percona-release_0.1-4.$(lsb_release -sc)_all.deb
popd
rm -rf /tmp/percona

sudo -E apt-add-repository -y ppa:brightbox/ruby-ng
sudo -E apt-get update
# echo "percona-server-server-5.7 percona-server-server/root_password password password" | debconf-set-selections
# echo "percona-server-server-5.7 percona-server-server/root_password_again password password" | debconf-set-selections
sudo -E apt-get -y install \
  percona-server-common-5.7 \
  percona-server-client-5.7 \
  percona-server-server-5.7 \
  git \
  ruby2.4 ruby2.4-dev \
  build-essential

sudo -E service mysql stop
sudo -E update-rc.d mysql disable
