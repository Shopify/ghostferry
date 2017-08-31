# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.provision "shell", inline: <<-SHELL
    set -e
    apt-get update
    apt-get install -y squid-deb-proxy-client

    if [ ! -f /usr/bin/gimme ]; then
      wget -O /usr/bin/gimme https://raw.githubusercontent.com/travis-ci/gimme/master/gimme
      chmod +x /usr/bin/gimme
      sudo -H -u vagrant gimme 1.8.3
      echo "source /home/vagrant/.gimme/envs/go1.8.3.env" >> /home/vagrant/.bashrc
      echo 'export GOPATH=$HOME/go' >> /home/vagrant/.bashrc
    fi

    mkdir -p /home/vagrant/.ssh
    ssh-keyscan github.com > /home/vagrant/.ssh/known_hosts
  SHELL

  config.vm.provision "shell", path: "ci/01-install-packages.sh"
  config.vm.provision "shell", path: "ci/02-setup-mysql.sh", privileged: false

  config.vm.synced_folder ".", "/vagrant", disabled: true
  config.vm.synced_folder ".", "/home/vagrant/go/src/github.com/Shopify/ghostferry"

  config.ssh.forward_agent = true
end
