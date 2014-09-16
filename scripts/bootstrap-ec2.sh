#!/usr/bin/env bash
wget $1 --no-check-certificate -O pe.tar.gz
tar -xzvf pe.tar.gz
ls
cd `ls -d puppet*`
yes | sudo ./puppet-enterprise-installer -A ./answers/all-in-one.answers.txt

# Install required modules
sudo puppet apply /tmp/manifests/modules/packer/manifests/sshd.pp
sudo puppet apply /tmp/manifests/modules/packer/manifests/networking.pp
sudo puppet apply /tmp/manifests/modules/packer/manifests/git.pp

sudo ./puppet-enterprise-uninstaller -y
cd ..
rm -rf `ls -d puppet*`
