#!/usr/bin/env bash
cd `ls -d puppet*`
yes | sudo ./puppet-enterprise-installer -A ./answers/all-in-one.answers.txt

# Install required modules

for i in "$@"
do
  puppet module install $i --modulepath=/tmp/manifests/modules >/dev/null 2>&1
done

puppet apply /tmp/manifests/ec2.pp --modulepath=/tmp/manifests/modules

sudo ./puppet-enterprise-uninstaller -y
cd ..
rm -rf `ls -d puppet*`
