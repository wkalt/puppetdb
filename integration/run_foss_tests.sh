#!/bin/sh

if [ -z $1 ]; then
  echo "usage: $0 <classifier-version>" 1>&2
  exit 22
fi

REF=$1 CLASSIFIER_INSTALL_TYPE=package bundle exec beaker --debug --no-color --type git --preserve-hosts onfail --keyfile ~/.ssh/id_rsa-acceptance --options-file integration/options.rb --config integration/config/vcloud/fedora20.cfg --tests integration/tests
