class ec2::sshd {

  class { 'ssh::server':
    storeconfigs_enabled => false,
    options => {
      'PermitRootLogin'      => 'yes',
      'UseDNS'               => 'no',
      'GSSAPIAuthentication' => 'no',
    },
  }

  class { 'ssh::client':
    storeconfigs_enabled => false,
    options => {
      'Host *' => {
        'User' => 'root',
      },
    },
  }
}
