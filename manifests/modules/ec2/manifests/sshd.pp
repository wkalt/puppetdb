class packer::sshd {

   class { 'ssh::server':
     storeconfigs_enabled => false,
     options => {
       'PermitRootLogin'      => 'yes',
       'UseDNS'               => 'no',
       'GSSAPIAuthentication' => 'no',
       'User'                 => 'admin',
     },
   }

  file {'/etc/ssh/config'
    ensure => file,
    source => '/tmp/manifests/modules/ec2/manifests/files/ssh_config',
  }
}
