class packer::git {
  package {'git':
    ensure => present,
    providor => 'gem',
    require => Package[ $ruby_package ],
  }
}
