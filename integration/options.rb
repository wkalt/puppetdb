integration_root = File.expand_path(File.dirname(options_file_path))

{
  :helper => [File.join(integration_root, 'helper.rb')],
  :pre_suite => [File.join(integration_root, 'setup', 'early'),
                 File.join(integration_root, 'setup', 'pre_suite')],
  :classifier_repo_puppet => "https://github.com/puppetlabs/puppet.git",
  :classifier_repo_hiera => "https://github.com/puppetlabs/hiera.git",
  :classifier_repo_facter => "https://github.com/puppetlabs/facter.git",
}
