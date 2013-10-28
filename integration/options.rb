integration_root = File.expand_path(File.dirname(options_file_path))

{
  :helper => [File.join(integration_root, 'helper.rb')],
  :pre_suite => [File.join(integration_root, 'setup', 'early'),
                 File.join(integration_root, 'setup', 'pre_suite')]
}
