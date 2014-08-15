step "Configure the classifier's conf file" do
  cert = on(classifier, 'puppet agent --configprint hostcert').stdout.strip
  key = on(classifier, 'puppet agent --configprint hostprivkey').stdout.strip
  cacert = on(classifier, 'puppet agent --configprint localcacert').stdout.strip
  if classifier == master
    postgres_host = on(classifier, 'facter ipaddress').stdout.strip
  else
    postgres_host = "localhost"
  end

  conf = {}

  conf['webserver'] = {
    'classifier' => {
      'default-server' => true,
      'host' => '0.0.0.0',
      'port' => CLASSIFIER_PORT,
      'ssl-host' => '0.0.0.0',
      'ssl-port' => CLASSIFIER_SSL_PORT,
      'ssl-cert' => cert,
      'ssl-key' => key,
      'ssl-ca-cert' => cacert
    }
  }

  conf['classifier'] = {
    'url-prefix' => '',
    'puppet-master' => "https://#{master}:8140",
    'database' => {
      'subname' => "//#{postgres_host}:5432/pe-classifier",
      'user' => 'pe-classifier',
      'password' => 'classifier'
    }
  }
  set_classifier_configuration(classifier, conf)

  on(classifier, "usermod -G pe-puppet pe-classifier")
end

step "Start classifier" do
  start_classifier(classifier)
end
