step "Configure the classifier's conf file" do
  cert = on(database, 'puppet agent --configprint hostcert').stdout.strip
  key = on(database, 'puppet agent --configprint hostprivkey').stdout.strip
  cacert = on(database, 'puppet agent --configprint localcacert').stdout.strip
  ipaddress = on(database, 'facter ipaddress').stdout.strip

  conf = {}

  conf['webserver'] = {
    'host' => '0.0.0.0',
    'port' => CLASSIFIER_PORT,
    'ssl-host' => '0.0.0.0',
    'ssl-port' => CLASSIFIER_SSL_PORT,
    'ssl-cert' => cert,
    'ssl-key' => key,
    'ssl-ca-cert' => cacert
  }

  conf['classifier'] = {
    'url-prefix' => '',
    'puppet-master' => "https://#{master}:8140"
  }
  conf['database'] = {
    'dbname' => "//#{ipaddress}:5432/pe-classifier",
    'user' => 'pe-classifier',
    'password' => 'classifier'
  }
  create_remote_file(database, '/etc/puppetlabs/classifier/conf.d/classifier.conf', conf.to_json)
  on(database, "chmod 644 /etc/puppetlabs/classifier/conf.d/classifier.conf")

  on(database, "usermod -G pe-puppet pe-classifier")
end

step "Start classifier" do
  start_classifier(database)
end
