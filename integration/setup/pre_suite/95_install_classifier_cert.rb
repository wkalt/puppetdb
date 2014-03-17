step "Configure SSL on classifier" do
  cert = on(database, 'puppet agent --configprint hostcert').stdout.strip
  key = on(database, 'puppet agent --configprint hostprivkey').stdout.strip
  cacert = on(master, 'puppet agent --configprint localcacert').stdout.strip

  ssldir = "/etc/classifier/ssl"

  on(database, "cp #{cert} #{ssldir}/cert.pem")
  on(database, "cp #{key} #{ssldir}/key.pem")
  on(database, "cp #{cacert} #{ssldir}/ca.pem")
  on(database, "chown -R classifier:classifier #{ssldir}")
  on(database, "chmod 600 #{ssldir}/*")

  conf = IniFile.new
  conf['webserver'] = {
    'host' => '0.0.0.0',
    'port' => CLASSIFIER_PORT,
    'ssl-host' => '0.0.0.0',
    'ssl-port' => CLASSIFIER_SSL_PORT,
    'ssl-cert' => "#{ssldir}/cert.pem",
    'ssl-key' => "#{ssldir}/key.pem",
    'ssl-ca-cert' => "#{ssldir}/ca.pem"
  }

  conf['classifier'] = {
    'url-prefix' => '',
    'puppet-master' => "https://#{master}:8140"
  }

  create_remote_file(database, '/etc/classifier/classifier.ini', conf.to_s)
end

step "Start classifier" do
  start_classifier(database)
end
