require 'json'

step "Configure SSL on classifier" do
  cert = on(database, 'puppet agent --configprint hostcert').stdout.strip
  key = on(database, 'puppet agent --configprint hostprivkey').stdout.strip
  cacert = on(database, 'puppet agent --configprint localcacert').stdout.strip

  ssldir = "/etc/classifier/ssl"

  on(database, "mkdir -p #{ssldir}")
  on(database, "cp #{cert} #{ssldir}/cert.pem")
  on(database, "cp #{key} #{ssldir}/key.pem")
  on(database, "cp #{cacert} #{ssldir}/ca.pem")
  on(database, "chown -R classifier:classifier #{ssldir}")
  on(database, "chmod 600 #{ssldir}/*")

  conf = {}
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

  create_remote_file(database, '/etc/classifier/conf.d/classifier.conf', conf.to_json)
  on(database, "chmod 644 /etc/classifier/conf.d/classifier.conf")
end


step "Add fqdn of the classifier to the master's host file" do
  fqdn = on(database, 'facter fqdn').stdout.strip
  
  manifest = ''
  manifest << <<-EOS
  host {'#{fqdn}':
    ensure => present,
    ip => '#{database.ip}',
    target => '/etc/hosts',
  } 
  EOS

  apply_manifest_on(master, manifest)
end


step "Start classifier" do
  start_classifier(database)
end
