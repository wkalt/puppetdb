require 'json'

step "Configure SSL on classifier" do
  cert = on(classifier, 'puppet agent --configprint hostcert').stdout.strip
  key = on(classifier, 'puppet agent --configprint hostprivkey').stdout.strip
  cacert = on(classifier, 'puppet agent --configprint localcacert').stdout.strip

  ssldir = "/etc/classifier/ssl"

  on(classifier, "mkdir -p #{ssldir}")
  on(classifier, "cp #{cert} #{ssldir}/cert.pem")
  on(classifier, "cp #{key} #{ssldir}/key.pem")
  on(classifier, "cp #{cacert} #{ssldir}/ca.pem")
  on(classifier, "chown -R classifier:classifier #{ssldir}")
  on(classifier, "chmod 600 #{ssldir}/*")

  conf = {}
  conf['webserver'] = {
    'classifier' => {
      'default-server' => true,
      'host' => '0.0.0.0',
      'port' => CLASSIFIER_PORT,
      'ssl-host' => '0.0.0.0',
      'ssl-port' => CLASSIFIER_SSL_PORT,
      'ssl-cert' => "#{ssldir}/cert.pem",
      'ssl-key' => "#{ssldir}/key.pem",
      'ssl-ca-cert' => "#{ssldir}/ca.pem"
    }
  }

  conf['classifier'] = {
    'puppet-master' => "https://#{master}:8140",

    'ssl-cert' => "#{ssldir}/cert.pem",
    'ssl-key' => "#{ssldir}/key.pem",
    'ssl-ca-cert' => "#{ssldir}/ca.pem",
    'access-control' => false
  }

  conf['web-router-service'] = {
    'puppetlabs.classifier.main/classifier-service' => ''
  }

  set_classifier_configuration(classifier, conf)
end

step "Start classifier" do
  start_classifier(classifier)
end
