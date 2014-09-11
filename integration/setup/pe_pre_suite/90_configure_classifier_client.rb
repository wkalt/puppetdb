require 'tempfile'

Classifier.base_uri("https://#{classifier.reachable_name}:#{CLASSIFIER_SSL_PORT}#{classifier_prefix(classifier)}")

ca_cert = on(master, "cat `puppet agent --configprint localcacert`").stdout
cert = on(master, "cat `puppet agent --configprint hostcert`").stdout
key = on(master, "cat `puppet agent --configprint hostprivkey`").stdout

cert_dir = Dir.mktmpdir("pe_classifier_certs")

ca_cert_file = File.join(cert_dir, "cacert.pem")
File.open(ca_cert_file, "w+") do |f|
  f.write(ca_cert)
end

Classifier.pem(cert+key)

Classifier.ssl_ca_file(ca_cert_file)
