require File.expand_path(File.dirname(__FILE__) + '/../nc_service_helpers.rb')

Classifier.base_uri("#{classifier.reachable_name}:#{CLASSIFIER_PORT}")

step "set up master denying requests"

testdir = master.tmpdir('puppet_network_errors')

create_remote_file(master, "#{testdir}/auth.conf", <<-AUTHCONF)
path /

AUTHCONF

on master, "chown -R #{master['user']}:#{master['group']} #{testdir}"
on master, "chmod -R ug+rwX,o+rX #{testdir}"

master_opts = {
  'master' => {
    'environmentpath' => "#{testdir}/environments",
    'rest_authconfig' => "#{testdir}/auth.conf",
    'verbose' => true,
    'debug' => true,
    'trace' => true
  }
}

step "Trigger an update on the classifier"

with_puppet_running_on(master, master_opts, testdir) do
  response = Classifier.post("/v1/update-classes")

  assert(response.code == 500,
         "Error with response code, expected 500, got #{response.code}")
  assert(response["kind"] == "unexpected-response",
         "Expected 'unexpected-response' error, received #{response["kind"]}")
  assert(response["details"]["status"] == 403,
         "Expected 403 upstream server status, received #{response["details"]["status"]}")
  assert(response["msg"] =~ /^Received an unexpected 403 status response while trying to access/,
         "Expected error message 'Received an unexpected...', received #{response["msg"]}")
end
