require File.expand_path(File.dirname(__FILE__) + '/../nc_service_helpers.rb')

Classifier.base_uri("#{database.reachable_name}:#{CLASSIFIER_PORT}")


clear_and_restart_classifier(database)

step "verify the classifier doesn't fail when no classes specified"


step "Create a manifest"

testdir = master.tmpdir('no_classes_specified')

on master, "mkdir -p #{testdir}/environments/one/manifests"

create_remote_file(master, "#{testdir}/auth.conf", <<-AUTHCONF)
path /
method find, search
auth any
allow *
AUTHCONF

create_remote_file(master, "#{testdir}/environments/one/manifests/site.pp", <<-PP)
notify {"whee":}
PP

on master, "chown -R #{master['user']}:#{master['group']} #{testdir}"
on master, "chmod -R ug+rwX,o+rX #{testdir}"


master_opts = {
  'master' => {
    'node_terminus' => 'classifier',
    'environmentpath' => "#{testdir}/environments",
    'rest_authconfig' => "#{testdir}/auth.conf",
    'basemodulepath' => "#{testdir}/modules",
    'modulepath' => "#{testdir}/modules",
    'environment_timeout' => 0,
    'verbose' => true,
    'debug' => true,
    'trace' => true
  }
}

step "Trigger an update on the classifier"

with_puppet_running_on(master, master_opts, testdir) do
  response = Classifier.post("/v1/update-classes")

  assert(response.code == 201,
         "Error with response code, expected 201, got #{response.code}")
  step "Check initial classes again"
  Classifier.post("/v1/update-classes")

end

