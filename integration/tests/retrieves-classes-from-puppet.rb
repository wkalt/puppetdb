test_name "classifier gets class definition from puppet"

class Classifier
  include HTTParty
  debug_output $stdout
end

Classifier.base_uri "#{database.reachable_name}:#{CLASSIFIER_PORT}"


step "Create a manifest"

testdir = master.tmpdir('test_classifies')

on master, "mkdir -p #{testdir}/environments/one/manifests"
on master, "mkdir -p #{testdir}/environments/two/manifests"

create_remote_file(master, "#{testdir}/auth.conf", <<-AUTHCONF)
path /
method find, search
auth any
allow *
AUTHCONF

create_remote_file(master, "#{testdir}/environments/one/manifests/site.pp", <<-PP)
class noargs {
}

class args($a, $b=["1", "2"], $c) {
}
PP

create_remote_file(master, "#{testdir}/environments/two/manifests/site.pp", <<-PP)
class noargs {
}

class args($a, $b=["1", "2"], $c) {
}
PP

on master, "chown -R #{master['user']}:#{master['group']} #{testdir}"
on master, "chmod -R ug+rwX,o+rX #{testdir}"

master_opts = {
  'master' => {
    'node_terminus' => 'classifier',
    'environmentpath' => "#{testdir}/environments",
    'rest_authconfig' => "#{testdir}/auth.conf",
    'modulepath' => "#{testdir}/modules",
    'verbose' => true,
    'debug' => true,
    'trace' => true
  }
}

step "Trigger an update on the classifier"

with_puppet_running_on(master, master_opts, testdir) do
  Classifier.post("/v1/update-classes")

  step "Check classifier classes"

  classes = Classifier.get("/v1/classes")

  assert(classes.include?({"name" => "noargs",
                           "parameters" => {},
                           "environment" => "one"}))

  assert(classes.include?({"name" => "args",
                           "parameters" => {"a" => nil,
                                            "b" => '["1", "2"]',
                                            "c" => nil},
                           "environment" => "one"}))

  assert(classes.include?({"name" => "noargs",
                           "parameters" => {},
                           "environment" => "two"}))

  assert(classes.include?({"name" => "args",
                           "parameters" => {"a" => nil,
                                            "b" => '["1", "2"]',
                                            "c" => nil},
                           "environment" => "two"}))

end
