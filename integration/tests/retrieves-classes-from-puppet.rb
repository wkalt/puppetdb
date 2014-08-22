test_name "classifier gets class definition from puppet"

step "Create a manifest"

testdir = master.tmpdir('test_retrieves_classes')

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

  classes_one = Classifier.get("/v1/environments/one/classes")

  assert(classes_one.include?({"name" => "noargs",
                               "parameters" => {},
                               "environment" => "one"}))

  assert(classes_one.include?({"name" => "args",
                               "parameters" => {"a" => nil,
                                                "b" => '["1", "2"]',
                                                "c" => nil},
                               "environment" => "one"}))

  classes_two = Classifier.get("/v1/environments/two/classes")

  assert(classes_two.include?({"name" => "noargs",
                               "parameters" => {},
                               "environment" => "two"}))

  assert(classes_two.include?({"name" => "args",
                               "parameters" => {"a" => nil,
                                                "b" => '["1", "2"]',
                                                "c" => nil},
                               "environment" => "two"}))

end
