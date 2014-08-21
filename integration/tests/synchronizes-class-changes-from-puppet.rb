require 'uuidtools'

test_name "classifier handles changes to puppet classes properly"

clear_and_restart_classifier(classifier)

step "Create a manifest"

testdir = master.tmpdir('test_synchronizes_changes')

on master, "mkdir -p #{testdir}/environments/one/manifests"

create_remote_file(master, "#{testdir}/auth.conf", <<-AUTHCONF)
path /
method find, search
auth any
allow *
AUTHCONF

create_remote_file(master, "#{testdir}/environments/one/manifests/site.pp", <<-PP)
class changed($changed="1", $unreferred="5", $referred="6") {
}

class referred {
}

class unreferred($a="b") {
}
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
  Classifier.post("/v1/update-classes")

  step "Check initial classes"

  initial = Classifier.get("/v1/environments/one/classes")

  assert(initial.include?({"name" => "changed",
                           "parameters" => {"changed" => '"1"',
                                            "unreferred" => '"5"',
                                            "referred" => '"6"'},
                           "environment" => "one"}),
        "Missing or wrong initial 'changed' class")

  assert(initial.include?({"name" => "referred",
                           "parameters" => {},
                           "environment" => "one"}),
        "Missing or wrong initial 'referred' class")

  assert(initial.include?({"name" => "unreferred",
                           "parameters" => {"a" => '"b"'},
                           "environment" => "one"}),
        "Missing or wrong initial 'unreferred' class")

  step "Create a group"

  RootUUID = "00000000-0000-4000-8000-000000000000"
  group_uuid = UUIDTools::UUID.random_create()

  group = {
    "name" => "referrer",
    "environment" => "one",
    "parent" => RootUUID,
    "classes" => { "referred" => {}, "changed" => {"referred" => "82"} },
    "rule" => ["=", "fact", "value"]
  }

  group_response = Classifier.put("/v1/groups/#{group_uuid}",
                                  :body => group.to_json)

  assert(group_response.response.is_a?(Net::HTTPSuccess),
         "Received failure response when trying to create the group: " +
         "HTTP Code #{group_response.code}: #{group_response.message}")


  step "Change classes"

  create_remote_file(master, "#{testdir}/environments/one/manifests/site.pp", <<-PP)
  class changed($changed="2", $added="five") {
  }

  class added {
  }
  PP

  Classifier.post("/v1/update-classes")

  step "Check classes"

  updated = Classifier.get("/v1/environments/one/classes")

  assert(updated.include?({"name" => "changed",
                           "parameters" => {"changed" => '"2"',
                                            "added" => '"five"'},
                           "environment" => "one"}),
        "Missing or wrong 'changed' class after update")

  assert(updated.include?({"name" => "added",
                           "parameters" => {},
                           "environment" => "one"}),
        "Missing or wrong 'added' class after update")

  step "Add back deleted classes"

  create_remote_file(master, "#{testdir}/environments/one/manifests/site.pp", <<-PP)
  class changed($changed="1", $unreferred="5", $referred="6") {
  }

  class referred {
  }

  class unreferred($a="b") {
  }
  PP

  Classifier.post("/v1/update-classes")

  step "Check final classes"

  final = Classifier.get("/v1/environments/one/classes")

  assert(final.include?({"name" => "changed",
                         "parameters" => {"changed" => '"1"',
                                          "unreferred" => '"5"',
                                          "referred" => '"6"'},
                         "environment" => "one"}),
        "Missing or wrong final 'changed' class")

  assert(final.include?({"name" => "referred",
                         "parameters" => {},
                         "environment" => "one"}),
        "Missing or wrong final 'referred' class")

  assert(final.include?({"name" => "unreferred",
                         "parameters" => {"a" => '"b"'},
                         "environment" => "one"}),
        "Missing or wrong final 'unreferred' class")
end
