test_name "classifier handles changes to puppet classes properly"

class Classifier
  include HTTParty
  debug_output $stdout
end

Classifier.base_uri "#{database.reachable_name}:#{CLASSIFIER_PORT}"


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
    'modulepath' => "#{testdir}/modules",
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

  group = {"classes" => {"referred" => {},
                         "changed" => {"referred" => "82"}},
           "rule" => {"when" => ["=", "fact", "value"],
           "environment" => "one"}

  group_response = Classifier.put(
    "/v1/groups/referrer",
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
