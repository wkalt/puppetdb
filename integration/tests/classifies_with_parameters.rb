require 'httparty'

test_name "puppet understands parameters from classification"

testdir = master.tmpdir('test_fields')

on master, "mkdir -p #{testdir}/environments/production/manifests"

create_remote_file(master, "#{testdir}/environments/production/manifests/site.pp", <<-PP)
class param_test($a) {
  notify {"Parameter a: $a": }
}

notify {"Hello: $hello": }
PP

create_remote_file(master, "#{testdir}/auth.conf", <<-AUTHCONF)
path /
method find, search
auth any
allow *
AUTHCONF

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

class Classifier
  include HTTParty
  debug_output($stdout)
  headers({'Content-Type' => 'application/json'})
end

Classifier.base_uri("#{database.reachable_name}:#{CLASSIFIER_PORT}")


step "Collect node names"

node_names = nil
with_puppet_running_on(master, master_opts, testdir) do
  node_names = agents.collect do |agent|
    on(agent, 'puppet agent --configprint node_name_value').stdout.strip
  end
end

match_nodes = ["or", *node_names.map{|nn| ["=", "name", nn]}]


group = {"classes" => {"param_test" => {"a" => "parameterized"}},
         "parent" => "default",
         "variables" => {"hello" => "goodbye"},
         "rule" => {"when" => match_nodes}}

step "Run puppet"

with_puppet_running_on(master, master_opts, testdir) do

  step "Update classes from puppet"

  Classifier.post("/v1/update-classes")

  step "Create group"

  Classifier.get("/v1/environments/production/classes")
  group_response = Classifier.put(
    "/v1/groups/paramgroup",
    :body => group.to_json)
  assert(group_response.response.is_a?(Net::HTTPSuccess),
         "Received failure response when trying to create the group: " +
         "HTTP Code #{group_response.code}: #{group_response.message}")

  step "Classify the agent nodes"

  agents.each do |agent|
    run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}")

    assert_match("Notice: Hello: goodbye", stdout)
    assert_match("Notice: Parameter a: parameterized", stdout)
  end
end
