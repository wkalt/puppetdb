require 'uuidtools'

test_name "puppet retrieves a static classification"

# disable class sync
conf = {'classifier' => {'synchronization-period' => 0}}
write_conf_file(classifier, "sync.conf", conf)
clear_and_restart_classifier(classifier)

testdir = master.tmpdir('test_classifies')

on master, "mkdir -p #{testdir}/environments/production/manifests"
create_remote_file(master, "#{testdir}/environments/production/manifests/site.pp", <<-PP)
class foo {
  notify { "classified as foo": }
}
PP

on master, "chown -R #{master['user']}:#{master['group']} #{testdir}"
on master, "chmod -R ug+rwX,o+rX #{testdir}"

master_opts = {
  'master' => {
    'node_terminus' => 'classifier',
    'environmentpath' => "#{testdir}/environments",
    'basemodulepath' => "#{testdir}/modules",
    'modulepath' => "#{testdir}/modules",
    'verbose' => true,
    'debug' => true,
    'trace' => true
  }
}

step "Create class"

class_response = Classifier.put(
  "/v1/environments/production/classes/foo",
  :body => {"parameters" => {}}.to_json)
assert(class_response.response.is_a?(Net::HTTPSuccess),
       "Received failure response when trying to create the class: " +
       "HTTP Code #{class_response.code}: #{class_response.message}")

node_names = nil

step "Collect node names"

with_puppet_running_on(master, master_opts, testdir) do
  node_names = agents.collect do |agent|
    on(agent, 'puppet agent --configprint node_name_value').stdout.strip
  end
end

match_nodes = ["or", *node_names.map{|nn| ["=", "name", nn]}]

step "Create group"

RootUUID = "00000000-0000-4000-8000-000000000000"
group_uuid = UUIDTools::UUID.random_create()
group = {
  "name" => "foogroup",
  "classes" => {"foo" => {}},
  "parent" => RootUUID.to_str,
  "rule" => match_nodes
}

group_response = Classifier.put("/v1/groups/#{group_uuid.to_str}",
                                :body => group.to_json)
assert(group_response.response.is_a?(Net::HTTPSuccess),
       "Received failure response when trying to create the group: " +
       "HTTP Code #{group_response.code}: #{group_response.message}")

step "Run puppet"

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}")

    assert_match("classified as foo", stdout)
  end
end
