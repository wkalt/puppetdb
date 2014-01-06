require 'httparty'

test_name "puppet retrieves a static classification"

# TODO Reset classifier state

testdir = master.tmpdir('test_classifies')

create_remote_file(master, "#{testdir}/site.pp", <<-PP)
class foo {
  notify { "classified as foo": }
}
PP

on master, "chown -R #{master['user']}:#{master['group']} #{testdir}"
on master, "chmod -R ug+rwX,o+rX #{testdir}"

master_opts = {
  'master' => {
    'node_terminus' => 'classifier',
    'manifest' => "#{testdir}/site.pp",
    'verbose' => true,
    'debug' => true,
    'trace' => true
  }
}

class Classifier
  include HTTParty
  debug_output $stdout
end

Classifier.base_uri "#{database.reachable_name}:#{CLASSIFIER_PORT}"

class_response = Classifier.put(
  "/v1/classes/foo",
  :body => {"parameters" => {}}.to_json)
assert(class_response.response.is_a?(Net::HTTPSuccess),
       "Received failure response when trying to create the class: " +
       "HTTP Code #{class_response.code}: #{class_response.message}")

group_response = Classifier.put(
  "/v1/groups/foogroup",
  :body => {"classes" => ["foo"]}.to_json)
assert(group_response.response.is_a?(Net::HTTPSuccess),
       "Received failure response when trying to create the group: " +
       "HTTP Code #{group_response.code}: #{group_response.message}")

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    node_name = on(agent, 'puppet agent --configprint node_name_value').stdout.strip

    rule_response = Classifier.post(
      "/v1/rules",
      :body => {
        "when" => ["=", "name", node_name],
        "groups" => ["foogroup"]
      }.to_json)

    assert(rule_response.response.is_a?(Net::HTTPSuccess),
           "Received failure response when trying to create the rule: " +
           "HTTP Code #{rule_response.code}: #{rule_response.message}")

    run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}")

    assert_match("classified as foo", stdout)
  end
end
