require 'net/http'

test_name "puppet retrieves a static classification"

# TODO Reset classifier state

testdir = master.tmpdir('test_classifies')

create_remote_file(master, "#{testdir}/site.pp", <<-PP)
class foo {
  notify { "classified as foo": }
}
PP

Net::HTTP.start(database['ip'], CLASSIFIER_PORT) do |http|
  group_body = {"classes" => ["foo"]}.to_json
  group_request = Net::HTTP::Put.new("/v1/groups/foogroup")
  group_request.body = group_body

  group_response = http.request(group_request)
  assert(group_response.is_a?(Net::HTTPSuccess), "Received failure response when trying to create the group: HTTP Code #{group_response.code}: #{group_response.message}")

  rule_body = {
    "when" => ["=", "name", agent_nodename],
    "groups" => ["foogroup"]
  }.to_json
  rule_request = Net::HTTP::Put.new("/v1/rules")
  rule_request.body = rule_body

  rule_response = http.request(rule_request)
  assert(rule_response.is_a?(Net::HTTPSuccess), "Received failure response when trying to create the rule: HTTP Code #{rule_response.code}: #{rule_response.message}")
end

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

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}")

    assert_match("classified as foo", stdout)
  end
end
