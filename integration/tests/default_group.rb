test_name "a fresh install of nc classifies into default group"

clear_and_restart_classifier(classifier)

testdir = master.tmpdir('test_default')

on master, "mkdir -p #{testdir}/environments/production/manifests"
create_remote_file(master, "#{testdir}/environments/production/manifests/site.pp", <<-PP)
notify { $default_group: }
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

RootUUID = "00000000-0000-4000-8000-000000000000"
group_response = Classifier.get("/v1/groups/#{RootUUID}")
assert(group_response.response.is_a?(Net::HTTPSuccess),
       "Failed to retrieve the default group.")

update_response = Classifier.post(
  "/v1/groups/#{RootUUID}",
  :body => {"variables" => {"default_group" => "Classified as default"}}.to_json)
assert(update_response.response.is_a?(Net::HTTPSuccess),
       "Failed to update the default group.")

step "Run puppet"

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}")

    assert_match("Classified as default", stdout)
  end
end
