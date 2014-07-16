require File.expand_path(File.dirname(__FILE__) + '/../nc_service_helpers.rb')

Classifier.base_uri("#{database.reachable_name}:#{CLASSIFIER_PORT}")

groups = []

step "clear classifier database"

clear_and_restart_classifier(database)

root_group = get_root_group
groups.push(root_group)
verify_groups(groups)

step "create a basic class and add it to the classifier"

testdir = master.tmpdir('test_synchronized_inheritance_changes')

on master, "mkdir -p #{testdir}/environments/production/manifests"
on master, "mkdir -p #{testdir}/modules"

create_remote_file(master, "#{testdir}/auth.conf", <<-AUTHCONF)
path /
method find, search
auth any
allow *
AUTHCONF

production_body = <<-BODY
notify {"car is ${car}, bike is ${bike}" :}
BODY


production_site_pp = <<-PP
#{make_class("production_class", parameters={"car" => "chevy", "bike" => "schwinn"}, body=production_body)}
#{make_class("root_empty_class")}
PP

create_remote_file(master, "#{testdir}/environments/production/manifests/site.pp", production_site_pp)

on master, "chown -R #{master['user']}:#{master['group']} #{testdir}"
on master, "chmod -R ug+rwX,o+rX #{testdir}"

master_opts = {
  'master' => {
    'node_terminus' => 'classifier',
    'environmentpath' => "#{testdir}/environments",
    'rest_authconfig' => "#{testdir}/auth.conf",
    'basemodulepath' => "#{testdir}/modules",
    'environment_timeout' => 0,
    'verbose' => true,
    'debug' => true,
    'trace' => true
  }
}

with_puppet_running_on(master, master_opts, testdir) do 
  response = Classifier.post("/v1/update-classes")
  assert(response.code == 201,
         "Unexpected response, got #{response.code} and body: #{response.body}")
end

step "create a rule to match the agent nodes"
node_names = agents.collect do |agent|
  on(agent, 'puppet agent --configprint node_name_value').stdout.strip
end

match_nodes = ["or", *node_names.map{|nn| ["=", "name", nn]}]

parent = create_group(options = {'environment' => 'production', 'rule' => match_nodes})
groups.push(parent)
verify_groups(groups)

step "verify the group does not have the class applied to it"

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    stdout = run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}").stdout
    assert_no_match(/chevy|schwinn/, stdout)
  end
end

step "add the production class to the parent group"

update_group(parent, {"classes" => {"production_class" => {}}})  

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    stdout = run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}").stdout
    assert_match("car is chevy, bike is schwinn", stdout)
  end
end

step "add parameters to the production class"

update_group(parent, {"classes" => {"production_class" => {'car' => 'bronco', 'bike' => 'fuji' }}})  

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    stdout = run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}").stdout
    assert_match("car is bronco, bike is fuji", stdout)
  end
end

step "add child of the parent"

child = create_group(options = {"parent" => "#{parent['id']}", 
                                "environment" => 'production', 
                                'rule' => match_nodes})
update_group(parent, {'rule' => RandomRule.generate})
groups.push(child)
verify_groups(groups)

step "verify the child inherits from the parent"

verify_inheritance(child)

step "run puppet on the agent nodes and verify the parent's class parameters are applied"

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    stdout = run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}").stdout
    assert_match("car is bronco, bike is fuji", stdout)
  end
end

step "add the inherited class without parameters"

update_group(child, { "classes" => { "production_class" => {}}})

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    stdout = run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}").stdout
    assert_match("car is bronco, bike is fuji", stdout)
  end
end

step "add the inherited class with a single parameter"

update_group(child, { "classes" => { "production_class" => { "bike" => "peugeot" }}})

verify_groups(groups)
verify_inheritance(child)

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    stdout = run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}").stdout
    assert_match("car is bronco, bike is peugeot", stdout)
  end
end

step "add a grandchild and verify it inherits from both the child and parent"

grandchild = create_group(options = {"parent" => "#{child['id']}", 
                                "environment" => 'production', 
                                'rule' => match_nodes,
                                'classes' => {"production_class" => {}}
                                })

update_group(child, {'rule' => RandomRule.generate})
groups.push(grandchild)
verify_groups(groups)
verify_inheritance(grandchild)

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    stdout = run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}").stdout
    assert_match("car is bronco, bike is peugeot", stdout)
  end
end
