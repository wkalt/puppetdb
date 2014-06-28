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

on master, "mkdir -p #{testdir}/environments/staging/manifests"
on master, "mkdir -p #{testdir}/environments/production/manifests"
on master, "mkdir -p #{testdir}/modules"

create_remote_file(master, "#{testdir}/auth.conf", <<-AUTHCONF)
path /
method find, search
auth any
allow *
AUTHCONF

staging_body = <<-BODY
notify {"staging notification message: food is ${food}, drink is ${drink}" :}
BODY

production_body = <<-BODY
notify {"production notification message: car is ${car}, bike is ${bike}" :}
BODY

staging_site_pp = <<-PP
#{make_class("staging_class", parameters={"food" => "pizza", "drink" => "beer"}, body=staging_body)}
#{make_class("root_empty_class")}
PP

production_site_pp = <<-PP
#{make_class("production_class", parameters={"car" => "chevy", "bike" => "schwinn"}, body=production_body)}
#{make_class("root_empty_class")}
PP

create_remote_file(master, "#{testdir}/environments/staging/manifests/site.pp", staging_site_pp)

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

step "create a group for the staging environment"

parent = create_group(options = {'environment' => 'staging', 'rule' => match_nodes})
groups.push(parent)
verify_groups(groups)

step "Check initial classes"

initial = Classifier.get("/v1/environments/staging/classes")

assert(initial.include?({"name" => "staging_class",
                         "parameters" => {'food' => 'pizza', 'drink' => 'beer'},
                         "environment" => "staging"}),
      "Missing or wrong initial 'staging_class'")

step "add staging class to the parent group with parameters  "

update_group(parent, {"classes" => {"staging_class" => {'food' => 'burger', 'drink' => 'beer' }}})  

step "add child of the parent"

child = create_group(options = {"parent" => "#{parent['id']}", 
                                "environment" => 'staging', 
                                'rule' => match_nodes})
groups.push(child)
verify_groups(groups)

step "verify the child inherits from the parent"

verify_inheritance(child)

step "run puppet on the agent nodes and verify the class is applied"

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    stdout = run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}").stdout
    assert_match(/staging notification message: food is burger, drink is beer/, stdout)
  end
end

step "remove a parameter from the staging class"

updated_staging_site_pp = <<-PP
#{make_class("staging_class", parameters={"food" => "pizza"}, body=staging_body)}
#{make_class("root_empty_class")}
PP
create_remote_file(master, "#{testdir}/environments/staging/manifests/site.pp", updated_staging_site_pp)

step "update the classes on the classifier"

with_puppet_running_on(master, master_opts, testdir) do 
  Classifier.post("/v1/update-classes")
end

step "update the model with the deleted class parameter"
update_deleted_classes(parent, ['staging_class','drink'])

verify_groups(groups)

verify_inheritance(child)

step "verify that trying to run puppet returns a 400"

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    stderr = run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}").stderr
    assert_match(/Error 400 on SERVER/, stderr)
  end
end

step "Delete the class parameter so that puppet runs successfully"
update_group(parent, {"classes" => {"staging_class" => {"drink" => nil}}})

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    stdout = run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}").stdout
    assert_match(/staging notification message: food is burger/, stdout)
  end
end

step "change the parent group's rule so it no longer applies to the node"
update_group(parent, {"classes" => {'staging_class' => nil}})
update_group(parent, {'rule' => RandomRule.generate})

verify_inheritance(child)
verify_groups(groups)

#step "run puppet to ensure the node is no longer classified as staging"

#with_puppet_running_on(master, master_opts, testdir) do
#  agents.each do |agent|
#    stdout = run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}").stdout
#    assert_no_match(/staging notification message/, stdout)
#  end
#end

step "Change the child environment to production"
update_group(child, {'environment' => 'production',
                     'rule' => match_nodes,
                     'classes' => {'production_class' => {"car" => "subaru", "bike" => "litespeed"}}
                    })

with_puppet_running_on(master, master_opts, testdir) do
  agents.each do |agent|
    stdout = run_agent_on(agent, "--no-daemonize --onetime --verbose --server #{master}").stdout
    assert_match(/production notification message: car is subaru, bike is litespeed/, stdout)
  end
end

verify_inheritance(child)
verify_groups(groups)

