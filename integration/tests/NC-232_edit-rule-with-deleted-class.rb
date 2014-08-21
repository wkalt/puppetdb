require File.expand_path(File.dirname(__FILE__) + '/../nc_service_helpers.rb')

#declare empty array of groups
#add to this group as necessary, send through verify_groups method to check
#current groups against server state
groups = []


step "create a basic class and add it to the classifier"

clear_and_restart_classifier(classifier)
testdir = master.tmpdir('test_synchronized_inheritance_changes')

on master, "mkdir -p #{testdir}/environments/one/manifests"
on master, "mkdir -p #{testdir}/environments/production/manifests"
on master, "mkdir -p #{testdir}/modules"

create_remote_file(master, "#{testdir}/auth.conf", <<-AUTHCONF)
path /
method find, search
auth any
allow *
AUTHCONF

one_body = <<-BODY
notify {"notification message" :}
BODY

one_site_pp = <<-PP
#{make_class("one_class", parameters={"key" => "default_value", "key2" => "default_value2"}, body=one_body)}
#{make_class("root_empty_class")}
PP

create_remote_file(master, "#{testdir}/environments/one/manifests/site.pp", one_site_pp)

create_remote_file(master, "#{testdir}/environments/production/manifests/site.pp", <<-PP)
#{make_class("root_empty_class", parameters = {}, body = 'notify {"I am a message":}')}
PP

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
  Classifier.post("/v1/update-classes")
end


step "create a rule to match the agent nodes"
node_names = agents.collect do |agent|
  on(agent, 'puppet agent --configprint node_name_value').stdout.strip
end
match_nodes = ["or", *node_names.map{|nn| ["=", "name", nn]}]


step "Check initial classes"
initial = Classifier.get("/v1/environments/one/classes")
assert(initial.include?({"name" => "one_class",
                         "parameters" => {'key' => 'default_value', 'key2' => 'default_value2'},
                         "environment" => "one"}),
      "Missing or wrong initial 'one_class'")


step "create a group for the environment under test"

root_group = get_root_group
groups.push(root_group)

parent = create_group(options = {'environment' => 'one',
                                 'rule' => match_nodes,
                                 'classes' => {'one_class' => {'key' => 'parent_value'}}})
groups.push(parent)


step "add child of the parent"

child = create_group(options = {"parent" => "#{parent['id']}",
                                "environment" => 'one',
                                'rule' => match_nodes,
                                'classes' => {'one_class' => {'key' => 'child_value'}}})
groups.push(child)


step "remove the class and update the classes on the classifier"

create_remote_file(master, "#{testdir}/environments/one/manifests/site.pp", <<-PP)
#{make_class("root_empty_class")}
PP

sleep 5

with_puppet_running_on(master, master_opts, testdir) do
  Classifier.post("/v1/update-classes")
end


step "update the model to reflect the deleted class"
parent["deleted"] = {"one_class" => {"puppetlabs.classifier/deleted" => true,
                                     "key" => {"puppetlabs.classifier/deleted" => true,
                                               "value" => "parent_value"}}}
child["deleted"] = {"one_class" => {"puppetlabs.classifier/deleted" => true,
                                    "key" => {"puppetlabs.classifier/deleted" => true,
                                               "value" => "child_value"}}}
verify_groups(groups)


step "update the groups that refer to deleted classes"

response = update_group(child, {"variables" => {"changed" => true}})
assert(response == true, "Unexpected response: #{response}")
response = update_group(parent, {'rule' => RandomRule.generate})
assert(response == true, "Unexpected response: #{response}")
