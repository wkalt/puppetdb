require File.expand_path(File.dirname(__FILE__) + '/../nc_service_helpers.rb')

#declare empty array of groups
#add to this group as necessary, send through verify_groups method to check
#current groups against server state
groups = []

step "clear classifier database"

clear_and_restart_classifier(classifier)

step "ensure we can create a group without specifying an id"

parent = create_group
groups.push(parent)
verify_groups(groups)

step "ensure we can create a 2nd group with a user supplied UUID and user supplied name"

child = create_group({"id" => SecureRandom.uuid})
groups.push(child)
verify_groups(groups)

step "Actually make the parent the parent"

response = Classifier.post("/v1/groups/#{child['id']}",
                           :body => {"parent" => parent['id']}.to_json)

child['parent'] = parent['id']

verify_groups(groups)

step "Create a grandparent"

grandparent = create_group

response = Classifier.post("/v1/groups/" + parent['id'],
                          :body => {"parent" => grandparent['id']}.to_json)

parent['parent'] = grandparent['id']

groups.push(grandparent)
verify_groups(groups)

step "add great grandchild with long name"

great_grandchild = create_group({"name" => RandomString.generate(256),
                                "parent" => child['id']})

groups.push(great_grandchild)
verify_groups(groups)

step "change the grandparent to a new group and delete the old grandparent"

new_grandparent = create_group

response = Classifier.post("/v1/groups/#{parent['id']}",
                           :body => {"parent" => new_grandparent['id']}.to_json)

parent['parent'] = new_grandparent['id']

#Delete the old grandparent
Classifier.delete("/v1/groups/#{grandparent['id']}")
groups.delete(grandparent)

grandparent = new_grandparent

groups.push(grandparent)
verify_groups(groups)

