require File.expand_path(File.dirname(__FILE__) + '/../nc_service_helpers.rb')

Classifier.base_uri("#{database.reachable_name}:#{CLASSIFIER_PORT}")

test_name "Basic error cases"

step "Drop database and reload classifier service"
clear_and_restart_classifier(database)

#this seems to be a bug, can create a group with an empty string for a name
step "test you cannot create a group with an empty name"
response = create_group({'name' => ''})

#not sure about this one, ca group names have spaces?
step "test you cannot add a group name with spaces"
response = create_group({'name' => RandomString.generate(6) + " "})

step "test you cannot add a group as a parent of itself"
group = create_group
response = create_group({'id' => group['id'], 'parent' => group['id']})

assert(response.code == 422,
       "Unexpected response code, expected 422, got #{response.code}")
