require File.expand_path(File.dirname(__FILE__) + '/../nc_service_helpers.rb')

Classifier.base_uri("#{classifier.reachable_name}:#{CLASSIFIER_PORT}")

test_name "Groups API endpoint validation, parameter name"

step "clear and restart the classifier service"
clear_and_restart_classifier(classifier)

step "ensure we can create a group with single character name"
group = create_group({'name' => RandomString.generate(1)})

step "and ensure that we can update that group's name to something long"
update_group(group, {'name' => RandomString.generate(300)})

step "ensure we can create a group with the same name but different environment"
group2 = create_group({"name" => group['name'],
                       "environment" => RandomString.generate})

step "ensure we cannot change the environment to one that already exists with the same name"
response = update_group(group, {'environment' => group2['environment']})
assert(response.code == 422,
       "Expected response code 422, got #{response.code}")                        
