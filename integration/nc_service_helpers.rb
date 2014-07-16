require 'httparty'
require 'securerandom'

class RandomString
  def self.generate(length = 32)
    characters = [('0'..'9'), ('a'..'z'), ('A'..'Z')]
    characters = characters.map{ |i| i.to_a }.flatten
    (0...length).map{ characters[rand(characters.length)] }.join
  end
end

class RandomRule
  def self.generate
    operators = ["=", "~", ">", ">=", "<", "<="]
    operator = operators[rand(operators.length)]
    [operator, RandomString.generate(6), RandomString.generate(6)]
  end
end

class Classifier
  include HTTParty
  debug_output($stdout)
  headers({'Content-Type' => 'application/json'})
end

def compare(hash1, hash2)
  return_hash= {}
  hash1.keys.each do |key|
    unless hash1[key] == hash2[key]
      return_hash[key] = {"expected" => hash1[key], "got" => hash2[key]}
    end
  end
  return_hash  
end

Uuidregex = /[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}/i 
Rootuuid = '00000000-0000-4000-8000-000000000000'

#This method returns a hash of a group OR a response object if it encounters an unexpected HTTP code
def create_group(options={})
  name = options['name'] || RandomString.generate
  rule = options['rule'] || RandomRule.generate
  classes = options['classes'] || {}
  parent = options['parent'] || Rootuuid
  id = options['id']
  environment = options['environment']
  variables = options['variables']

  hash = { "name" => name,
           "parent" => parent,
           "rule"=> rule,
           "classes" => classes
         }

  if environment
    hash['environment'] = environment
  end
  if variables
    hash['variables'] = variables
  end

  model = {} 

  if id
    response = Classifier.put("/v1/groups/" + id,
                              :body => hash.to_json)

    if !response.response.is_a?(Net::HTTPSuccess)
      return response
    end

    assert(compare(hash, JSON.parse(response.body)) == {},
           "Something went wrong, #{compare(hash, JSON.parse(response.body))}")

    model = JSON.parse(response.body)
  else

    response = Classifier.post("/v1/groups",
                               :body => hash.to_json,
                               :follow_redirects => false)

    if !response.response.is_a?(Net::HTTPSeeOther)
      return response
    end

    group = response.headers['location']
    response = Classifier.get(group)

    assert(compare(hash, JSON.parse(response.body)) == {},
           "Something went wrong")

    model = JSON.parse(response.body)
     
  end
  
  model

end

def verify_groups(array)
  array.each do |group|
    response = Classifier.get("/v1/groups/#{group['id']}")
    assert(group == JSON.parse(response.body),
           "Something went wrong, #{compare(group, JSON.parse(response.body))}")
  end
end

def remove_nil_values(hash)
  hash.each do |k,v| 
    case v
    when Hash
      remove_nil_values(v)
    else
      hash.delete(k) if v == nil
    end
  end
end
private :remove_nil_values

#This method is used to update the model after you have changed a class definition
#using the update-classes endpoint. It will add the group['deleted'] key and fill
#out the object accordingly for a deleted parameter.
def update_deleted_classes(group, patharray)
  key = patharray.pop
  group['deleted'] ||= {}
  if patharray != []
    deleted_object = patharray.inject(group['deleted']) do |hash, el|
      hash[el] ||= {'puppetlabs.classifier/deleted' => false}   
    end
    actual_object = patharray.inject(group['classes']) do |hash, el|
      hash[el]
    end
    deleted_object[key] =  actual_object[key]
    add_pl_deleted_keys(deleted_object)
  else
    group['deleted'][key] = group['classes'][key]
    add_pl_deleted_keys(group['deleted'][key], class_delete_flag = true)
  end
end

def add_pl_deleted_keys(deleted_object, class_delete_flag = false)
  deleted_object.each do |k,v|
    if v.is_a? Hash
      add_pl_deleted_keys(deleted_object[k])
    else
      deleted_object[k] = {'puppetlabs.classifier/deleted' => true, 'value' => v}
    end
  end
  deleted_object['puppetlabs.classifier/deleted'] = class_delete_flag
end

def deep_merge(group, update_hash)
  update_hash.each do |k,v|
    if v.is_a? Hash
      group[k] ||= {}
      deep_merge(group[k], update_hash[k])
    else
      group[k] = update_hash[k]
    end
  end
end

def update_group(group, hash)
  response = Classifier.post("/v1/groups/#{group['id']}",
                             :body => hash.to_json)
  
  if !response.response.is_a?(Net::HTTPSuccess)
    return response
  end

  deep_merge(group, hash)
  group = remove_nil_values(group)

  #check to see if the update hash had any class changes that require transforms to the groups['deleted'] object 
  if group['deleted'] && group['classes'] != {} && hash['classes']
    hash['classes'].each do |classname, parameters|
      if group['deleted'][classname] == nil
        next
      end
      parameters.each do |parameter, value|
        if value == nil
          group['deleted'][classname].delete(parameter)
        else
          group['deleted'][classname][parameter]['value'] = value
        end
      end
    end
  end

  #check to see if we need to delete any classes from the model's group{'deleted'] key
  if group['deleted']
    group['deleted'].each do |classname, parameters|
      if group['classes'][classname] == nil || group['deleted'][classname].keys == ['puppetlabs.classifier/deleted']
        group['deleted'].delete(classname)
      end
    end
    group.delete('deleted') if group['deleted'] == {}
  end

  assert(group == JSON.parse(response.body),
         "Something went wrong, #{compare(group, JSON.parse(response.body))}")
end

def verify_inheritance(group)
  response = JSON.parse(Classifier.get("/v1/groups/#{group['id']}?inherited=true").body)
  traits = get_traits_with_inheritance(group)

  traits.each_key do |trait|
    assert(traits[trait] == response[trait],
           "something went wrong, expected #{traits[trait]}, got #{response[trait]}")
  end
end

def get_traits_with_inheritance(group)
  group = Marshal.load(Marshal.dump(group))
  get_traits_with_inheritance_recursion(group)
end

def get_traits_with_inheritance_recursion(group, traits={'classes' => {}, 'variables' => {}})
  group['classes'].each_key do |classname|
    traits['classes'][classname] ||= group['classes'][classname]
    group['classes'][classname].each_key do |parameter|
      traits['classes'][classname][parameter] ||= group['classes'][classname][parameter]
    end
  end

  group['variables'].each_key do |variable|
    traits['variables'][variable] ||= group['variables'][variable]
  end

  return traits if group['id'] == Rootuuid && group['parent'] == Rootuuid

  group_parent = JSON.parse(Classifier.get("/v1/groups/#{group['parent']}").body)
  get_traits_with_inheritance_recursion(group_parent, traits)
end
private :get_traits_with_inheritance_recursion

def get_root_group
  JSON.parse(Classifier.get("/v1/groups/#{Rootuuid}").body)
end

def make_class(classname, parameters={}, body="")
  <<-PP
  class #{classname}(#{parameters_to_string(parameters)}) { 
  #{body}
  }
  PP
end

def parameters_to_string(hash)
  string = ""
  hash.each do |key,value|
    string << "$#{key} = #{value},"
  end
  string.chomp!(",")
  string
end
private :parameters_to_string
