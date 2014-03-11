---
title: "Node Classifier 1.0 >> API >> v1 >> Groups"
---

## Groups Endpoint

### GET /v1/groups

Retrieve a list of all groups in the classifier.

#### Response Format

The response is a JSON array of group objects.
Each group object contains the following keys:

* `name`: the name of the group (a string).
* `environment`: the name of the group's environment (a string), which indirectly defines what classes will be available for the group to set, and will be the environment that nodes classified into this group will run under.
* `rule`: an object with a single key, "when", whose value is a boolean condition on node facts.
          See the "Rule Condition Grammar" section below for more information on how this condition must be structured.
* `classes`: an object that defines both the classes consumed by nodes in this group and any non-default values for their parameters.
             The keys of the object are the class names (strings), and the values are more objects mapping between the parameter name (a string) and the value set by the group (which could be any sort of JSON value).
             If the group does not set any parameters for a class, the object under that class's name will be empty.
* `variables`: an object that defines the values of any top-level variables set by the group.
               The object is a mapping between variable names (strings) and their values, which can be any JSON value.

Here is an example of group object:

    {
      "name": "Webservers",
      "environment": "production",
      "rule": {
        "when": ["and", ["~", "certname", "www"],
                        [">=", "total_ram", "512"]]
      },
      "classes": {
        "apache": {
          "serveradmin": "bofh@travaglia.net",
          "keepalive_timeout": 5
        }
      },
      "variables": {
        "ntp_servers": ["0.us.pool.ntp.org", "1.us.pool.ntp.org", "2.us.pool.ntp.org"]
      }
    }

##### Rule Condition Grammar

The grammar for a rule condition is:

    condition : [ {bool} {condition}+ ] | [ "not" {condition} ] | operation
         bool : "and" | "or"
    operation : [ {operator} {fact-name} {value} ]
     operator : "=" | "~" | ">" | ">=" | "<" | "<="
    fact-name : string
        value : string

For the regex operator "~", the value will be interpreted as a Java regular expression, and literal backslashes will have to be used to escape regex characters in order to match those characters in the fact value.
For the numeric comparison operators (">", ">=", "<", and "<="), the fact value (which is always a string) will be coerced to a number (either integral or floating-point).
If the value cannot be coerced to a number, then the numeric operation will always evaluate to false.

#### Error Responses

No error responses specific to this request are expected.

### GET /v1/groups/\<name-or-uuid\>

Retrieve the group with the given name or UUID.

#### Response Format

If the group exists, the response will be a group object as described above, in JSON format.

#### Error Responses

If the group with the given name cannot be found, a 404 Not Found response with an empty body will be returned.

### PUT /v1/groups/\<name\>

Create a new group with the given name.

#### Request Format

The request body must be a JSON object describing the group to be created.
The keys allowed in this object are:

* `environment`: the name of the group's environment.
                 This key is optional; if not provided, the default environment (`production`) will be used.
* `variables`: an object that defines the names and values of any top-level variables set by the group.
               The keys of the object are the variable names, and the corresponding value is that variable's value, which can be any sort of JSON value.
               The `variables` keys is optional, and if a group does not define any top-level variables then it may be omitted.
* `classes`: An object that defines the classes to be used by nodes in the group, as well as custom values for those classes' parameters.
             This is a two-level object; that is, the keys of the object are class names (strings), and each key's value is another object that defines class parameter values.
             This innermost object maps between class parameter names and their values.
             The keys are the parameter names (strings), and each value is the parameter's value, which can be any kind of JSON value.
             The `classes` keys is _not_ optional; if it is missing, a 400 Bad Request response will be returned by the server.

#### Response Format

If the group was successfully created, the server will return a 201 Created response, with the group object (in JSON) as the body.
See above for a complete description of a group object.

#### Error Responses

If the `classes` key of a request's group object is not defined, if the values of any of the defined keys do not match the required type, or if the request's body could not be parsed as JSON, the server will return a 400 Bad Request response.
In the first two cases, the response will contain a JSON object with `submitted`, `schema`, and `error` keys which respectively describe the submitted object, the schema that object should conform to, and how the submitted object failed to conform to the schema.
In the last case, the response will be plain text, will state that the request's body could not be parsed, and will echo back the request's body.

If any environments, classes, or class parameters specified in the request do not exist, the server will return a 500 Server Error response when the attempted insertion fails due to unsatisfied database constraints.

### POST /v1/groups/\<name-or-uuid\>

Update the environment, rule, classes, class parameters, and variables of the group with the given name or UUID by submitting a group delta.

#### Request Format

The request body must be JSON object describing the delta to be applied to the group.
The `classes` and `variables` keys of the delta will be merged with the group, and then any keys of the resulting object that have a null value will be deleted.
This allows you to remove classes, class parameters, or variables from the group by setting them to null in the delta.
The `environment` and `rule` keys, if present in the delta, will replace the old values wholesale with their values.

For example, given the following group:

    {
      "name": "Webservers",
      "environment": "staging",
      "rule": {
        "when": ["~", "certname", "www"]
      },
      "classes": {
        "apache": {
          "serveradmin": "bofh@travaglia.net",
          "keepalive_timeout": 5
        },
        "ssl": {
          "keystore": "/etc/ssl/keystore"
        }
      },
      "variables": {
        "ntp_servers": ["0.us.pool.ntp.org", "1.us.pool.ntp.org", "2.us.pool.ntp.org"]
      }
    }

and this delta:

    {
      "environment": "production",
      "classes": {
        "apache": {
          "serveradmin": "roy@reynholm.co.uk",
          "keepalive_timeout": null
        },
        "ssl": null
      },
      "variables": {
        "dns_servers": ["dns.reynholm.co.uk"]
      }
    }

then the value of the group after the update will be:

    {
      "name": "Webservers",
      "environment": "production",
      "rule": {
        "when": ["~", "certname", "www"]
      },
      "classes": {
        "apache": {
          "serveradmin": "roy@reynholm.co.uk",
        }
      },
      "variables": {
        "ntp_servers": ["0.us.pool.ntp.org", "1.us.pool.ntp.org", "2.us.pool.ntp.org"]
        "dns_servers": ["dns.reynholm.co.uk"]
      }
    }

Note how the "ssl" class was deleted because its entire object was mapped to null, whereas for the "apache" class only the "keepalive_timeout" parameter was deleted.

### DELETE /v1/groups/\<name-or-uuid\>

Delete the group with the given name or UUID.

#### Response Format

If the delete operation is successful, then a 204 No Content with an empty body will be returned.

#### Error Responses

No error responses specific to this operation are anticipated.
