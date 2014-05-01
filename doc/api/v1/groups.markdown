---
title: "Node Classifier 1.0 >> API >> v1 >> Groups"
---

## Groups Endpoint

### General Error Responses

Whenever the request path contains a group's ID, which must be a UUID, there is the potential for that ID to be malformed.
If it is, a 400 Bad Request response will be returned to the client.
The body of the response will contain a JSON error object as described in the [errors documentation](errors.markdown).
The object's `kind` key will be "malformed-uuid", and the value of the `details` key will be a string containing the malformed UUID as received by the server.

### GET /v1/groups

Retrieve a list of all groups in the classifier.

#### Response Format

The response is a JSON array of group objects.
Each group object contains the following keys:

* `name`: the name of the group (a string).
* `id`: the group's ID, which is a string containing a type-4 (random) UUID.
* `description`: an optional key containing an arbitrary string describing the group.
* `environment`: the name of the group's environment (a string), which indirectly defines what classes will be available for the group to set, and will be the environment that nodes classified into this group will run under.
* `parent`: the ID of the group's parent (a string).
            A group is not permitted to be its own parent, unless it is the default group (which is the root of the hierarchy).
            Note that the root group always has the lowest-possible random UUID, `00000000-0000-4000-8000-000000000000`.
* `rule`: a boolean condition on node properties.
          When a node's properties satisfies this condition, it will be classified into the group.
          See the ["Rule Condition Grammar"](#rule-condition-grammar) section below for more information on how this condition must be structured.
* `classes`: an object that defines both the classes consumed by nodes in this group and any non-default values for their parameters.
             The keys of the object are the class names, and the values are objects describing the parameters.
             The parameter objects' keys are parameter names, and the values are what the group sets for that parameter (always a string).
* `deleted`: an object similar the `classes` object that shows which classes and class parameters set by the group have since been deleted from Puppet.
             If none of the group's classes or parameters have been deleted, this key will not be present, so checking the presence of this key is an easy way to check whether the group has references that need updating.
             The keys of this object are class names, and the values are further objects.
             These secondary objects always contain the `puppetlabs.classifier/deleted` key, whose value is a boolean indicating whether the entire class has been deleted from Puppet.
             The other keys of these objects are parameter names, and the other values are objects that always contain two keys: `puppetlabs.classifier/deleted`, mapping to a boolean indicating whether the specific class parameter has been deleted from Puppet; and `value`, mapping to the string value set by the group for this parameter (the value is duplicated for convenience, as it also appears in the `classes` object).
* `variables`: an object that defines the values of any top-level variables set by the group.
               The object is a mapping between variable names and their values (which can be any JSON value).

Here is an example of group object:

    {
      "name": "Webservers",
      "id": "fc500c43-5065-469b-91fc-37ed0e500e81",
      "environment": "production",
      "description": "This group captures configuration relevant to all web-facing production webservers, regardless of location."
      "parent": "00000000-0000-4000-8000-000000000000",
      "rule": ["and", ["~", ["trusted", "certname"], "www"],
                      [">=", ["facts", "total_ram"], "512"]],
      "classes": {
        "apache": {
          "serveradmin": "bofh@travaglia.net",
          "keepalive_timeout": "5"
        }
      },
      "variables": {
        "ntp_servers": ["0.us.pool.ntp.org", "1.us.pool.ntp.org", "2.us.pool.ntp.org"]
      }
    }

Here is an example of a group object that refers to some classes and parameters that have been deleted from Puppet:

    {
      "name": "Spaceship",
      "id": "fc500c43-5065-469b-91fc-37ed0e500e81",
      "environment": "space",
      "parent": "00000000-0000-4000-8000-000000000000",
      "rule": ["=", ["facts", "is_spaceship"], "true"],
      "classes": {
        "payload": {
          "type": "cubesat",
          "count": "8",
          "mass": "10.64"
        },
        "rocket": {
          "stages": "3"
        }
      },
      "deleted": {
        "payload": {"puppetlabs.classifier/deleted": true},
        "rocket": {
          "puppetlabs.classifier/deleted": false,
          "stages": {
            "puppetlabs.classifier/deleted": true,
            "value": "3"
          }
        }
      },
      "variables": {}
    }

The entire `payload` class has been deleted, since its deleted parameters object's `puppetlabs.classifier/deleted` key maps to `true`, in contrast to the `rocket` class, which has only had its `stages` parameter deleted.

##### Rule Condition Grammar

The grammar for a rule condition is:

    condition  : [ {bool} {condition}+ ] | [ "not" {condition} ] | {operation}
         bool  : "and" | "or"
    operation  : [ {operator} {fact-path} {value} ]
     operator  : "=" | "~" | ">" | ">=" | "<" | "<="
    fact-path  : {field-name} | [ {field-name} + ]
    field-name : string
        value  : string

For the regex operator `"~"`, the value will be interpreted as a Java regular expression, and literal backslashes will have to be used to escape regex characters in order to match those characters in the fact value.
For the numeric comparison operators (`">"`, `">="`, `"<"`, and `"<="`), the fact value (which is always a string) will be coerced to a number (either integral or floating-point).
If the value cannot be coerced to a number, then the numeric operation will always evaluate to false.

For the fact path, this can be either a string representing a top level field (the only current meaningful value here would be "name" representing the node name) or a list of strings that represent looking up a field in a nested data structure. Regular facts will all start with "facts" (e.g. `["facts", "architecture"] ` and trusted facts start with "trusted" (e.g. `["trusted", "certname"]`).

#### Error Responses

No error responses specific to this request are expected.

### POST /v1/groups

Create a new group without specifying its ID (one will be randomly generated by the service).

#### Request Format

The request body must be a JSON object describing the group to be created.
The keys allowed in this object are:

* `name`: the name of the group (required).
* `environment`: the name of the group's environment.
                 This key is optional; if it's not provided, the default environment (`production`) will be used.
* `description`: a string describing the group.
                 This key is optional; if it's not provided, the group will have no description and this key will not appear in responses.
* `parent`: the ID of the group's parent (required).
* `rule`: the condition that must be satisfied for a node to be classified into this group (required).
          The structure of this condition is described in the ["Rule Condition Grammar"](#rule-condition-grammar) section above.
* `variables`: an object that defines the names and values of any top-level variables set by the group.
               The keys of the object are the variable names, and the corresponding value is that variable's value, which can be any sort of JSON value.
               The `variables` key is optional, and if a group does not define any top-level variables then it may be omitted.
* `classes`: An object that defines the classes to be used by nodes in the group, as well as custom values for those classes' parameters (required).
             This is a two-level object; that is, the keys of the object are class names (strings), and each key's value is another object that defines class parameter values.
             This innermost object maps between class parameter names and their values.
             The keys are the parameter names (strings), and each value is the parameter's value, which can be any kind of JSON value.
             The `classes` key is _not_ optional; if it is missing, a 400 Bad Request response will be returned by the server.

#### Response Format

If the group was successfully created, the server will return a 303 See Other response, with the path to retrieve the created group in the "location" header of the repsonse.

#### Error Responses

If any of the required keys are missing, or if the values of any of the defined keys do not match the required type, or if the request's body could not be parsed as JSON, the server will return a 400 Bad Request response.
In all cases, the response will contain an error object as described in the [errors documentation](errors.markdown).
In the first two cases, the `kind` key will be "schema-violation", and the  `details` key of the error will be an object with `submitted`, `schema`, and `error` keys which respectively describe the submitted object, the schema that object should conform to, and how the submitted object failed to conform to the schema.
In the last case, the `kind` key will be "malformed-request" and the `details` key will be an object with `body` and `error` keys, which respectively hold the request body as received and the error message encountered while trying to parse the JSON.

If attempting to create the group violates uniqueness constraints (such as the constraint that each group name's must be unique within its environment), the server will return a 422 Unprocessable Entity response.
The `kind` key of the error object will be "uniqueness-violation", and the `msg` will describe which fields of the group caused the constraint to be violated, along with their values.
The `details` key will contain an object that itself has two keys:

* `conflict`: an object whose keys are the fields of the group that violated the constraint and whose values are the corresponding field values.
* `constraintName`: the name of the database constraint that was violated.

If any classes or class parameters definedy by the group or inherited by the group from its parents do not exist in the submitted group's environment, the server will return a 422 Unprocessable Entity response.
In both cases the response object's `kind` key will be "missing-referents" and the `msg` key will describe the number of missing referents.
The `details` key of the error object will be an array of objects, where each object describes a single missing referent, and has the following keys:

* `kind`: "missing-class" or "missing-parameter", depending on whether the entire class doesn't exist, or the class just doesn't have the parameter.
* `missing`: The name of the missing class or class parameter.
* `environment`: The environment that the class or parameter is missing from; i.e. the environment of the group where the error was encountered.
* `group`: The name of the group where the error was encountered.
           Note that this may not be the group where the class or parameter was defined due to inheritance.
* `defined-by`: The name of the group that defines the class or parameter.

If the parent of the group does not exist the server will return a 422 Unprocessable Entity response. The `kind` key will be "missing-parent" and the `msg` key will include the parent UUID that did not exist. The `details` key will contain the full submitted group.

If the request would cause an inheritance cycle to be created the server will return a 422 Unprocessable Entity response. The response will contain a [error object](errors.markdown) whose `kind` key will be "inheritance-cycle".  The `details` key will be an array of group objects, and will contain each group involved in the cycle. The `msg` key will contain a shortened description of the cycle, including a list of the group names with each followed by its parent until the first group is repeated.

### GET /v1/groups/:id

Retrieve the group with the given ID.

#### Response Format

If the group exists, the response will be a group object as described above, in JSON format.

#### Error Responses

In addition to the general `malformed-uuid` error response, if the group with the given ID cannot be found, a 404 Not Found response with an empty body will be returned.

### PUT /v1/groups/:id

Create a group with the given ID.
Note that any existing group with that ID will be overwritten!

#### Request Format

The request format is the same as the format for the [POST group creation endpoint](#post-v1groups) above.

#### Response Format

If the group was successfully created, the server will return a 201 Created response, with the group object (in JSON) as the body.
If the group already exists and is identical to the submitted group, then the server will take no action and return a 200 OK response, again with the group object as the body.
See above for a complete description of a group object.

#### Error Responses

If the request's group object contains the `id` key and its value differs from the UUID specified in the request's path, a 400 Bad Request response will be returned.
The response will contain an error object as described in the [errors documentation](errors.markdown).
The object's `kind` key will be "conflicting-ids", and its `details` key will itself contain an object with two keys: `submitted`, which contains the ID submitted in the request's body, and `fromUrl`, which contains the ID taken from the request's URL.

In addition, this operation can produce the general `malformed-error` response and any response that could also be generated by the [POST group creation endpoint](#post-v1groups) above.

### POST /v1/groups/:id

Update the name, environment, parent, rule, classes, class parameters, and variables of the group with the given ID by submitting a group delta.

#### Request Format

The request body must be JSON object describing the delta to be applied to the group.
The `classes`, `variables`, and `rule` keys of the delta will be merged with the group, and then any keys of the resulting object that have a null value will be deleted.
This allows you to remove classes, class parameters, variables, or the rule from the group by setting them to null in the delta.
The `name`, `environment`, `description`, and `parent` keys, if present in the delta, will replace the old values wholesale with their values.
Note that the root group's `rule` cannot be edited; any attempts to do so will be met with a 422 Unprocessable Entity response.

For example, given the following group:

    {
      "name": "Webservers",
      "id": "58463036-0efa-4365-b367-b5401c0711d3",
      "environment": "staging",
      "parent": "00000000-0000-4000-8000-000000000000",
      "rule": ["~", ["trusted", "certname"], "www"],
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
      "name": "Production Webservers",
      "id": "58463036-0efa-4365-b367-b5401c0711d3",
      "environment": "production",
      "parent": "01522c99-627c-4a07-b28e-a25dd563d756",
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
      "name": "Production Webservers",
      "id": "58463036-0efa-4365-b367-b5401c0711d3",
      "environment": "production",
      "parent": "01522c99-627c-4a07-b28e-a25dd563d756",
      "rule": ["~", ["trusted", "certname"], "www"],
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

#### Error Responses

This operation can return any of the errors that could be returned to a PUT request on this same endpoint.
See [above](#response-format) for details on these responses.
Note that 422 responses to POST requests can include errors that were caused by the group's children, but a group being created with a PUT request cannot have any children.

### DELETE /v1/groups/:id

Delete the group with the given ID.

#### Response Format

If the delete operation is successful, then a 204 No Content with an empty body will be returned.

#### Error Responses

In addition to the general `malformed-uuid` response, if the group with the given ID does not exist, then a 404 Not Found response as described in the [errors documentation](errors.markdown) will be returned to the client.
