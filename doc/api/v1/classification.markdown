---
title: "Node Classifier 1.0 >> API >> v1 >> Classification"
---

## Classification Endpoint

### GET /v1/classified/nodes/\<name\>

Retrieve the classification information for the node with the given name.

#### Response Format

The response will be a JSON object describing the node post-classification.
The keys of this object are:

* `name`: the name of the node (a string).
* `groups`: an array of the groups (strings) that this node was classified into.
* `environment`: the name of the environment that this node will use, which is taken from the groups the node was classified into.
* `classes`: an array of the classes (strings) that this node received from the groups it was classified into.
* `parameters`: an object describing class parameter values for the above classes wherever they differ from the default.
                The keys of this object are class names, and the values are further objects describing the parameters for just the associated class.
                The keys of that innermost object are parameter names, and the values are the parameter values, which can be any sort of JSON value.

Here is an example of a response from this endpoint:

    {
      "name": "foo.example.com",
      "groups": ["Staging", "Webservers"],
      "environment": "staging",
      "classes": ["apache"],
      "parameters": {
        "apache": {
          "keepalive_timeout": 30,
          "log_level": "notice"
        }
      }
    }

#### Error Responses

If the node is classified into multiple groups that define conflicting classifications for the node, then a 500 Server Error response will be returned.
The body of this response will contain the usual JSON error object described in the [errors documentation](errors.markdown).
The `kind` key of the error will be "classification-conflict", the `msg` will describe generally why this happens, and the `details` key will contain an object that describes the specific conflicts encountered.

The details object may have between one and all of the following three keys: `environment`, `classes`, and `variables`.
The `environment` key will map directly to an array of value detail objects (described below).
The `variables` key will contain an object with a key for each conflicting variable, whose values are an array of value detail objects.
The `classes` key will contain an object with a key for each class that had conflicting parameter definitions, whose values are further objects that describe the conflicts for that class's parameters.
The keys of these objects are the names of parameters that had conflicting values defined, and the values are arrays of value detail objects.

A value details object describes one of the conflicting values defined for the environment, a variable, or a class parameter.
Each such object contains the following three keys:
  * `value`: the defined value, which will be a string for environment and class parameters, but for a variable may be any JSON value.
  * `from`: the group that the node was classified into that caused this value to be added to the node's classification.
            This group may not define the value, because it may be inherited from an ancestor of this group.
  * `defined-by`: the group that actually defined this value.
                  This is often the `from` group, but could instead be an ancestor of that group.

Here's an example of a classification conflict error object (groups truncated for clarity):

    {
      "kind": "classification-conflict",
      "msg": "The node was classified into multiple unrelated groups that defined conflicting class parameters or top-level variables. See `details` for a list of the specific conflicts.",
      "details": {
        "classes": {
          "songColors": {
            "blue": [
              {
                "value": "Blue Suede Shoes",
                "from": {
                  "name": "Elvis Presley",
                  "classes": {},
                  "rule": ["=", "nodename", "the-node"],
                  ...
                },
                "defined-by" {
                  "name": "Carl Perkins",
                  "classes": {"songColors": {"blue": "Blue Suede Shoes"}},
                  "rule": ["not", ["=", "nodename", "the-node"]],
                  ...
                }
              },
              {
                "value": "Since You've Been Gone",
                "from": {
                  "name": "Aretha Franklin",
                  "classes": {"songColors": {"blue": "Since You've Been Gone"}},
                  ...
                },
                "defined-by": {
                  "name": "Aretha Franklin",
                  "classes": {"songColors": {"blue": "Since You've Been Gone"}},
                  ...
                }
              }
            ]
          }
        }
      }
    }

Note how, in this example, the conflicting "Blue Suede Shoes" value was included in the classification because the node matched the "Elvis Presley" group (since that is the value of the "from" key), but that group doesn't define the "Blue Suede Shoes" value.
That value is defined by the "Carl Perkins" group, which is an ancestor of the "Elvis Presley" group, causing the latter to inherit the value from the former.
The other conflicting value, "Since You've Been Gone", is defined by the same group that the node matched.
