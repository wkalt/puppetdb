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

No error responses specific to this request are expected.
