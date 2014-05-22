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
                "defined-by": {
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

## Classification Explanation Endpoint

### POST /v1/classified/nodes/\<name\>/explanation

Retrieve an explanation of how a node with nodename `name` will be classified by submitting its facts.

#### Request Format

The body of the request must be a JSON object describing the node's facts.
This object is allowed to have these keys:
  * `facts`: this key describes the regular (i.e. non-trusted) facts of the node.
             Its value must be a further object whose keys are fact names, and whose values are the corresponding fact values.
             Structured facts may be included here; structured fact values are further objects or arrays.
  * `trusted`: this optional key describes the trusted facts of the node.
               Its value has exactly the same format as the `facts` key's value.
The node's name from the request's URL will be merged into this object under the `name` key.

Here is an example of a valid request body:

    {
      "facts": {
        "ear-tips": "pointed",
        "eyebrow pitch": "40",
        "hair": "dark",
        "resting bpm": "120",
        "blood oxygen transporter": "hemocyanin",
        "anterior tricuspids": "2",
        "appendices": "1",
        "spunk": "10"
      }
    }


#### Response Format

The response will be a JSON object that describes how the node would be classified.
If the node would be successfully classified, then this object will contain the final classification.
If the classification would fail due to conflicts, then this object will contain a description of the conflicts.

This response is intended to provide insight into the entire classification process, since there are several stages where reality can deviate from your expectations.
It will be easier to understand the keys of this object if you understand the steps involved in classification:
  1. First, all group rules are tested on the node's facts and name, and all those groups that don't match the node are culled, leaving the _matching groups_.
  2. Inheritance relations are used to further cull the matching groups, by removing any matching group that has a descendent that is also a matching group.
     Those groups that are left over are the _leaf groups_.
  3. Each of the leaf groups is then transformed into its _inherited classification_ by adding all the inherited values from its ancestors.
  4. Finally, all the inherited classifications are inspected for conflicts.
     A conflict occurrs whenever two inherited classifications define different values for the environment, a class parameter, or a top-level variable.
     If no conflicts are found, the inherited classifications are merged together to form the _final classification_, which is returned to the client.

The JSON object returned by this endpoint has keys that correspond to each of the four above steps:
  * `match_explanations`: this corresponds to step 1 of classification, finding the matching groups.
                          This key's value is an explanation object just like those found in [node check-ins](nodes.markdown#get-v1nodes), which maps between a matching group's ID and an explained condition object that demonstrates why the node matched that group's rule.
  * `leaf_groups`: this corresponds to step 2 of classification, finding the leaves.
                   This key's value is an array of the leaf groups (i.e., those groups that are not related to any of the other matching groups).
  * `inherited_classifications`: this corresponds to step 3 of classification, adding inherited values.
                                 This key's value is an object mapping from a leaf group's ID to the classification values provided by that group (after inheritance).
  * `conflicts`: this key, which corresponds to step 4 of classification, will only be present if there are conflicts between the inherited classifications.
                 Its value will be similar to a classification object, but wherever there was a conflict there will be an array of conflict details instead of a single classification value,
                 Each of these details is an object with three keys: `value`, `from`, and `defined-by`.
                 The `value` key is a conflicting value, the `from` key is the group whose inherited classification provided this value, and the `defined-by` key is the group that actually defined the value (which may be an ancestor of the `from` group).
  * `final_classification`: this key, which also corresponds to step 4 of classification, will only be present if there are no conflicts between the inherited classifications.
                            Its value will be the result of merging all the inherited classifications.

Finally, the response object contains one additional key that does not correspond to any of the classification steps:
  * `node_as_received`: the submitted node object as received by the service, after adding the `name` and, if not supplied by the client, an empty `trusted` object.

The structure of these objects may be more easily understood through example.
Here's an example of a response the endpoint could return in the case of a successful classification:

    {
      "node_as_received": {
        "name": "Tuvok",
        "trusted": {},
        "facts": {
          "ear-tips": "pointed",
          "eyebrow pitch": "30",
          "blood oxygen transporter": "hemocyanin",
          "anterior tricuspids": "2",
          "hair": "dark",
          "resting bpm": "200",
          "appendices": "0",
          "spunk": "0"
        }
      },
      "match_explanations": {
        "00000000-0000-4000-8000-000000000000": {
          "value": true,
          "form": ["~", {"path": "name", "value": "Tuvok"}, ".*"]
        },
        "8aeeb640-8dca-4b99-9c40-3b75de6579c2": {
          "value": true,
          "form": ["and",
                   {
                     "value": true,
                     "form": [">=", {"path": ["facts", "eyebrow pitch"], "value": "30"}, "25"]
                   },
                   {
                     "value": true,
                     "form": ["=", {"path": ["facts", "ear-tips"], "value": "pointed"}, "pointed"]
                   },
                   {
                     "value": true,
                     "form": ["=", {"path": ["facts", "hair"], "value": "dark"}, "dark"]
                   },
                   {
                     "value": true,
                     "form": [">=", {"path": ["facts", "resting bpm"], "value": "200"}, "100"]
                   },
                   {
                     "value": true,
                     "form": ["=",
                              {
                                "path": ["facts", "blood oxygen transporter"],
                                "value": "hemocyanin"
                              },
                              "hemocyanin"
                     ]
                   }
          ]
        }
      },
      "leaf_groups": {
                       "8aeeb640-8dca-4b99-9c40-3b75de6579c2": {
                         "name": "Vulcans",
                         "id": "8aeeb640-8dca-4b99-9c40-3b75de6579c2",
                         "parent": "00000000-0000-4000-8000-000000000000",
                         "rule": ["and", [">=", ["facts", "eyebrow pitch"], "25"]
                                         ["=", ["facts", "ear-tips"], "pointed"]
                                         ["=", ["facts", "hair"], "dark"]
                                         [">=", ["facts", "resting bpm"], "100"]
                                         ["=", ["facts", "blood oxygen transporter"], "hemocyanin"]
                         ],
                         "environment": "alpha quadrant",
                         "variables": {},
                         "classes": {
                           "emotion": {"importance": "ignored"},
                           "logic": {"importance": "primary"}
                         }
                       }
      },
      "inherited_classifications": {
        "8aeeb640-8dca-4b99-9c40-3b75de6579c2": {
          "environment": "alpha quadrant",
          "variables": {},
          "classes": {
            "logic": {"importance": "primary"},
            "emotion": {"importance": "ignored"}
          }
        }
      },
      "final_classification": {
        "environment": "alpha quadrant",
        "variables": {},
        "classes": {
          "logic": {"importance": "primary"},
          "emotion": {"importance": "ignored"}
        }
      }
    }

and here's an example of a response in the case of conflicts:

    {
      "node_as_received": {
        "name": "Spock",
        "trusted": {},
        "facts": {
          "ear-tips": "pointed",
          "eyebrow pitch": "40",
          "blood oxygen transporter": "hemocyanin",
          "anterior tricuspids": "2",
          "hair": "dark",
          "resting bpm": "120",
          "appendices": "1",
          "spunk": "10"
        }
      },
      "match_explanations": {
        "00000000-0000-4000-8000-000000000000": {
          "value": true,
          "form": ["~", {"path": "name", "value": "Spock"}, ".*"]
        },
        "a130f715-c929-448b-82cd-fe21d3f83b58": {
          "value": true,
          "form": [">=": {"path": ["facts", "spunk"], "value": "10"}, "5"]
        },
        "8aeeb640-8dca-4b99-9c40-3b75de6579c2": {
          "value": true,
          "form": ["and",
                   {
                     "value": true,
                     "form": [">=", {"path": ["facts", "eyebrow pitch"], "value": "30"}, "25"]
                   },
                   {
                     "value": true,
                     "form": ["=", {"path": ["facts", "ear-tips"], "value": "pointed"}, "pointed"]
                   },
                   {
                     "value": true,
                     "form": ["=", {"path": ["facts", "hair"], "value": "dark"}, "dark"]
                   },
                   {
                     "value": true,
                     "form": [">=", {"path": ["facts", "resting bpm"], "value": "200"}, "100"]
                   },
                   {
                     "value": true,
                     "form": ["=",
                              {
                                "path": ["facts", "blood oxygen transporter"],
                                "value": "hemocyanin"
                              },
                              "hemocyanin"
                     ]
                   }
          ]
        }
      },
      "leaf_groups": {
                       "a130f715-c929-448b-82cd-fe21d3f83b58": {
                         "name": "Humans",
                         "id": "a130f715-c929-448b-82cd-fe21d3f83b58",
                         "parent":: "00000000-0000-4000-8000-000000000000",
                         "rule": [">=", ["facts", "spunk"], "5"],
                         "environment": "alpha quadrant",
                         "variables": {},
                         "classes": {
                           "emotion": {"importance": "primary"},
                           "logic": {"importance": "secondary"}
                         }
                       },
                       "8aeeb640-8dca-4b99-9c40-3b75de6579c2": {
                         "name": "Vulcans",
                         "id": "8aeeb640-8dca-4b99-9c40-3b75de6579c2",
                         "parent": "00000000-0000-4000-8000-000000000000",
                         "rule": ["and", [">=", ["facts", "eyebrow pitch"], "25"]
                                         ["=", ["facts", "ear-tips"], "pointed"]
                                         ["=", ["facts", "hair"], "dark"]
                                         [">=", ["facts", "resting bpm"], "100"]
                                         ["=", ["facts", "blood oxygen transporter"], "hemocyanin"]
                         ],
                         "environment": "alpha quadrant",
                         "variables": {},
                         "classes": {
                           "emotion": {"importance": "ignored"},
                           "logic": {"importance": "primary"}
                         }
                       }
      },
      "inherited_classifications": {
        "a130f715-c929-448b-82cd-fe21d3f83b58": {
          "environment": "alpha quadrant",
          "variables": {},
          "classes": {
            "logic": {"importance": "secondary"},
            "emotion": {"importance": "primary"}
          }
        },
        "8aeeb640-8dca-4b99-9c40-3b75de6579c2": {
          "environment": "alpha quadrant",
          "variables": {},
          "classes": {
            "logic": {"importance": "primary"},
            "emotion": {"importance": "ignored"}
          }
        }
      },
      "conflicts": {
        "classes": {
          "logic": {
            "importance": [
                            {
                              "value": "secondary",
                              "from": {
                                "name": "Humans",
                                "id": "a130f715-c929-448b-82cd-fe21d3f83b58",
                                ...
                              },
                              "defined-by": {
                                "name": "Humans",
                                "id": "a130f715-c929-448b-82cd-fe21d3f83b58",
                                ...
                              }
                            },
                            {
                              "value": "primary",
                              "from": {
                                "name": "Vulcans",
                                "id": "8aeeb640-8dca-4b99-9c40-3b75de6579c2",
                                ...
                              },
                              "defined-by": {
                                "name": "Vulcans",
                                "id": "8aeeb640-8dca-4b99-9c40-3b75de6579c2",
                                ...
                              }
                            }
            ]
          },
          "emotion": {
            "importance": [
                            {
                              "value": "ignored",
                              "from": {
                                "name": "Vulcans",
                                "id": "8aeeb640-8dca-4b99-9c40-3b75de6579c2",
                                ...
                              },
                              "defined-by": {
                                "name": "Vulcans",
                                "id": "8aeeb640-8dca-4b99-9c40-3b75de6579c2",
                                ...
                              }
                            },
                            {
                              "value": "primary",
                              "from": {
                                "name": "Humans",
                                "id": "a130f715-c929-448b-82cd-fe21d3f83b58",
                                ...
                              },
                              "defined-by": {
                                "name": "Humans",
                                "id": "a130f715-c929-448b-82cd-fe21d3f83b58",
                                ...
                              }
                            }
            ]
          }
        }
      }
