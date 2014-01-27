---
title: "Node Classifier 1.0 >> API >> v1 >> Rules"
---

[pdb-fact-query]: http://docs.puppetlabs.com/puppetdb/1.5/api/query/tutorial.html#facts-walkthrough

## Rules Endpoint

### POST /v1/rules

Create a new rule for classifying nodes into groups.

##### Request Format

The request body must be a JSON object describing the rule to be created.
The required keys of this object are:

* `groups`: an array of the names of groups to add to the nodes that match this rule.
* `when`: an array describing a boolean expression on node facts.
          When this expression evaluates to true, the node is said to match this rule and so it is added to the groups listed in the `groups` array.
          The goal is to eventually support [puppetdb fact query syntax][pdb-fact-query] for expressions, but currently only single-operation expressions where the operation is equality or regular-expression matching are allowed (i.e. no boolean or numeric comparison operators are allowed).

#### Response Format

If the request is well-formed and creating it is successful, the server will return a 201 Created response with an empty body.

#### Error Responses

If the request is malformed because it is not valid JSON or because the JSON object does not meet the schema for rules objects, the server will return a 400 Bad Request response.
If the request body is not valid JSON, the response body will be plain text, will state that the request's body could not be parsed, and will echo back the received request body.
Otherwise the response body will be a JSON object with `submitted`, `schema`, and `error` keys which respectively describe the submitted object, the schema that object should conform to, and how the submitted object failed to conform to the schema.

If any of the groups referred to by the request do not exist, the server will return a 500 Server Error response when the attempted insertion fails due to unsatisfied database constraints.
