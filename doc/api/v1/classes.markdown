---
title: "Node Classifier 1.0 >> API >> v1 >> Classes"
---

## Classes Endpoint

### GET /v1/classes

Retrieve a list of all classes known to the classifier. (Note that all
other operations on classes require using the environment-specific
endpoints below.)

### GET /v1/environments/\<environment\>/classes

Retrieve a list of all classes known to the classifier within the given
environment.

#### Response Format

The response is a JSON array of class objects.
Each class object contains the following keys:

* `name`: the name of the class (a string).
* `environment`: the name of the environment that this class exists in.
                 Note that the same class can exist in different environments with different parameters.
* `parameters`: an object describing the class's parameters and their default values.
                The keys of this object are the parameter names (strings), and each value is the associated parameter's default value (which can be any legal JSON value).

Here is an example of a class object:

    {
      "name": "apache",
      "environment": "production",
      "parameters": {
        "default_mods": true,
        "default_vhost": true,
        ...
      }
    }

#### Error Responses

No error responses specific to this request are expected.

### GET /v1/environments/\<environment\>/classes/\<name\>

Retrieve the class with the given name in the given environment.

#### Response Format

If the class exists, the response will be a class object as described above, in JSON format.

#### Error Responses

If the class with the given name cannot be found, the server will return a 404 Not Found response with an empty body.
