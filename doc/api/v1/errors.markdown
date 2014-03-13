---
title: "Node Classifier 1.0 >> API >> v1 >> Errors
---

## Error Responses

Every non-500 error response from the classifier service will be a JSON response.
Each such response will be an object containing the following keys:

* `kind`: A string classifying the error.
          It should be the same for all errors that have the same kind of thing in their `details` key.
* `msg`: A human-readable message describing the error, suitable for presentation to the user.
* `details`: Additional machine-readable information about the error condition.
             The format of this key's value will vary between kinds of errors but will be the same for any given error kind.
