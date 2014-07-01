---
title: "Node Classifier 1.0 >> API >> v1 >> Update Classes"
---

## update-classes endpoint

### POST /v1/update-classes

Trigger the classifier to update class and environment definitions from the puppet master.

#### Response

A successful update will return a 201 response with an empty body.

#### Error Responses

If the puppet master returns an unexpected status to the classifier, a 500 Server Error response will be returned, with a JSON body with standard error structure as described in the [errors documentation](errors.markdown). The `kind` key will contain "unexpected-response". The `msg` key will contain a description of the error returned. The `details` key will contain a further JSON object, which has `url`, `status`, `headers`, and `body` keys describing the response the classifier received from the puppet master.
