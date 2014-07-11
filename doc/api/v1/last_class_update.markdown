---
title: "Node Classifier 1.0 >> API >> v1 >> Classes"
---

## Last Class Update Endpoint

### GET /v1/last-class-update

Retrieve the time that classes were last updated from the puppet master.

### Response

The response will always be an object with one field, `last_update`. If there has been an update the value of `last_update` will be the time of the last update in ISO8601 format. If the classifier has never updated from puppet it will be null.
