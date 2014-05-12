---
title: "Node Classifier 1.0 >> API >> v1 >> Endpoints"
---

# Node Classifier v1 API Endpoints

This page lists the endpoints for the Node Classifier v1 API. To see how these endpoints correspond with the Puppet Enterprise Console's rake API, see [this page](console-rake-api.markdown).

## Classes

The `classes` endpoint is used to retrieve a list of known classes within a given environment. The output from this endpoint is especially useful for creating new groups, which require at least one class to be specified.

The Classifier gets its information about classes from Puppet, so this endpoint should not be used to create, update, or delete them.

>See the [classes endpoint](classes.markdown) page for detailed information.

## Classification

The `classified` endpoint takes a node name and returns information about how that node should be classified. The output can help you test your classification rules.

>See the [classification endpoint](classification.markdown) page for detailed information.

## Environments

The `environments` endpoint returns information about environments. The output will either tell you which environments are available or whether a named environment exists. The output can be helpful when creating new groups, which must be associated with an environment.

The Classifier gets its information about environments from Puppet, so this endpoint should not be used to create, update, or delete them.

>See the [environments endpoint](environments.markdown) page for detailed information.

## Groups

The `groups` endpoint is used to create, read, update, and delete groups. A group belongs to an environment, applies classes (possibly with parameters), and matches nodes based on rules. Because groups are so central to the classification process, this endpoint is where most of the action is.

>See the [groups endpoint](groups.markdown) page for detailed information. To validate a group object without modifying the database in any way, use the [validate](validate.markdown) endpoint.
