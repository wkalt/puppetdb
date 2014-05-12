---
title: "Node Classifier 1.0 >> API >> v1 >> The PE Console Rake API"
---

# How Node Classifier Endpoints Correspond to the PE Console Rake API

The Node Classifier's REST API will provide a more flexible alternative to the [Puppet Enterprise Console's rake API](http://docs.puppetlabs.com/pe/3.2/console_rake_api.html). This page provides a mapping between PE Console rake tasks and the Node Classifier REST endpoints that most closely replicate their functionality.

- [Node Tasks: Getting Info](http://docs.puppetlabs.com/pe/3.2/console_rake_api.html#node-tasks-getting-info)

All of the available information for a given node is available at the [classification](classification.markdown) endpoint. It's not possible to retrieve a list of nodes from the Node Classifier, so you may need to query the PuppetDB API's [nodes endpoint](http://docs.puppetlabs.com/puppetdb/latest/api/query/v3/nodes.html) if that's part of your existing workflow.

- [Node Tasks: Modifying Info](http://docs.puppetlabs.com/pe/3.2/console_rake_api.html#node-tasks-modifying-info):

These tasks are generally no longer applicable due to the switch to rules-based classification. The [groups endpoint](groups.markdown) makes it possible to update the rules that classify nodes.

- [Class Tasks: Getting Info](http://docs.puppetlabs.com/pe/3.2/console_rake_api.html#class-tasks-getting-info):

The [classes endpoint](classes.markdown) returns information about all of the known classes within a given environment.

- [Class Tasks: Modifying Info](http://docs.puppetlabs.com/pe/3.2/console_rake_api.html#class-tasks-modifying-info)

These tasks are longer applicable. Puppet is now the authoritative source for class information for the Node Classifier, which will query puppet periodically to update its class information.

- [Group Tasks: Getting Info](http://docs.puppetlabs.com/pe/3.2/console_rake_api.html#group-tasks-getting-info)

All of the available information about groups is available from the [groups endpoint](groups.markdown).

- [Group Tasks: Modifying Info](http://docs.puppetlabs.com/pe/3.2/console_rake_api.html#group-tasks-modifying-info)

The switch to rules-based classification means that most of these tasks are no longer applicable. That said, the [groups endpoint](groups.markdown) is where groups (and rules) are defined.
