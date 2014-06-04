# 0.5.0
 * Namespace the configuration files previously located under `resources/` to avoid conflicting with other TrapperKeeper apps.
 * Change the inherited group view from being at a seperate path to just being a query parameter on the normal group GET endpoint.
 * Add the 'environment_trumps' field to groups to allow users to resolve environment conflicts at the expense of some safety.
 * If group rules are malformed, instruct the user to consult the documentation, rather than showing them a cryptic error.
 * All JSON payloads now consistently use underscores as word seperators in their keys.
 * Fixed a bug where the terminus would fail if the 'classifier.yaml' config file was not present; now, defaults are used instead.

## 0.4.0
 * Add an endpoint to explain classification (given facts submitted with the request).
 * Add an endpoint to view a group with its inherited values.
 * Store classifications in node check-ins.
 * Add integration tests against the PE ecosystem.
 * Allow integration tests to run classifier on a different box from Puppet.

### 0.3.3
 * Fixed a bug where classes without parameters could not be added to a group.
 * Fixed a bug where changing a class parameter's default value in Puppet would cause later operations that used the class's default value to error out.
 * Added a new node check-in history endpoint at /v1/nodes (see doc/api/v1/nodes.markdown).
 * Added support for Puppet transaction UUIDs in node check-ins.
 * Added PE database configuration instructions to the README.

### 0.3.2
 * Fixed a bug where the Location header of redirects did not include the service's url prefix.

### 0.3.1
 * Fixed a bug where groups didn't work properly if a url prefix was set

## 0.3.0
 * The configuration format has changed to HOCON and the included
   configuration file renamed classifier.conf. Ini files will still work
   but this is now the preferred format.
 * Updated README and included classifier.conf to show how to set up SSL
   and interact with puppet
 * Allow groups to not have rules
 * The root group now matches all nodes

### 0.2.4
 * Added group descriptions
 * Allow structured data for parameter values
 * Added an endpoint to validate groups without saving changes
 * Added an endpoint to translate rules to puppetdb queries
 * Fixed a bug where updating a group without changing the environment
   always changed it to `production`
 * Fixed a bug where it was impossible to update the root group
 * Made the root group's rules immutable

