### 0.7.3
 * Fix bug causing a 500 in the /v1/classes endpoint when access-control was enabled.
 * Fix bug where groups were not annotated when access-control was enabled.

### 0.7.2
 * Fix bug when triggering class updates

### 0.7.1
 * Fix bug when creating groups with RBAC
 * Fix bug where liberator representation clashed with other services
 * Fix bug in /group/:id

### 0.7.0
 * RBAC integration. Authentication uses whitelisted client certs, authorization is handled by RBAC service.

### 0.6.5
 * Separate config for client ssl certificates
 * Fix a bug with groups not being annotated
 * Coerce booleans and numbers to strings in equality tests
 * Use webrouting service to configure jetty

### 0.6.4
 * Make it possible to configure the api prefix in the terminus

### 0.6.3
 * Fix updating classes from puppet

### 0.6.2
 * Set the correct content-type header in the classifier's node terminus.

### 0.6.1
 * Update the `puppetlabs/http-client` dependency to 0.2.2

### 0.6.0
 * Change the namespace of the classifier service definition from `puppetlabs.classifier.application` to `puppetlabs.classifier.main`; TK dependents will have to update the name in their .cfg files.

### 0.5.6
 * Fix a bug where groups without rules and their descendents could not be viewed as-inherited.
 * Fix a bug where the `environment_trumps` flag could not be changed from false to true.

### 0.5.5
 * Periodically synchronize classes with Puppet starting at service startup, instead of only synchronizing on-demand.
 * Namespace the database configuration by nesting inside the `classifier` configuration section, rather than putting it at the top level.
 * Change the `dbname` database configuration key to `subname`, to match what it's called by the JDBC.
 * Namespace the webserver configuration by the classifier service id.
 * Fixed a bug where the root group's parent could be changed.
 * Improve the error message for group creation and edit requests that refer to missing classes or class parameters.

### 0.5.4
 * Rules now inherit during classification

### 0.5.3
 * Store the time classes were last updated from puppet
 * Add the ability to import a group hierarchy

### 0.5.2
 * Return a particular 500 error response if Puppet gives us a response we don't understand.
 * Fix a bug where a 404 response from Puppet would cause the classifier to return a general 500 response.
 * Fix a bug that caused spurious 404s when trying to view the inherited version of a group.
 * Fix a terminus bug that would cause Puppet to fail to create a catalog for the node when it first checked in.
 * Change the path to the facts in a node object (and rules) to start with `fact`, not `facts`.

### 0.5.1
 * Allow updates that don't introduce errors. This means it's possible
   to update a group that contains an error, so long as the update
   doesn't introduce the error.
 * Improve response when attempting to delete a group with children.
 * Add an endpoint for enumerating all classes (as opposed to just in an
   environment).

## 0.5.0
 * Namespace the configuration files previously located under `resources/` to avoid conflicting with other TrapperKeeper apps.
 * Change the inherited group view from being at a seperate path to just being a query parameter on the normal group GET endpoint.
 * Add the `environment_trumps` field to groups to allow users to resolve environment conflicts at the expense of some safety.
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

