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

