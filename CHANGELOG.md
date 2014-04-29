### 0.2.4
 * Added group descriptions
 * Allow structured data for parameter values
 * Added an endpoint to validate groups without saving changes
 * Added an endpoint to translate rules to puppetdb queries
 * Fixed a bug where updating a group without changing the environment
   always changed it to `production`
 * Fixed a bug where it was impossible to update the root group
 * Made the root group's rules immutable

