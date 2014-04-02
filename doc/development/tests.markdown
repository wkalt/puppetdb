# Running Node Classifier Tests

## Clojure Tests

The majority of the Node Classifier's tests are unit and acceptance tests written in clojure.
All of the tests can be run with `lein test :all`, and subsets can be run using the following test selectors:
  * `:acceptance`: run the acceptance-level tests, which test a classifier instance isolated in a subprocess from the perspective of an API consumer.
  * `:database`: run tests of the storage layer's interaction with the database.
  * `:unit`: run all unit tests (i.e. those not marked with `:acceptance` or `:database`).
The `:acceptance` and `:database` selectors require your machine to be running postgres.
The tests require a database and a user with permissions to create, drop, and alter tables in said database to be configured.
The default values for the database, user, and password are all `classifier_test`, and can be altered by setting the `CLASSIFIER_DBNAME`, `CLASSIFIER_DBUSER`, and `CLASSIFIER_DBPASS` environment variables.

## Beaker Integration Tests (PuppetLabs Employees Only)

The integration tests to test Puppet-NC interaction are run by [beaker][beaker-repo].
Currently, the tests install the classifier using packages that beaker pulls down from PuppetLabs' local CI infrastructure, so it is not possible for anyone outside of PuppetLabs to run the integration tests.
We are planning to have the capability to install the classifier from source in order to allow others to run these tests, but that'll be in, you know, "The Future."
In the meantime, if you have access to the PuppetLabs LAN, then first make sure that you have the packaging-related rake tasks available by running
```
rake package:bootstrap
```
In order to test your changes, you need to first build a package against a git commit containing the changes you want to test.
Note that this commit doesn't have to exist anywhere besides your local repo.
With the commit in question checked out, run
```
rake pl:jenkins:uber_build
```


[beaker-repo]: https://github.com/puppetlabs/beaker
