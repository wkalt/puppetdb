# Classifier

A service for classifying puppet nodes based on user-defined rules.

# Getting Started

NB: This is in an early stage of development. These instructions are
really only useful if you want to develop on the classifier.

To run the classifier you will need

* Postgres
* A JVM
* [Leiningen](http://leiningen.org)

Create a postgres database named "classifier" with a role of the same
name and "classifier" as the password. These will be configurable soon.

This can be done as a postgres superuser (usually `su - postgres` on a
default installation) with the following commands:

```
createuser -d classifier -P
createdb classifier -U classifier
```

Then you can start the service with
```
lein run --config ext/classifier.ini
```

You can create a node and see that it exists:

```
% curl localhost:8080/v1/nodes/test
Resource not found.%

% curl -X PUT localhost:8080/v1/nodes/test
{"name":"test1"}%

% curl localhost:8080/v1/nodes/test
{"name":"test1"}%
```

Groups and classes can be created in the same manner. Most of their
properties haven't been fleshed out yet. Rules are also coming soon, at
which point this will be more useful.
