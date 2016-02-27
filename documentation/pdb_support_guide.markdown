---
title: "PuppetDB 3.2: Support and Troublshooting Guide
layout: default
---

[commands]: https://docs.puppetlabs.com/puppetdb/latest/api/command/v1/commands.html#list-of-commands
[threads]: https://docs.puppetlabs.com/puppetdb/latest/configure.html#threads
[erd]: https://docs.puppetlabs.com/puppetdb/latest/erd.html

## Support and Troubleshooting for PuppetDB

This document aims to be a technical guide for troubleshooting PuppetDB (PDB)
and understanding its internals.

## PDB Architectural Overview

Data stored in PuppetDB flows through four distinct components: the terminus, a
message queue, the actual application, and the database. From an ordering
perspective, it goes terminus -> application -> queue -> application ->
database. When thinking about PuppetDB abstractly it may be useful to consider
the terminus, application plus queue, and database as residing on three
separate machines.

### terminus
The terminus is a ruby plugin that is installed on the Puppet master and serves
to redirect agent/client data to PuppetDB in the form of "commands". PuppetDB
has four commands, as described in the [commands documentation][commands].

### queue
When the terminus sends a command to the PDB application, the command is
immediately dumped to an ActiveMQ message queue on disk, essentially
unprocessed. The command sits on the queue until it is processed, at which
point it is sent to the database. Note that commands may be processed out of
order, though an approximate ordering can generally be assumed.

### command processing
Queue processing is handled by a set of concurrently running threads, the
number of which is defined [in configuration][threads]. When a command is
pulled off the queue, it will be parsed as JSON and modified for storage in the
database. Depending on which type of command it is, there may also be some
chatter with the database to determine whether storing the whole command is
needed.

* `store-report`: store-report commands are the least expensive of the three.
  Storing a report is mainly a matter of inserting a row in the reports table.
* `replace-catalog`: When a replace-catalog command is received, PuppetDB will
   first check if a more recent catalog already exists for the node in the
   database. If so, the catalog is discarded and no action is taken. If not, PDB
   will perform a diff of the catalog in hand and the catalog in the database, and
   insert only the resources and edges that have changed.
* `replace-facts`: At a high level, PuppetDB stores facts as key-value
  associations between "paths" and "values". The term "path" refers to a
  specific path from the root of a tree to a leaf value, and "value" refers to
  the leaf value itself. Conceptually, every fact is stored as a tree. To
  illustrate, the fact

      "foo" => "bar"

  is stored as

      "foo" => "bar"

  while the fact

      "foo" => {"a" => "bar", "b" => "baz"}

  is stored as

      "foo#~a" => "bar"
      "foo#~b" => "baz"

  For the array case, the fact

      "foo" => ["bar", "baz"]

  is stored as

      "foo#~0" => "bar"
      "foo#~1" => "baz"

  The same rules apply recursively for larger structures. When a replace-facts
  command is received, PDB will compare the fact in hand against the fact paths
  and values in the database, add whatever new paths/values are required, and
  delete any pairs that have become invalidated.

* `deactivate-node`: The deactivate-node command just updates a column in the
  certnames table, and isn't likely to be the source of performance issues.

### database

PuppetDB uses PostgreSQL. The only good way to get familiar with the schema is
to examine an [erd diagram][erd] and investigate for yourself on a running
instance via psql. The PuppetDB team is available in #puppet and #puppet-dev on
freenode to answer any questions.

## PDB Diagnostics

When any issue is encountered with PuppetDB, the first priority should be
collecting and inspecting the following:

* PuppetDB logs
* PostgreSQL logs
* Screenshot of PuppetDB dashboard
* PostgreSQL table, index, and database sizes
* atop output on PDB system

### PuppetDB logs
Search the PuppetDB logs for recurring errors that line up with the timing of
the issue. Some common errors that may show in the PDB logs are:
* database constraint violations: These will appear from time to time in most
  installs due to concurrent command processing, but if they occur frequently
  and in relation to multiple nodes it usually indicates an issue. Note that
  every time one of these is hit, the command will be retried 16 times over a
  period of a day or so, so don't get distracted by the same command retrying
  repeatedly.

* 

PuppetDB will occasionally throw an error during normal operation,
