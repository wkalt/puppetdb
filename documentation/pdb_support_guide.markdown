---
title: "PuppetDB 3.2: Support and Troublshooting Guide
layout: default
---

[commands]: https://docs.puppetlabs.com/puppetdb/latest/api/command/v1/commands.html#list-of-commands
[threads]: https://docs.puppetlabs.com/puppetdb/latest/configure.html#threads
[erd]: https://docs.puppetlabs.com/puppetdb/latest/erd.html
[pdb-880]: https://tickets.puppetlabs.com/browse/PDB-880
[pgstattuple]: http://www.postgresql.org/docs/9.4/static/pgstattuple.html
[pgtune]: https://github.com/gregs1104/pgtune
[postgres-config]: http://www.postgresql.org/docs/current/static/runtime-config-resource.html

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
number of which is defined [in configuration][threads].  When a command is
pulled off the queue, it will be parsed as JSON and modified for storage in the
database. Depending on which type of command it is, there may also be some
chatter with the database to determine whether storing the whole command is
needed.

* `store-report`: store-report commands are the least expensive of the three.
  Storing a report is mainly a matter of inserting a row in the reports table.
* `replace-catalog`: When a replace-catalog command is received, PuppetDB will
  first check if a more recent catalog already exists for the node in the
  database. If so, the catalog is discarded and no action is taken. If not, PDB
  will perform a diff of the catalog in hand and the catalog in the database,
  and insert only the resources and edges that have changed.
* `replace-facts`: At a high level, PuppetDB stores facts as key-value
  associations between "paths" and "values". The term "path" refers to a
  specific path from the root of a tree (e.g structured fact) to a leaf value,
  and "value" refers to the leaf value itself. Conceptually, every fact is
  stored as a tree. To illustrate, the fact

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
instance via the psql interactive console. The PuppetDB team is available for
questions on the mailing list and in #puppet and #puppet-dev on freenode to
answer any questions.

## PDB Diagnostics

When any issue is encountered with PuppetDB, the first priority should be
collecting and inspecting the following:

* PuppetDB logs (puppetdb.log, puppetdb-daemon.log, .hprof files in the log
  directory)
* PostgreSQL logs
* Screenshot of PuppetDB dashboard
* PostgreSQL table, index, and database sizes
* atop output on PDB system

### PuppetDB logs
Search the PuppetDB logs for recurring errors that line up with the timing of
the issue. Some common errors that may show in the PDB logs
are:
* database constraint violations: These will appear from time to time in most
  installs due to concurrent command processing, but if they occur frequently
  and in relation to multiple nodes it usually indicates an issue. Note that
  when a command fails to process due to a constraint violation, it will be
  retried 16 times over a period of a day or so, with the retry count displayed
  in the log.
* ActiveMQ/KahaDB errors: PuppetDB can occasionally get into a state where
  ActiveMQ becomes corrupt and the queue itself needs to be deleted. This can
  be caused by a failed PDB upgrade, a JVM crash, or running out of space on
  disk, among other things. If you see frequent errors in the logs related to
  ActiveMQ, you try stopping PuppetDB, moving the mq directory somewhere else,
  and restarting PuppetDB (which will recreate the mq directory). Note that
  this message:

      2016-02-29 15:20:53,571 WARN  [o.a.a.b.BrokerService] Store limit is
      102400 mb (current store usage is 1 mb). The data directory:
      /home/wyatt/work/puppetdb-mq-tmp/mq/localhost/KahaDB only has 71730 mb of
      usable space. - resetting to maximum available disk space: 71730 mb

  is harmless noise and does not indicate an issue. Users can make it go away
  by lowering the store-usage or temp-usage settings in their PuppetDB
  configuration.

  Note additionally that 2.x versions of PuppetDB will sometimes throw harmless
  AMQ noise on shutdown. If you are on 2.x and the errors you're seeing occur
  on shutdown only, you are probably running into [PDB-880][PDB-880] and your
  problem likely lies somewhere else.

* Out of memory errors: PuppetDB can crash if it receives a command too large
  for its heap. This can be trivially fixed by raising the Xmx setting in the
  JAVA_ARGS entry in /etc/sysconfig/puppetdb on redhat or
  /etc/defaults/puppetdb on Debian derivatives. Usually though, crashes due to
  OOMs indicate that PDB is getting used in ways that it should not be, and
  it's important to identify and inspect the commands that cause the crash to
  figure out whether there is some misuse of Puppet that can be corrected. The
  most common causes of OOMs on command processing are blobs of binary data
  stored in catalog resources, huge structured facts, and large numbers of log
  entries within a report. Out of memory errors should generate a heap dump
  suffixed with .hprof in the log directory, which should contain the offending
  command.

### PostgreSQL logs
Have the following settings or sensible equivalents enabled in postgresql.conf
prior to log examination:

    log_line_prefix = '%m [db:%d,sess:%c,pid:%p,vtid:%v,tid:%x] '
    log_min_duration_statement = 5000

Check the postgres logs for:

    * Postgres errors (marked "ERROR") coming from the puppetdb database
    * Slow queries

Slow queries are the main concern here, since most errors here have already
been seen in the PuppetDB logs. Slow queries frequently occur on deletes
related to garbage collection and in queries against event-counts and
aggregate-event-counts. Garbage collection deletes only run periodically, so
some degree of slowness there is not generally an issue.

For slow queries connected to queries against PDB's REST API, the two most
common exacerbators are insufficient memory allocated to PostgreSQL and table
bloat. In either case, the first step should be to copy the query from the log
and get the plan Postgres is choosing by looking at the output of `explain
analyze <query>;` in psql. This will tell you which tables the query is
spending the most time on, after which you can look for conspicuous bloat using
the pgstattuple module:

    create extension pgstattuple;
    select * from pg_stat_tuple('reports'); -- (in the case of reports)

Familiarize yourself with the pgstattuple module with the [postgres
documentation][pgstattuple].

On the memory usage side, the main tipoff will be queries with explain plans
that mention sorts on disk. To examine your options for optimization, you might
try running [pgtune][pgtune] against your postgresql.conf and examining the
configuration it chooses. Note that the pgtune output is likely not perfect for
your needs, as it assumes that PostgreSQL is the only application running on
your server.  It should be used as a guide rather than a prescription. The most
meaningful options to look at are typically `work_mem`, `shared_buffers`, and
`effective_cache_size`. Consult the [PostgreSQL documentation][postgres-config]
for information on these settings.

### PuppetDB dashboard screenshot

There are a few things to watch for in the PuppetDB dashboard:

* Deep command queue: Under sustainable conditions, the command queue should be
  in the neighborhood of 0-100 most of the time, with occasional spikes
  allowed. If the command queue is deeper than 10,000 for any extended period
  of time, then commands are being processed too slow. Causes of slow command
  processing include:

  - large, particularly array-valued, structured facts
  - insufficient hardware

  Large array-valued structured facts are problematic because as illustrated in
  the 


