.. _introduction:

==========================
Introduction to Ghostferry
==========================

Ghostferry is a library that enables you to copy data from one MySQL instance
to another with minimal amount of downtime. This is accomplished by tailing
and replaying the binlog while the data is being copied by a separate process.

Ghostferry is a library rather than an application. The decision to make it so
is because Ghostferry has the capability to selectively filter data to copy
[#fselective]_. The filtering could be arbitrarily complex and thus cannot be
easily expressed in some configuration file.

That said, there is a generic tool known as ``ghostferry-copydb`` that will copy
whole schemas from one MySQL to another without the basic database/table name
filtering.

Ghostferry is inspired by Github's `gh-ost <https://github.com/github/gh-ost>`_.
However, instead of copying data from and to the same database, Ghostferry
copies data from one database to another.

.. [#fselective] The selective data copying capability is not ready yet,
                 although the work to enable it is not very difficult.

Why do I need this?
===================

Traditionally, there are several approaches to move data from one database
to another:

1. Using mysqldump, one can dump the data on the source database, restore it
   on the target database, and then replay the binlogs.

  - This is a very manual process that is slow and error prone.
  - mysqldump will take a long time on larger tables. It will hold a
    transaction open for the duration of the dump, which can cause a large
    amount of disk space to be used, along with potential increased IO load.

2. Using some sort of backup and restore system, and then setup replication
   between the source and target database, then do a failover.

  - This is not always possible as servers with GTID enabled cannot replicate
    from servers without GTID enabled as of MySQL 5.7.

Furthermore, it is not possible to move a subset of the data filtered by some
arbitrary `WHERE` condition, as it is not possible to setup MySQL replication
like this.
