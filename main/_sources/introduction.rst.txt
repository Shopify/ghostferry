.. _introduction:

==========================
Introduction to Ghostferry
==========================

Ghostferry is a library that enables you to copy data from one MySQL instance
to another with minimal amount of downtime. This is accomplished by tailing
and replaying the binlog while the data is being copied by a separate process.

Ghostferry is a library rather than an application. The decision to make it so
is because Ghostferry has the capability to selectively filter data to copy.
The filtering could be arbitrarily complex and thus cannot be easily expressed
in some configuration file.

That said, there is a generic tool called ``ghostferry-copydb`` that will copy
tables and the data contained in them from one MySQL to another with only the
basic database/table name filtering.

Ghostferry is inspired by Github's `gh-ost <https://github.com/github/gh-ost>`_.
However, instead of copying data from and to the same database, Ghostferry
copies data from one database to another.

Why do I need this?
===================

Traditionally, moving data from one database to another involves some sort of
backup and restore along with replaying the changes via replication. Backup is
traditionally done via mysqldump or Percona Xtrabackup. It may not be possible
to use either of these methods if holding a transaction for a very long time is
not feasible (mysqldump) or if the filesystem of the database host is not
available (for Perconal Xtrabackup), such as in the case of cloud provided
database as a service.

The traditional process can only move a single table at minimum. Ghostferry
allows you to use the filtering capability to move rows subject to a custom
constraint.

Additionally, traditional tools present themselves as a complicated process
that require a lot of manual intervention from a reasonably experienced
database administrator. Ghostferry provides a single binary solution that can
be operated by most without in depth knowledge of MySQL.
