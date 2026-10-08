<a name="introduction"></a>

# Introduction to Ghostferry

Ghostferry is a library that enables you to copy data from one MySQL instance
to another with minimal amount of downtime. This is accomplished by tailing
and replaying the binlog while the existing data is being copied by concurrent
goroutines in the same Ghostferry process.

Ghostferry is a library rather than an application. The decision to make it so
is because Ghostferry has the capability to selectively filter data to copy.
The filtering could be arbitrarily complex and thus cannot be easily expressed
in some configuration file.

That said, there is a generic tool called `ghostferry-copydb` that will copy
tables and the data contained in them from one MySQL to another with only the
basic database/table name filtering.

Ghostferry is inspired by Github's [gh-ost](https://github.com/github/gh-ost).
However, instead of copying data from and to the same database, Ghostferry
copies data from one database to another.

## Why do I need this?

Traditionally, moving data from one database to another involves some sort of
backup and restore along with replaying the changes via replication. Backup is
traditionally done via mysqldump or Percona Xtrabackup. It may not be possible
to use either of these methods if holding a transaction for a very long time is
not feasible (mysqldump) or if the filesystem of the database host is not
available (for Percona Xtrabackup), such as in the case of cloud provided
database as a service.

With backup/restore plus replication, any row-level filtering applied to the
initial copy is not carried over to the replication stream, whose filters work
on whole databases or tables. Ghostferry lets an application supply a custom
`CopyFilter`: its `BuildSelect` restricts the rows read during the bulk copy and
its `ApplicableEvent` decides which streamed binlog changes apply, so the same
application-specific filter covers both the initial copy and the ongoing
changes.

Additionally, traditional tools present themselves as a complicated process
that require a lot of manual intervention from a reasonably experienced
database administrator. Ghostferry provides a single binary solution that can
be operated by most without in depth knowledge of MySQL.
