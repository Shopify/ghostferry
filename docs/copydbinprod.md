<a name="copydbinprod"></a>

# Running `ghostferry-copydb` in production

Assuming you have gone through [Tutorial for ghostferry-copydb](tutorialcopydb.md), you probably want to run
`ghostferry-copydb` in production. The general workflow is relatively
similar, with some differences. You should keep the tutorial as a starting
point for your own playbook as most steps will largely be the same.

## Prerequisites

Before you start, you need to know if you can even use Ghostferry. Some points
to consider about this are:

- Ghostferry on its own does not enable zero downtime moves. The downtime for the
  app will be minimal compared to other methods but still non-zero.

    - Figure out how much downtime you are willing to tolerate. Using Ghostferry,
      one can realistically achieve downtime in the order of seconds.

- The source database must be running with binary logging,
  [ROW based replication](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format)
  and [FULL row images](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_row_image).

    - Without this, it is not possible to run Ghostferry safely and Ghostferry
      will error out during initialization if `binlog_format` is not `ROW` or
      `binlog_row_image` is not `FULL`.
    - With target verification enabled (the default), the source must also have
      `binlog_rows_query_log_events=ON`, and the target needs binary logging with
      `binlog_rows_query_log_events=ON`.

- Every table to be copied has one pagination column with unique, non-NULL
  values: by default its single-column primary key, otherwise a column
  configured with `CascadingPaginationColumnConfig`. Integer columns and
  binary-comparable string columns are supported. See
  [Limitations](technicaloverview.md#limitations) for the exact selection
  order, supported types and boundary values.

- There are no foreign key constraints in your tables.

    - Tables with foreign key constraints are rejected by default. You should
      remove these constraints before running Ghostferry.
    - `SkipForeignKeyConstraintsCheck: true` bypasses the check but does not
      make the migration correct, and `TablesToBeCreatedFirst` only orders table
      creation on the target.

- `ghostferry-copydb` can only copy a whole table at a time.

    - If you need to copy a subset, use ghostferry as a library to build your own
      application.

- The source is either the writer (master) itself, or a replica configured with
  `RunFerryFromReplica`; see [Running from a replica](#running-from-a-replica).

<a name="prodtesting"></a>

## Testing Ghostferry with Production Data

You can run Ghostferry without running the cutover to test the entire flow
without actually moving your database. This allows you to verify that the move
will indeed work with your setup. Additionally, the target location where you
performed this test move can be used by a staging version of your app to verify
that the target MySQL will not cause trouble for your app, especially if the
target MySQL has different version/configurations.

If you want to rehearse the entire flow including the cutover, use an isolated
setup rather than a live production writer: a writer with a replica that
replicates from it, and a separate target. Run Ghostferry from the replica as
described in [Running from a replica](#running-from-a-replica) and follow the
same cutover procedure as in production, including verification. Do not simply
stop replication on the replica as a substitute for stopping writes: Ghostferry
waits for the replica to catch up to the writer during cutover.

A rehearsal measures compatibility and downtime. It is not evidence that the
data of a different, later run is correct.

## Running from a replica

To copy from a replica, set in the copydb configuration:

- `RunFerryFromReplica: true`;
- `SourceReplicationMaster`: connection settings of the upstream writer;
- `ReplicatedMasterPositionQuery`: a query executed on the replica that returns
  one row with the writer's binlog file name (string) and position (integer)
  up to which the replica has applied replication, for example from a
  heartbeat table written only by the writer (such as `meta.heartbeat` in
  `examples/copydb/run-on-replica.conf.json`, or a pt-heartbeat table);
- optionally `WaitForReplicationTimeout`, a duration string such as `"5m"`.
  Empty means Ghostferry effectively waits without limit.

The example configuration only shows these fields: the repository's compose
file does not set up replication or a heartbeat table for you. The source
replica must log replicated updates (`log_replica_updates` /
`log_slave_updates`), because Ghostferry reads the replica's own binlog.

During cutover, keep replication running. Stop application writes on the
upstream writer, then allow cutover. Ghostferry reads the writer's current
binlog position, waits until `ReplicatedMasterPositionQuery` on the replica
reports that position or later, and then drains the replica's own binlog.
**Keep the upstream writer at `read_only=OFF`** and stop writes by other means:
Ghostferry checks `@@read_only` on `SourceReplicationMaster` and aborts if it is
`ON`, treating it as a writer that has been demoted to a replica.

## To Verify Or Not To Verify

Ghostferry has built-in data verifiers, selected with `VerifierType`. They are
designed to give you certainty that the data of the source and the target are
identical after a move and nothing was corrupted/missed, within the scope of
the selected verifier. Independently, the TargetVerifier monitors the target
for unexpected writes by default; it does not compare data and is no substitute
for a final data verification. See the [Verifiers](verifiers.md) page for more
details on what they are and how to choose a verifier.

Source writes continue normally while Ghostferry copies data. Ghostferry
writes to the target throughout the run, so no other writer may modify the
copied tables on the target. Application writes must be stopped for the final
binlog drain and the final verification, and stay stopped until they are
routed to the target. The verifiers have different downtime characteristics:
`ChecksumTable` scans every copied table during this window, `Inline` only
reverifies rows changed since the copy.

In order to know how much downtime you will incur during the verification
process, you can measure it with the rehearsal described in
[Testing Ghostferry with Production Data](#prodtesting). During the cutover
stages, run verification as normal and measure the time taken. A successful
rehearsal does not make verification of the production run unnecessary: it
says nothing about the data copied by a different run.

ghostferry-copydb does not run the final verification automatically. After the
state is `done` (source binlog streaming has stopped and copydb has already
stopped the TargetVerifier and called `CutoverUnlock`), click Run Verification
in the web UI and wait until Verified Correct is `true` with no error before
allowing application writes to the target. `CutoverUnlock` is therefore not a
signal that the data has been verified. `CutoverLock` and `CutoverUnlock` are
HTTP callbacks that copydb calls at the start and end of cutover;
`ControlServerConfig.CustomScripts` adds separate buttons in the UI to run
scripts on demand. Allowing automatic cutover does not stop application writes
by itself.

It is also possible to run an additional verification after a move and
possibly address any issues after the fact:

1. Setup a slave of the source database.
2. Setup a slave of the target database.
3. Run ghostferry as normal between the master source and target database.
4. During the cutover, also stop replication to the slaves setup in step 1 and
   2.
5. Manually compare the table on the slaves using something like `CHECKSUM TABLE`.

## Dealing with Errors and Restarting Runs

It is possible for Ghostferry to encounter an unrecoverable error (such as a
network partition with the database). In these scenarios, the target will be
left alone as the Ghostferry process panics and quits. It may be possible to
resume these runs using the experimental interrupt & resume feature, which
keeps the tables already created on the target. See
[Interrupt and resuming `ghostferry-copydb`](copydbinterruptresume.md).

If the resume doesn't work, you can start a brand new Ghostferry run. For
copydb, use an empty target, or remove only the tables created by the failed
run (after making sure that nothing else uses them). A fresh copydb run uses
`CREATE DATABASE IF NOT EXISTS` but plain `CREATE TABLE`, so it fails if a
copied table already exists. `AllowExistingTargetTable: true` changes this to
`CREATE TABLE IF NOT EXISTS` without validating the existing schema or data; it
is not an automatic recovery mechanism.

## Configuration for `ghostferry-copydb`

The configuration for `ghostferry-copydb` is a JSON file passed as the
positional argument (`ghostferry-copydb [options] conf.json`); unlike
`ghostferry-sharding`, copydb does not read its configuration from stdin. The
schema it is based on the
[Config struct of ghostferry](https://pkg.go.dev/github.com/Shopify/ghostferry#Config),
embedded in copydb's configuration, with some differences:

- You cannot specify `TableFilter` and `CopyFilter`.
- Data verification is selected with `VerifierType` of the embedded ghostferry
  Config.
- The web UI location is `ControlServerConfig.WebBasedir` (the top-level
  `WebBasedir` field is deprecated). If you are using the debian package, you
  don't need to specify it as it is compiled into the binary.

It also allows you specify some options according to fields defined by the
[Config struct of copydb](https://pkg.go.dev/github.com/Shopify/ghostferry/copydb#Config),
such as `Databases` and `Tables` to filter and rename the databases/tables to
copy, and the replica settings above. The API documentation is versioned; for
the `main` branch, the configuration structs in this repository are
authoritative.
