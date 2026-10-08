<a name="technicaloverview"></a>

# Technical Overview

Ghostferry is a Go library to move data from one MySQL instance to another
while the source (and possibly the target) databases are online. In order to do
this, Ghostferry must be able to copy the data from a source database to a
target database while keeping track of all changes in the source database to
apply them to the target database. This is implemented by SELECTing from the
source database and INSERTing them into the target (copy) and applying the
binlog changes from the source on the target (change synchronization).

This setup has the advantage of working with essentially any configuration of
MySQL. It also provides a seamless process as it is all contained in a single
package, as opposed to split between multiple applications
(mysqldump/xtrabackup and MySQL replication).

It is important to note that Ghostferry is simply a part of the puzzle of a
live database migration. For data integrity reasons, Ghostferry mandates that
you stop writes to the dataset you are copying at a stage of execution called
cutover. During the same stage of execution, you'll also likely need to
instruct any applications accessing the source database to access the target
database instead.

To gain a better understanding of the overall process, let's take a look how
Ghostferry works:

1. `Ferry.Start` records the source's current binlog coordinates (and, unless
   target verification is disabled, the target's) before any rows are read, so
   no change made during the copy is missed.
2. `Ferry.Run` starts the background tasks: the binlog streamer pulls events
   from the source from the recorded coordinates and applies those applicable
   to the target, while the data iterator SELECTs rows from the source and
   INSERTs them into the target, table by table.
3. Ghostferry finishes copying all data from the source to the target. The
   binlog apply operation of (2) continues in the background.
4. `Ferry.Run` waits until cutover is allowed (`AutomaticCutover`), runs the
   selected verifier's `VerifyBeforeCutover`, if any, and then notifies the
   application that the row copy is complete (`WaitUntilRowCopyIsComplete`
   returns) while binlog streaming continues.
5. The application waits until the binlog apply is close to caught up to the
   latest available position on the source database
   (`WaitUntilBinlogStreamerCatchesUp`), so that cutover does not start with
   a large backlog of binlog entries, thereby reducing the downtime required.
   copydb then calls the `CutoverLock` callback, if configured.
6. Writes to the copied dataset on the source must now be stopped, via either
   a read_only flag (for whole database copies) or some sort of application
   level lock (for partial database copies), with in-flight transactions
   finished. The application then calls `FlushBinlogAndStopStreaming`:
   Ghostferry records the source's current binlog position, applies all events
   up to it and stops, and `Ferry.Run` returns. copydb runs steps 4 to 6
   without pausing once Allow Automatic Cutover is clicked in the web UI, so
   with copydb the writes must be stopped before clicking it, or by the
   `CutoverLock` callback, which copydb calls and waits on before draining.
7. The application stops the target verifier (`StopTargetVerifier`), runs the
   final verification (`VerifyDuringCutover`) if a verifier is used, and only
   then points the application at the target database and enables writes on
   it. copydb calls `StopTargetVerifier` and then the `CutoverUnlock` callback
   (`EndCutover`) automatically, but does **not** call `VerifyDuringCutover`:
   the operator runs it with Run Verification in the web UI. The control
   server and any other application components keep running after
   `Ferry.Run` returns; returning does not mean that the final verification
   has been done.

This process has some downtime between step 6 and step 7. The window of
downtime is proportional to how fast these steps can be done. In most cases
this should be on the order of seconds to minutes.

## Architecture

Ghostferry has three levels of public APIs that you can use: the Ferry level,
the DataIterator/BinlogStreamer level, and the Cursor level. Most of the time
you only need to call methods on the Ferry. The other two levels only come in
handy when you want to implement an alternative Ferry and alternative
verifiers.

There are several auxiliary components to the system: the throttlers, the
verifiers, the control server. These components are optional to Ghostferry
runs.

The main components, all running as goroutines of one process, are:

| Component | Role |
|---|---|
| `Ferry` | Initializes the components, coordinates the lifecycle and overall state, and reports fatal errors to the `ErrorHandler`. |
| `DataIterator` → `Cursor` → `BatchWriter` | Copies tables concurrently (one table per worker goroutine), reading each table in pagination-key order in batches and writing each batch to the target; with the Inline verifier, each batch is checked as it is written. |
| Source `BinlogStreamer` → `BinlogWriter` | Streams the source's binlog from the recorded start position and applies applicable changes to the target, until the stop position recorded at cutover is reached. |
| `InlineVerifier` | Re-verifies the pagination keys of rows changed by streamed binlog events (only when the Inline verifier is selected). |
| Target `BinlogStreamer` → `TargetVerifier` | Streams the target's binlog and fails the run on row changes to copied tables that do not carry Ghostferry's expected SQL annotation, i.e. unexpected writers (unless `SkipTargetVerification` is set). |
| `ControlServer` | Web UI and HTTP endpoints exposing status and actions such as pause, cutover and verification. |

You can see an example of an application built with Ghostferry in the
`copydb` package.

## Limitations

- Each table is paginated by **one column with unique, non-NULL values**. It
  does not need to be an auto-incrementing primary key.

    - Supported column types are integers and binary-comparable strings:
      `BINARY`/`VARBINARY` (for example `BINARY(16)` UUIDs) and `CHAR`/`VARCHAR`
      with a binary collation such as `utf8mb4_bin`. Other types and non-binary
      collations fail schema loading.
    - Numeric pagination keys must be positive integers: the cursor starts at
      zero and selects values greater than the last key, so zero and negative
      values are not supported. Likewise, binary pagination starts from the
      empty value, so empty string/binary keys are not supported. Ghostferry
      does not validate these boundary values.
    - The column is selected in this order:
      `CascadingPaginationColumnConfig.PerTable[database][table]`, otherwise the
      primary key if it consists of a single column, otherwise
      `CascadingPaginationColumnConfig.FallbackColumn`. The fallback applies only
      when there is no single-column primary key (including tables with a
      composite primary key), not when a single-column primary key has an
      unsupported type or collation. A selected column that does not exist or
      has an unsupported type or collation fails schema loading.
    - Tables with a composite primary key can be copied by configuring one
      separate unique column; pagination by multiple columns is not
      implemented. You must guarantee that the configured column is unique
      (preferably with a UNIQUE index): Ghostferry does not check this, and
      duplicate values can cause rows to be skipped at batch boundaries.

    Configuration fragment (not a complete configuration):

    ```json
    "CascadingPaginationColumnConfig": {
      "PerTable": {
        "abc": {
          "table1": "id"
        }
      },
      "FallbackColumn": "id"
    }
    ```

- Ghostferry can only be used on a source database with FULL row-based
  replication.

    - The source must have binary logging enabled with `binlog_format=ROW` and
      `binlog_row_image=FULL`; an error will be emitted during initialization
      otherwise. With target verification enabled (the default), the source must
      also have `binlog_rows_query_log_events=ON`, and the target needs binary
      logging with `binlog_rows_query_log_events=ON` as well.
    - Without FULL RBR, the integrity of the data cannot be guaranteed.

- Tables with foreign key constraints are rejected by default.

    - For tables with foreign key constraints, the constraints should be removed
      before performing the data migration.
    - `SkipForeignKeyConstraintsCheck: true` bypasses the check but does not
      make the migration correct: rows are copied and changes applied without
      regard to the constraints. copydb's `TablesToBeCreatedFirst` only orders
      table creation on the target.

## Algorithm Correctness

The high-level copy algorithm is described by a simplified TLA+ model in the
`tlaplus` directory of the source tree, with a TLC model configuration in
`tlaplus/ghostferry.toolbox`. The model is small and finite and makes the
simplifying assumptions stated at the top of `tlaplus/ghostferry.tla`. Checking
it with TLC is not a proof that the current Go implementation is correct.
