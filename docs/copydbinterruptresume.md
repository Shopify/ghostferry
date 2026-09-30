<a name="copydbinterruptresume"></a>

# Interrupt and resuming `ghostferry-copydb`

*Note that this is a new and experimental feature. Please ensure you test it
thoroughly in your environment to ensure there are no data loss. See the bottom
of this page for important information on caveats of using this feature.*

To capture a state dump when copydb receives SIGINT or SIGTERM (and thus
interrupt & resume), the configuration given to copydb must contain **both**:

```json
"DumpStateOnSignal": true,
"DumpStateToStdoutOnError": true
```

`DumpStateOnSignal` installs the SIGINT/SIGTERM handler, which reports the
signal as a fatal error. `DumpStateToStdoutOnError` makes the default error
handler write the run state as JSON to stdout whenever it reports a fatal
error, whether caused by a signal or by an error within Ghostferry. With only
`DumpStateOnSignal`, a signal stops the run without writing a dump to stdout.
Not every failure produces a usable dump: errors during early initialization
or configuration, and arbitrary panics outside the error handler, do not. A
signal received during cutover is refused (logged and ignored) and the run
continues; after the run is done, a signal exits the process.

Capture stdout in its own file and keep the logs (stderr) separate:

```console
$ ghostferry-copydb -verbose conf.json >state-dump.json 2>ghostferry.log
```

`ghostferry-copydb` should write only the state dump JSON to stdout and all
logs to stderr. Check that `state-dump.json` contains exactly one JSON object;
if not, file a bug report.

The following dump was captured from the tutorial's configuration with
`"VerifierType": "Inline"` and `"DoNotIncludeSchemaCacheInStateDump": true`
(hence `LastKnownTableSchemaCache` is `null`), interrupted with SIGTERM while
waiting for cutover. It is illustrative only: resume from your own, unedited
dump and never paste this example.

```json
{
 "LastSuccessfulPaginationKeys": {
  "abc.table1": {
   "type": "uint64",
   "value": 351,
   "column": "id"
  }
 },
 "GhostferryVersion": "1.3.1+20260929140923+5bbb419",
 "LastKnownTableSchemaCache": null,
 "CompletedTables": {
  "abc.table1": true
 },
 "LastWrittenBinlogPosition": {
  "Name": "mysql-bin.036631",
  "Pos": 276
 },
 "BinlogVerifyStore": {
  "abc": {
   "table1": {}
  }
 },
 "LastStoredBinlogPositionForInlineVerifier": {
  "Name": "mysql-bin.036631",
  "Pos": 276
 },
 "LastStoredBinlogPositionForTargetVerifier": {
  "Name": "mysql-bin.032481",
  "Pos": 276
 }
}
```

Pagination keys are stored as objects with their type, column and value;
binary pagination keys use `"type": "binary"` with the value hex-encoded.

To resume, pass `state-dump.json` as a flag back to `ghostferry-copydb`:

```console
$ ghostferry-copydb -verbose -resumestate state-dump.json conf.json
```

Requirements for resuming:

- Use the **same built binary** that produced the dump. Ghostferry compares
  the entire `GhostferryVersion` string, including the build timestamp and
  commit, and refuses to resume on any difference. Do not rebuild between
  capture and resume, and do not edit the version field to bypass the check.
- Keep the tables already created on the target, and use the same
  configuration, in particular the same database/table filters and rewrites.
- The source binlogs at the positions recorded in the dump must still be
  available, and so must the target binlogs at
  `LastStoredBinlogPositionForTargetVerifier` if target verification is
  enabled. **If you interrupt Ghostferry for longer than your binlog retention
  time, you will not be able to resume it.**
- If the dump does not include the schema cache
  (`DoNotIncludeSchemaCacheInStateDump`), the schemas are reloaded from the
  source on resume and must still be compatible with the copied data.

If there is no dump, the required binlogs have been purged, or production has
already been switched over to the target, do not fabricate or edit resume
coordinates; start a new run instead (see
[Dealing with Errors and Restarting Runs](copydbinprod.md#dealing-with-errors-and-restarting-runs)).

Some other considerations/notes:

- Verification state on resume:

    - Inline: the reverify store (`BinlogVerifyStore`) and its source binlog
      checkpoint are saved and restored.
    - TargetVerifier: its target binlog checkpoint is saved and restored.
    - Iterative (deprecated): its in-memory verification progress is not saved;
      verification must be repeated.
    - ChecksumTable: runs as a fresh scan during cutover, so there is nothing to
      restore.

- The integration tests exercise resuming after signals, including the Inline
  verifier and target verifier state, repeated interrupts and resumes, and
  processes killed abruptly that resume from an earlier valid state (for
  example one delivered through the state callback). This does not prove that
  every failure is recoverable.

    - Rehearse interrupting and resuming in your environment, and validate the
      correctness of resumed data with a final verification. See
      [Testing Ghostferry with Production Data](copydbinprod.md#prodtesting).

- While we are confident the algorithm is correct, this is still a
  highly experimental feature. USE AT YOUR OWN RISK.
