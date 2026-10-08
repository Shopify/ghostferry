# Verifiers

Verifiers in Ghostferry are designed to ensure that Ghostferry did not
corrupt/miss data. There are two independent mechanisms:

- A **data verifier**, selected with `VerifierType` in the embedded
  `ghostferry.Config`: `"ChecksumTable"`, `"Inline"`, the deprecated
  `"Iterative"`, or `"NoVerification"`. Leaving `VerifierType` empty means no
  data verifier in copydb; in library code, an empty `VerifierType` lets you
  supply your own `Ferry.Verifier` instead.
- The **`TargetVerifier`**, which monitors the target's binlog for writes that
  did not come from Ghostferry. It is enabled by default
  (`SkipTargetVerification: false`) whichever data verifier is chosen,
  including `NoVerification`. It does not compare row contents and is not a
  substitute for a final data verification.

A comparison of the `ChecksumTableVerifier` and `InlineVerifier` data
verifiers is given below:

| | ChecksumTableVerifier | InlineVerifier |
|---|---|---|
| Mechanism | `CHECKSUM TABLE` | Verify row after insert; Reverify changed rows before and during cutover. |
| Impacts on Cutover Time | Linear w.r.t data size | Linear w.r.t. change rate [^1] |
| Impacts on Copy Time [^2] | None | Linear w.r.t data size |
| Memory Usage | Minimal | Linear w.r.t rows changed |
| Partial table copy | Not supported | Supported |
| Worst Case Scenario | Large databases causes unacceptable downtime | Verification is slower than the change rate of the DB |

[^1]: Additional improvements could be made to reduce this as long as
    Ghostferry is faster than the rate of change. See
    <https://github.com/Shopify/ghostferry/issues/13>.

[^2]: Increase in copy time does not increase downtime. Downtime occurs only
    in cutover.

`ChecksumTable` is a simple choice for small whole-table copies such as the
[tutorial](tutorialcopydb.md), because it scans every copied table during
cutover. `Inline` is the non-deprecated incremental option for larger datasets
or partial copies. A successful verification in a rehearsal does not verify
the data of a later run; see
[Running `ghostferry-copydb` in production](copydbinprod.md).

Note that the `InlineVerifier` on its own may potentially miss some
cases, and keeping the `TargetVerifier` enabled is recommended if these
cases are possible. In the table below, "Yes" means the condition is detected
within the scope of the selected verifier: the copied tables, and for the
Inline verifier the compared columns (columns configured in
`IgnoredColumnsForVerification` are skipped, and columns listed in
`CompressedColumnsForVerification` are compared after decompression).

| Conditions | ChecksumTable | Inline | Inline + Target |
|---|---|---|---|
| Data inconsistency due to Ghostferry issuing an incorrect UPDATE on the target database (example: encoding-type issues). | Yes [^3] | Yes | Yes |
| Data inconsistency due to Ghostferry failing to INSERT on the target database. | Yes | Yes | Yes |
| Data inconsistency due to Ghostferry failing to DELETE on the target database. | Yes | Yes | Yes |
| Data inconsistency due to rogue application issuing writes (INSERT/UPDATE/DELETE) against the target database. | Yes | Sometimes [^4] | Yes |
| Data inconsistency due to missing binlog events when Ghostferry is resumed from the wrong binlog coordinates. | Yes | Sometimes [^5] | Sometimes [^5] |
| Data inconsistency if Ghostferry's Binlog writing implementation is incorrect and modified the wrong row on the target (example, an UPDATE is supposed to go to id = 1 but Ghostferry instead issued a query for id = 2). This is an unrealistic scenario, but is included for illustrative purposes. | Yes | Probably not [^6] | Probably not [^6] |

[^3]: Note that the CHECKSUM TABLE statement is broken in MySQL 5.7 for tables
    with JSON columns. These tables will result in a false positive event:
    even if two tables are identical, they can emit different checksums. See
    <https://bugs.mysql.com/bug.php?id=87847>. This applies to every row in
    this table.

[^4]: If the rows modified by the rogue application are modified again on
    the source after Ghostferry starts, the InlineVerifier's binlog tailer
    should pick up that row and attempt to reverify it.

[^5]: If the rows missed after resume are modified again on the source after
    Ghostferry starts, the InlineVerifier's binlog tailer should pick up
    that row and attempt to reverify it.

[^6]: If the implementation of the Ghostferry algorithm is so broken, chances
    are the InlineVerifier won't catch it either as it relies on the same
    algorithm to enumerate the table and tail the binlogs.

## IterativeVerifier (Deprecated)

**NOTE! This is a deprecated verifier. Use the InlineVerifier instead.**

IterativeVerifier verifies the source and target in a couple of steps:

1. After the data copy, it first compares the hashes of each applicable rows
    of the source and the target together to make sure they are the same. This
    is known as the initial verification.

    1. If they are the same: the verification for that row is complete.
    2. If they are not the same: add it into a reverify queue.

2. For any rows changed during the initial verification process, add it into
    the reverify queue.

3. After the initial verification, verify the rows' hashes in the
    reverification queue again. This is done to reduce the time needed to
    reverify during the cutover as we assume the reverification queue will
    become smaller during this process.

4. During the cutover stage, verify all rows' hashes in the reverify queue.

    1. If they are the same: the verification for that row is complete.
    2. If they are not the same: the verification fails.

5. If no verification failure occurs, the compared rows of the source and the
    target are identical within the scope of the verifier. If verification
    failure does occur (4b), then the source and target are not identical.

A proof of concept TLA+ verification of this algorithm is done in
<https://github.com/Shopify/ghostferry/tree/iterative-verifier-tla>.

## InlineVerifier

InlineVerifier verifies the source and target inline with the other components with
a few slight differences from the IterativeVerifier above. The primary difference
being that this verification process happens while the data is being copied by the
DataIterator instead of after the fact.

With regards to the `DataIterator` and `BatchWriter`:

1. While selecting the data in the `DataIterator`, a fingerprint is appended
    to the end of the statement that `SELECT`s data from the source as
    `SELECT *, MD5(...) FROM ...`

2. The fingerprint, gathered from the `MD5(...)` of the query above is stored
    on the `RowBatch` to be used in the next verification step.

3. The `BatchWriter` then writes the `RowBatch` as follows:

    1. A transaction is opened on the target.
    2. The data contained in the `RowBatch` is inserted.
    3. The pagination key and fingerprint of these rows are then `SELECT`ed
        from the target in the same transaction.
    4. The target fingerprints are compared with the source fingerprints stored
        on the `RowBatch`. The pagination keys of mismatched rows are added to
        the `reverifyStore` to be verified again later.
    5. The transaction is committed.

    Query and write failures are retried (with a limit) and fail the run if
    the retry limit is exceeded. A mismatch alone does not abort the copy: it
    is enqueued for reverification. The exception is when
    `EnforceInlineVerification` is set on the `BatchWriter` (used for
    standalone copies without binlog streaming, such as
    `Ferry.RunStandaloneDataCopy`), where a mismatch fails the batch.

With regards to the BinlogStreamer:

1. As DMLs are observed by the `BinlogStreamer`, the pagination keys of the
    changed rows are placed into the `reverifyStore` to be periodically verified
    for correctness, every `InlineVerifierConfig.VerifyBinlogEventsInterval`
    (default `"1s"`).

2. This continues to happen in the background until cutover is allowed.

3. If a row is found not to match, its pagination key is added back into the
    `reverifyStore` to be verified again.

4. `VerifyBeforeCutover` reverifies the `reverifyStore` in at most 30 passes,
    stopping early once at most 1000 rows remain queued or a pass no longer
    shrinks the queue. If `InlineVerifierConfig.MaxExpectedDowntime` is set
    (non-empty and non-zero) and the last pass took longer, the run fails. This
    is an estimate, not a hard guarantee on downtime.

5. When `VerifyDuringCutover` begins, all of the remaining rows in the
    `reverifyStore` are verified. If any mismatch remains, the result has
    `DataCorrect: false` and a message listing the mismatched pagination keys.
    `VerifyDuringCutover` can only be started once, and any source binlog event
    received after it started is an error.

`VerifyDuringCutover` must be called after binlog streaming has stopped and
before the target receives application writes. ghostferry-copydb does **not**
call it automatically: the operator must click Run Verification in the web UI
after cutover and check that Verified Correct is `true` and no error is shown
before letting applications write to the target. The copydb state `done` only
means that copying and streaming have finished. Custom applications must call
`VerifyDuringCutover` themselves.

## TargetVerifier

TargetVerifier detects writes to the copied tables on the target that were not
made by Ghostferry during the move process. It is enabled by default and is
meant to be used in conjunction with one of the data verifiers above; it does
not compare row contents.

Ghostferry prepends an SQL annotation (`Target.Marginalia`, default
`application:ghostferry`) to its statements on the target. The TargetVerifier
checks for this expected annotation:

1. A BinlogStreamer is created and attached to the Target. This requires the
    target to have binary logging with `binlog_rows_query_log_events=ON`, and
    the target user to have replication privileges.

2. As this BinlogStreamer receives DML events for the copied tables, it
    extracts the annotation from the query event preceding each `RowsEvent`.

3. If no annotation is found for the DML, or the extracted annotation text does
    not match `Target.Marginalia`, an error is returned and the run fails.

This detects unexpected writers; it is not cryptographic authentication and
does not protect against a writer that deliberately uses the same annotation.

The TargetVerifier must be stopped (`Ferry.StopTargetVerifier`) after source
binlog processing has finished and before the target is opened to application
writes; otherwise it would fail the run on the application's writes. Stopping
it also lets it process all target binlog events up to the stop point.
ghostferry-copydb does this automatically after the source binlog streaming
stops and before calling `CutoverUnlock`. Custom applications must do the
equivalent themselves.
