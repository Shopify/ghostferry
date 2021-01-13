.. _verifiers:

=========
Verifiers
=========

Verifiers in Ghostferry are designed to ensure that Ghostferry did not
corrupt/miss data. There are three different verifiers: the
``ChecksumTableVerifier``, the ``InlineVerifier``, and the ``TargetVerifier``. A comparison of the 
``ChecksumTableVerifier`` and ``InlineVerifier`` are given below:

+-----------------------+-----------------------+-----------------------------+
|                       | ChecksumTableVerifier | InlineVerifier              |
+-----------------------+-----------------------+-----------------------------+
|Mechanism              | ``CHECKSUM TABLE``    | Verify row after insert;    |
|                       |                       | Reverify changed rows before|
|                       |                       | and during cutover.         |
+-----------------------+-----------------------+-----------------------------+
|Impacts on Cutover Time| Linear w.r.t data size| Linear w.r.t. change rate   |
|                       |                       | [1]_                        |
+-----------------------+-----------------------+-----------------------------+
|Impacts on Copy Time   | None                  | Linear w.r.t data size      |
|[2]_                   |                       |                             |
+-----------------------+-----------------------+-----------------------------+
|Memory Usage           | Minimal               | Linear w.r.t rows changed   |
+-----------------------+-----------------------+-----------------------------+
|Partial table copy     | Not supported         | Supported                   |
+-----------------------+-----------------------+-----------------------------+
|Worst Case Scenario    | Large databases causes| Verification is slower than |
|                       | unacceptable downtime | the change rate of the DB   |
+-----------------------+-----------------------+-----------------------------+

.. [1] Additional improvements could be made to reduce this as long as
       Ghostferry is faster than the rate of change. See
       `<https://github.com/Shopify/ghostferry/issues/13>`_.

.. [2] Increase in copy time does not increase downtime. Downtime occurs only
       in cutover.

If you want verification, you should try with the ``ChecksumTableVerifier``
first if you're copying whole tables at a time. If that takes too long, you can
try using the ``InlineVerifier``.  Alternatively, you can verify in a staging
run and not verify during the production run (see :ref:`copydbinprod`).

Note that the ``InlineVerifier`` on its own may potentially miss some 
cases, and using it with the ``TargetVerifier`` is recommended if these
cases are possible.

+---------------------------------------------------+---------------+---------------+-----------------+
| Conditions                                        | ChecksumTable | Inline        | Inline + Target |
+---------------------------------------------------+---------------+---------------+-----------------+
| Data inconsistency due to Ghostferry issuing an   | Yes [3]_      | Yes           | Yes             |
| incorrect UPDATE on the target database (example: |               |               |                 |
| encoding-type issues).                            |               |               |                 |
+---------------------------------------------------+---------------+---------------+-----------------+
| Data inconsistency due to Ghostferry failing to   | Yes           | Yes           | Yes             |
| INSERT on the target database.                    |               |               |                 |
+---------------------------------------------------+---------------+---------------+-----------------+
| Data inconsistency due to Ghostferry failing to   | Yes           | Yes           | Yes             |
| DELETE on the target database.                    |               |               |                 |
+---------------------------------------------------+---------------+---------------+-----------------+
| Data inconsistency due to rogue application       | Yes           | Sometimes [4]_| Yes             |
| issuing writes (INSERT/UPDATE/DELETE) against the |               |               |                 |
| target database.                                  |               |               |                 |
+---------------------------------------------------+---------------+---------------+-----------------+
| Data inconsistency due to missing binlog events   | Yes           | Sometimes [5]_| Sometimes [5]_  |
| when Ghostferry is resumed from the wrong         |               |               |                 |
| binlog coordinates.                               |               |               |                 |
+---------------------------------------------------+---------------+---------------+-----------------+
| Data inconsistency if Ghostferry's Binlog writing | Yes           | Probably not  | Probably not    |
| implementation is incorrect and modified the      |               | [6]_          | [6]_            |
| wrong row on the target (example, an UPDATE is    |               |               |                 |
| supposed to go to id = 1 but Ghostferry instead   |               |               |                 |
| issued a query for id = 2). This is an unrealistic|               |               |                 |
| scenario, but is included for illustrative        |               |               |                 |
| purposes.                                         |               |               |                 |
+---------------------------------------------------+---------------+---------------+-----------------+


.. [3] Note that the CHECKSUM TABLE statement is broken in MySQL 5.7 for tables
       with JSON columns. These tables will result in a false positive event:
       even if two tables are identical, they can emit different checksums. See
       https://bugs.mysql.com/bug.php?id=87847. This applies to every row in
       this table.

.. [4] If the rows modified by the rogue application are modified again on
       the source after Ghostferry starts, the InlineVerifier's binlog tailer
       should pick up that row and attempt to reverify it.

.. [5] If the rows missed after resume are modified again on the source after
       Ghostferry starts, the InlineVerifier's binlog tailer should pick up
       that row and attempt to reverify it.

.. [6] If the implementation of the Ghostferry algorithm is so broken, chances
       are the InlineVerifier won't catch it either as it relies on the same
       algorithm to enumerate the table and tail the binlogs.

IterativeVerifier (Deprecated)
-----------------

**NOTE! This is a deprecated verifier. Use the InlineVerifier instead.**

IterativeVerifier verifies the source and target in a couple of steps:

1. After the data copy, it first compares the hashes of each applicable rows
   of the source and the target together to make sure they are the same. This
   is known as the initial verification.

   a. If they are the same: the verification for that row is complete.
   b. If they are not the same: add it into a reverify queue.

2. For any rows changed during the initial verification process, add it into
   the reverify queue.

3. After the initial verification, verify the rows' hashes in the
   reverification queue again. This is done to reduce the time needed to
   reverify during the cutover as we assume the reverification queue will
   become smaller during this process.

4. During the cutover stage, verify all rows' hashes in the reverify queue.

   a. If they are the same: the verification for that row is complete.
   b. If they are not the same: the verification fails.

5. If no verification failure occurs, the source and the target are identical.
   If verification failure does occur (4b), then the source and target are not
   identical.

A proof of concept TLA+ verification of this algorithm is done in
`<https://github.com/Shopify/ghostferry/tree/iterative-verifier-tla>`_.

InlineVerifier
-----------------

InlineVerifier verifies the source and target inline with the other components with
a few slight differences from the IterativeVerifier above. The primary difference
being that this verification process happens while the data is being copied by the
DataIterator instead of after the fact.

With regards to the ``DataIterator`` and ``BatchWriter``:

1. While selecting the data in the ``DataIterator``, a fingerprint is appended
   to the end of the statement that ``SELECT`` s data from the source as
   ``SELECT *, MD5(...) FROM ...``

2. The fingerprint, gathered from the ``MD5(...)`` of the query above is stored
   on the ``RowBatch`` to be used in the next verification step.

3. The ``BatchWriter`` then attempts to write the ``RowBatch``, but instead of inserting
   it directly, the following process is taken:

   a. A transaction is opened.
   b. The data contained in the ``RowBatch`` is inserted.
   c. The PK and fingerprint is then ``SELECT`` ed from the Target
      as ``SELECT pk, MD5(....) FROM ...``.
   d. The fingerprint (``MD5``) is then checked against the fingerprint currently
      stored on the ``RowBatch``.

   The process in step 3 above is retried (with a limit) if there happens to be
   a failure or mismatch, and will fail the run if they are not verified within
   the retry limits.

With regards to the BinlogStreamer:

1. As DMLs are observed by the ``BinlogStreamer``, the PKs of the events are placed into
   a ``reverifyStore`` to be periodically verified for correctness.

2. This continues to happen in the background throughout the process of the Run.

3. If a PK is found not to match, it is added back into the reverifyStore to be verified
   again.

4. When ``VerifyBeforeCutover`` starts, the InlineVerifier will verify enough of the
   events in the ``reverifyStore`` to ensure it has a sufficiently small number of events
   that can be successfully verified before cutover.

5. When ``VerifyDuringCutover`` begins, all of the remaining events in the ``reverifyStore``
   are verified and any mismatches are returned.

TargetVerifier
-----------------

TargetVerifier ensures data on the Target is not corrupted during the move process
and is meant to be used in conjunction with another verifier above.

It uses a configurable annotation string that is prepended to DMLs that acts as
a verified "signature" of all of Ghostferry's operations on the Target:

1. A BinlogStreamer is created and attached to the Target

2. As this BinlogStreamer receives DML events, it attempts to extract the annotation
   from each for each of the ``RowsEvents``.

3. If an annotation is not found for the DML, or the extracted annotation does not
match the configured annotation of Ghostferry, an error is returned and the process fails.

The TargetVerifier needs to be manually stopped before cutover. If it is not stopped,
it may detect writes from the application (that are not from Ghostferry) and fail the run.
Stopping before cutover also gives the TargetVerifier the opportunity to inspect all
of the DMLs in its ``BinlogStreamer`` queue to ensure no corruption of the data has occurred.
