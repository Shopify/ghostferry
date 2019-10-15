.. _verifiers:

=========
Verifiers
=========

Verifiers in Ghostferry is designed to ensure that Ghostferry did not
corrupt/miss data. There are two different verifiers: the
``ChecksumTableVerifier`` and the ``InlineVerifier``. A comparison of them
are given below:

+-----------------------+-----------------------+-----------------------------+
|                       | ChecksumTableVerifier | InlineVerifier              |
+-----------------------+-----------------------+-----------------------------+
|Mechanism              | ``CHECKSUM TABLE``    | Each row is validated via an|
|                       |                       | MD5 type query on the MySQL |
|                       |                       | database after it is copied.|
|                       |                       | Any entries copied due to   |
|                       |                       | binlog activity is verified |
|                       |                       | periodically during the copy|
|                       |                       | process and ultimately      |
|                       |                       | during the cutover.         |
+-----------------------+-----------------------+-----------------------------+
|Impacts on Cutover Time| Linear w.r.t data     | Linear w.r.t. change rate   |
|                       | size.                 | [1]_.                       |
+-----------------------+-----------------------+-----------------------------+
|Impacts on Copy Time   | None.                 | Linear w.r.t data size [3]_.|
|[2]_                   |                       |                             |
+-----------------------+-----------------------+-----------------------------+
|Memory Usage           | Minimal.              | Minimal.                    |
+-----------------------+-----------------------+-----------------------------+
|Partial table copy     | Not supported.        | Supported.                  |
+-----------------------+-----------------------+-----------------------------+
|Worst Case Scenario    | Large databases causes| Verification during cutover |
|                       | unacceptable downtime.| is slower than the change   |
|                       |                       | rate of the DB.             |
+-----------------------+-----------------------+-----------------------------+

.. [1] Additional improvements could be made to reduce this as long as
       Ghostferry is faster than the rate of change. See
       `<https://github.com/Shopify/ghostferry/issues/13>`_.

.. [2] Increase in copy time does not increase downtime. Downtime occurs only
       in cutover.

.. [3] The increase should be minimal as the verification is done immediately
       after copy, when the data most likely still live in RAM.

If you want verification, you should try with the ``ChecksumTableVerifier``
first if you're copying whole tables at a time. If that takes too long, you can
try using the ``InlineVerifier``. Alternatively, you can verify in a staging
run and not verify during the production run (see :ref:`copydbinprod`).

InlineVerifier
--------------

Ghostferry's core algorithm has run for millions of times and is backed by a
TLA+ specification. There's a high degree of confidence in its correctness.
However, correctness analysis assumed that the data is perfectly copied from
the source to the target MySQL. However, this may not be the case as the data
is transformed from MySQL -> Go -> MySQL. Different encodings could change the
unintentionally data, resulting in corruptions. Some observed (and fixed) cases
includes: floating point values and datetime columns.

The InlineVerifier is designed to catch these type of problems and fail the
run if discrepencies are detected. **It is not designed to verify that certain
records are missing**. Only the ChecksumTableVerifier can do that. The way the
InlineVerifier catches encoding type issues are as follows:

* During the DataIterator
    1. While selecting a row to copy, a MD5 checksum is calculated on the source
       MySQL server.
    2. After the data is copied onto the target, the same MD5 checksum is
       calculated on the target.
    3. The hash is compared. If it is different, the run is aborted with an
       error.
* During the BinlogStreamer
    1. Since the MD5 checksum cannot be synchronously generated with the Binlog
       event, any rows seen in the BinlogStreamer is added to a background
       verification queue.
    2. Periodically in the background, the checksum for all rows within the
       queue are computed both on the source and the target database.
    3. The checksums are compared. If they match, the row is removed from the
       verification queue. Otherwise, it remains in the queue.
    4. During the cutover, when the source and target are read-only, checksums
       for all rows within the verification queue are checked. If any
       mismatches are detected, an error is raised.
