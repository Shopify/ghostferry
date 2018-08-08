.. _verifiers:

=========
Verifiers
=========

Verifiers in Ghostferry is designed to ensure that Ghostferry did not
corrupt/miss data. There are two different verifiers: the
``ChecksumTableVerifier`` and the ``IterativeVerifier``. A comparison of them
are given below:

+-----------------------+-----------------------+-----------------------------+
|                       | ChecksumTableVerifier | IterativeVerifier           |
+-----------------------+-----------------------+-----------------------------+
|Mechanism              | ``CHECKSUM TABLE``    | Verify row before cutover;  |
|                       |                       | Reverify changed rows during|
|                       |                       | cutover.                    |
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
|                       | unacceptable downtime | the change rate of the DB;  |
+-----------------------+-----------------------+-----------------------------+

.. [1] Additional improvements could be made to reduce this as long as
       Ghostferry is faster than the rate of change. See
       `<https://github.com/Shopify/ghostferry/issues/13>`_.

.. [2] Increase in copy time does not increase downtime. Downtime occurs only
       in cutover.

If you want verification, you should try with the ``ChecksumTableVerifier``
first if you're copying whole tables at a time. If that takes too long, you can
try using the ``IterativeVerifier``. Alternatively, you can verify in a staging
run and not verify during the production run (see :ref:`copydbinprod`).

IterativeVerifier
-----------------

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
