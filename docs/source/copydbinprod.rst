.. _copydbinprod:

===========================================
Running ``ghostferry-copydb`` in production
===========================================

Assuming you have gone through :ref:`tutorialcopydb`, you probably want to run
``ghostferry-copydb`` in production. The general workflow is relatively
similar, with some differences. You should keep the tutorial as a starting
point for your own playbook as most steps will largely be the same.

Prerequisites
-------------

Before you start, you need to know if you can even use Ghostferry. Some points
to consider about this are:

- Ghostferry on its own does not enable 0 downtime moves. The downtime for the
  app will be minimal compared to other methods but still non-zero.

  - Figure out how much downtime you are willing to tolerate. Using Ghostferry,
    one can realistically achieve downtime in the order of seconds.

- The source database must be running with `FULL image`_ `ROW based replication`_.

  - Without this, it is not possible to run Ghostferry safely and Ghostferry
    will error out if it detects ``binlog_row_image`` is not set to ``FULL``.

- Tables to be copied have integer primary keys.

  - An issue exists to fix this limitation here:
    `<https://github.com/Shopify/ghostferry/issues/2>`_.
  - As a work around, you can use mysqldump during the cutover stage to migrate
    those tables.

- There are no foreign key constraints in your tables.

  - You should remove these constraints before running Ghostferry.

- ``ghostferry-copydb`` can only copy a whole table at a time.

  - If you need to copy a subset, use ghostferry as a library to build your own
    application.

- In cases of a multi-node replication setup, Ghostferry should only be run on
  the master database where writes occur. Otherwise there may be a race
  condition causing some binlog entries to be missed.

  - There may be a way to fix this in the future.

.. _`FULL image`: https://dev.mysql.com/doc/refman/5.7/en/replication-options-binary-log.html#sysvar_binlog_row_image
.. _`ROW based replication`: https://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.html#sysvar_binlog_format

.. _prodtesting:

Testing Ghostferry with Production Data
---------------------------------------

You can run Ghostferry without running the cutover to test the entire flow
without actually moving your database. This allows you to verify that the move
will indeed work with your setup. Additionally, the target location where you
performed this test move can be used by a staging version of your app to verify
that the target MySQL will not cause trouble for your app, especially if the
target MySQL has different version/configurations.

If you want to test the entire flow including the cutover, you can as well with
some additional setup:

1. Setup a slave of the source database. Use this slave as the source database.
2. Use Ghostferry to copy the data from the slave to some target.
3. Stop replication on the source slave. This is the equivalent to setting the
   database to read only.
4. Perform the cutover as normal. Can even run verification with this.

To Verify Or Not To Verify
--------------------------

Ghostferry has two built-in verifiers. They are designed to give you certainty
that the data of the source and the target are identical after a move and
nothing was corrupted/missed. They are designed to be used during the cutover
process, when writes to the source database has stopped and writes to the
target have not yet started. During the run, both the source and target must
be kept read only and thus incur downtime for your dataset. The two different
verifiers have different downtime characteristics. See the :ref:`verifiers`
page for more details on what they are and how to choose a verifier. This means
you have to decide if you want to verify or not.

In order to know how much downtime you will incur during the verification
process, you can test it with the slave based staging move described in
`Testing Ghostferry with Production Data`_. During the cutover stages, run
verification as normal and measure the time taken.

Since the ultimate objective of the verifier is to verify that Ghostferry did
not make a mistake while copying the data and give you peace of mind, if you
were able to successfully perform the staging verification, you would know that
the system already works with your setup and data. At this point it may no
longer be necessary to run a verification during the production move as the
chance of a row that has changed between the staging run and the actual run
causing an issue is slim.

It is also possible to run a verification after a move and possibly address any
issues after the fact:

1. Setup a slave of the source database.
2. Setup a slave of the target database.
3. Run ghostferry as normal between the master source and target database.
4. During the cutover, also stop replication to the slaves setup in step 1 and
   2.
5. Manually compare the table on the slaves using something like ``CHECKSUM
   TABLE``.

Dealing with Errors and Restarting Runs
---------------------------------------

It is possible for Ghostferry to encounter an unrecoverable error (such as a
network partition with the database). In these scenarios, the target will be
left alone as the Ghostferry process panics and quits. It may be possible to
resume these runs using the experimental interrupt & resume feature. See
:ref:`copydbinterruptresume`.

If the resume doesn't work, starting a brand new Ghostferry run is perfectly
fine.  For copydb specifically, you need to drop the databases created by
copydb on the target as it will try to recreate it.

Configuration for ``ghostferry-copydb``
---------------------------------------

The configuration for ``ghostferry-copydb`` is a JSON file. The schema it is
based on the `Config struct of ghostferry
<https://godoc.org/github.com/Shopify/ghostferry#Config>`__, with some
differences:

- You cannot specify ``TableFilter`` and ``CopyFilter``.
- If you are using the debian package, you don't need to specify
  ``WebBasedir`` as it is compiled into the binary.

It also allows you specify some options according to fields defined by the
`Config struct of copydb
<https://godoc.org/github.com/Shopify/ghostferry/copydb#Config>`__. This allows
you to filter the databases/tables to copy as well as specify the type of
verifier.
