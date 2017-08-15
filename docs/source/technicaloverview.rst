.. _technicaloverview:

==================
Technical Overview
==================

Ghostferry must be able to copy the data from a source database to a target
database while keeping track of all changes in the source database to apply
it to the target database. To accomplish this, Ghostferry iterates through
the source database to copy data and it pulls the binlog from the source
database to apply any changes to the target database.

To accomplish this, several steps must be taken by either Ghostferry and
some actions external to Ghostferry:

1. Ghostferry begins to pull binlog events from the source database, applying
   the entries that are applicable to the target. This continues in the
   background.
2. Ghostferry SELECTs data from the source database and copies them to the
   target database.
3. Ghostferry finishes copying all data from the source and target database.
   The binlog apply operation continues in the background.
4. Wait until the binlog apply is roughly caught up to the latest available
   position on the source database.
5. Set the application to be read only for the data that Ghostferry has copied
   (or even the whole source database, if possible). This is an out of band
   step.
6. Instruct Ghostferry to catch up to the latest available position on the
   source database and stop synchronizing data.
7. Change the application to use the target database and enable writes.

The tool will cause some amount of downtime: between step 5 and step 7. The
window of downtime is dependent on how fast those steps can be done and they
should be relatively small (on the order of seconds - minutes).

Architecture
------------

Ghostferry is built as a Go library. The library part will handle the
background data copy and background binlog streaming while handing off to
how to perform the initialization and cutover part to the consumer of the
library.

As a library, Ghostferry consists of several background threads (implemented
as goroutines):

- **DataIterator**:   This is a background task in charge of iterating through
                      the data on the source database and applying them to the
                      target database.
- **BinlogStreamer**: This is a background task that pulls the binlog from the
                      source database and apply them to the target database.
- **ControlServer**:  A HTTP server that exposes a status page as well as some
                      basic controls to the Ghostferry process.
- **ErrorHandler**:   Handles errors in the background and terminate the entire
                      application after saving the last known good state
                      [#ferr]_.
- **Throttler**:      Handles collecting load information on the source
                      database and throttle if necessary [#fthrottle]_.

Tying all this together is an object known as Ferry. It encapsulates all of the
components above. The Ferry object will be the main interface to the library
consumer, which will be used to create the application to move data as the
consumer desires.

You can see an example of an application built with Ghostferry in the
``copydb`` package.

.. [#ferr] The ErrorHandler in this version does not save the last known state.
           This, however, is planned.

.. [#fthrottle] The throttler current does not collect information and serves
                only to throttle when the user request it to.

The overall, simplified architecture of Ghostferry can be summarized with the
figure below. It shows the basic flow of all the background tasks, along with
how each task is spawned (starting from ``Ferry.Run``). Yellow boxes indicates
at which the data is inserted into the target database. Arrows pointing towards
outside of an encapsulating box indicate the task will exit.

.. image:: _static/ghostferry-architecture.png
   :align: center

The red arrows with "Error action" indicates an error has occured and the error
is sent to the ``ErrorHandler``, at which the ErrorHandler flow takes over.

Throttler
*********

Ghostferry should throttle based on both the source and target server's load.
However, this functionality is currently not implemented. A "stub" throttler
implementation is available so this feature can be done in the future.

Right now, the throttler is used to Pause the data copy, as commanded via the
ControlServer.

ControlServer
*************

A Web UI is available for one to monitor the Ghostferry run, as well as
controlling Pause/Unpause.

Limitations
-----------

- Right now, Ghostferry can only be used on tables with auto incrementing,
  numeric, and unique primary keys.

  - An error will be emitted during the beginning of the run if such a primary
    key is not detected.

- Ghostferry can only be used on a source database with FULL RBR.

  - An error will be emitted during the beginning of the run if FULL RBR is
    not turned on the source database.
  - Without FULL RBR, the integrity of the data cannot be guarenteed.

Algorithm Correctness
---------------------

The overall algorithm of Ghostferry is specified in a TLA+ specification and
validated via TLC. This currently is not included in this repo, although this
is planned.
