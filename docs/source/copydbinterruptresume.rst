.. _copydbinterruptresume:

============================================
Interrupt and resuming ``ghostferry-copydb``
============================================

*Note that this is a new and experimental feature. Please ensure you test it
thoroughly in your environment to ensure there are no data loss. See the bottom
of this page for important information on caveats of using this feature.*

To enable state dumps of Ghostferry on panic (and thus interrupt & resume), the
configuration given to copydb must have the entry ``"DumpStateOnSignal":
true``. Once this is configured, any time the process panics, which can be
caused by both SIGTERM/SIGINT or due to an error within Ghostferry, the run
state will be dumped to stdout with JSON. An example of this can be seen below:

.. code-block:: json

  {
    "GhostferryVersion": "1.1.0+20190311205252+88a1c5c",
    "LastKnownTableSchemaCache": {
      "abc.table1": {
        "Schema": "abc",
        "Name": "table1",
        "Columns": [
          {
            "Name": "id",
            "Type": 1,
            "Collation": "",
            "RawType": "bigint(20)",
            "IsAuto": true,
            "IsUnsigned": false,
            "EnumValues": null,
            "SetValues": null
          },
          {
            "Name": "data",
            "Type": 5,
            "Collation": "utf8mb4_unicode_ci",
            "RawType": "varchar(16)",
            "IsAuto": false,
            "IsUnsigned": false,
            "EnumValues": null,
            "SetValues": null
          }
        ],
        "Indexes": [
          {
            "Name": "PRIMARY",
            "Columns": [
              "id"
            ],
            "Cardinality": [
              1
            ]
          }
        ],
        "PKColumns": [
          0
        ],
        "UnsignedColumns": null
      }
    },
    "CurrentStage": "COPY",
    "CopyStage": {
      "LastProcessedBinlogPosition": {
        "Name": "mysql-bin.000008",
        "Pos": 193989
      },
      "CompletedTables": {}
    },
    "VerifierStage": null
  }


To resume, you first need to save this JSON into a file. Alternatively, you
could pipe the stdout of ``ghostferry-copydb`` directly into a file via:

.. code-block:: shell-session

  $ ghostferry-copydb -verbose conf.json >state-dump.json 2>ghostferry.log

Theoretically, ``ghostferry-copydb`` should write only the state dump json into
stdout and all logs in stderr. However, check the files to make sure this is
true. If not, file a bug report.

To resume, pass ``state-dump.json`` as a flag back to ``ghostferry-copydb``:

.. code-block:: shell-session

  $ ghostferry-copydb -verbose -resumestate state-dump.json conf.json

**Note: if you interrupt Ghostferry for a period of time longer than your
binlog retention time, you will not be able to resume Ghostferry. Ensure that
the binlog at the position recorded in the state dump is available when
resuming Ghostferry.**

Some other considerations/notes:

* While Ghostferry will dump the state when it encounters an unrecoverable
  error (such as a network issue to the databases), the only tested use case
  for now is due to an interrupt with SIGTERM/SIGINT.

  * Errored runs *should* be theoretically safe to resume, but this is not
    validated in any form.  If you resume an errored run, it is recommended to
    validate the correctness of the data using the CHECKSUM TABLE verifier.
  * As the project develops, we want to validate the safety of resuming errored
    runs.
  * To test resuming errored runs further, see :ref:`prodtesting`.

* Verifiers are not resumable, including the IterativeVerifier. This may change
  in the future.
* While we are confident that the algorithm to be correct, this is still a
  highly experimental feature. USE AT YOUR OWN RISK.
