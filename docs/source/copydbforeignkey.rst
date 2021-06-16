.. _copydbforeignkey:

===================================================================================
Running ``ghostferry-copydb`` in production for tables with Foreign Key Constraints
===================================================================================

Migrating tables with foreign keys constraints is an experimental feature in copydb and should be used at your own risk in production. 
 

Prerequisites
-------------

Before migrating tables with foreign key constraints via copydb there are a couple of things to take care of 

- Ghostferry needs to be ran with ``SkipForeignKeyConstraintsCheck = true``, which will disable ghostferry to check foreign key 
  constraints during initialization.

- Source DB should be ``read_only``.

- Need to disable foreign key constraint checks on target DB by passing the following config to target db
 
 .. code-block:: json

  "Params": {
    "foreign_key_checks": "0"
  }

- Even though foreign key constraint checks are disabled on target db, table and db creation must happen in a specific order (eg parent should be created
  before child table). This creation order can be specified by passing ``TablesToBeCreatedFirst`` in the config, or else the table creation order will be 
  figured out by copydb. 

Limitations
-------------

- Currently migrating tables with foreign key constraints is only possible if the source database is in read_only mode. Since tables with foreign key constraints can have referential actions for a foreign key such as ``ON DELETE CASCADE``, ``ON UPDATE CASCADE``. Cascading deletes and updates in child tablees caused by foreign key constraints don't show up in binlogs because these referential actions are dealt internally by InnoDB. This issue descibes briefly why the source database should be read_only during the migration - https://github.com/Shopify/ghostferry/issues/289.

- ``Interrupt-Resume`` functionality can be used as long as source database is read_only also during the interrupt period

- ``Inline Verifier`` can be used as long as it is ensured that the source database is read_only (even during the interrupt period)
