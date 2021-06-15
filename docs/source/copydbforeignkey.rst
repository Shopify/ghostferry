.. _copydbforeignkey:

===================================================================================
Running ``ghostferry-copydb`` in production for tables with Foreign Key Constraints
===================================================================================

Migrating tables with foreign keys constraints is an experimental feature in copydb and should be used at your own risk in production.
 

Prerequisites
-------------

Before migrating tables with foreign key constraints via copydb there are a couple of things to take care of 

- Ghostferry needs to be ran with `SkipForeignKeyConstraintsCheck = true`, which will disable ghostferry to check foreign key 
  constraints during initialization.

- Source DB should be read_only.

Limitations
-------------

- While migrating tables with foreign key constraints the source db should be read_only as there are some fundamental issues when migrating tables with foreign key constraints at the same time when writes are occurring to the source database. This issue descibes briefly why the source database should be read_only during the migration - https://github.com/Shopify/katesql-migration-backend/issues/194.

- `Interrupt-Resume functionality` can be used as long as source database is read_only also during the interrupt period

- `Inline Verifier` can be used as long as it is ensured that the source database is read_only (even during the interrupt period)
