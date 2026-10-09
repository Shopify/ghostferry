---
layout: default
title: Cutover dependency copy (POC)
---

# Cutover dependency copy (POC)

This opt-in prototype copies small dependency sets during sharding cutover.
It separates three keys:

- The sharding key selects reference rows.
- Join columns select dependency rows.
- A source pagination key orders the copy. A separate logical key identifies the destination row.

The feature does not require a sharding-key column on the dependency table.
It does not replace `JoinedTables` or change that option's behavior.

## Caller contract

**Do not enable this prototype until the caller can enforce these conditions.**

- `CutoverLock` drains in-flight writes and prevents new writes to the selected
  references and dependencies until cutover completes.
- The same protection covers destination dependency updates and cleanup.
  A shop lock alone does not prove that pod-wide jobs honor this contract.
- All required source data exists before cutover. This feature cannot recover
  dependencies that were lost before the move.
- Reference tables use InnoDB. Source and target dependency tables use InnoDB,
  have matching column definitions, and have no target triggers or generated columns.
- Each dependency has a single integer primary key for pagination and a separate
  non-nullable, full-column unique logical key on both databases.
- Query plans and the configured row, byte, and time budgets fit the write-stop window.
- Selected reference rows follow the normal migration path. The caller must not
  exclude them from normal copying while it relies on them at the destination.

Ghostferry checks the schema restrictions at cutover. It cannot establish the
application's lock or reference-copy contract from the configuration.

## Example

This example uses fictional `installations`, `versions`, and `modules` tables.
The sharding key is `tenant_id` and the selected tenant is `1`.
Add this section to an otherwise valid sharding configuration with a real
`CutoverLock` callback:

```json
{
  "CutoverDependencies": {
    "MaxRowsPerTable": 1000,
    "MaxBytes": 16777216,
    "TimeoutSeconds": 10,
    "Tables": [
      {
        "Table": "versions",
        "ReferenceTable": "installations",
        "PaginationColumn": "id",
        "IdentityColumns": ["app_id", "app_version_id"],
        "JoinColumns": [
          {"Column": "app_id", "ReferenceColumn": "app_id"},
          {"Column": "app_version_id", "ReferenceColumn": "deployment_id"}
        ],
        "ReferenceEquals": {"development": true},
        "Equals": {"release_id": 0},
        "RequireMatch": true
      },
      {
        "Table": "modules",
        "ReferenceTable": "installations",
        "PaginationColumn": "id",
        "IdentityColumns": ["app_id", "app_version_id", "module_uuid"],
        "JoinColumns": [
          {"Column": "app_id", "ReferenceColumn": "app_id"},
          {"Column": "app_version_id", "ReferenceColumn": "deployment_id"}
        ],
        "ReferenceEquals": {"development": true},
        "Equals": {"release_id": 0}
      }
    ]
  }
}
```

`ReferenceEquals` and `Equals` support parameterized scalar equality predicates.
A null value means `IS NULL` through MySQL's null-safe equality operator.
`RequireMatch` requires at least one matching dependency for every selected
reference. It does not assert a module count. The example permits a version with
zero modules but rejects a missing version.

These explicit dependency tables remain outside normal row copying, binlog
streaming, and the pagination-based verifiers, even if they have a sharding-key
column. `IncludedTables` and `IgnoredTables` govern the normal copy; they do not
cancel an explicit dependency declaration. Do not also declare a dependency in
`JoinedTables` or `PrimaryKeyTables`. Dependency chains are not supported.

## Cutover sequence

1. Obtain the existing cutover lock callback's acknowledgment.
2. Wait for the configured source replica to catch up, then flush binlog writes.
3. Complete the existing joined-table delta copy.
4. Open a repeatable-read source snapshot and one target transaction.
5. Select dependencies through the final source references, in batches of 100.
6. Insert each source row with its existing surrogate key.
7. If an insert encounters a duplicate key, leave the destination unchanged.
8. Read the destination by logical key with `FOR UPDATE`.
9. Compare every column except the pagination key byte-for-byte, including nullness.
10. Commit all dependency tables together only after every comparison succeeds.
11. Continue normal verification and cutover.

For example, source `(id=100, app_id=7, app_version_id=42)` can match destination
`(id=900, app_id=7, app_version_id=42)`. The destination keeps `id=900`.
An unrelated row with `id=100` does not count as a successful copy. A missing
logical identity or any other payload difference aborts the dependency transaction.

The prototype does not overwrite payload differences, delete destination rows,
allocate replacement IDs, or rewrite references. Timestamps also participate in
verification. This deliberately rejects ambiguous cases instead of choosing a
winner for shared data.

## Limits and failure behavior

- `MaxRowsPerTable` must be between 1 and 100000.
- `MaxBytes` bounds the total source field bytes across all dependency tables.
  It is not a hard bound on driver buffers or one oversized row allocation.
- `TimeoutSeconds` must be between 1 and 300. It bounds the dependency phase,
  not the entire cutover.
- An error rolls back the dependency transaction and takes the existing fatal
  error path. The success unlock callback must not run.
- The caller retains responsibility for failure recovery and lock release.
- The dependency phase restarts from the beginning on retry; identical existing
  rows make the operation idempotent.
- If a later cutover phase fails, the committed dependency rows remain. This
  prototype does not remove them, since they might have other owners.
- A successful copy does not protect against writers after the dependency
  transaction commits. The caller contract must hold through route switch.

## Validation and remaining work

The MySQL tests cover composite joins, duplicate references, unrelated tenants,
released-row exclusion, multiple modules per version, different destination IDs,
repeat copies, binary/null payloads, keyset pagination, missing dependencies,
surrogate collisions, payload mismatches, schema restrictions, budgets, and a
blocked-write deadline. End-to-end tests cover a final write in the lock callback,
normal target verification, and no success unlock on conflict.

This remains a POC. Before production use:

- Confirm all writer and cleanup paths honor the caller contract on both sides.
- Add an application-level lock test with a concurrent development request.
- Validate real query plans and cutover latency with representative data.
- Design an explicit payload-conflict policy if strict equality is too restrictive.
- Add source-replica and failure/retry scenario coverage.
- Add version-gated caller configuration. This patch enables no application tables.

The feature applies only to Ghostferry's MySQL sharding path, not other migration backends.
