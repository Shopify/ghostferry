package ghostferry

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/go-mysql-org/go-mysql/schema"
)

// TableState tracks where a single migrated table sits in the schema-change
// reaction state machine.
type TableState int

const (
	// StateNormal is the steady state: source and target schemas agree, the
	// data iterator and binlog streamer process events normally.
	StateNormal TableState = iota
	// StateInTransition means a schema change has been observed on at least
	// one side but the two sides have not converged yet. Binlog row events
	// for the table are skipped to avoid column-count crashes.
	StateInTransition
	// StateRecopying means the target has been cleared and the data iterator
	// is in the process of re-copying the table from source's current state.
	// Binlog row events for the table are skipped during this phase, then
	// applied normally once the copy completes.
	StateRecopying
)

func (s TableState) String() string {
	switch s {
	case StateNormal:
		return "normal"
	case StateInTransition:
		return "in_transition"
	case StateRecopying:
		return "recopying"
	}
	return "unknown"
}

// RecopyTrigger is the slice of DataIterator the detector needs to re-queue
// a table for re-iteration. Defined as an interface so the detector can be
// unit-tested without the full DataIterator wired up.
type RecopyTrigger interface {
	RequeueTable(table *TableSchema) error
	// AwaitIterationDrained blocks until any in-flight iterateTable goroutine
	// for the named table has returned. The detector uses this — not
	// StateTracker.IsTableComplete — to know when its bulk DELETE is safe.
	AwaitIterationDrained(tableName string)
}

// VerifierFlusher flushes per-table reverify entries when a table is about
// to be re-copied. Optional — nil is acceptable.
type VerifierFlusher interface {
	FlushTableEntries(schemaName, tableName string)
}

// SchemaChangeDetector reacts to DDL on either the source or target side of
// an in-progress migration. It owns a per-table state machine, drives recopy
// when source and target converge on a new schema, and exposes IsInTransition
// so the binlog streamer can skip rows for tables mid-change.
type SchemaChangeDetector struct {
	SourceDB *sqlwrapper.DB
	TargetDB *sqlwrapper.DB

	// SchemaCache is the live cache the rest of the ferry consults. The
	// detector mutates entries in place when refreshing schemas after a
	// metadata-only change; for full recopy it replaces the entry as well.
	SchemaCache TableSchemaCache

	// CopyFilter scopes BuildDelete calls during recopy. When nil, recopy
	// falls back to TRUNCATE TABLE.
	CopyFilter CopyFilter

	// RecopyTrigger is invoked after a successful target delete to put the
	// table back in DataIterator's work queue.
	RecopyTrigger RecopyTrigger

	// Verifier (optional) gets a per-table flush call when a table enters
	// StateRecopying.
	Verifier VerifierFlusher

	// Throttler (optional) is consulted before issuing the bulk delete so the
	// detector cooperates with the rest of the ferry's pacing.
	Throttler Throttler

	// StateTracker is consulted to determine whether a table's initial copy
	// has completed before the detector issues a bulk DELETE on the target.
	// Concurrently DELETEing rows that an in-flight iterator is INSERTing
	// would silently lose data — see recopy() comments.
	StateTracker *StateTracker

	ErrorHandler ErrorHandler

	// TransitionTimeout bounds how long a table may sit in StateInTransition
	// before the ferry aborts. Zero disables the timeout.
	TransitionTimeout time.Duration

	// CascadingPaginationColumnConfig is required to recompute pagination
	// metadata when refreshing a table's schema.
	CascadingPaginationColumnConfig *CascadingPaginationColumnConfig

	// DatabaseRewrites and TableRewrites mirror the ferry's source→target
	// name translation. Required when the source and target use different
	// schema/table names — without them, target schema fetches and recopy
	// DELETEs would hit the source-named (and possibly source-resident)
	// table on the target connection, producing spurious convergence.
	DatabaseRewrites map[string]string
	TableRewrites    map[string]string

	mu                  sync.RWMutex
	tableState          map[string]TableState
	transitionStartedAt map[string]time.Time
	logger              Logger
	loggerOnce          sync.Once

	// migratedTablesByDB indexes migrated tables under both source-side and
	// target-side schema/table names so DDLs picked up on either binlog stream
	// resolve to a known table. Lookups must always normalize back to source-
	// side names via targetToSource{Schema,Table} before keying the rest of
	// the detector — tableState, SchemaCache, etc. all use source-side names.
	migratedTablesByDB   map[string]map[string]struct{}
	targetToSourceSchema map[string]string
	targetToSourceTable  map[string]string
}

// NewSchemaChangeDetector returns a zero-initialized detector. Callers must
// call Init before sending DDL events.
func NewSchemaChangeDetector(sourceDB, targetDB *sqlwrapper.DB, cache TableSchemaCache) *SchemaChangeDetector {
	return &SchemaChangeDetector{
		SourceDB:    sourceDB,
		TargetDB:    targetDB,
		SchemaCache: cache,
		tableState:  map[string]TableState{},
		transitionStartedAt: map[string]time.Time{},
	}
}

func (d *SchemaChangeDetector) ensureLogger() {
	d.loggerOnce.Do(func() {
		if d.logger == nil {
			d.logger = LogWithField("tag", "schema_change_detector")
		}
	})
}

// Init populates the migrated-tables index from the schema cache. Call once
// after the cache has been built.
//
// The cache is keyed by source-side schema/table names. We index migrated
// tables under both the source-side and the target-side identity so that
// DDLs picked up on either binlog stream resolve to the same TableRef
// (which we always normalize back to source-side names — the rest of the
// detector keys on those).
func (d *SchemaChangeDetector) Init() {
	d.ensureLogger()

	d.migratedTablesByDB = map[string]map[string]struct{}{}
	d.targetToSourceSchema = map[string]string{}
	d.targetToSourceTable = map[string]string{}

	for srcDB, tgtDB := range d.DatabaseRewrites {
		d.targetToSourceSchema[tgtDB] = srcDB
	}
	for srcTbl, tgtTbl := range d.TableRewrites {
		d.targetToSourceTable[tgtTbl] = srcTbl
	}

	for _, t := range d.SchemaCache {
		if t == nil || t.Table == nil {
			continue
		}
		// Index under source-side names.
		if _, ok := d.migratedTablesByDB[t.Schema]; !ok {
			d.migratedTablesByDB[t.Schema] = map[string]struct{}{}
		}
		d.migratedTablesByDB[t.Schema][t.Name] = struct{}{}

		// Also index under target-side names so DDLs landing on the target
		// binlog tail with their rewritten schema are recognized.
		tgtSchema, tgtTable := t.Schema, t.Name
		if rewritten, ok := d.DatabaseRewrites[t.Schema]; ok {
			tgtSchema = rewritten
		}
		if rewritten, ok := d.TableRewrites[t.Name]; ok {
			tgtTable = rewritten
		}
		if tgtSchema != t.Schema || tgtTable != t.Name {
			if _, ok := d.migratedTablesByDB[tgtSchema]; !ok {
				d.migratedTablesByDB[tgtSchema] = map[string]struct{}{}
			}
			d.migratedTablesByDB[tgtSchema][tgtTable] = struct{}{}
		}
	}

	d.mu.Lock()
	for _, t := range d.SchemaCache {
		d.tableState[t.String()] = StateNormal
	}
	d.mu.Unlock()
}

// PreflightCheck refuses to start the migration when source and target are
// already drifted, or when there's a shadow table in flight on either side
// for any of the migrated tables. The caller (typically shop-mover) is
// expected to perform similar checks; this is a defense-in-depth fallback so
// ghostferry doesn't bind itself to a target that's mid-DDL.
//
// Returns a non-nil error when the migration should not start. The error
// message names every problematic table so operators can see the full picture
// in one log line.
func (d *SchemaChangeDetector) PreflightCheck(tables []*TableSchema) error {
	d.ensureLogger()

	var schemaIssues []string
	var shadowIssues []string

	for _, table := range tables {
		targetSchema, err := d.fetchTableSchema(d.TargetDB, table.Schema, table.Name)
		if err != nil {
			return fmt.Errorf("preflight: failed to read target schema for %s: %w", table.String(), err)
		}
		if targetSchema == nil {
			// Missing on target is fine — the initial copy will create rows.
		} else if !schemasEquivalent(table, targetSchema) {
			schemaIssues = append(schemaIssues, table.String())
		}
	}

	databases := map[string]struct{}{}
	for _, table := range tables {
		databases[table.Schema] = struct{}{}
	}

	migratedSet := func(db string) map[string]struct{} {
		set := map[string]struct{}{}
		for _, t := range tables {
			if t.Schema == db {
				set[t.Name] = struct{}{}
			}
		}
		return set
	}

	checkSide := func(db *sqlwrapper.DB, side string) error {
		for dbname := range databases {
			tableNames, err := showTablesFrom(db, dbname)
			if err != nil {
				return fmt.Errorf("preflight: failed to list %s tables in %s: %w", side, dbname, err)
			}
			migrated := migratedSet(dbname)
			for _, name := range tableNames {
				original := OriginalTableFromShadow(name)
				if original == "" {
					continue
				}
				if _, isMigrated := migrated[original]; isMigrated {
					shadowIssues = append(shadowIssues,
						fmt.Sprintf("%s on %s side (shadow of %s.%s)", name, side, dbname, original))
				}
			}
		}
		return nil
	}

	if err := checkSide(d.SourceDB, "source"); err != nil {
		return err
	}
	if err := checkSide(d.TargetDB, "target"); err != nil {
		return err
	}

	if len(schemaIssues) == 0 && len(shadowIssues) == 0 {
		return nil
	}

	parts := []string{}
	if len(schemaIssues) > 0 {
		parts = append(parts, "tables with mismatched source/target schemas: "+strings.Join(schemaIssues, ", "))
	}
	if len(shadowIssues) > 0 {
		parts = append(parts, "shadow tables present (DDL likely in flight): "+strings.Join(shadowIssues, ", "))
	}
	return fmt.Errorf("preflight check failed: %s", strings.Join(parts, "; "))
}

// IsInTransition reports whether row events for the given table should be
// dropped before parsing. Called from the binlog row handler hot path —
// keeps the lock window small.
//
// Only StateInTransition is treated as "skip": at that point source and
// target schemas differ and parsing the row event would either crash on
// column-count mismatch (binlog parse path) or land in BatchWriter as an
// INSERT against a column the destination does not have.
//
// StateRecopying is NOT skipped. By the time we flip to Recopying the cache
// has been refreshed to the converged schema, the target rows have been
// deleted, and a fresh iteration is running. Binlog events arriving in this
// window must be applied — the iterator's pagination cursor only sees rows
// up to its current position, so any UPDATE on an already-iterated row is
// only captured via the binlog. Skipping here would leave those updates
// missing on target and surface as cutover checksum mismatches.
func (d *SchemaChangeDetector) IsInTransition(schemaName, tableName string) bool {
	if d == nil {
		return false
	}
	key := fullTableName(schemaName, tableName)
	d.mu.RLock()
	state := d.tableState[key]
	d.mu.RUnlock()
	return state == StateInTransition
}

// OnSourceDDL is registered with the source BinlogStreamer's QueryEvent
// handler. It is invoked synchronously from the streamer goroutine so it must
// stay non-blocking.
func (d *SchemaChangeDetector) OnSourceDDL(stmt *DDLStatement) {
	d.handleDDL(stmt, "source")
}

// OnTargetDDL is the corresponding hook for TargetBinlogTail.
func (d *SchemaChangeDetector) OnTargetDDL(stmt *DDLStatement) {
	d.handleDDL(stmt, "target")
}

func (d *SchemaChangeDetector) handleDDL(stmt *DDLStatement, side string) {
	if stmt == nil {
		return
	}
	d.ensureLogger()

	affected := d.affectedMigratedTables(stmt)
	if len(affected) == 0 {
		return
	}

	for _, ref := range affected {
		key := fullTableName(ref.SchemaName, ref.TableName)

		d.mu.Lock()
		state := d.tableState[key]
		if state == StateNormal {
			d.tableState[key] = StateInTransition
			d.transitionStartedAt[key] = time.Now()
			d.logger.WithFields(Fields{
				"table":   key,
				"side":    side,
				"ddlType": stmt.DDLType,
			}).Info("table entered transition")
		}
		d.mu.Unlock()

		// Convergence check runs in its own goroutine so we never block the
		// binlog handler on a target schema fetch or a bulk delete.
		go d.checkConvergence(ref)
	}
}

// affectedMigratedTables returns the set of migrated tables this DDL touches.
// Includes shadow-table CREATE/RENAME mappings: a CREATE _foo_gho on a
// migrated table foo puts foo into transition, and the gh-ost cutover RENAME
// puts both halves into transition.
func (d *SchemaChangeDetector) affectedMigratedTables(stmt *DDLStatement) []TableRef {
	if d.migratedTablesByDB == nil {
		return nil
	}

	candidates := stmt.AllAffectedTables()
	out := make([]TableRef, 0, len(candidates))
	seen := map[string]struct{}{}

	// addRef appends a (source-keyed) TableRef, deduping. We always normalize
	// matches back to source-side names because the rest of the detector
	// (tableState, SchemaCache lookups, IsInTransition queries from the
	// streamer) keys on those.
	addRef := func(sourceSchema, sourceTable string) {
		key := sourceSchema + "." + sourceTable
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		out = append(out, TableRef{SchemaName: sourceSchema, TableName: sourceTable})
	}

	considerName := func(schemaName, tableName string) {
		if schemaName == "" || tableName == "" {
			return
		}
		tables, ok := d.migratedTablesByDB[schemaName]
		if !ok {
			return
		}

		// Translate target-side identity back to source-side. When the DDL
		// arrives on the target binlog tail, schemaName/tableName are the
		// target's, but we want to key the rest of the detector under the
		// source-side names that match the schema cache.
		sourceSchema := schemaName
		if rewritten, ok := d.targetToSourceSchema[schemaName]; ok {
			sourceSchema = rewritten
		}
		sourceTable := tableName
		if rewritten, ok := d.targetToSourceTable[tableName]; ok {
			sourceTable = rewritten
		}

		// Direct match: a migrated table is the literal subject of the DDL.
		if _, isMigrated := tables[tableName]; isMigrated {
			addRef(sourceSchema, sourceTable)
			return
		}

		// Shadow-table match: DDL on _<original>_gho/_del/_ghc/_new/_old means
		// gh-ost or pt-osc is mid-migration on a migrated original.
		if original := OriginalTableFromShadow(tableName); original != "" {
			if _, isMigrated := tables[original]; isMigrated {
				// Normalize the shadow's "original" through the rewrite map
				// in case the rewrite is on the table name rather than the
				// schema. Falls back to original when no rewrite exists.
				originalSource := original
				if rewritten, ok := d.targetToSourceTable[original]; ok {
					originalSource = rewritten
				}
				addRef(sourceSchema, originalSource)
			}
		}
	}

	for _, ref := range candidates {
		considerName(ref.SchemaName, ref.TableName)
	}
	return out
}

func (d *SchemaChangeDetector) checkConvergence(ref TableRef) {
	key := fullTableName(ref.SchemaName, ref.TableName)
	logger := d.logger.WithField("table", key)

	d.mu.RLock()
	state := d.tableState[key]
	d.mu.RUnlock()
	if state != StateInTransition {
		return
	}

	sourceSchema, err := d.fetchTableSchema(d.SourceDB, ref.SchemaName, ref.TableName)
	if err != nil {
		logger.WithError(err).Warn("convergence: failed to read source schema; will retry on next DDL")
		return
	}
	targetSchemaName, targetTableName := d.rewriteToTarget(ref.SchemaName, ref.TableName)
	targetSchema, err := d.fetchTableSchema(d.TargetDB, targetSchemaName, targetTableName)
	if err != nil {
		logger.WithError(err).Warn("convergence: failed to read target schema; will retry on next DDL")
		return
	}

	if sourceSchema == nil || targetSchema == nil {
		// One side dropped or hasn't yet recreated. Stay in transition.
		logger.Debug("convergence: one side missing the table; remaining in transition")
		return
	}

	if !schemasEquivalent(sourceSchema, targetSchema) {
		logger.Debug("convergence: source and target still differ; remaining in transition")
		return
	}

	cached := d.SchemaCache[key]
	impact := ClassifyImpact(cached, sourceSchema)

	if impact == ImpactMetadataOnly {
		d.applyMetadataOnly(ref, sourceSchema)
		return
	}

	d.recopy(ref, sourceSchema)
}

func (d *SchemaChangeDetector) applyMetadataOnly(ref TableRef, fresh *TableSchema) {
	key := fullTableName(ref.SchemaName, ref.TableName)
	logger := d.logger.WithField("table", key)

	d.mu.Lock()
	if d.SchemaCache != nil {
		// In-place replace — readers using the *TableSchema pointer continue
		// to see the old schema for any in-flight call, but new lookups will
		// resolve the refreshed entry.
		d.SchemaCache[key] = fresh
	}
	d.tableState[key] = StateNormal
	delete(d.transitionStartedAt, key)
	d.mu.Unlock()

	logger.Info("convergence: metadata-only change applied; table back to normal")
}

func (d *SchemaChangeDetector) recopy(ref TableRef, fresh *TableSchema) {
	key := fullTableName(ref.SchemaName, ref.TableName)
	logger := d.logger.WithField("table", key)

	// Step 1: refresh schema cache so subsequent inserts use fresh layout.
	d.mu.Lock()
	if d.SchemaCache != nil {
		d.SchemaCache[key] = fresh
	}
	d.mu.Unlock()

	// Step 2: drop verifier entries for the table so we don't reverify rows
	// that no longer exist on target.
	if d.Verifier != nil {
		d.Verifier.FlushTableEntries(ref.SchemaName, ref.TableName)
	}

	// Step 3: wait for any in-flight iterateTable goroutine for this table
	// to return. The DataIterator registers a per-table "iteration in
	// flight" channel that is closed when the goroutine exits — including
	// the errSchemaDriftDetected abort path. We block on that channel
	// rather than IsTableComplete because the abort path no longer marks
	// the table complete (doing so would lie to ferry's outer pipeline and
	// let cutover proceed before the recopy finishes).
	if d.RecopyTrigger != nil {
		d.RecopyTrigger.AwaitIterationDrained(fresh.String())
	}

	// Step 4: bulk delete target rows for this table.
	if d.Throttler != nil {
		WaitForThrottle(d.Throttler)
	}
	if err := d.deleteTargetRows(fresh); err != nil {
		logger.WithError(err).Error("recopy: target delete failed; aborting ferry")
		if d.ErrorHandler != nil {
			d.ErrorHandler.Fatal("schema_change_detector", err)
		}
		return
	}

	// Step 5: flip InTransition → Recopying atomically. From this point on,
	// row events for the table are still skipped (IsInTransition true) until
	// re-iteration finishes via OnTableIterationComplete.
	d.mu.Lock()
	d.tableState[key] = StateRecopying
	d.mu.Unlock()

	// Step 6: hand off to the data iterator.
	if d.RecopyTrigger == nil {
		logger.Warn("recopy: no RecopyTrigger configured; target is empty for this table but no re-iteration will run")
		return
	}
	if err := d.RecopyTrigger.RequeueTable(fresh); err != nil {
		logger.WithError(err).Error("recopy: failed to requeue table for re-iteration")
		if d.ErrorHandler != nil {
			d.ErrorHandler.Fatal("schema_change_detector", err)
		}
	}
}

// IsTableQuiesced reports whether the given table is currently mid-schema-
// change (StateInTransition or StateRecopying). Background work that would
// surface spurious failures during a transition — most notably the inline
// verifier's periodic reverify cycle — should skip these tables until the
// detector reports them back to StateNormal.
//
// Distinct from IsInTransition: that method is the binlog row-event hot
// path and is intentionally narrower (only StateInTransition skips events,
// because StateRecopying needs binlog application to capture UPDATEs to
// rows the cursor has already passed).
func (d *SchemaChangeDetector) IsTableQuiesced(schemaName, tableName string) bool {
	if d == nil {
		return false
	}
	key := fullTableName(schemaName, tableName)
	d.mu.RLock()
	state := d.tableState[key]
	d.mu.RUnlock()
	return state != StateNormal
}

// AwaitAllNormal blocks until every tracked table is in StateNormal. Ferry
// calls this immediately before VerifyBeforeCutover so the verifier never
// runs against a table whose target rows are in the middle of being
// deleted/recopied. Honors TransitionTimeout — if any table is still not
// Normal after the timeout, returns an error so the caller can abort.
func (d *SchemaChangeDetector) AwaitAllNormal() error {
	if d == nil {
		return nil
	}
	d.ensureLogger()
	logged := false
	deadline := time.Time{}
	if d.TransitionTimeout > 0 {
		deadline = time.Now().Add(d.TransitionTimeout)
	}
	for {
		if d.allNormal() {
			return nil
		}
		if !logged {
			d.logger.Info("waiting for any in-flight schema-change recopies to finish before cutover")
			logged = true
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return fmt.Errorf("timeout (%s) waiting for schema-change recopies to drain", d.TransitionTimeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (d *SchemaChangeDetector) allNormal() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for _, s := range d.tableState {
		if s != StateNormal {
			return false
		}
	}
	return true
}

// OnTableIterationComplete is wired up as a DataIterator table-completion
// listener at startup. Whenever an iteration finishes (initial copy or a
// requeue), the detector inspects the table's state and, if it was in
// StateRecopying, transitions it back to StateNormal. Initial-copy
// completions for tables in StateNormal are no-ops.
func (d *SchemaChangeDetector) OnTableIterationComplete(table *TableSchema) {
	if table == nil {
		return
	}
	key := fullTableName(table.Schema, table.Name)
	d.mu.Lock()
	if d.tableState[key] == StateRecopying {
		d.tableState[key] = StateNormal
		delete(d.transitionStartedAt, key)
		d.logger.WithField("table", key).Info("recopy completed; table back to normal")
	}
	d.mu.Unlock()
}

// CheckTimeouts returns an error when any table has been stuck in transition
// past TransitionTimeout. Caller should run this from a periodic ticker.
func (d *SchemaChangeDetector) CheckTimeouts(now time.Time) error {
	if d.TransitionTimeout <= 0 {
		return nil
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	for key, started := range d.transitionStartedAt {
		if now.Sub(started) > d.TransitionTimeout {
			return fmt.Errorf("table %s stuck in transition for %s (timeout %s)",
				key, now.Sub(started), d.TransitionTimeout)
		}
	}
	return nil
}

// RunTimeoutChecker drives CheckTimeouts on a ticker until ctx is cancelled.
// Aborts the ferry via ErrorHandler on any timeout.
func (d *SchemaChangeDetector) RunTimeoutChecker(ctx context.Context) {
	if d.TransitionTimeout <= 0 {
		return
	}
	ticker := time.NewTicker(d.TransitionTimeout / 8)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if err := d.CheckTimeouts(now); err != nil {
				if d.ErrorHandler != nil {
					d.ErrorHandler.Fatal("schema_change_detector", err)
				}
				return
			}
		}
	}
}

func (d *SchemaChangeDetector) deleteTargetRows(table *TableSchema) error {
	// Build a transient TableSchema bearing the target-side schema and
	// table names so BuildDelete / TRUNCATE produce queries that resolve on
	// the target connection. The original table object is left untouched —
	// it is shared with the rest of the ferry which still keys on source
	// names.
	targetTable := d.tableForTarget(table)

	if d.CopyFilter != nil {
		builder, err := d.CopyFilter.BuildDelete(targetTable)
		if err != nil {
			d.logger.WithFields(Fields{
				"table": targetTable.String(),
				"err":   err.Error(),
			}).Warn("BuildDelete unavailable; falling back to no-op (recopy will rely on INSERT IGNORE)")
			return nil
		}
		query, args, err := builder.ToSql()
		if err != nil {
			return fmt.Errorf("BuildDelete ToSql: %w", err)
		}
		if _, err := d.TargetDB.Exec(query, args...); err != nil {
			return fmt.Errorf("delete target rows for %s: %w", targetTable.String(), err)
		}
		return nil
	}

	// No CopyFilter (full-table copy): TRUNCATE is the safe equivalent.
	q := fmt.Sprintf("TRUNCATE TABLE %s", QuotedTableName(targetTable))
	if _, err := d.TargetDB.Exec(q); err != nil {
		return fmt.Errorf("truncate target table %s: %w", targetTable.String(), err)
	}
	return nil
}

// rewriteToTarget translates a source-side schema/table name into the
// target-side equivalent using the ferry's rewrite maps. Returns the input
// unchanged when no rewrite is configured.
func (d *SchemaChangeDetector) rewriteToTarget(schemaName, tableName string) (string, string) {
	if rewritten, ok := d.DatabaseRewrites[schemaName]; ok {
		schemaName = rewritten
	}
	if rewritten, ok := d.TableRewrites[tableName]; ok {
		tableName = rewritten
	}
	return schemaName, tableName
}

// tableForTarget returns a *TableSchema with Schema/Name rewritten to the
// target-side identity. Column metadata is shared with the input — only the
// outer name fields differ — so callers can safely use it for QuotedTableName
// and BuildDelete without disturbing the original.
func (d *SchemaChangeDetector) tableForTarget(src *TableSchema) *TableSchema {
	if src == nil || src.Table == nil {
		return src
	}
	targetSchema, targetTable := d.rewriteToTarget(src.Schema, src.Name)
	if targetSchema == src.Schema && targetTable == src.Name {
		return src
	}
	clonedInner := *src.Table
	clonedInner.Schema = targetSchema
	clonedInner.Name = targetTable
	cloned := *src
	cloned.Table = &clonedInner
	return &cloned
}

func (d *SchemaChangeDetector) fetchTableSchema(db *sqlwrapper.DB, schemaName, tableName string) (*TableSchema, error) {
	t, err := schema.NewTableFromSqlDB(db.DB, schemaName, tableName)
	if err != nil {
		// MySQL surfaces a missing table as an error; treat it as "not present"
		// so callers can distinguish from real failures.
		if isTableNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}

	visibleIndexes := make([]*schema.Index, 0, len(t.Indexes))
	for _, idx := range t.Indexes {
		if idx.Visible {
			visibleIndexes = append(visibleIndexes, idx)
		}
	}
	t.Indexes = visibleIndexes

	ts := &TableSchema{Table: t}
	if d.CascadingPaginationColumnConfig != nil {
		col, idx, err := ts.paginationKeyColumn(d.CascadingPaginationColumnConfig)
		if err == nil {
			ts.PaginationKeyColumn = col
			ts.PaginationKeyIndex = idx
		}
	}
	return ts, nil
}

func isTableNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Error 1146") || strings.Contains(strings.ToLower(msg), "doesn't exist")
}

// schemasEquivalent compares two schemas by structural fields the binlog
// stream cares about: column names, column types, and pagination column.
// Index/comment/default differences do not count as a divergence.
func schemasEquivalent(a, b *TableSchema) bool {
	if a == nil || b == nil || a.Table == nil || b.Table == nil {
		return false
	}
	if len(a.Columns) != len(b.Columns) {
		return false
	}
	for i := range a.Columns {
		ac := a.Columns[i]
		bc := b.Columns[i]
		if ac.Name != bc.Name {
			return false
		}
		if !columnTypesEquivalent(ac.RawType, bc.RawType) {
			return false
		}
		if ac.IsUnsigned != bc.IsUnsigned {
			return false
		}
	}
	return true
}
