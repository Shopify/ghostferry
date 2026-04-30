package ghostferry

import (
	"context"
	"fmt"
	"io"
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

	// TraceWriter, when non-nil, receives one short line per decision the
	// detector makes (parsed DDL, affected tables, state transitions,
	// convergence outcomes, recopy steps). Diagnosing whether a recopy fired
	// or stalled is a cat-the-file operation when this is wired up.
	TraceWriter io.Writer

	mu                  sync.RWMutex
	tableState          map[string]TableState
	transitionStartedAt map[string]time.Time
	// clearTargetDone tracks per-table channels closed when the early
	// target-row DELETE finishes. The DELETE fires at the moment a table
	// first transitions Normal → InTransition (on either source or target
	// DDL) so the slow part of recopy is overlapped with whatever DDL is
	// still pending on the other side. recopy() waits on the channel before
	// requeuing iteration. Reset to nil when the table returns to Normal.
	clearTargetDone     map[string]chan struct{}
	logger              Logger
	loggerOnce          sync.Once
	traceMu             sync.Mutex

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
		clearTargetDone: map[string]chan struct{}{},
	}
}

func (d *SchemaChangeDetector) ensureLogger() {
	d.loggerOnce.Do(func() {
		if d.logger == nil {
			d.logger = LogWithField("tag", "schema_change_detector")
		}
	})
}

// trace appends a short timestamped line to TraceWriter. No-op when
// TraceWriter is nil. Errors writing to the trace are intentionally swallowed
// — the trace is for diagnostics; it must never fail the migration.
func (d *SchemaChangeDetector) trace(format string, args ...interface{}) {
	if d == nil || d.TraceWriter == nil {
		return
	}
	line := fmt.Sprintf(format, args...)
	d.traceMu.Lock()
	defer d.traceMu.Unlock()
	_, _ = fmt.Fprintf(d.TraceWriter, "%s %s\n", time.Now().UTC().Format(time.RFC3339Nano), line)
}

// Trace satisfies DDLTracer — same body as trace(), exposed so DataIterator
// can emit lines through the detector's tracer.
func (d *SchemaChangeDetector) Trace(format string, args ...interface{}) {
	d.trace(format, args...)
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

	d.trace("OnDDL side=%s ddlType=%s schema=%s table=%s rawQuery=%q",
		side, stmt.DDLType, stmt.SchemaName, stmt.TableName, truncateQuery(stmt.RawQuery))

	affected := d.affectedMigratedTables(stmt)
	d.trace("affectedMigratedTables side=%s out=%s", side, formatTableRefs(affected))
	if len(affected) == 0 {
		return
	}

	for _, ref := range affected {
		key := fullTableName(ref.SchemaName, ref.TableName)

		d.mu.Lock()
		state := d.tableState[key]
		transitioned := false
		var clearDone chan struct{}
		if state == StateNormal {
			d.tableState[key] = StateInTransition
			d.transitionStartedAt[key] = time.Now()
			transitioned = true
			clearDone = make(chan struct{})
			d.clearTargetDone[key] = clearDone
			d.logger.WithFields(Fields{
				"table":   key,
				"side":    side,
				"ddlType": stmt.DDLType,
			}).Info("table entered transition")
		}
		d.mu.Unlock()

		if transitioned {
			d.trace("transition table=%s from=normal to=in_transition side=%s ddlType=%s", key, side, stmt.DDLType)
			// Spawn the early target DELETE. Runs in parallel with whatever
			// DDL is still pending on the other side, so by the time the
			// schemas converge, the slow part of recopy is already done.
			go d.runClearTarget(ref, clearDone)
		} else {
			d.trace("transition_skipped table=%s already_state=%s side=%s", key, state, side)
		}

		// Convergence check runs in its own goroutine so we never block the
		// binlog handler on a target schema fetch.
		go d.checkConvergence(ref)
	}
}

// runClearTarget issues the bulk DELETE on target rows for ref, using the
// currently cached schema for name resolution. Closes done when finished
// (success or failure). Spawned exactly once per Normal → InTransition
// transition.
func (d *SchemaChangeDetector) runClearTarget(ref TableRef, done chan struct{}) {
	defer close(done)

	key := fullTableName(ref.SchemaName, ref.TableName)
	logger := d.logger.WithField("table", key)

	// Wait for any in-flight iterateTable goroutine to drain. The iterator
	// aborts itself via TransitionChecker once it observes InTransition,
	// but we must not start DELETE until the abort path has actually
	// returned — concurrent INSERT IGNORE from the iterator would survive
	// the DELETE and leave inconsistent state.
	if d.RecopyTrigger != nil {
		d.RecopyTrigger.AwaitIterationDrained(key)
	}

	if d.Throttler != nil {
		WaitForThrottle(d.Throttler)
	}

	cached := d.SchemaCache[key]
	if cached == nil {
		d.trace("clear_target_skipped table=%s reason=no_cached_schema", key)
		return
	}

	d.trace("clear_target_start table=%s", key)
	if err := d.deleteTargetRows(cached); err != nil {
		logger.WithError(err).Error("clear_target failed")
		d.trace("clear_target_failed table=%s err=%q", key, err.Error())
		if d.ErrorHandler != nil {
			d.ErrorHandler.Fatal("schema_change_detector", err)
		}
		return
	}
	d.trace("clear_target_done table=%s", key)
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
		d.trace("checkConvergence_skip table=%s state=%s", key, state)
		return
	}

	sourceSchema, err := d.fetchTableSchema(d.SourceDB, ref.SchemaName, ref.TableName)
	if err != nil {
		logger.WithError(err).Warn("convergence: failed to read source schema; will retry on next DDL")
		d.trace("checkConvergence_error table=%s side=source err=%q", key, err.Error())
		return
	}
	targetSchemaName, targetTableName := d.rewriteToTarget(ref.SchemaName, ref.TableName)
	targetSchema, err := d.fetchTableSchema(d.TargetDB, targetSchemaName, targetTableName)
	if err != nil {
		logger.WithError(err).Warn("convergence: failed to read target schema; will retry on next DDL")
		d.trace("checkConvergence_error table=%s side=target target_table=%s.%s err=%q",
			key, targetSchemaName, targetTableName, err.Error())
		return
	}

	d.trace("checkConvergence table=%s source_cols=%d target_cols=%d source_present=%t target_present=%t",
		key, columnCount(sourceSchema), columnCount(targetSchema), sourceSchema != nil, targetSchema != nil)

	if sourceSchema == nil || targetSchema == nil {
		// One side dropped or hasn't yet recreated. Stay in transition.
		logger.Debug("convergence: one side missing the table; remaining in transition")
		d.trace("convergence_pending table=%s reason=one_side_missing", key)
		return
	}

	if !schemasEquivalent(sourceSchema, targetSchema) {
		logger.Debug("convergence: source and target still differ; remaining in transition")
		d.trace("convergence_pending table=%s reason=schemas_differ source_cols=%s target_cols=%s",
			key, formatColumnNames(sourceSchema), formatColumnNames(targetSchema))
		return
	}

	cached := d.SchemaCache[key]
	impact := ClassifyImpact(cached, sourceSchema)
	d.trace("convergence_reached table=%s impact=%s cached_cols=%d fresh_cols=%d",
		key, impactString(impact), columnCount(cached), columnCount(sourceSchema))

	if impact == ImpactMetadataOnly {
		d.applyMetadataOnly(ref, sourceSchema)
		return
	}

	d.recopy(ref, sourceSchema)
}

func (d *SchemaChangeDetector) applyMetadataOnly(ref TableRef, fresh *TableSchema) {
	key := fullTableName(ref.SchemaName, ref.TableName)
	logger := d.logger.WithField("table", key)

	// The early DELETE may have already wiped target rows for this table.
	// For metadata-only changes we still need to re-iterate to repopulate.
	// Wait for the DELETE first, then requeue so iteration starts with
	// clean target.
	d.mu.RLock()
	clearDone := d.clearTargetDone[key]
	d.mu.RUnlock()
	if clearDone != nil {
		d.trace("metadata_only_wait_clear_target table=%s", key)
		<-clearDone
	}

	d.mu.Lock()
	if d.SchemaCache != nil {
		// In-place replace — readers using the *TableSchema pointer continue
		// to see the old schema for any in-flight call, but new lookups will
		// resolve the refreshed entry.
		d.SchemaCache[key] = fresh
	}
	if d.tableState[key] == StateRecopying {
		// Another path beat us. Just clean up state and exit.
		d.mu.Unlock()
		d.trace("metadata_only_skipped table=%s reason=already_recopying", key)
		return
	}
	requireRequeue := clearDone != nil
	if requireRequeue {
		d.tableState[key] = StateRecopying
	} else {
		d.tableState[key] = StateNormal
		delete(d.transitionStartedAt, key)
	}
	d.mu.Unlock()

	if !requireRequeue {
		logger.Info("convergence: metadata-only change applied (no early DELETE); table back to normal")
		d.trace("metadata_only_applied table=%s state=in_transition→normal", key)
		return
	}

	d.trace("metadata_only_requeue table=%s state=in_transition→recopying", key)
	if d.RecopyTrigger != nil {
		if err := d.RecopyTrigger.RequeueTable(fresh); err != nil {
			logger.WithError(err).Error("metadata_only: failed to requeue after early DELETE")
			d.trace("metadata_only_requeue_failed table=%s err=%q", key, err.Error())
			if d.ErrorHandler != nil {
				d.ErrorHandler.Fatal("schema_change_detector", err)
			}
		}
	}
}

func (d *SchemaChangeDetector) recopy(ref TableRef, fresh *TableSchema) {
	key := fullTableName(ref.SchemaName, ref.TableName)
	logger := d.logger.WithField("table", key)

	d.trace("recopy step=1_refresh_cache table=%s", key)
	d.mu.Lock()
	if d.SchemaCache != nil {
		d.SchemaCache[key] = fresh
	}
	clearDone := d.clearTargetDone[key]
	d.mu.Unlock()

	d.trace("recopy step=2_flush_verifier table=%s verifier_present=%t", key, d.Verifier != nil)
	if d.Verifier != nil {
		d.Verifier.FlushTableEntries(ref.SchemaName, ref.TableName)
	}

	// Wait for the early target DELETE (spawned at first DDL) to finish.
	// Channel is nil only if no DDL ever fired for this table — defensive,
	// shouldn't happen since we only get here from checkConvergence.
	d.trace("recopy step=3_wait_clear_target table=%s clear_present=%t", key, clearDone != nil)
	if clearDone != nil {
		<-clearDone
	}
	d.trace("recopy step=3_wait_clear_target_done table=%s", key)

	// Atomically claim the requeue. If a second concurrent recopy() reaches
	// this point first, it has already flipped the state and we abort —
	// avoids double-spawning iterateTable goroutines for the same table.
	d.mu.Lock()
	if d.tableState[key] == StateRecopying {
		d.mu.Unlock()
		d.trace("recopy step=4_skipped table=%s reason=already_recopying", key)
		return
	}
	d.tableState[key] = StateRecopying
	d.mu.Unlock()
	d.trace("recopy step=4_state table=%s state=in_transition→recopying", key)

	if d.RecopyTrigger == nil {
		logger.Warn("recopy: no RecopyTrigger configured; target is empty for this table but no re-iteration will run")
		d.trace("recopy step=5_requeue_skipped table=%s reason=no_trigger", key)
		return
	}
	d.trace("recopy step=5_requeue table=%s", key)
	if err := d.RecopyTrigger.RequeueTable(fresh); err != nil {
		logger.WithError(err).Error("recopy: failed to requeue table for re-iteration")
		d.trace("recopy step=5_requeue_failed table=%s err=%q", key, err.Error())
		if d.ErrorHandler != nil {
			d.ErrorHandler.Fatal("schema_change_detector", err)
		}
		return
	}
	d.trace("recopy step=5_requeue_done table=%s", key)
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
	prev := d.tableState[key]
	if prev == StateRecopying {
		d.tableState[key] = StateNormal
		delete(d.transitionStartedAt, key)
		// Drop the completed clearTargetDone channel so a future DDL on
		// this table allocates a fresh one for its early DELETE.
		delete(d.clearTargetDone, key)
		d.logger.WithField("table", key).Info("recopy completed; table back to normal")
	}
	d.mu.Unlock()
	d.trace("OnTableIterationComplete table=%s prev_state=%s flipped_to_normal=%t",
		key, prev, prev == StateRecopying)
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

// Helpers used to format trace lines. Kept here so trace strings stay
// consistent across decision points.

func truncateQuery(q string) string {
	const max = 200
	q = strings.ReplaceAll(q, "\n", " ")
	if len(q) <= max {
		return q
	}
	return q[:max] + "…"
}

func formatTableRefs(refs []TableRef) string {
	if len(refs) == 0 {
		return "[]"
	}
	parts := make([]string, len(refs))
	for i, r := range refs {
		parts[i] = fmt.Sprintf("%s.%s", r.SchemaName, r.TableName)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func columnCount(t *TableSchema) int {
	if t == nil || t.Table == nil {
		return -1
	}
	return len(t.Columns)
}

func formatColumnNames(t *TableSchema) string {
	if t == nil || t.Table == nil {
		return "[]"
	}
	names := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		names[i] = c.Name
	}
	return "[" + strings.Join(names, ",") + "]"
}

func impactString(i SchemaImpact) string {
	switch i {
	case ImpactMetadataOnly:
		return "metadata_only"
	case ImpactRequiresRecopy:
		return "requires_recopy"
	}
	return "unknown"
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
