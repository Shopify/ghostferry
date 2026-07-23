package ghostferry

import (
	"sync"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
)

// SourceRuntime owns the live connection to the source MySQL server and the
// DatabaseConfig it was opened from.
//
// It exists so that the rest of Ghostferry can talk about "the current source"
// as a single, swappable thing rather than each component holding its own copy
// of a *sql.DB and a *DatabaseConfig. Today the source never changes during a
// run, but a source master failover needs to atomically repoint every source
// consumer at a newly promoted writer. Centralising ownership here is the
// prerequisite for doing that without hand-editing a field on every consumer.
//
// A SourceRuntime is safe for concurrent use. Readers obtain the current handle
// via DB()/Config(); a swap (Replace) is serialised against those readers.
//
// This type is intentionally introduced without migrating any consumers yet:
// the Ferry constructs and holds one, but components continue to read
// Ferry.SourceDB / Config.Source directly. A follow-up change migrates the
// consumers to depend on the runtime, and a further change uses Replace to
// implement failover.
type SourceRuntime struct {
	mu     sync.RWMutex
	db     *sql.DB
	config *DatabaseConfig

	// retired holds source DB handles replaced by a swap. They are not closed
	// at swap time because in-flight cursors and cached prepared statements may
	// still reference them; CloseRetired releases them once no source work is
	// running (e.g. at teardown).
	retired []*sql.DB
}

// NewSourceRuntime creates a SourceRuntime around an already-open source
// connection and the config it was opened from. Both may be nil for tests that
// only exercise Replace.
func NewSourceRuntime(db *sql.DB, config *DatabaseConfig) *SourceRuntime {
	return &SourceRuntime{
		db:     db,
		config: config,
	}
}

// DB returns the current source connection.
func (r *SourceRuntime) DB() *sql.DB {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.db
}

// Config returns the current source DatabaseConfig.
func (r *SourceRuntime) Config() *DatabaseConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config
}

// Replace opens a new source connection from the given config and installs it
// as the current source, returning the newly opened *sql.DB.
//
// The previously current handle is retired (retained, not closed) so callers
// that still hold a reference to it — an in-flight cursor, a cached prepared
// statement — do not see it closed out from under them. Use CloseRetired at a
// safe point to release retired handles.
//
// If opening the new connection fails, the current source is left unchanged and
// the error is returned.
func (r *SourceRuntime) Replace(config *DatabaseConfig, logger Logger) (*sql.DB, error) {
	newDB, err := config.SqlDB(logger)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.db != nil {
		r.retired = append(r.retired, r.db)
	}
	r.db = newDB
	r.config = config
	return newDB, nil
}

// CloseRetired closes every retired source handle and clears the retired list.
// It is best-effort and intended to be called during teardown, once no source
// cursors or verification passes remain in flight.
func (r *SourceRuntime) CloseRetired() {
	r.mu.Lock()
	retired := r.retired
	r.retired = nil
	r.mu.Unlock()

	for _, db := range retired {
		if db != nil {
			_ = db.Close()
		}
	}
}
