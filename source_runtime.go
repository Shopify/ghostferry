package ghostferry

import (
	"sync"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
)

// SourceRuntime owns the source connection and its config; safe for concurrent use.
type SourceRuntime struct {
	mu     sync.RWMutex
	db     *sql.DB
	config *DatabaseConfig

	// retired holds replaced handles, kept open because in-flight work may
	// still reference them; released by CloseRetired.
	retired []*sql.DB
}

// NewSourceRuntime wraps an already-open source connection and its config.
func NewSourceRuntime(db *sql.DB, config *DatabaseConfig) *SourceRuntime {
	return &SourceRuntime{
		db:     db,
		config: config,
	}
}

func (r *SourceRuntime) DB() *sql.DB {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.db
}

func (r *SourceRuntime) Config() *DatabaseConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config
}

// Replace opens a new connection from config and installs it as current,
// retiring (not closing) the previous handle; on failure the source is unchanged.
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

// CloseRetired closes the handles retired by Replace; callers must ensure no
// source work is still in flight.
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
