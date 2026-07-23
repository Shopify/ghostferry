package ghostferry

import (
	sql "github.com/Shopify/ghostferry/sqlwrapper"
)

// swapSource repoints every source-side consumer at the promoted writer after
// the binlog streamer has recovered from a master failover. It is wired as the
// streamer's OnFailover callback (see failoverRecoveryConfigForSource).
//
// The binlog streamer has already switched its own connection; this method
// brings the rest of the Ferry along so the data iterator, inline/iterative
// verifiers, and ad-hoc source queries (SHOW CREATE ...) all talk to the new
// master instead of the demoted one.
//
// Ownership: the DB handle in the event is owned by the binlog streamer, which
// closes it on its next failover or on exit. The Ferry must NOT adopt that
// handle, or a post-Run consumer (e.g. sharding delta copy) would hit a closed
// pool. Instead the Ferry opens its OWN connection to the new master and
// installs that everywhere. If it cannot open one, it logs and leaves the
// previous handles in place (streaming has still recovered).
//
// The previous Ferry-owned source DB is deliberately NOT closed here: it may
// still be used by an in-flight data-iterator cursor (each Cursor captures its
// DB by value) or a cached prepared statement, and closing it out from under a
// running query would surface as a spurious error. Previous handles are
// retained so a teardown can close them; leaking a bounded number of pools
// (one per failover) is the safer tradeoff.
//
// Concurrency: the swap is serialized by sourceSwapMu against callers that read
// through CurrentSourceDB. Cursors and prepared statements created before the
// swap keep their old DB reference; only work started after the swap observes
// the new handle. In practice failover recovery matters most at or after
// row-copy completion (binlog catchup and cutover), where this is sufficient.
func (f *Ferry) swapSource(ev MasterFailoverEvent) error {
	if ev.NewMasterConfig == nil {
		return nil
	}

	// Open a Ferry-owned connection to the new master, independent of the
	// streamer's handle so their lifecycles do not entangle.
	newDB, err := ev.NewMasterConfig.SqlDB(f.logger.WithField("dbname", "source_failover"))
	if err != nil {
		f.logger.WithError(err).Error("failover: could not open Ferry connection to promoted master; source consumers still target the old master")
		return err
	}

	f.sourceSwapMu.Lock()
	defer f.sourceSwapMu.Unlock()

	// Retain the previous handle rather than closing it (in-flight cursors /
	// cached statements may still reference it).
	if f.SourceDB != nil {
		f.previousSourceDBs = append(f.previousSourceDBs, f.SourceDB)
	}

	f.SourceDB = newDB
	f.Config.Source = ev.NewMasterConfig

	// Repoint the data iterator and its cursor factory so any cursors created
	// after this point read from the new master.
	if f.DataIterator != nil {
		f.DataIterator.DB = newDB
		if f.DataIterator.CursorConfig != nil {
			f.DataIterator.CursorConfig.DB = newDB
		}
	}

	// Repoint the inline verifier. Its prepared-statement cache is keyed only by
	// query text and holds statements bound to the old DB, so it must be reset
	// or verification would keep querying the demoted master.
	if f.inlineVerifier != nil {
		f.inlineVerifier.SourceDB = newDB
		f.inlineVerifier.sourceStmtCache = NewStmtCache()
	}

	// Repoint the configured verifier. Both source handles of the iterative
	// verifier (direct SourceDB and its CursorConfig used for pagination scans)
	// must move together. The typed-nil guards avoid a panic if a nil typed
	// pointer was stored in the Verifier interface.
	switch v := f.Verifier.(type) {
	case *IterativeVerifier:
		if v != nil {
			v.SourceDB = newDB
			if v.CursorConfig != nil {
				v.CursorConfig.DB = newDB
			}
		}
	case *ChecksumTableVerifier:
		if v != nil {
			v.SourceDB = newDB
		}
	}

	prevHost, prevPort := "", uint16(0)
	if ev.PreviousMasterConfig != nil {
		prevHost, prevPort = ev.PreviousMasterConfig.Host, ev.PreviousMasterConfig.Port
	}
	f.logger.WithFields(Fields{
		"previous_host": prevHost,
		"previous_port": prevPort,
		"new_host":      ev.NewMasterConfig.Host,
		"new_port":      ev.NewMasterConfig.Port,
	}).Info("repointed source consumers at the promoted master after failover")
	return nil
}

// CurrentSourceDB returns the source DB handle, accounting for a possible
// master failover swap. Callers that may run concurrently with a failover
// should use this rather than reading f.SourceDB directly.
func (f *Ferry) CurrentSourceDB() *sql.DB {
	f.sourceSwapMu.Lock()
	defer f.sourceSwapMu.Unlock()
	return f.SourceDB
}

// closePreviousSourceDBs closes any source DB handles retained across master
// failovers. It is best-effort and intended to be called during teardown once
// no cursors or verification passes are running.
func (f *Ferry) closePreviousSourceDBs() {
	f.sourceSwapMu.Lock()
	previous := f.previousSourceDBs
	f.previousSourceDBs = nil
	f.sourceSwapMu.Unlock()

	for _, db := range previous {
		if db != nil {
			_ = db.Close()
		}
	}
}
