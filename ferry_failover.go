package ghostferry

import (
	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// ferrySourceReconnector implements SourceReconnector for the Ferry. It is the
// single place a source master failover repoints the whole run: it resolves the
// promoted writer, opens one Ferry-owned connection to it via SourceRuntime
// (which atomically becomes the source for every consumer that starts new work
// afterwards — data iterator, verifiers, ...), validates it is a safe target,
// and hands the connection back to the binlog streamer.
//
// Because every source consumer already reads its connection from the
// SourceRuntime, there is nothing to hand-repoint here: the Replace is the swap.
type ferrySourceReconnector struct {
	ferry    *Ferry
	resolver MasterWriterResolver
}

// Reconnect satisfies SourceReconnector. previous is the config the streamer
// last used; appliedGTIDSet is everything already emitted downstream, which the
// promoted writer must contain.
func (r *ferrySourceReconnector) Reconnect(previous *DatabaseConfig, appliedGTIDSet mysql.GTIDSet) (*sql.DB, *DatabaseConfig, error) {
	candidate, err := r.resolver.ResolveCurrentMaster(previous)
	if err != nil {
		return nil, nil, err
	}
	if candidate == nil {
		return nil, nil, errNilCandidate
	}

	// Install the candidate as the current source ONLY if it validates. Fail
	// closed: the promoted writer must be a writable server with GTID mode on
	// and must contain everything already applied downstream. A candidate that
	// fails validation is never published to consumers. On success the old
	// handle is retired (retained, not closed), since in-flight cursors / cached
	// statements may still reference it.
	newDB, err := r.ferry.sourceRuntime.ReplaceValidated(
		candidate,
		r.ferry.logger.WithField("dbname", "source_failover"),
		func(db *sql.DB) error {
			return validateFailoverTarget(db, candidate, appliedGTIDSet)
		},
	)
	if err != nil {
		return nil, nil, err
	}

	r.ferry.logger.WithFields(Fields{
		"new_host": candidate.Host,
		"new_port": candidate.Port,
	}).Info("source repointed at promoted master after failover")
	return newDB, candidate, nil
}

// newSourceReconnector builds the reconnector wired to this Ferry, or nil when
// failover recovery is not configured.
func (f *Ferry) newSourceReconnector() SourceReconnector {
	if f.Config.MasterFailoverRecovery == nil || f.Config.MasterFailoverRecovery.Resolver == nil {
		return nil
	}
	return &ferrySourceReconnector{
		ferry:    f,
		resolver: f.Config.MasterFailoverRecovery.Resolver,
	}
}
