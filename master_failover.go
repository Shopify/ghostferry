package ghostferry

import (
	"errors"
	"fmt"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// errNilCandidate is returned when a resolver yields no candidate master.
var errNilCandidate = errors.New("failover: resolver returned a nil candidate master")

// MasterWriterResolver resolves the current source master writer after a
// suspected failover. Implementations typically consult an external topology
// service (orchestrator, a service-discovery endpoint, etc.) to find the host
// that is now the writable primary.
//
// ResolveCurrentMaster receives the DatabaseConfig the source was last
// connected to (so the implementation can, for example, exclude the now-dead
// host) and returns the connection details of the host that should now be
// streamed from. Returning an error causes recovery to be retried after a
// backoff.
type MasterWriterResolver interface {
	ResolveCurrentMaster(previous *DatabaseConfig) (*DatabaseConfig, error)
}

// MasterWriterResolverFunc adapts a plain function to MasterWriterResolver.
type MasterWriterResolverFunc func(previous *DatabaseConfig) (*DatabaseConfig, error)

func (f MasterWriterResolverFunc) ResolveCurrentMaster(previous *DatabaseConfig) (*DatabaseConfig, error) {
	return f(previous)
}

// MasterFailoverRecoveryConfig configures automatic reconnection to a new
// source master when the source connection is lost.
//
// Failover recovery is only supported in GTID binlog coordinate mode: GTID sets
// are server-independent, so a resume set valid on the old master is meaningful
// on the new one. File/position coordinates are per-host and cannot be carried
// across a failover, so recovery is refused in that mode.
type MasterFailoverRecoveryConfig struct {
	// Resolver discovers the new master writer. Required; recovery is disabled
	// when nil.
	Resolver MasterWriterResolver

	// MaxAttempts bounds how many times recovery is attempted for a single
	// disconnect before giving up and surfacing a fatal error. Zero means retry
	// indefinitely.
	MaxAttempts int

	// RetryWait is how long to wait between recovery attempts. Defaults to
	// DefaultFailoverRetryWait when zero.
	RetryWait time.Duration

	// SyncerMaxReconnectAttempts bounds how many times go-mysql's binlog syncer
	// retries reconnecting to the SAME (now-dead) host before giving up and
	// surfacing the error, which is what triggers our reconnect-to-a-new-host
	// recovery. Without a bound the syncer retries the dead host forever and the
	// failure never surfaces. Defaults to DefaultSyncerMaxReconnectAttempts.
	SyncerMaxReconnectAttempts int
}

// DefaultSyncerMaxReconnectAttempts bounds go-mysql's same-host reconnect
// retries when failover recovery is enabled, so a lost source surfaces as a
// GetEvent error (triggering failover) within a few seconds.
const DefaultSyncerMaxReconnectAttempts = 3

func (c *MasterFailoverRecoveryConfig) syncerMaxReconnectAttempts() int {
	if c.SyncerMaxReconnectAttempts <= 0 {
		return DefaultSyncerMaxReconnectAttempts
	}
	return c.SyncerMaxReconnectAttempts
}

// DefaultFailoverRetryWait is the wait between failover recovery attempts when
// MasterFailoverRecoveryConfig.RetryWait is unset.
const DefaultFailoverRetryWait = 500 * time.Millisecond

func (c *MasterFailoverRecoveryConfig) retryWait() time.Duration {
	if c.RetryWait <= 0 {
		return DefaultFailoverRetryWait
	}
	return c.RetryWait
}

// SourceReconnector reconnects the source after the binlog streamer loses its
// connection. It is the single seam between the streamer (which only knows "I
// lost the source and here is the safe GTID set I must not fall behind") and
// the Ferry (which owns the SourceRuntime and therefore how the whole run
// repoints at a promoted writer).
//
// Reconnect is called on the streamer's Run goroutine when a non-timeout
// GetEvent error occurs. appliedGTIDSet is everything the streamer has already
// emitted downstream (committed set plus any in-flight transaction); the
// implementation must guarantee the returned source contains it, or fail
// closed, so recovery never resumes against a master that lost applied
// transactions (which would silently diverge the target).
//
// On success it returns the new source connection and the config it was opened
// from; the streamer rebuilds its binlog syncer against them. Returning an
// error means this attempt failed and the streamer will retry (subject to
// MaxAttempts).
type SourceReconnector interface {
	Reconnect(previous *DatabaseConfig, appliedGTIDSet mysql.GTIDSet) (*sql.DB, *DatabaseConfig, error)
}

// validateFailoverTarget verifies that db (already opened against candidate) is
// a safe source to resume streaming from:
//
//   - it is a writer (@@read_only = OFF), not a demoted master or replica;
//   - GTID mode is enabled (@@GLOBAL.gtid_mode = ON); and
//   - its executed GTID set contains appliedSet, i.e. everything already
//     emitted downstream. A candidate missing any applied transaction is
//     rejected (fail closed) so recovery cannot silently diverge the target.
//
// It does not close db; the caller owns its lifecycle.
func validateFailoverTarget(db *sql.DB, candidate *DatabaseConfig, appliedSet mysql.GTIDSet) error {
	if candidate == nil {
		return fmt.Errorf("failover: nil candidate master config")
	}

	isReadOnly, err := CheckDbIsAReplica(db)
	if err != nil {
		return fmt.Errorf("failover: checking candidate master %s:%d read_only: %w", candidate.Host, candidate.Port, err)
	}
	if isReadOnly {
		return fmt.Errorf("failover: candidate master %s:%d is read_only (not a writer); rejecting", candidate.Host, candidate.Port)
	}

	if err := CheckServerGTIDModeEnabled(db); err != nil {
		return fmt.Errorf("failover: candidate master %s:%d rejected: %w", candidate.Host, candidate.Port, err)
	}

	candidateSetStr, err := ReadExecutedGTIDSet(db)
	if err != nil {
		return fmt.Errorf("failover: reading candidate master executed GTID set: %w", err)
	}
	candidateSet, err := mysql.ParseMysqlGTIDSet(candidateSetStr)
	if err != nil {
		return fmt.Errorf("failover: parsing candidate master executed GTID set %q: %w", candidateSetStr, err)
	}

	if appliedSet != nil && !candidateSet.Contain(appliedSet) {
		return fmt.Errorf(
			"failover: candidate master %s:%d executed set %q does not contain already-applied set %q; resuming there would diverge from the source",
			candidate.Host, candidate.Port, candidateSet.String(), appliedSet.String(),
		)
	}

	return nil
}
