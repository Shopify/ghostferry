package ghostferry

import (
	"context"
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

var errNilStreamVerifier = errors.New("failover: missing stream validation callback")

// SourceReconnector is the seam between the streamer (which knows the GTID
// set it must not fall behind) and the Ferry (which owns the SourceRuntime).
// Implementations must fail closed: a source not containing requiredGTIDSet
// would silently diverge the target or wait forever for a stop target.
//
// verifyStream is mandatory because StartSyncGTID returns as soon as
// COM_BINLOG_DUMP_GTID is written — a refusal surfaces only through GetEvent.
// Only a successful callback may publish the connection, and a requirement it
// added must be revalidated against the server first.
//
// On failure it returns the config of the candidate it attempted, or nil when
// resolution itself failed.
type SourceReconnector interface {
	Reconnect(previous *DatabaseConfig, requiredGTIDSet mysql.GTIDSet,
		verifyStream func(*sql.DB, *DatabaseConfig) (mysql.GTIDSet, error),
	) (*sql.DB, *DatabaseConfig, error)
}

// A parsed but empty requiredSet is valid (a fresh source has executed
// nothing); a nil set is not, because it cannot be told apart from an
// uninitialised caller. ctx bounds every query, including the first lazy
// dial. It does not close db.
func validateFailoverTarget(ctx context.Context, db *sql.DB, candidate *DatabaseConfig, requiredSet mysql.GTIDSet) error {
	if candidate == nil {
		return fmt.Errorf("failover: nil candidate master config")
	}
	if db == nil {
		return fmt.Errorf("failover: nil candidate master connection")
	}
	if requiredSet == nil {
		return fmt.Errorf("failover: nil required GTID set; refusing to validate candidate master %s:%d", candidate.Host, candidate.Port)
	}

	isReadOnly, err := checkDbIsAReplicaContext(ctx, db)
	if err != nil {
		return fmt.Errorf("failover: checking candidate master %s:%d read_only: %w", candidate.Host, candidate.Port, err)
	}
	if isReadOnly {
		return fmt.Errorf("failover: candidate master %s:%d is read_only (not a writer); rejecting", candidate.Host, candidate.Port)
	}

	if err := checkServerGTIDModeEnabledContext(ctx, db); err != nil {
		return fmt.Errorf("failover: candidate master %s:%d rejected: %w", candidate.Host, candidate.Port, err)
	}

	candidateSetStr, err := readExecutedGTIDSetContext(ctx, db)
	if err != nil {
		return fmt.Errorf("failover: reading candidate master executed GTID set: %w", err)
	}
	candidateSet, err := mysql.ParseMysqlGTIDSet(candidateSetStr)
	if err != nil {
		return fmt.Errorf("failover: parsing candidate master executed GTID set %q: %w", candidateSetStr, err)
	}

	if !candidateSet.Contain(requiredSet) {
		return fmt.Errorf(
			"failover: candidate master %s:%d executed set %q does not contain required GTID set %q; refusing to resume",
			candidate.Host, candidate.Port, candidateSet.String(), requiredSet.String(),
		)
	}

	return nil
}
