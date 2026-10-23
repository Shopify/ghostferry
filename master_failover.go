package ghostferry

import (
	"context"
	"errors"
	"fmt"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-mysql-org/go-mysql/mysql"
)

var errNilCandidate = errors.New("failover: resolver returned a nil candidate master")

// MasterWriterResolver finds the host that is now the writable primary after a
// failover, typically via a topology service. previous is the config last
// connected to (e.g. to exclude the dead host); an error retries after backoff.
type MasterWriterResolver interface {
	ResolveCurrentMaster(previous *DatabaseConfig) (*DatabaseConfig, error)
}

// MasterWriterResolverFunc adapts a plain function to MasterWriterResolver.
type MasterWriterResolverFunc func(previous *DatabaseConfig) (*DatabaseConfig, error)

func (f MasterWriterResolverFunc) ResolveCurrentMaster(previous *DatabaseConfig) (*DatabaseConfig, error) {
	return f(previous)
}

// MasterFailoverRecoveryConfig configures automatic reconnection to a new
// source master when the source connection is lost. GTID mode only.
type MasterFailoverRecoveryConfig struct {
	// Discovers the new master writer. Required.
	Resolver MasterWriterResolver

	// Recovery attempts per disconnect before a fatal error; 0 retries indefinitely.
	MaxAttempts int

	// Wait between attempts; defaults to DefaultFailoverRetryWait when zero.
	RetryWait time.Duration

	// Bounds go-mysql's retries against the SAME (now-dead) host so the error
	// surfaces and triggers failover. Defaults to DefaultSyncerMaxReconnectAttempts.
	SyncerMaxReconnectAttempts int
}

const DefaultSyncerMaxReconnectAttempts = 3

func (c *MasterFailoverRecoveryConfig) syncerMaxReconnectAttempts() int {
	if c.SyncerMaxReconnectAttempts <= 0 {
		return DefaultSyncerMaxReconnectAttempts
	}
	return c.SyncerMaxReconnectAttempts
}

const DefaultFailoverRetryWait = 500 * time.Millisecond

func (c *MasterFailoverRecoveryConfig) retryWait() time.Duration {
	if c.RetryWait <= 0 {
		return DefaultFailoverRetryWait
	}
	return c.RetryWait
}

var errNilStreamVerifier = errors.New("failover: missing stream validation callback")

// SourceReconnector connects the streamer (which knows the required GTID set)
// with the Ferry (which owns the SourceRuntime); implementations must fail
// closed. verifyStream is mandatory because StartSyncGTID returns before the
// server can refuse: only a successful callback may publish, and a requirement
// it raised must be revalidated first. On failure returns the attempted
// candidate config, or nil if resolution itself failed.
type SourceReconnector interface {
	Reconnect(previous *DatabaseConfig, requiredGTIDSet mysql.GTIDSet,
		verifyStream func(*sql.DB, *DatabaseConfig) (mysql.GTIDSet, error),
	) (*sql.DB, *DatabaseConfig, error)
}

// A parsed empty requiredSet is valid (fresh source); nil is not (cannot be
// told apart from an uninitialised caller). ctx bounds every query; not closed.
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
