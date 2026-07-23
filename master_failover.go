package ghostferry

import (
	"fmt"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// MasterWriterResolver resolves the current source master writer after a
// suspected failover. Implementations typically consult an external topology
// service (orchestrator, a service-discovery endpoint, etc.) to find the host
// that is now the writable primary.
//
// ResolveCurrentMaster is called by the binlog streamer when it loses its
// connection to the source and master-failover recovery is enabled. It receives
// the DatabaseConfig the streamer was last connected to so the implementation
// can, for example, exclude the now-dead host. It must return the connection
// details of the host that should now be streamed from. Returning an error (or
// the same dead host) causes recovery to be retried after a backoff.
//
// The returned DatabaseConfig is used both to open a validation DB connection
// and to reconfigure the binlog syncer, so credentials/TLS must be complete.
type MasterWriterResolver interface {
	ResolveCurrentMaster(previous *DatabaseConfig) (*DatabaseConfig, error)
}

// MasterWriterResolverFunc adapts a plain function to MasterWriterResolver.
type MasterWriterResolverFunc func(previous *DatabaseConfig) (*DatabaseConfig, error)

func (f MasterWriterResolverFunc) ResolveCurrentMaster(previous *DatabaseConfig) (*DatabaseConfig, error) {
	return f(previous)
}

// MasterFailoverRecoveryConfig configures automatic reconnection to a new
// source master when the current source connection is lost.
//
// Failover recovery is only supported in GTID binlog coordinate mode: GTID sets
// are server-independent, so a resume set that is valid on the old master is
// meaningful on the new one. File/position coordinates are per-host and cannot
// be safely carried across a failover, so recovery is refused in that mode.
type MasterFailoverRecoveryConfig struct {
	// Resolver discovers the new master writer. It is required; recovery is
	// disabled when nil.
	Resolver MasterWriterResolver

	// MaxAttempts bounds how many times recovery is attempted for a single
	// disconnect before giving up and surfacing a fatal error. Zero means
	// retry indefinitely.
	MaxAttempts int

	// RetryWait is how long to wait between recovery attempts. Defaults to
	// DefaultFailoverRetryWait when zero.
	RetryWait time.Duration

	// OnFailover, when set, is invoked by the binlog streamer after it has
	// successfully reconnected to a new master and swapped its own DB/DBConfig.
	// It lets the embedding application (e.g. Ferry) repoint every other source
	// consumer — the data iterator, verifiers, SHOW CREATE queries, etc. — at
	// the promoted writer so the whole run follows the failover, not just the
	// binlog stream.
	//
	// It runs on the streamer's Run goroutine; it must be quick and must not
	// call back into the streamer. The provided DB handle is owned by the
	// streamer and must NOT be closed by the callback. Returning an error is
	// logged but does not abort streaming (the stream has already recovered).
	OnFailover func(MasterFailoverEvent) error
}

// MasterFailoverEvent describes a completed source master failover. It is
// passed to MasterFailoverRecoveryConfig.OnFailover so consumers can repoint at
// the new writer.
type MasterFailoverEvent struct {
	// NewMasterConfig is the DatabaseConfig of the promoted writer the streamer
	// is now connected to.
	NewMasterConfig *DatabaseConfig

	// NewMasterDB is the live connection the streamer opened to the new writer.
	// It is owned by the streamer; callers may use it but must not close it.
	NewMasterDB *sql.DB

	// PreviousMasterConfig is the DatabaseConfig of the master that was lost. It
	// may be nil if the streamer had no prior config.
	PreviousMasterConfig *DatabaseConfig
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

// validateFailoverTarget opens a short-lived connection to the candidate new
// master and verifies it is a safe target to resume streaming from:
//
//   - the candidate is not read-only, i.e. it really is the writer (a demoted
//     old master or a replica must not be accepted); and
//   - GTID mode is enabled (@@GLOBAL.gtid_mode = ON), otherwise GTID streaming
//     cannot start there; and
//   - the candidate's executed GTID set is a superset of appliedSet, the set of
//     everything Ghostferry has already emitted downstream. If the candidate is
//     MISSING any already-applied transaction (e.g. it was promoted having lost
//     the old master's tail), resuming there would leave the target holding data
//     that no longer exists on the source — a silent divergence. Validation
//     therefore fails closed against appliedSet (which is >= the resume floor),
//     and recovery is retried against another candidate.
//
// It returns the opened *sql.DB on success so the caller can reuse it; the
// caller owns closing it. On any validation failure the DB (if opened) is
// closed and an error returned.
func validateFailoverTarget(candidate *DatabaseConfig, appliedSet mysql.GTIDSet, logger Logger) (*sql.DB, error) {
	if candidate == nil {
		return nil, fmt.Errorf("failover: resolver returned a nil candidate master")
	}

	db, err := candidate.SqlDB(logger)
	if err != nil {
		return nil, fmt.Errorf("failover: connecting to candidate master %s:%d: %w", candidate.Host, candidate.Port, err)
	}

	// The candidate must be the writer. CheckDbIsAReplica reads @@read_only; a
	// demoted old master or a replica is read-only and must be rejected so we do
	// not resume against (and later read a stop coordinate from) a non-writer.
	isReadOnly, err := CheckDbIsAReplica(db)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failover: checking candidate master %s:%d read_only: %w", candidate.Host, candidate.Port, err)
	}
	if isReadOnly {
		db.Close()
		return nil, fmt.Errorf("failover: candidate master %s:%d is read_only (not a writer); rejecting", candidate.Host, candidate.Port)
	}

	if err := CheckServerGTIDModeEnabled(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failover: candidate master %s:%d rejected: %w", candidate.Host, candidate.Port, err)
	}

	candidateSetStr, err := ReadExecutedGTIDSet(db)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failover: reading candidate master executed GTID set: %w", err)
	}

	candidateSet, err := mysql.ParseMysqlGTIDSet(candidateSetStr)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failover: parsing candidate master executed GTID set %q: %w", candidateSetStr, err)
	}

	// The candidate must contain everything we have already applied downstream.
	// A nil/empty appliedSet is contained by any set, which is the correct
	// behavior on a fresh run.
	if appliedSet != nil && !candidateSet.Contain(appliedSet) {
		db.Close()
		return nil, fmt.Errorf(
			"failover: candidate master %s:%d executed set %q does not contain already-applied set %q; resuming there would diverge from the source",
			candidate.Host, candidate.Port, candidateSet.String(), appliedSet.String(),
		)
	}

	return db, nil
}
