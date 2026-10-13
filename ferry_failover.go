package ghostferry

import (
	"context"
	"fmt"
	"maps"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// Bounds the candidate's SQL validation including its first lazy dial; the
// resolver owns its own deadline.
const failoverValidationTimeout = 10 * time.Second

// Publishing through SourceRuntime is the swap; prior handles retire via CloseRetired.
type ferrySourceReconnector struct {
	ferry    *Ferry
	resolver MasterWriterResolver
}

// Validate mutates its receiver and Params, so run it against a copy.
func normalizeFailoverCandidate(candidate *DatabaseConfig) (*DatabaseConfig, error) {
	if candidate == nil {
		return nil, errNilCandidate
	}

	normalized := *candidate
	normalized.Params = maps.Clone(candidate.Params)
	if candidate.TLS != nil {
		tlsCopy := *candidate.TLS
		normalized.TLS = &tlsCopy
	}

	if err := normalized.Validate(); err != nil {
		return nil, fmt.Errorf("failover: invalid candidate master config: %w", err)
	}
	return &normalized, nil
}

func (r *ferrySourceReconnector) Reconnect(
	previous *DatabaseConfig,
	requiredGTIDSet mysql.GTIDSet,
	verifyStream func(*sql.DB, *DatabaseConfig) (mysql.GTIDSet, error),
) (*sql.DB, *DatabaseConfig, error) {
	if verifyStream == nil {
		return nil, nil, errNilStreamVerifier
	}

	candidate, err := r.resolver.ResolveCurrentMaster(previous)
	if err != nil {
		return nil, nil, err
	}

	normalized, err := normalizeFailoverCandidate(candidate)
	if err != nil {
		return nil, nil, err
	}

	// Fail closed: a rejected candidate is never published.
	newDB, err := r.ferry.sourceRuntime.ReplaceValidated(
		normalized,
		r.ferry.logger.WithField("dbname", "source_failover"),
		func(db *sql.DB) error {
			if err := r.validateCandidateServer(db, normalized, requiredGTIDSet); err != nil {
				return err
			}

			finalRequired, err := verifyStream(db, normalized)
			if err != nil {
				return err
			}
			if finalRequired == nil {
				return fmt.Errorf("failover: stream validation returned a nil required GTID set")
			}

			// The trial can raise the requirement; the server was not checked for that.
			if requiredGTIDSet != nil && requiredGTIDSet.Contain(finalRequired) {
				return nil
			}
			return r.validateCandidateServer(db, normalized, finalRequired)
		},
	)
	if err != nil {
		// So the caller can tell the resolver which host it just tried.
		return nil, normalized, err
	}

	r.ferry.logger.WithFields(Fields{
		"new_host": normalized.Host,
		"new_port": normalized.Port,
	}).Info("source repointed at promoted master after failover")
	return newDB, normalized, nil
}

// The context is cancelled before the caller's open-ended stream trial.
func (r *ferrySourceReconnector) validateCandidateServer(db *sql.DB, candidate *DatabaseConfig, required mysql.GTIDSet) error {
	ctx, cancel := context.WithTimeout(context.Background(), failoverValidationTimeout)
	defer cancel()
	return validateFailoverTarget(ctx, db, candidate, required)
}

func (f *Ferry) newSourceReconnector() SourceReconnector {
	if f.Config.MasterFailoverRecovery == nil || f.Config.MasterFailoverRecovery.Resolver == nil {
		return nil
	}
	return &ferrySourceReconnector{
		ferry:    f,
		resolver: f.Config.MasterFailoverRecovery.Resolver,
	}
}
