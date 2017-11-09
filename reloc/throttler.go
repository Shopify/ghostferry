package reloc

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
)

type ThrottlerConfig struct {
	Connection      ghostferry.DatabaseConfig
	MaxLag          int
	HeartbeatTable  string
	HeartbeatColumn string
	UpdateInterval  string
}

type LagThrottler struct {
	ghostferry.PauserThrottler
	config *ThrottlerConfig

	DB       *sql.DB
	lag      int
	logger   *logrus.Entry
	interval time.Duration
}

func NewLagThrottler(config *ThrottlerConfig) (*LagThrottler, error) {
	if config.MaxLag <= 0 {
		config.MaxLag = 1
	}

	if config.UpdateInterval == "" {
		config.UpdateInterval = "1s"
	}

	if config.HeartbeatTable == "" {
		return nil, fmt.Errorf("HeartbeatTable required")
	}

	if config.HeartbeatColumn == "" {
		return nil, fmt.Errorf("HeartbeatColumn required")
	}

	interval, err := time.ParseDuration(config.UpdateInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid UpdateInterval: %s", err)
	}

	if err := config.Connection.Validate(); err != nil {
		return nil, fmt.Errorf("connection invalid: %s", err)
	}

	dbCfg, err := config.Connection.MySQLConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build database config: %s", err)
	}

	logger := logrus.WithField("tag", "throttler")
	logger.WithField("dsn", ghostferry.MaskedDSN(dbCfg)).Info("connecting to throttling database")

	db, err := sql.Open("mysql", dbCfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %s", err)
	}

	return &LagThrottler{
		config:   config,
		DB:       db,
		logger:   logger,
		interval: interval,
	}, nil
}

func (t *LagThrottler) Throttled() bool {
	return t.PauserThrottler.Throttled() || t.lag > t.config.MaxLag
}

func (t *LagThrottler) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(t.interval):
		}

		err := ghostferry.WithRetriesContext(ctx, 5, t.interval, nil, "update lag", func() error {
			return t.updateLag(ctx)
		})

		if err != nil {
			return err
		}
	}
}

func (t *LagThrottler) updateLag(ctx context.Context) error {
	query := "SELECT MAX(TIMESTAMPDIFF(SECOND, %s, NOW())) FROM %s"
	query = fmt.Sprintf(query, t.config.HeartbeatColumn, t.config.HeartbeatTable)

	var newLag int
	err := t.DB.QueryRowContext(ctx, query).Scan(&newLag)
	if err != nil {
		return err
	}

	t.lag = newLag
	return nil
}
