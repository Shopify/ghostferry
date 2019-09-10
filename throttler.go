package ghostferry

import (
	"context"
	sqlorig "database/sql"
	"fmt"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

type Throttler interface {
	Throttled() bool
	Disabled() bool
	SetDisabled(bool)
	SetPaused(bool)
	Run(context.Context) error
}

func WaitForThrottle(t Throttler) {
	if t.Disabled() || !t.Throttled() {
		return
	}

	metrics.Measure("WaitForThrottle", nil, 1.0, func() {
		for {
			time.Sleep(500 * time.Millisecond)

			if t.Disabled() || !t.Throttled() {
				break
			}
		}
	})
}

type ThrottlerBase struct {
	disabled int32
}

func (t *ThrottlerBase) Disabled() bool {
	return atomic.LoadInt32(&t.disabled) != 0
}

func (t *ThrottlerBase) SetDisabled(disabled bool) {
	var val int32
	if disabled {
		val = 1
	}
	atomic.StoreInt32(&t.disabled, val)
}

type PauserThrottler struct {
	ThrottlerBase
	paused int32
}

func (t *PauserThrottler) Throttled() bool {
	return atomic.LoadInt32(&t.paused) != 0
}

func (t *PauserThrottler) SetPaused(paused bool) {
	var val int32
	if paused {
		val = 1
	}
	atomic.StoreInt32(&t.paused, val)
}

func (t *PauserThrottler) Run(ctx context.Context) error {
	return nil
}

type LagThrottlerConfig struct {
	Connection     *DatabaseConfig
	MaxLag         int
	Query          string
	UpdateInterval string
}

type LagThrottler struct {
	ThrottlerBase
	PauserThrottler
	config *LagThrottlerConfig

	DB       *sql.DB
	lag      int
	logger   *logrus.Entry
	interval time.Duration
}

func NewLagThrottler(config *LagThrottlerConfig) (*LagThrottler, error) {
	if config.MaxLag <= 0 {
		config.MaxLag = 1
	}

	if config.UpdateInterval == "" {
		config.UpdateInterval = "1s"
	}

	if config.Query == "" {
		return nil, fmt.Errorf("lag Query required")
	}

	interval, err := time.ParseDuration(config.UpdateInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid UpdateInterval: %s", err)
	}

	if err := config.Connection.Validate(); err != nil {
		return nil, fmt.Errorf("connection invalid: %s", err)
	}

	logger := logrus.WithField("tag", "throttler")
	db, err := config.Connection.SqlDB(logger)
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

		err := WithRetriesContext(ctx, 5, t.interval, nil, "update lag", func() error {
			return t.updateLag(ctx)
		})

		if err != nil {
			return err
		}
	}
}

func (t *LagThrottler) updateLag(ctx context.Context) error {
	var newLag sqlorig.NullInt64
	err := t.DB.QueryRowContext(ctx, t.config.Query).Scan(&newLag)
	if err == sqlorig.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if !newLag.Valid {
		return nil
	}

	t.lag = int(newLag.Int64)
	return nil
}
