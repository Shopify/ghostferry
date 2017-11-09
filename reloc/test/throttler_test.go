package test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/reloc"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/assert"
)

func newThrottler() *reloc.LagThrottler {
	testConfig := testhelpers.NewTestConfig()

	config := &reloc.ThrottlerConfig{
		Connection:      testConfig.Target,
		MaxLag:          6,
		HeartbeatTable:  "meta.heartbeat",
		HeartbeatColumn: "ts",
		UpdateInterval:  "5ms",
	}

	throttler, err := reloc.NewLagThrottler(config)
	testhelpers.PanicIfError(err)
	return throttler
}

func setupHeartbeatTable(db *sql.DB, ctx context.Context) {
	_, err := db.Exec("DROP DATABASE IF EXISTS meta")
	testhelpers.PanicIfError(err)

	_, err = db.Exec("CREATE DATABASE meta")
	testhelpers.PanicIfError(err)

	_, err = db.Exec("CREATE TABLE meta.heartbeat (ts varchar(26) NOT NULL, server_id int unsigned NOT NULL PRIMARY KEY)")
	testhelpers.PanicIfError(err)
}

func heartbeat(throttler *reloc.LagThrottler, serverId int, beat time.Time) {
	_, err := throttler.DB.Exec("INSERT INTO meta.heartbeat (ts, server_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE ts = ?", beat, serverId, beat)
	testhelpers.PanicIfError(err)
	time.Sleep(10 * time.Millisecond)
}

func TestThrottlerThrottlesAndUnthrottles(t *testing.T) {
	ctx, done := context.WithCancel(context.Background())
	defer done()

	throttler := newThrottler()
	setupHeartbeatTable(throttler.DB, ctx)

	go func() {
		assert.Equal(t, context.Canceled, throttler.Run(ctx))
	}()

	assert.False(t, throttler.Throttled())

	heartbeat(throttler, 1, time.Now())
	assert.False(t, throttler.Throttled())

	heartbeat(throttler, 1, time.Now().Add(-5*time.Second))
	assert.False(t, throttler.Throttled())

	heartbeat(throttler, 1, time.Now().Add(-8*time.Second))
	assert.True(t, throttler.Throttled())

	heartbeat(throttler, 1, time.Now().Add(-5*time.Second))
	assert.False(t, throttler.Throttled())
}

func TestThrottlerMultipleServerIDs(t *testing.T) {
	ctx, done := context.WithCancel(context.Background())
	defer done()

	throttler := newThrottler()
	setupHeartbeatTable(throttler.DB, ctx)

	go func() {
		assert.Equal(t, context.Canceled, throttler.Run(ctx))
	}()

	heartbeat(throttler, 1, time.Now())
	assert.False(t, throttler.Throttled())

	heartbeat(throttler, 2, time.Now().Add(-10*time.Second))
	assert.True(t, throttler.Throttled())

	heartbeat(throttler, 1, time.Now().Add(-10*time.Second))
	heartbeat(throttler, 2, time.Now())
	assert.True(t, throttler.Throttled())

	heartbeat(throttler, 1, time.Now())
	assert.False(t, throttler.Throttled())
}

func TestNewThrottlerConfigErrors(t *testing.T) {
	connConfig := ghostferry.DatabaseConfig{
		Host: "foo",
		Port: 42,
		User: "hunter2",
	}

	okConfig := reloc.ThrottlerConfig{
		Connection:      connConfig,
		HeartbeatTable:  "meta.heartbeat",
		HeartbeatColumn: "ts",
	}

	config := okConfig
	_, err := reloc.NewLagThrottler(&config)
	assert.Nil(t, err)

	config = okConfig
	config.HeartbeatTable = ""
	_, err = reloc.NewLagThrottler(&config)
	assert.NotNil(t, err)

	config = okConfig
	config.HeartbeatColumn = ""
	_, err = reloc.NewLagThrottler(&config)
	assert.NotNil(t, err)

	config = okConfig
	config.Connection.Host = ""
	_, err = reloc.NewLagThrottler(&config)
	assert.NotNil(t, err)

	config = okConfig
	config.UpdateInterval = "hunter2"
	_, err = reloc.NewLagThrottler(&config)
	assert.NotNil(t, err)
}

func TestThrottlerRunErrors(t *testing.T) {
	ctx, done := context.WithCancel(context.Background())

	throttler := newThrottler()

	_, err := throttler.DB.Exec("DROP DATABASE IF EXISTS meta")
	testhelpers.PanicIfError(err)

	err = throttler.Run(ctx)
	assert.NotNil(t, err)
	assert.Equal(t, "Error 1146: Table 'meta.heartbeat' doesn't exist", err.Error())

	done()
	err = throttler.Run(ctx)
	assert.Equal(t, context.Canceled, err)
}
