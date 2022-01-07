package test

import (
	"context"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/assert"
)

func newThrottlerWithQuery(query string) *ghostferry.LagThrottler {
	testConfig := testhelpers.NewTestConfig()

	config := &ghostferry.LagThrottlerConfig{
		Connection:     testConfig.Target,
		MaxLag:         6,
		Query:          query,
		UpdateInterval: "5ms",
	}

	throttler, err := ghostferry.NewLagThrottler(config)
	testhelpers.PanicIfError(err)
	return throttler
}

func newThrottler() *ghostferry.LagThrottler {
	return newThrottlerWithQuery("SELECT MAX(lag) FROM meta.lag_table")
}

func setupLagTable(db *sql.DB, ctx context.Context) {
	_, err := db.Exec("DROP DATABASE IF EXISTS meta")
	testhelpers.PanicIfError(err)

	_, err = db.Exec("CREATE DATABASE meta")
	testhelpers.PanicIfError(err)

	_, err = db.Exec("CREATE TABLE meta.lag_table (lag_value FLOAT NOT NULL, server_id int unsigned NOT NULL PRIMARY KEY)")
	testhelpers.PanicIfError(err)
}

func setLag(throttler *ghostferry.LagThrottler, serverId int, lag float32) {
	_, err := throttler.DB.Exec("INSERT INTO meta.lag_table (lag_value, server_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE lag_value = ?", lag, serverId, lag)
	testhelpers.PanicIfError(err)
	time.Sleep(10 * time.Millisecond)
}

func TestThrottlerThrottlesAndUnthrottles(t *testing.T) {
	ctx, done := context.WithCancel(context.Background())
	defer done()

	throttler := newThrottler()
	setupLagTable(throttler.DB, ctx)

	go func() {
		assert.Equal(t, context.Canceled, throttler.Run(ctx))
	}()

	assert.False(t, throttler.Throttled())

	setLag(throttler, 1, 0)
	assert.False(t, throttler.Throttled())

	setLag(throttler, 1, 5)
	assert.False(t, throttler.Throttled())

	setLag(throttler, 1, 9)
	assert.True(t, throttler.Throttled())

	setLag(throttler, 1, 5)
	assert.False(t, throttler.Throttled())
}

func TestThrottlerMultipleServerIDs(t *testing.T) {
	ctx, done := context.WithCancel(context.Background())
	defer done()

	throttler := newThrottler()
	setupLagTable(throttler.DB, ctx)

	go func() {
		assert.Equal(t, context.Canceled, throttler.Run(ctx))
	}()

	setLag(throttler, 1, 0)
	assert.False(t, throttler.Throttled())

	setLag(throttler, 2, 10)
	assert.True(t, throttler.Throttled())

	setLag(throttler, 1, 10)
	setLag(throttler, 2, 0)
	assert.True(t, throttler.Throttled())

	setLag(throttler, 1, 0)
	assert.False(t, throttler.Throttled())
}

func TestNewThrottlerConfigErrors(t *testing.T) {
	connConfig := &ghostferry.DatabaseConfig{
		Host: "foo",
		Port: 42,
		User: "hunter2",
	}

	okConfig := ghostferry.LagThrottlerConfig{
		Connection: connConfig,
		Query:      "SELECT MAX(lag) FROM meta.lag_table",
	}

	config := okConfig
	_, err := ghostferry.NewLagThrottler(&config)
	assert.Nil(t, err)

	config = okConfig
	config.Query = ""
	_, err = ghostferry.NewLagThrottler(&config)
	assert.NotNil(t, err)

	config = okConfig
	config.Connection.Host = ""
	_, err = ghostferry.NewLagThrottler(&config)
	assert.NotNil(t, err)

	config = okConfig
	config.UpdateInterval = "hunter2"
	_, err = ghostferry.NewLagThrottler(&config)
	assert.NotNil(t, err)
}

func TestThrottlerRunErrors(t *testing.T) {
	ctx, done := context.WithCancel(context.Background())

	throttler := newThrottler()

	_, err := throttler.DB.Exec("DROP DATABASE IF EXISTS meta")
	testhelpers.PanicIfError(err)

	err = throttler.Run(ctx)
	assert.NotNil(t, err)
	assert.Equal(t, "Error 1146: Table 'meta.lag_table' doesn't exist", err.Error())

	done()
	err = throttler.Run(ctx)
	assert.Equal(t, context.Canceled, err)
}

func TestThrottlerWithNullReturned(t *testing.T) {
	ctx, done := context.WithCancel(context.Background())

	throttler := newThrottlerWithQuery("SELECT MAX(lag) FROM meta.lag_table")
	setupLagTable(throttler.DB, ctx)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := throttler.Run(ctx)
		assert.Equal(t, context.Canceled, err)
	}()

	time.Sleep(50 * time.Millisecond)
	done()
	wg.Wait()
}

func TestThrottlerWithNoRowsReturned(t *testing.T) {
	ctx, done := context.WithCancel(context.Background())

	throttler := newThrottlerWithQuery("SELECT * FROM meta.lag_table")
	setupLagTable(throttler.DB, ctx)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := throttler.Run(ctx)
		assert.Equal(t, context.Canceled, err)
	}()

	time.Sleep(50 * time.Millisecond)
	done()
	wg.Wait()
}
