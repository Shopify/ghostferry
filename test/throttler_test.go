package test

import (
	"context"
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type ThrottlerTestSuite struct {
	suite.Suite

	throttler ghostferry.Throttler
}

func (t *ThrottlerTestSuite) SetupTest() {
	t.throttler = &ghostferry.PauserThrottler{}
}

func (t *ThrottlerTestSuite) TestPauseUnpause() {
	t.Require().False(t.throttler.Throttled())

	t.throttler.SetPaused(true)
	t.Require().True(t.throttler.Throttled())

	t.throttler.SetPaused(false)
	t.Require().False(t.throttler.Throttled())
}

func (t *ThrottlerTestSuite) TestEnableDisable() {
	t.throttler.SetPaused(true)
	t.Require().False(t.throttler.Disabled())
	t.Require().True(t.throttler.Throttled())

	t.throttler.SetDisabled(true)
	t.Require().True(t.throttler.Disabled())
	ghostferry.WaitForThrottle(context.Background(), t.throttler)

	t.throttler.SetDisabled(false)
	t.Require().False(t.throttler.Disabled())

	done := make(chan bool)
	resumed := false
	go func() {
		ghostferry.WaitForThrottle(context.Background(), t.throttler)
		resumed = true
		done <- true
	}()

	time.Sleep(200 * time.Millisecond)
	t.Require().False(resumed)

	t.throttler.SetDisabled(true)
	select {
	case <-time.After(5 * time.Second):
		t.Require().Fail("goroutine did not resume in time")
	case <-done:
	}
}

func TestThrottlerTestSuite(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, new(ThrottlerTestSuite))
}
