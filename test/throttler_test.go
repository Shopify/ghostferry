package test

import (
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type ThrottlerTestSuite struct {
	suite.Suite

	throttler *ghostferry.Throttler
}

func (this *ThrottlerTestSuite) SetupTest() {
	// Since the implementation is still a stub implementation, we don't
	// need to pass it any data for now.
	this.throttler = &ghostferry.Throttler{}
	this.Require().Nil(this.throttler.Initialize())
}

func (this *ThrottlerTestSuite) TestAddSetThrottleDuration() {
	this.throttler.AddThrottleDuration(10 * time.Second)
	t1 := this.throttler.ThrottleUntil()

	if !t1.After(time.Now()) {
		this.T().Errorf("throttle until %v is not after now\n", t1)
	}

	this.throttler.AddThrottleDuration(10 * time.Second)
	t2 := this.throttler.ThrottleUntil()

	if !t2.After(t1) {
		this.T().Errorf("throttle until %v is not after %v", t2, t1)
	}

	this.throttler.SetThrottleDuration(40 * time.Second)
	t3 := this.throttler.ThrottleUntil()

	if !t3.After(t2) {
		this.T().Errorf("throttle until %v is not after %v", t3, t2)
	}
}

func (this *ThrottlerTestSuite) TestSetThrottleDurationWillNotOverrideIfThrottleUntilIsAfterTheDurationSet() {
	this.throttler.SetThrottleDuration(10 * time.Second)
	t1 := this.throttler.ThrottleUntil()
	if !t1.After(time.Now()) {
		this.T().Errorf("throttle until %v is not after now\n", t1)
	}

	this.throttler.SetThrottleDuration(5 * time.Second)
	t2 := this.throttler.ThrottleUntil()
	this.Require().Equal(t1, t2)
}

func (this *ThrottlerTestSuite) TestPauseUnpause() {
	this.throttler.Pause()
	t1 := this.throttler.ThrottleUntil()
	if !t1.After(time.Now()) {
		this.T().Errorf("throttle until %v is not after now\n", t1)
	}

	this.throttler.Unpause()
	t2 := this.throttler.ThrottleUntil()
	if !t2.Before(time.Now()) {
		this.T().Errorf("throttle until %v is not before now\n", t2)
	}
}

func TestThrottlerTestSuite(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, new(ThrottlerTestSuite))
}
