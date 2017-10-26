package test

import (
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type ThrottlerTestSuite struct {
	suite.Suite

	throttler ghostferry.Throttler
}

func (this *ThrottlerTestSuite) SetupTest() {
	this.throttler = &ghostferry.PauserThrottler{}
}

func (this *ThrottlerTestSuite) TestPauseUnpause() {
	this.Require().False(this.throttler.Throttled())

	this.throttler.SetPaused(true)
	this.Require().True(this.throttler.Throttled())

	this.throttler.SetPaused(false)
	this.Require().False(this.throttler.Throttled())
}

func TestThrottlerTestSuite(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, new(ThrottlerTestSuite))
}
