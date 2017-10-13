package test

import (
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
)

type UtilsTestSuite struct {
	suite.Suite

	logger *logrus.Entry
}

func (this *UtilsTestSuite) SetupTest() {
	this.logger = logrus.WithField("tag", "utils_test")
}

func (this *UtilsTestSuite) TestReturnsErrAsIs() {
	called := false
	expected := fmt.Errorf("test error")

	actual := ghostferry.WithRetries(5, 0, this.logger, "test", func() error {
		called = true
		return expected
	})

	this.Require().True(called)
	this.Require().Equal(expected, actual)
}

func (this *UtilsTestSuite) TestRespectsMaxRetries() {
	called := 0

	err := ghostferry.WithRetries(5, 0, this.logger, "test", func() error {
		called++
		if called >= 10 {
			return nil
		}
		return fmt.Errorf("test error")
	})

	this.Require().NotNil(err)
	this.Require().Equal("test error", err.Error())
	this.Require().Equal(5, called)
}

func (this *UtilsTestSuite) Test0UnlimitedRetries() {
	called := 0

	err := ghostferry.WithRetries(0, 0, this.logger, "test", func() error {
		called++
		if called >= 10 {
			return nil
		}
		return fmt.Errorf("test error")
	})

	this.Require().Nil(err)
	this.Require().Equal(10, called)
}

func TestUtils(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, new(UtilsTestSuite))
}
