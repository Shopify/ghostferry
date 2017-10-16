package test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
)

type ConfigTestSuite struct {
	suite.Suite

	config ghostferry.Config
	tls    ghostferry.TLSConfig
}

func (this *ConfigTestSuite) SetupTest() {
	this.config = ghostferry.Config{
		SourceHost: "example.com/host",
		SourcePort: 3306,
		SourceUser: "ghostferry",

		TargetHost: "example.com/target",
		TargetPort: 3306,
		TargetUser: "ghostferry",

		MyServerId: 99399,

		Applicability: &testhelpers.TestApplicability{nil, nil},
	}

	this.tls = ghostferry.TLSConfig{
		CertPath:   testhelpers.FixturePath("dummy-cert.pem"),
		ServerName: "dummy-server",
	}
}

func (this *ConfigTestSuite) TestRequireApplicability() {
	this.config.Applicability = nil
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "Applicability filter function must be provided")
}

func (this *ConfigTestSuite) TestRequireSourceHost() {
	this.config.SourceHost = ""
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "source host is empty")
}

func (this *ConfigTestSuite) TestRequireSourcePort() {
	this.config.SourcePort = 0
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "source port is not specified")
}

func (this *ConfigTestSuite) TestRequireSourceUser() {
	this.config.SourceUser = ""
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "source user is empty")
}

func (this *ConfigTestSuite) TestRequireTargetHost() {
	this.config.TargetHost = ""
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "target host is empty")
}

func (this *ConfigTestSuite) TestRequireTargetPort() {
	this.config.TargetPort = 0
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "target port is not specified")
}

func (this *ConfigTestSuite) TestRequireTargetUser() {
	this.config.TargetUser = ""
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "target user is empty")
}

func (this *ConfigTestSuite) TestRequireMyServerId() {
	this.config.MyServerId = 0
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "MyServerId must be non 0")
}

func (this *ConfigTestSuite) TestDefaultValues() {
	err := this.config.ValidateConfig()
	this.Require().Nil(err)

	this.Require().Equal(5, this.config.MaxWriteRetriesOnTargetDBError)
	this.Require().Equal(uint64(200), this.config.IterateChunksize)
	this.Require().Equal(4, this.config.NumberOfTableIterators)
	this.Require().Equal(5, this.config.MaxIterationReadRetries)
	this.Require().Equal("0.0.0.0:8000", this.config.ServerBindAddr)
	this.Require().Equal(".", this.config.WebBasedir)
	this.Require().NotNil(this.config.MaxVariableLoad)
}

func (this *ConfigTestSuite) TestCorruptCert() {
	this.tls.CertPath = testhelpers.FixturePath("dummy-corrupt-cert.pem")
	_, err := this.tls.BuildConfig()
	this.Require().EqualError(err, "unable to append pem")
}

func (this *ConfigTestSuite) TestNonExistentFileErr() {
	this.tls.CertPath = "/doesnotexists"
	_, err := this.tls.BuildConfig()
	this.Require().EqualError(err, "open /doesnotexists: no such file or directory")
}

func (this *ConfigTestSuite) TestBuildTLSConfiguredAlready() {
	expectedConfig, err := this.tls.BuildConfig()
	this.Require().Nil(err)

	actualConfig, err := this.tls.BuildConfig()
	this.Require().Nil(err)

	this.Require().Equal(expectedConfig, actualConfig)
}

func TestConfig(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, new(ConfigTestSuite))
}
