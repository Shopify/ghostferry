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
		Source: ghostferry.DatabaseConfig{
			Host: "example.com/host",
			Port: 3306,
			User: "ghostferry",
		},

		Target: ghostferry.DatabaseConfig{
			Host: "example.com/target",
			Port: 3306,
			User: "ghostferry",
		},

		MyServerId: 99399,

		TableFilter: &testhelpers.TestTableFilter{nil, nil},
	}

	this.tls = ghostferry.TLSConfig{
		CertPath:   testhelpers.FixturePath("dummy-cert.pem"),
		ServerName: "dummy-server",
	}
}

func (this *ConfigTestSuite) TestRequireTableFilter() {
	this.config.TableFilter = nil
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "Table filter function must be provided")
}

func (this *ConfigTestSuite) TestRequireSourceHost() {
	this.config.Source.Host = ""
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "source: host is empty")
}

func (this *ConfigTestSuite) TestRequireSourcePort() {
	this.config.Source.Port = 0
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "source: port is not specified")
}

func (this *ConfigTestSuite) TestRequireSourceUser() {
	this.config.Source.User = ""
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "source: user is empty")
}

func (this *ConfigTestSuite) TestRequireTargetHost() {
	this.config.Target.Host = ""
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "target: host is empty")
}

func (this *ConfigTestSuite) TestRequireTargetPort() {
	this.config.Target.Port = 0
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "target: port is not specified")
}

func (this *ConfigTestSuite) TestRequireTimezoneUTC() {
	this.config.Target.Params = map[string]string{
		"time_zone": "'+08:00'",
	}
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "target: time_zone must be set to '+00:00'")
}

func (this *ConfigTestSuite) TestRequireSQLmode() {
	this.config.Target.Params = map[string]string{
		"sql_mode": "'NO_BACKSLASH_ESCAPES'",
	}
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "target: sql_mode must be set to 'STRICT_ALL_TABLES,NO_BACKSLASH_ESCAPES'")
}

func (this *ConfigTestSuite) TestRequireTargetUser() {
	this.config.Target.User = ""
	err := this.config.ValidateConfig()
	this.Require().EqualError(err, "target: user is empty")
}

func (this *ConfigTestSuite) TestDefaultValues() {
	err := this.config.ValidateConfig()
	this.Require().Nil(err)

	this.Require().Equal(5, this.config.DBWriteRetries)
	this.Require().Equal(uint64(200), this.config.DataIterationBatchSize)
	this.Require().Equal(4, this.config.DataIterationConcurrency)
	this.Require().Equal(5, this.config.DBReadRetries)
	this.Require().Equal("0.0.0.0:8000", this.config.ServerBindAddr)
	this.Require().Equal(".", this.config.WebBasedir)
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

func (this *ConfigTestSuite) TestParamsAndCollationGetsPassedToMysqlConfig() {
	this.config.Source.Collation = "utf8mb4_general_ci"
	this.config.Source.Params = map[string]string{
		"charset": "utf8mb4",
	}
	err := this.config.ValidateConfig()
	this.Require().Nil(err)

	mysqlConfig, err := this.config.Source.MySQLConfig()
	this.Require().Nil(err)

	this.Require().Equal("utf8mb4", mysqlConfig.Params["charset"])
	this.Require().Equal("utf8mb4_general_ci", mysqlConfig.Collation)
	this.Require().Equal("'+00:00'", mysqlConfig.Params["time_zone"])
	this.Require().Equal("'STRICT_ALL_TABLES,NO_BACKSLASH_ESCAPES'", mysqlConfig.Params["sql_mode"])
}

func TestConfig(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, new(ConfigTestSuite))
}
