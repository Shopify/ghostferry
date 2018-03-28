package test

import (
	"database/sql"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"

	"github.com/stretchr/testify/suite"
)

type FerryTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	config         *ghostferry.Config
	binlogStreamer *ghostferry.BinlogStreamer
}

func (this *FerryTestSuite) SetupTest() {
	this.GhostferryUnitTestSuite.SetupTest()

	testFerry := testhelpers.NewTestFerry()

	sourceConfig, err := testFerry.Source.MySQLConfig()
	this.Require().Nil(err)

	sourceDSN := sourceConfig.FormatDSN()
	sourceDb, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		this.Fail("failed to connect to source database")
	}

	this.binlogStreamer = &ghostferry.BinlogStreamer{
		Db:           sourceDb,
		Config:       testFerry.Config,
		ErrorHandler: testFerry.ErrorHandler,
		Filter:       testFerry.CopyFilter,
	}

	this.Require().Nil(this.binlogStreamer.Initialize())
}

func (this *FerryTestSuite) TestConnectWithIdKeepsId() {
	this.binlogStreamer.Config.MyServerId = 1421

	err := this.binlogStreamer.ConnectBinlogStreamerToMysql()

	this.Require().Nil(err)
	this.Require().Equal(uint32(1421), this.binlogStreamer.Config.MyServerId)
}

func (this *FerryTestSuite) TestConnectWithZeroIdGetsRandomServerId() {
	this.binlogStreamer.Config.MyServerId = 0

	err := this.binlogStreamer.ConnectBinlogStreamerToMysql()

	this.Require().Nil(err)
	this.Require().NotZero(this.binlogStreamer.Config.MyServerId)
}

func (this *FerryTestSuite) TestConnectErrorsOutIfErrorInServerIdGeneration() {
	this.binlogStreamer.Config.MyServerId = 0

	this.binlogStreamer.Db.Close()

	err := this.binlogStreamer.ConnectBinlogStreamerToMysql()

	this.Require().NotNil(err)
	this.Require().Zero(this.binlogStreamer.Config.MyServerId)
}

func TestFerryTestSuite(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &FerryTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
