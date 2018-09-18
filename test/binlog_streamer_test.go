package test

import (
	"database/sql"
	"strings"
	"sync"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"

	"github.com/stretchr/testify/suite"
)

type BinlogStreamerTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	config         *ghostferry.Config
	binlogStreamer *ghostferry.BinlogStreamer
	sourceDb       *sql.DB
}

func (this *BinlogStreamerTestSuite) SetupTest() {
	this.GhostferryUnitTestSuite.SetupTest()

	testFerry := testhelpers.NewTestFerry()

	sourceConfig, err := testFerry.Source.MySQLConfig()
	this.Require().Nil(err)

	sourceDSN := sourceConfig.FormatDSN()
	this.sourceDb, err = sql.Open("mysql", sourceDSN)
	if err != nil {
		this.Fail("failed to connect to source database")
	}

	testhelpers.SeedInitialData(this.sourceDb, "gftest", "test_table_1", 0)
	tableSchemaCache, err := ghostferry.LoadTables(
		this.sourceDb,
		&testhelpers.TestTableFilter{
			DbsFunc:    testhelpers.DbApplicabilityFilter([]string{testhelpers.TestSchemaName}),
			TablesFunc: nil,
		},
	)
	this.Require().Nil(err)

	this.binlogStreamer = &ghostferry.BinlogStreamer{
		Db:           this.sourceDb,
		Config:       testFerry.Config,
		ErrorHandler: testFerry.ErrorHandler,
		Filter:       testFerry.CopyFilter,
		TableSchema:  tableSchemaCache,
	}

	this.Require().Nil(this.binlogStreamer.Initialize())
}

func (this *BinlogStreamerTestSuite) TestConnectWithIdKeepsId() {
	this.binlogStreamer.Config.MyServerId = 1421

	err := this.binlogStreamer.ConnectBinlogStreamerToMysql()

	this.Require().Nil(err)
	this.Require().Equal(uint32(1421), this.binlogStreamer.Config.MyServerId)
}

func (this *BinlogStreamerTestSuite) TestConnectWithZeroIdGetsRandomServerId() {
	this.binlogStreamer.Config.MyServerId = 0

	err := this.binlogStreamer.ConnectBinlogStreamerToMysql()

	this.Require().Nil(err)
	this.Require().NotZero(this.binlogStreamer.Config.MyServerId)
}

func (this *BinlogStreamerTestSuite) TestConnectErrorsOutIfErrorInServerIdGeneration() {
	this.binlogStreamer.Config.MyServerId = 0

	this.binlogStreamer.Db.Close()

	err := this.binlogStreamer.ConnectBinlogStreamerToMysql()

	this.Require().NotNil(err)
	this.Require().Zero(this.binlogStreamer.Config.MyServerId)
}

func (this *BinlogStreamerTestSuite) TestBinlogStreamerSetsBinlogPositionOnDMLEvent() {
	err := this.binlogStreamer.ConnectBinlogStreamerToMysql()
	this.Require().Nil(err)

	eventAsserted := false

	this.binlogStreamer.AddEventListener(func(evs []ghostferry.DMLEvent) error {
		eventAsserted = true
		this.Require().Equal(1, len(evs))
		this.Require().True(strings.HasPrefix(evs[0].BinlogPosition().Name, "mysql-bin."))
		this.Require().True(evs[0].BinlogPosition().Pos > 0)
		this.binlogStreamer.FlushAndStop()
		return nil
	})

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		this.binlogStreamer.Run()
	}()

	_, err = this.sourceDb.Exec("INSERT INTO gftest.test_table_1 VALUES (null, 'testdata')")
	this.Require().Nil(err)

	wg.Wait()
	this.Require().True(eventAsserted)
}

func TestBinlogStreamerTestSuite(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &BinlogStreamerTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
