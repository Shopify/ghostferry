package test

import (
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"strings"
	"sync"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/siddontang/go-mysql/mysql"

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
	this.sourceDb, err = sql.Open("mysql", sourceDSN, testFerry.Source.Marginalia)
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
		nil,
		nil,
		nil,
	)
	this.Require().Nil(err)

	this.binlogStreamer = &ghostferry.BinlogStreamer{
		DB:           this.sourceDb,
		DBConfig:     testFerry.Config.Source,
		MyServerId:   testFerry.Config.MyServerId,
		ErrorHandler: testFerry.ErrorHandler,
		Filter:       testFerry.CopyFilter,
		TableSchema:  tableSchemaCache,
	}
}

func (this *BinlogStreamerTestSuite) TestConnectWithIdKeepsId() {
	this.binlogStreamer.MyServerId = 1421

	_, err := this.binlogStreamer.ConnectBinlogStreamerToMysql()

	this.Require().Nil(err)
	this.Require().Equal(uint32(1421), this.binlogStreamer.MyServerId)
}

func (this *BinlogStreamerTestSuite) TestConnectWithZeroIdGetsRandomServerId() {
	this.binlogStreamer.MyServerId = 0

	_, err := this.binlogStreamer.ConnectBinlogStreamerToMysql()

	this.Require().Nil(err)
	this.Require().NotZero(this.binlogStreamer.MyServerId)
}

func (this *BinlogStreamerTestSuite) TestConnectErrorsOutIfErrorInServerIdGeneration() {
	this.binlogStreamer.MyServerId = 0

	this.binlogStreamer.DB.Close()

	_, err := this.binlogStreamer.ConnectBinlogStreamerToMysql()

	this.Require().NotNil(err)
	this.Require().Zero(this.binlogStreamer.MyServerId)
}

func (this *BinlogStreamerTestSuite) TestBinlogStreamerSetsBinlogPositionOnDMLEvent() {
	_, err := this.binlogStreamer.ConnectBinlogStreamerToMysql()
	this.Require().Nil(err)

	eventAsserted := false

	this.binlogStreamer.AddEventListener(func(evs []ghostferry.DMLEvent) error {
		eventAsserted = true
		this.Require().Equal(1, len(evs))
		this.Require().True(strings.HasPrefix(evs[0].BinlogPosition().EventPosition.Name, "mysql-bin."))
		this.Require().True(evs[0].BinlogPosition().EventPosition.Pos > 0)
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

func (this *BinlogStreamerTestSuite) TestResumingFromInvalidResumePositionAfterEventPosition() {
	pos := ghostferry.BinlogPosition{
		EventPosition: mysql.Position{"mysql-bin.00002", 10},
		ResumePosition: mysql.Position{"mysql-bin.00002", 11},
	}
	_, err := this.binlogStreamer.ConnectBinlogStreamerToMysqlFrom(pos)
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "last event must not be before resume position")
}

func TestBinlogStreamerTestSuite(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &BinlogStreamerTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
