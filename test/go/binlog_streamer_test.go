package test

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"

	"github.com/stretchr/testify/suite"
)

type BinlogStreamerTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	config         *ghostferry.Config
	binlogStreamer *ghostferry.BinlogStreamer
	sourceDB       *sql.DB
}

func (this *BinlogStreamerTestSuite) SetupTest() {
	this.GhostferryUnitTestSuite.SetupTest()

	testFerry := testhelpers.NewTestFerry()

	sourceConfig, err := testFerry.Source.MySQLConfig()
	this.Require().Nil(err)

	sourceDSN := sourceConfig.FormatDSN()
	this.sourceDB, err = sql.Open("mysql", sourceDSN, testFerry.Source.Marginalia)
	if err != nil {
		this.Fail("failed to connect to source database")
	}

	testhelpers.SeedInitialData(this.sourceDB, "gftest", "test_table_1", 0)
	tableSchemaCache, err := ghostferry.LoadTables(
		this.sourceDB,
		&testhelpers.TestTableFilter{
			DbsFunc:    testhelpers.DbApplicabilityFilter([]string{testhelpers.TestSchemaName}),
			TablesFunc: nil,
		},
		nil,
		nil,
		nil,
		nil,
	)
	this.Require().Nil(err)

	this.binlogStreamer = &ghostferry.BinlogStreamer{
		DB:           this.sourceDB,
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

	_, err = this.sourceDB.Exec("INSERT INTO gftest.test_table_1 VALUES (null, 'testdata')")
	this.Require().Nil(err)

	wg.Wait()
	this.Require().True(eventAsserted)
}

func (this *BinlogStreamerTestSuite) TestBinlogStreamerSetsQueryEventOnRowsEvent() {
	_, err := this.binlogStreamer.ConnectBinlogStreamerToMysql()
	this.Require().Nil(err)

	eventAsserted := false
	this.binlogStreamer.AddEventListener(func(evs []ghostferry.DMLEvent) error {
		eventAsserted = true
		this.Require().Equal(1, len(evs))
		this.Require().True(strings.HasPrefix(evs[0].BinlogPosition().Name, "mysql-bin."))
		this.Require().True(evs[0].BinlogPosition().Pos > 0)

		annotation, err := evs[0].Annotation()
		this.Require().Nil(err)
		this.Require().Equal(annotation, fmt.Sprintf("%s", ghostferry.DefaultMarginalia))
		this.binlogStreamer.FlushAndStop()
		return nil
	})

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		this.binlogStreamer.Run()
	}()

	_, err = this.sourceDB.Exec("INSERT INTO gftest.test_table_1 VALUES (null, 'testdata')")
	this.Require().Nil(err)

	wg.Wait()
	this.Require().True(eventAsserted)
}

func TestBinlogStreamerTestSuite(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &BinlogStreamerTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
