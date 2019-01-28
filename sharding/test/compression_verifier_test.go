package test

import (
	"database/sql"
	"testing"

	"github.com/Shopify/ghostferry"
	sth "github.com/Shopify/ghostferry/sharding/testhelpers"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type CompressionVerifierTestSuite struct {
	*sth.ShardingUnitTestSuite
}

func (t *CompressionVerifierTestSuite) SetupTest() {
	t.ShardingUnitTestSuite.SetupTest()
}

func (t *CompressionVerifierTestSuite) TearDownTest() {
	t.ShardingUnitTestSuite.TearDownTest()
}

func (t *CompressionVerifierTestSuite) TestFailsVerificationForDifferentCompressedData() {
	tableCompressions := make(ghostferry.TableColumnCompressionConfig)
	tableCompressions[testhelpers.TestCompressedTable1Name] = make(map[string]string)
	tableCompressions[testhelpers.TestCompressedTable1Name][testhelpers.TestCompressedColumn1Name] = ghostferry.CompressionSnappy
	t.replaceCompressionVerifier(tableCompressions)

	t.Require().NotEqual(testhelpers.TestCompressedData1, testhelpers.TestCompressedData2)
	t.InsertCompressedRowInDb(43, "gftest1", testhelpers.TestCompressedData1, t.Ferry.Ferry.SourceDB)
	t.InsertCompressedRowInDb(43, "gftest2", testhelpers.TestCompressedData2, t.Ferry.Ferry.TargetDB)

	err := t.Ferry.Start()
	t.Require().Nil(err)

	errHandler := &testhelpers.ErrorHandler{}
	t.Ferry.Ferry.ErrorHandler = errHandler

	t.Ferry.Run()

	t.Require().NotNil(errHandler.LastError)
	t.Require().Equal("verifier detected data discrepancy: verification failed on table: gftest1.test_compressed_table_1 for pks: 43", errHandler.LastError.Error())
}

func (t *CompressionVerifierTestSuite) TestCanCopyDifferentCompressedDataButEqualDecompressedData() {
	tableCompressions := make(ghostferry.TableColumnCompressionConfig)
	tableCompressions[testhelpers.TestCompressedTable1Name] = make(map[string]string)
	tableCompressions[testhelpers.TestCompressedTable1Name][testhelpers.TestCompressedColumn1Name] = ghostferry.CompressionSnappy
	t.replaceCompressionVerifier(tableCompressions)

	t.Require().NotEqual(testhelpers.TestCompressedData3, testhelpers.TestCompressedData4)

	t.InsertCompressedRowInDb(43, "gftest1", testhelpers.TestCompressedData3, t.Ferry.Ferry.SourceDB)
	t.InsertCompressedRowInDb(43, "gftest2", testhelpers.TestCompressedData4, t.Ferry.Ferry.TargetDB)

	err := t.Ferry.Start()
	t.Require().Nil(err)

	t.Ferry.Run()
}

func (t *CompressionVerifierTestSuite) InsertCompressedRowInDb(id int, schema, data string, db *sql.DB) {
	tenantId := 2
	_, err := db.Exec("INSERT INTO "+schema+"."+testhelpers.TestCompressedTable1Name+" VALUES (?,?,?)", id, data, tenantId)
	t.Require().Nil(err)
}

func (t *CompressionVerifierTestSuite) replaceCompressionVerifier(tableCompressions ghostferry.TableColumnCompressionConfig) {
	var err error
	iterativeVerifier := t.Ferry.Ferry.Verifier.(*ghostferry.IterativeVerifier)
	iterativeVerifier.CompressionVerifier, err = ghostferry.NewCompressionVerifier(tableCompressions)
	t.Require().Nil(err)
}

func TestCompressionVerifierTestSuite(t *testing.T) {
	suite.Run(t, &CompressionVerifierTestSuite{ShardingUnitTestSuite: &sth.ShardingUnitTestSuite{}})
}
