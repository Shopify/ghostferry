package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type SchemaFingerPrintVerifierTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite
	tablename string
	sf        *ghostferry.SchemaFingerPrintVerifier
}

func alterTestTableSchema(this *SchemaFingerPrintVerifierTestSuite) {
	query := fmt.Sprintf("ALTER TABLE IF EXISTS %s.%s ADD COLUMN extracol VARCHAR(15)", testhelpers.TestSchemaName, this.tablename)
	this.Ferry.SourceDB.Query(query)
}

func (this *SchemaFingerPrintVerifierTestSuite) SetupTest() {
	this.GhostferryUnitTestSuite.SetupTest()

	this.tablename = "test_table_1"
	testhelpers.SeedInitialData(this.Ferry.SourceDB, testhelpers.TestSchemaName, this.tablename, 0)

	tableFilter := &testhelpers.TestTableFilter{
		DbsFunc:    testhelpers.DbApplicabilityFilter([]string{testhelpers.TestSchemaName}),
		TablesFunc: nil,
	}
	tableSchema, err := ghostferry.LoadTables(this.Ferry.SourceDB, tableFilter, nil, nil, nil, nil)
	this.Require().Nil(err)

	periodicallyVerifyInterval, _ := time.ParseDuration(this.Ferry.Config.PeriodicallyVerifySchemaFingerPrintInterval)

	this.sf = &ghostferry.SchemaFingerPrintVerifier{
		SourceDB:                   this.Ferry.SourceDB,
		ErrorHandler:               this.Ferry.ErrorHandler,
		TableRewrites:              map[string]string{},
		TableSchemaCache:           tableSchema,
		PeriodicallyVerifyInterval: periodicallyVerifyInterval,
		FingerPrints:               map[string]string{},
	}

	this.sf.Initialize()
}

func (this *SchemaFingerPrintVerifierTestSuite) TestVerifySchemaFingerprint() {
	err := this.sf.VerifySchemaFingerPrint()
	this.Require().Nil(err)

	alterTestTableSchema(this)
	err = this.sf.VerifySchemaFingerPrint()
	this.Require().Error(fmt.Errorf("failed to verifiy schema fingerprint for %s.%s", testhelpers.TestSchemaName, this.tablename))
}

func TestSchemaFingerPrintVerifier(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &SchemaFingerPrintVerifierTestSuite{
		GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{},
	})
}
