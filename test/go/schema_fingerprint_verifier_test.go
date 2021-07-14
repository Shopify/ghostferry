package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type SchemaFingerPrintVerifierTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite
	tablename string
	sf        *ghostferry.SchemaFingerPrintVerifier
}

func alterTestTableSchema(db *sql.DB, this *SchemaFingerPrintVerifierTestSuite) {
	query := fmt.Sprintf("ALTER TABLE IF EXISTS %s.%s ADD COLUMN extracol VARCHAR(15)", testhelpers.TestSchemaName, this.tablename)
	this.Ferry.SourceDB.Query(query)
}

func resetTestTableSchema(db *sql.DB, this *SchemaFingerPrintVerifierTestSuite) {
	query := fmt.Sprintf("ALTER TABLE IF EXISTS %s.%s DROP COLUMN extracol", testhelpers.TestSchemaName, this.tablename)
	db.Query(query)
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
		TargetDB:                   this.Ferry.TargetDB,
		ErrorHandler:               this.Ferry.ErrorHandler,
		DatabaseRewrites:           map[string]string{},
		TableSchemaCache:           tableSchema,
		PeriodicallyVerifyInterval: periodicallyVerifyInterval,
	}
}

func (this *SchemaFingerPrintVerifierTestSuite) TestVerifySchemaFingerprint() {
	err := this.sf.VerifySchemaFingerprint()
	this.Require().Nil(err)

	alterTestTableSchema(this.sf.SourceDB, this)
	err = this.sf.VerifySchemaFingerprint()
	this.Require().Error(fmt.Errorf("failed to verifiy schema fingerprint on source"))

	resetTestTableSchema(this.sf.SourceDB, this)
	this.Require().Nil(err)

	alterTestTableSchema(this.sf.TargetDB, this)
	err = this.sf.VerifySchemaFingerprint()
	this.Require().Error(fmt.Errorf("failed to verifiy schema fingerprint on target"))
}

func TestSchemaFingerPrintVerifier(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &SchemaFingerPrintVerifierTestSuite{
		GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{},
	})
}
