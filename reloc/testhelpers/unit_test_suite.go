package testhelpers

import (
	"fmt"

	"github.com/Shopify/ghostferry/reloc"
	"github.com/Shopify/ghostferry/testhelpers"

	"github.com/stretchr/testify/suite"
)

const (
	sourceDbName  = "gftest1"
	targetDbName  = "gftest2"
	testTable     = "table1"
	shardingKey   = "tenant_id"
	shardingValue = 2
)

type RelocUnitTestSuite struct {
	suite.Suite

	Ferry  *reloc.RelocFerry
	Config *reloc.Config
}

func (t *RelocUnitTestSuite) SetupTest() {
	t.setupRelocFerry()
	t.dropTestDbs()

	testhelpers.SeedInitialData(t.Ferry.Ferry.SourceDB, sourceDbName, testTable, 1000)
	testhelpers.SeedInitialData(t.Ferry.Ferry.TargetDB, targetDbName, testTable, 0)

	testhelpers.AddTenantID(t.Ferry.Ferry.SourceDB, sourceDbName, testTable, 3)
	testhelpers.AddTenantID(t.Ferry.Ferry.TargetDB, targetDbName, testTable, 3)
}

func (t *RelocUnitTestSuite) TearDownTest() {
	t.dropTestDbs()
}

func (t *RelocUnitTestSuite) AssertTenantCopied() {
	testhelpers.AssertTwoQueriesHaveEqualResult(
		t.T(),
		t.Ferry.Ferry,
		fmt.Sprintf("SELECT * FROM %s.%s WHERE %s = %d", sourceDbName, testTable, shardingKey, shardingValue),
		fmt.Sprintf("SELECT * FROM %s.%s", targetDbName, testTable),
	)
}

func (t *RelocUnitTestSuite) setupRelocFerry() {
	ghostferryConfig := testhelpers.NewTestConfig()

	t.Config = &reloc.Config{
		Config:        ghostferryConfig,
		ShardingKey:   shardingKey,
		ShardingValue: shardingValue,
		SourceDB:      sourceDbName,
		TargetDB:      targetDbName,
	}

	var err error
	t.Ferry, err = reloc.NewFerry(t.Config)
	t.Require().Nil(err)

	err = t.Ferry.Initialize()
	t.Require().Nil(err)
}

func (t *RelocUnitTestSuite) dropTestDbs() {
	_, err := t.Ferry.Ferry.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", sourceDbName))
	t.Require().Nil(err)

	_, err = t.Ferry.Ferry.TargetDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", targetDbName))
	t.Require().Nil(err)
}
