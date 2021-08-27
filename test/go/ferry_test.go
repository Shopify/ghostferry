package test

import (
	"sync"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type FerryTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	ferry *ghostferry.Ferry
}

func (t *FerryTestSuite) SetupTest() {
	t.GhostferryUnitTestSuite.SetupTest()
}

func (t *FerryTestSuite) TestSourceTargetDiffSchema() {
	t.SeedSourceDB(1)
	t.SeedTargetDB(0)

	ferry := testhelpers.NewTestFerry().Ferry // make new ferry that re-uses the same targetDB as t.Ferry

	err := ferry.Initialize()
	t.Require().Nil(err)

	_, err = ferry.TargetDB.Exec("ALTER TABLE gftest.test_table_1 ADD omglol varchar(255)")
	t.Require().Nil(err)

	err = ferry.Initialize()
	t.Require().Nil(err)

	ferry.Start()

	ferry.AutomaticCutover = true

	copyWG := &sync.WaitGroup{}
	copyWG.Add(1)
	go func() {
		defer copyWG.Done()
		ferry.Run()
	}()

	ferry.WaitUntilRowCopyIsComplete()
	ferry.FlushBinlogAndStopStreaming()

	copyWG.Wait()
}

func (t *FerryTestSuite) TestReadOnlyDatabaseFailsInitialization() {
	_, err := t.Ferry.TargetDB.Exec("SET GLOBAL read_only = ON")
	t.Require().Nil(err)

	ferry := testhelpers.NewTestFerry().Ferry // make new ferry that re-uses the same targetDB as t.Ferry
	err = ferry.Initialize()
	t.Require().Equal("@@read_only must be OFF on target db", err.Error())

	_, err = t.Ferry.TargetDB.Exec("SET GLOBAL read_only = OFF")
	t.Require().Nil(err)

	ferry = testhelpers.NewTestFerry().Ferry
	err = ferry.Initialize()
	t.Require().Nil(err)
}

func (t *FerryTestSuite) TestReadOnlyDatabaseDoesNotFailInitializationWithAllowSuperUserOnReadOnlyFlag() {
	_, err := t.Ferry.TargetDB.Exec("SET GLOBAL read_only = ON")
	t.Require().Nil(err)

	ferry := testhelpers.NewTestFerry().Ferry
	ferry.Config.AllowSuperUserOnReadOnly = true

	err = ferry.Initialize()
	t.Require().Nil(err)

	_, err = t.Ferry.TargetDB.Exec("SET GLOBAL read_only = OFF")
	t.Require().Nil(err)
}

func (t *FerryTestSuite) TestSourceDatabaseWithForeignKeyConstraintFailsInitialization() {
	createTableWithFkConstraint := `
		CREATE TABLE gftest.test_fk (
			id bigint(20) NOT NULL AUTO_INCREMENT,
			tt1_id BIGINT(20),
			PRIMARY KEY(ID),
			FOREIGN KEY(tt1_id) REFERENCES test_table_1(id)
		)
	`

	testhelpers.SeedInitialData(t.Ferry.SourceDB, "gftest", "test_table_1", 0)
	t.Ferry.SourceDB.Exec(createTableWithFkConstraint)

	ferry := testhelpers.NewTestFerry().Ferry
	err := ferry.Initialize()

	t.Require().Equal("found at least 1 foreign key constraint on source DB. table: gftest.test_fk, constraint: test_fk_ibfk_1", err.Error())

	// Initialize a ferry with SkipForeignKeyConstraintsCheck, assert no error
	ferry = testhelpers.NewTestFerry().Ferry
	ferry.Config.SkipForeignKeyConstraintsCheck = true

	err = ferry.Initialize()

	t.Require().Nil(err)
}

func TestFerryTestSuite(t *testing.T) {
	suite.Run(t, &FerryTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
