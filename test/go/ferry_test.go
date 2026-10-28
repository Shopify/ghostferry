package test

import (
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

func (t *FerryTestSuite) newFerryWithCleanup() *ghostferry.Ferry {
	ferry := testhelpers.NewTestFerry().Ferry
	t.T().Cleanup(func() {
		if ferry.SourceDB != nil {
			ferry.SourceDB.Close()
		}
		if ferry.TargetDB != nil {
			ferry.TargetDB.Close()
		}
	})
	return ferry
}

func (t *FerryTestSuite) TestInitializeRejectsInvalidReportFrequencyBeforeConnecting() {
	for _, kind := range []string{"progress", "state"} {
		t.Run(kind, func() {
			ferry := t.newFerryWithCleanup()
			// Change the reporting settings without calling ValidateConfig again.
			if kind == "progress" {
				ferry.Config.ProgressCallback.URI = "http://example.com/progress"
				ferry.Config.ProgressReportFrequency = 99
			} else {
				ferry.Config.StateCallback.URI = "http://example.com/state"
				ferry.Config.StateReportFrequency = 99
			}

			t.Require().Error(ferry.Initialize())
			t.NotNil(ferry.ErrorHandler)
			t.Nil(ferry.SourceDB)
			t.Nil(ferry.TargetDB)
		})
	}
}

func (t *FerryTestSuite) TestInitializeDefaultsReportFrequencies() {
	ferry := t.newFerryWithCleanup()
	ferry.Config.ProgressCallback.URI = "http://example.com/progress"
	ferry.Config.StateCallback.URI = "http://example.com/state"
	ferry.Config.ProgressReportFrequency = 0
	ferry.Config.StateReportFrequency = 0

	t.Require().NoError(ferry.Initialize())
	t.Equal(60000, ferry.Config.ProgressReportFrequency)
	t.Equal(60000, ferry.Config.StateReportFrequency)
}

func TestFerryTestSuite(t *testing.T) {
	suite.Run(t, &FerryTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
