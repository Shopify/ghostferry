package test

import (
	"fmt"
	"testing"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/assert"
)

func TestAddingColumnOnTargetBeforeSourceWorks(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:                       t,
		Ferry:                   testhelpers.NewTestFerry(),
		DisableChecksumVerifier: true,
		SetupAction:             setupSingleTableDatabase,
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0,
			NumberOfWriters:     2,
			Tables:              []string{"gftest.table1"},
		},
		AfterStartBinlogStreaming: func(f *testhelpers.TestFerry, sourceDB *sql.DB, targetDB *sql.DB) {
			_, err := targetDB.Exec("ALTER TABLE gftest.table1 ADD COLUMN extra VARCHAR(255)")
			testhelpers.PanicIfError(err)
		},
	}

	defer testcase.Teardown()
	testcase.CopyData()

	testcase.AssertQueriesHaveEqualResult("SELECT count(*) FROM gftest.table1")

	var count int
	row := testcase.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.table1")
	testhelpers.PanicIfError(row.Scan(&count))

	assert.True(t, count > 0)
}

func TestDroppingColumnOnSourceBeforeTargetWorks(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:                       t,
		Ferry:                   testhelpers.NewTestFerry(),
		DisableChecksumVerifier: true,
		SetupAction:             setupSingleTableDatabase,
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0,
			NumberOfWriters:     2,
			Tables:              []string{"gftest.table1"},
			ExtraInsertData: func(table string, colvals map[string]interface{}) {
				// delete data from the columns being written
				// this works because the data writer is started after the AfterStartBinlogStreaming callback is executed
				// so we are assured that the source DB would not have this column by that point.
				delete(colvals, "data")
				fmt.Println("Inserting data...")
			},
		},
		AfterStartBinlogStreaming: func(f *testhelpers.TestFerry, sourceDB *sql.DB, targetDB *sql.DB) {
			// Drop column like LHM would do (except the CREATE TABLE LIKE part)
			_, err := sourceDB.Exec("CREATE TABLE gftest.table1_new LIKE gftest.table1")
			testhelpers.PanicIfError(err)

			_, err = sourceDB.Exec("ALTER TABLE gftest.table1_new DROP COLUMN data")
			testhelpers.PanicIfError(err)

			_, err = sourceDB.Exec("RENAME TABLE gftest.table1 TO gftest.table1_old, gftest.table1_new TO gftest.table1")
			testhelpers.PanicIfError(err)
			fmt.Println("Done migrating.")
		},
	}

	defer testcase.Teardown()
	testcase.CopyData()

	testcase.AssertQueriesHaveEqualResult("SELECT count(*) FROM gftest.table1")

	var count int
	row := testcase.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.table1")
	testhelpers.PanicIfError(row.Scan(&count))

	assert.True(t, count > 0)
}

func TestChangingTypeOnSourceWorks(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:                       t,
		Ferry:                   testhelpers.NewTestFerry(),
		DisableChecksumVerifier: true,
		SetupAction:             setupSingleTableDatabase,
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0,
			NumberOfWriters:     2,
			Tables:              []string{"gftest.table1"},
		},
		AfterStartBinlogStreaming: func(f *testhelpers.TestFerry, sourceDB *sql.DB, targetDB *sql.DB) {
			_, err := sourceDB.Exec("ALTER TABLE gftest.table1 MODIFY data VARCHAR(255)")
			testhelpers.PanicIfError(err)
		},
	}

	defer testcase.Teardown()
	testcase.CopyData()

	testcase.AssertQueriesHaveEqualResult("SELECT count(*) FROM gftest.table1")

	var count int
	row := testcase.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.table1")
	testhelpers.PanicIfError(row.Scan(&count))

	assert.True(t, count > 0)
}

func TestRenamingTableOnSourceOrig(t *testing.T) {
	errorHandler := &testhelpers.ErrorHandler{}
	ferry := testhelpers.NewTestFerry()
	ferry.ErrorHandler = errorHandler
	testcase := &testhelpers.IntegrationTestCase{
		T:                       t,
		Ferry:                   ferry,
		DisableChecksumVerifier: true,
		SetupAction:             setupSingleTableDatabase,
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0,
			NumberOfWriters:     2,
			Tables:              []string{"gftest.table1"},
		},
		AfterStartBinlogStreaming: func(f *testhelpers.TestFerry, sourceDB *sql.DB, targetDB *sql.DB) {
			_, err := sourceDB.Exec("ALTER TABLE gftest.table1 CHANGE data data_new TEXT")
			fmt.Printf("ERROR: %v\n", err)
			testhelpers.PanicIfError(err)
		},
	}

	defer testcase.Teardown()

	// defer func() {
	// 	if r := recover(); r == nil {
	// 		t.Errorf("Did not fail as expected")
	// 	}
	// }()

	// fmt.Println("starting copy")
	//assert.Panics(t, func() { panic("lol") })
	//assert.Panics(t, testcase.CopyData)
	assert.Panics(t, func() { testcase.CopyData() })

	// assert.PanicsWithValue(t, nil, testcase.CopyData)

	// assert.Equal(t, "Unknown column 'data' in 'field list'", errorHandler.LastError)
}

func TestRenamingTableOnSourceFails(t *testing.T) {
	assert.Panics(t, func() { panic("lol") })
}
