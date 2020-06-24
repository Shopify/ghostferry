package test

import (
	sqlorig "database/sql"
	"fmt"
	"math/rand"
	"testing"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/Shopify/ghostferry/testhelpers"
)

func TestCopyDataWithoutAnyWritesToSource(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleTableDatabase,

		Ferry: testhelpers.NewTestFerry(),
	}

	testcase.Run()
}

func TestCopyDataWithInsertLoad(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleTableDatabase,
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0,
			ProbabilityOfUpdate: 0.0,
			ProbabilityOfDelete: 0.0,
			NumberOfWriters:     2,
			Tables:              []string{"gftest.table1"},
		},
		Ferry: testhelpers.NewTestFerry(),
	}

	testcase.Run()
}

func TestCopyDataWithUpdateLoad(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleTableDatabase,
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 0.0,
			ProbabilityOfUpdate: 1.0,
			ProbabilityOfDelete: 0.0,
			NumberOfWriters:     2,
			Tables:              []string{"gftest.table1"},
		},
		Ferry: testhelpers.NewTestFerry(),
	}

	testcase.Run()
}

func TestCopyDataWithDeleteLoad(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleTableDatabase,
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 0.0,
			ProbabilityOfUpdate: 0.0,
			ProbabilityOfDelete: 1.0,
			NumberOfWriters:     2,
			Tables:              []string{"gftest.table1"},
		},
		Ferry: testhelpers.NewTestFerry(),
	}

	testcase.Run()
}

func TestCopyDataWhileRenamingDatabaseAndTable(t *testing.T) {
	sourceDatabaseName := testhelpers.ApplicableTestDbs[0]
	targetDatabaseName := testhelpers.ApplicableTestDbs[1]
	sourceTableName := "table1"
	targetTableName := "table2"

	testcase := &testhelpers.IntegrationTestCase{
		T:                       t,
		Ferry:                   testhelpers.NewTestFerry(),
		DisableChecksumVerifier: true,
		SetupAction: func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			testhelpers.SeedInitialData(sourceDB, sourceDatabaseName, sourceTableName, 1111)
			testhelpers.SeedInitialData(targetDB, targetDatabaseName, targetTableName, 0)
		},
	}

	testcase.CustomVerifyAction = func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
		sourceQuery := fmt.Sprintf("CHECKSUM TABLE `%s`.`%s` EXTENDED", sourceDatabaseName, sourceTableName)
		targetQuery := fmt.Sprintf("CHECKSUM TABLE `%s`.`%s` EXTENDED", targetDatabaseName, targetTableName)

		var tablename string
		var sourceChecksum sqlorig.NullInt64
		var targetChecksum sqlorig.NullInt64

		sourceRow := f.SourceDB.QueryRow(sourceQuery)
		err := sourceRow.Scan(&tablename, &sourceChecksum)
		if err != nil {
			testcase.T.Errorf("failed to get source checksum: %v", err)
			return
		}
		if !sourceChecksum.Valid {
			testcase.T.Errorf("source table doesn't exist")
			return
		}

		targetRow := f.TargetDB.QueryRow(targetQuery)
		err = targetRow.Scan(&tablename, &targetChecksum)
		if err != nil {
			testcase.T.Errorf("failed to get source checksum: %v", err)
			return
		}

		if !targetChecksum.Valid {
			testcase.T.Errorf("target table doesn't exist")
			return
		}

		if sourceChecksum.Int64 != targetChecksum.Int64 {
			testcase.T.Fatalf("source and target checksum does not match: %v, %v", sourceChecksum.Int64, targetChecksum.Int64)
		}
	}

	testcase.Ferry.DatabaseRewrites = map[string]string{
		sourceDatabaseName: targetDatabaseName,
	}

	testcase.Ferry.TableRewrites = map[string]string{
		sourceTableName: targetTableName,
	}

	testcase.Run()
}

func TestCopyDataWithNullInColumn(t *testing.T) {
	ferry := testhelpers.NewTestFerry()

	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleTableDatabaseWithExtraNullColumn,
		Ferry:       ferry,

		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfUpdate: 1.0,
			NumberOfWriters:     2,
			Tables:              []string{"gftest.table1"},
		},
	}

	testcase.Run()
}

// ====================
// Helper methods below
// ====================

func setupSingleTableDatabase(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
	maxId := 1111
	testhelpers.SeedInitialData(sourceDB, "gftest", "table1", maxId)

	for i := 0; i < 140; i++ {
		query := "DELETE FROM gftest.table1 WHERE id = ?"
		_, err := sourceDB.Exec(query, rand.Intn(maxId-1)+1)
		testhelpers.PanicIfError(err)
	}

	testhelpers.SeedInitialData(targetDB, "gftest", "table1", 0)
}

func setupSingleTableDatabaseWithExtraNullColumn(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
	setupSingleTableDatabase(f, sourceDB, targetDB)

	testhelpers.AddTenantID(sourceDB, "gftest", "table1", 1)
	testhelpers.AddTenantID(targetDB, "gftest", "table1", 1)

	_, err := sourceDB.Exec("UPDATE gftest.table1 SET tenant_id = NULL")
	testhelpers.PanicIfError(err)
}
