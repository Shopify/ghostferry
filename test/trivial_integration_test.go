package test

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"

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

func TestCopyDataWithLargePrimaryKeyValues(t *testing.T) {
	ferry := testhelpers.NewTestFerry()

	ferry.Config.IterateChunksize = 10

	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleTableDatabaseWithHighBitUint64PKs,
		Ferry:       ferry,
	}

	testcase.Run()
}

func TestCopyDataWhileRenamingDatabaseAndTable(t *testing.T) {
	sourceDatabaseName := testhelpers.ApplicableTestDbs[0]
	targetDatabaseName := testhelpers.ApplicableTestDbs[1]
	sourceTableName := "table1"
	targetTableName := "table2"

	testcase := &testhelpers.IntegrationTestCase{
		T:     t,
		Ferry: testhelpers.NewTestFerry(),
		DisableChecksumVerifier: true,
		SetupAction: func(f *testhelpers.TestFerry) {
			testhelpers.SeedInitialData(f.SourceDB, sourceDatabaseName, sourceTableName, 1111)
			testhelpers.SeedInitialData(f.TargetDB, targetDatabaseName, targetTableName, 0)
		},
	}

	testcase.CustomVerifyAction = func(f *testhelpers.TestFerry) {
		sourceQuery := fmt.Sprintf("CHECKSUM TABLE `%s`.`%s` EXTENDED", sourceDatabaseName, sourceTableName)
		targetQuery := fmt.Sprintf("CHECKSUM TABLE `%s`.`%s` EXTENDED", targetDatabaseName, targetTableName)

		var tablename string
		var sourceChecksum sql.NullInt64
		var targetChecksum sql.NullInt64

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

func useUnsignedPK(db *sql.DB) {
	_, err := db.Exec("ALTER TABLE gftest.table1 MODIFY id bigint(20) unsigned not null")
	testhelpers.PanicIfError(err)
}

func setupSingleTableDatabase(f *testhelpers.TestFerry) {
	maxId := 1111
	testhelpers.SeedInitialData(f.SourceDB, "gftest", "table1", maxId)

	for i := 0; i < 140; i++ {
		query := "DELETE FROM gftest.table1 WHERE id = ?"
		_, err := f.SourceDB.Exec(query, rand.Intn(maxId-1)+1)
		testhelpers.PanicIfError(err)
	}

	testhelpers.SeedInitialData(f.TargetDB, "gftest", "table1", 0)
}

func setupSingleTableDatabaseWithHighBitUint64PKs(f *testhelpers.TestFerry) {
	setupSingleTableDatabase(f)

	_, err := f.SourceDB.Exec("TRUNCATE gftest.table1")
	testhelpers.PanicIfError(err)

	useUnsignedPK(f.SourceDB)
	useUnsignedPK(f.TargetDB)

	stmt, err := f.SourceDB.Prepare("INSERT INTO gftest.table1 (id, data) VALUES (?, ?)")
	testhelpers.PanicIfError(err)

	for i := uint64(0); i < 100; i++ {
		_, err = stmt.Exec(^uint64(0)-i, testhelpers.RandData())
		testhelpers.PanicIfError(err)
	}
}

func setupSingleTableDatabaseWithExtraNullColumn(f *testhelpers.TestFerry) {
	setupSingleTableDatabase(f)

	testhelpers.AddTenantID(f.SourceDB, "gftest", "table1", 1)
	testhelpers.AddTenantID(f.TargetDB, "gftest", "table1", 1)

	_, err := f.SourceDB.Exec("UPDATE gftest.table1 SET tenant_id = NULL")
	testhelpers.PanicIfError(err)
}
