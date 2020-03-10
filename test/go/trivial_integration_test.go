package test

import (
	sqlorig "database/sql"
	"fmt"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"math/rand"
	"testing"
	"time"

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

func TestCopyDataWithLargePaginationKeyValues(t *testing.T) {
	ferry := testhelpers.NewTestFerry()

	ferry.Config.DataIterationBatchSize = 10

	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleTableDatabaseWithHighBitUint64PaginationKeys,
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
		T:                       t,
		Ferry:                   testhelpers.NewTestFerry(),
		DisableChecksumVerifier: true,
		SetupAction: func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			testhelpers.SeedInitialData(sourceDB, sourceDatabaseName, sourceTableName, 1111)
			testhelpers.SeedInitialData(targetDB, targetDatabaseName, targetTableName, 0)
		},
	}

	testcase.CustomVerifyAction = func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
		sourceChecksum := checksumTable(t, sourceDB, sourceDatabaseName, sourceTableName, "source")
		targetChecksum := checksumTable(t, targetDB, targetDatabaseName, targetTableName, "target")
		if sourceChecksum != targetChecksum {
			testcase.T.Fatalf("source and target checksum does not match: %v, %v", sourceChecksum, targetChecksum)
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

func TestCopyDataInFixedSizeBinaryColumn(t *testing.T) {
	databaseName := testhelpers.ApplicableTestDbs[0]
	tableName := "table1"

	// we need a way to wait for replication to catch up. There really isn't a good way to do this
	// without a timeout without adding a notification from the binlog writer about updates (but how
	// would it know that "updates are complete"?)
	maxWaitForUpdates := 5 * time.Second

	// XXX: If one changes the below line to be
	//
	// insertedData := "ABC\x00"
	//
	// the update stops working. This is a bug in how we process fixed-length binary columns
	insertedData := "ABCD"
	updatedData := "DEFG"

	testcase := &testhelpers.IntegrationTestCase{
		T:                       t,
		Ferry:                   testhelpers.NewTestFerry(),
		DisableChecksumVerifier: true,
		AfterRowCopyIsComplete: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			update := fmt.Sprintf("UPDATE `%s`.`%s` SET data = '%s' WHERE id = 1 LIMIT 1", databaseName, tableName, updatedData)
			_, err := sourceDB.Exec(update)
			if err != nil {
				t.Errorf("updating source DB failed")
				return
			}
		},
		BeforeStoppingBinlogStreaming: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			query := fmt.Sprintf("SELECT data FROM `%s`.`%s` WHERE id = 1", databaseName, tableName)
			sourceRow := sourceDB.QueryRow(query)
			var data string
			err := sourceRow.Scan(&data)
			if err != nil {
				t.Errorf("selecting from source DB failed")
				return
			}
			if data != updatedData {
				t.Errorf("source table was not updated: '%s'", data)
				return
			}

			// wait for replication to catch up. Not ideal, see comment above.
			totalSleep := 0 * time.Millisecond
			for {
				targetRow := targetDB.QueryRow(query)
				err = targetRow.Scan(&data)
				if err != nil {
					t.Errorf("selecting from target DB failed")
					return
				}
				if data == updatedData {
					break
				}

				sleep := 100 * time.Millisecond
				time.Sleep(sleep)
				totalSleep += sleep
				if totalSleep > maxWaitForUpdates {
					t.Errorf("target table was not updated after timeout: '%s'", data)
					return
				}
			}
		},
		SetupAction: func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			setupSingleTableDatabaseWithBinaryColumn(sourceDB, databaseName, tableName)
			setupSingleTableDatabaseWithBinaryColumn(targetDB, databaseName, tableName)

			query := fmt.Sprintf("INSERT INTO `%s`.`%s` (id, data) VALUES (?, ?)", databaseName, tableName)
			_, err := sourceDB.Exec(query, 1, insertedData)
			testhelpers.PanicIfError(err)
		},
	}

	testcase.CustomVerifyAction = func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
		sourceChecksum := checksumTable(t, sourceDB, databaseName, tableName, "source")
		targetChecksum := checksumTable(t, targetDB, databaseName, tableName, "target")
		if sourceChecksum != targetChecksum {
			testcase.T.Fatalf("source and target checksum does not match: %v, %v", sourceChecksum, targetChecksum)
		}
	}

	testcase.Run()
}

// ====================
// Helper methods below
// ====================

func useUnsignedPaginationKey(db *sql.DB) {
	_, err := db.Exec("ALTER TABLE gftest.table1 MODIFY id bigint(20) unsigned not null")
	testhelpers.PanicIfError(err)
}

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

func setupSingleTableDatabaseWithHighBitUint64PaginationKeys(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
	setupSingleTableDatabase(f, sourceDB, targetDB)

	_, err := sourceDB.Exec("TRUNCATE gftest.table1")
	testhelpers.PanicIfError(err)

	useUnsignedPaginationKey(sourceDB)
	useUnsignedPaginationKey(targetDB)

	stmt, err := sourceDB.Prepare("INSERT INTO gftest.table1 (id, data) VALUES (?, ?)")
	testhelpers.PanicIfError(err)

	for i := uint64(0); i < 100; i++ {
		_, err = stmt.Exec(^uint64(0)-i, testhelpers.RandData())
		testhelpers.PanicIfError(err)
	}
}

func setupSingleTableDatabaseWithExtraNullColumn(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
	setupSingleTableDatabase(f, sourceDB, targetDB)

	testhelpers.AddTenantID(sourceDB, "gftest", "table1", 1)
	testhelpers.AddTenantID(targetDB, "gftest", "table1", 1)

	_, err := sourceDB.Exec("UPDATE gftest.table1 SET tenant_id = NULL")
	testhelpers.PanicIfError(err)
}

func setupSingleTableDatabaseWithBinaryColumn(db *sql.DB, dbname, tablename string) {
	createDBQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbname)
	_, err := db.Exec(createDBQuery)
	testhelpers.PanicIfError(err)

	createTableQuery := fmt.Sprintf("CREATE TABLE `%s`.`%s` (id bigint(20) not null auto_increment, data BINARY(4), primary key(id))", dbname, tablename)
	_, err = db.Exec(createTableQuery)
	testhelpers.PanicIfError(err)
}

func checksumTable(t *testing.T, db *sql.DB, databaseName, tableName, logicalName string) int64 {
	sourceQuery := fmt.Sprintf("CHECKSUM TABLE `%s`.`%s` EXTENDED", databaseName, tableName)

	var rowTablename string
	var checksum sqlorig.NullInt64

	sourceRow := db.QueryRow(sourceQuery)
	err := sourceRow.Scan(&rowTablename, &checksum)
	if err != nil {
		t.Errorf("failed to get %s checksum: %v", logicalName, err)
		return 0
	}
	if !checksum.Valid {
		t.Errorf("%s table doesn't exist", logicalName)
		return 0
	}

	return checksum.Int64
}
