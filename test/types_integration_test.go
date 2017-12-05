package test

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"

	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/sirupsen/logrus"
)

func addTypesToTable(db *sql.DB, dbName, tableName string) {
	query := "ALTER TABLE %s.%s " +
		"ADD tiny_col TINYINT," +
		"ADD float_col FLOAT," +
		"ADD double_col DOUBLE," +
		"ADD decimal_col DECIMAL(4, 2)," +
		"ADD date_col DATE," +
		"ADD time_col TIMESTAMP," + // TODO broken on master
		"ADD varchar_col VARCHAR(128)," +
		"ADD enum_col ENUM('foo', 'bar')," +
		"ADD utfmb4_col TEXT CHARSET utf8mb4," +
		"ADD utf32_col TEXT CHARSET utf32," + // TODO broken on master
		"ADD latin1_col TEXT CHARSET latin1 COLLATE latin1_swedish_ci," + // TODO broken on master
		"ADD blob_col BLOB"

	query = fmt.Sprintf(query, dbName, tableName)
	_, err := db.Exec(query)
	testhelpers.PanicIfError(err)
}

func setupMultiTypeTable(f *testhelpers.TestFerry) {
	testhelpers.SeedInitialData(f.SourceDB, "gftest", "table1", 0)
	testhelpers.SeedInitialData(f.TargetDB, "gftest", "table1", 0)

	addTypesToTable(f.SourceDB, "gftest", "table1")
	addTypesToTable(f.TargetDB, "gftest", "table1")

	tx, err := f.SourceDB.Begin()
	testhelpers.PanicIfError(err)

	for i := 0; i < 100; i++ {
		query := "INSERT INTO gftest.table1 " +
			"(id, data, tiny_col, float_col, double_col, decimal_col, date_col, varchar_col, enum_col, utfmb4_col, blob_col)" +
			"VALUES (NULL, ?, ?, 3.14, 2.72, 42.42, NOW(), ?, ?, ?, ?)"

		enumVal := "foo"
		if i%2 == 0 {
			enumVal = "bar"
		}
		randStr := testhelpers.RandData()
		randUStr := testhelpers.RandUTF8MB4Data()
		randBytes := testhelpers.RandByteData()

		_, err = tx.Exec(query, randStr, rand.Intn(2), randStr, enumVal, randUStr, randBytes)
		testhelpers.PanicIfError(err)
	}

	testhelpers.PanicIfError(tx.Commit())
}

func TestCopyDataWithManyTypes(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupMultiTypeTable,
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0 / 3.0,
			ProbabilityOfUpdate: 1.0 / 3.0,
			ProbabilityOfDelete: 1.0 / 3.0,
			NumberOfWriters:     3,
			Tables:              []string{"gftest.table1"},
		},
		Ferry: testhelpers.NewTestFerry(),
	}

	testcase.Run()
}
