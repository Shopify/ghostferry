package test

import (
	"database/sql"
	"math/rand"
	"testing"

	"github.com/Shopify/ghostferry/testhelpers"
)

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

func useUnsignedPK(db *sql.DB) {
	_, err := db.Exec("ALTER TABLE gftest.table1 MODIFY id bigint(20) unsigned not null")
	testhelpers.PanicIfError(err)
}

func setupSingleTableDatabaseWithHighBitUint64PKs(f *testhelpers.TestFerry) {
	setupSingleTableDatabase(f)

	useUnsignedPK(f.SourceDB)
	useUnsignedPK(f.TargetDB)

	stmt, err := f.SourceDB.Prepare("INSERT INTO gftest.table1 (id, data) VALUES (?, ?)")
	testhelpers.PanicIfError(err)

	_, err = stmt.Exec(^uint64(0), testhelpers.RandData())
	testhelpers.PanicIfError(err)
}

func TestCopyDataWithLargePrimaryKeyValues(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleTableDatabaseWithHighBitUint64PKs,
		Ferry:       testhelpers.NewTestFerry(),
	}

	testcase.Run()
}
