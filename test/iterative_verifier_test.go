package test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/suite"
)

type IterativeVerifierTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	verifier *ghostferry.IterativeVerifier
	db       *sql.DB
	table    *schema.Table
}

func (t *IterativeVerifierTestSuite) SetupTest() {
	t.GhostferryUnitTestSuite.SetupTest()
	t.SeedSourceDB(0)

	t.verifier = &ghostferry.IterativeVerifier{
		CursorConfig: &ghostferry.CursorConfig{
			DB:          t.Ferry.SourceDB,
			BatchSize:   t.Ferry.Config.DataIterationBatchSize,
			ReadRetries: t.Ferry.Config.MaxIterationReadRetries,
		},
		BinlogStreamer: t.Ferry.BinlogStreamer,
		SourceDB:       t.Ferry.SourceDB,
		TargetDB:       t.Ferry.TargetDB,

		Tables:      t.Ferry.Tables.AsSlice(),
		Concurrency: 1,
	}

	err := t.verifier.Initialize()
	testhelpers.PanicIfError(err)

	t.db = t.Ferry.SourceDB
	t.reloadTables()
}

func (t *IterativeVerifierTestSuite) TearDownTest() {
	t.GhostferryUnitTestSuite.TearDownTest()
}

func (t *IterativeVerifierTestSuite) TestChangingDataChangesHash() {
	t.InsertRow(42, "foo")
	old := t.GetHashes([]uint64{42})[0]

	t.UpdateRow(42, "bar")
	new := t.GetHashes([]uint64{42})[0]

	t.Require().NotEqual(old, new)
}

func (t *IterativeVerifierTestSuite) TestDeduplicatesHashes() {
	t.InsertRow(42, "foo")

	hashes, err := t.verifier.GetHashes(t.db, t.table.Schema, t.table.Name, t.table.GetPKColumn(0).Name, t.table.Columns, []uint64{42, 42})
	t.Require().Nil(err)
	t.Require().Equal(1, len(hashes))
}

func (t *IterativeVerifierTestSuite) TestDoesntReturnHashIfRecordDoesntExist() {
	hashes, err := t.verifier.GetHashes(t.db, t.table.Schema, t.table.Name, t.table.GetPKColumn(0).Name, t.table.Columns, []uint64{42, 42})
	t.Require().Nil(err)
	t.Require().Equal(0, len(hashes))
}

func (t *IterativeVerifierTestSuite) TestUnrelatedRowsDontAffectHash() {
	t.InsertRow(42, "foo")
	expected := t.GetHashes([]uint64{42})[0]

	t.InsertRow(43, "bar")
	actual := t.GetHashes([]uint64{42})[0]

	t.Require().Equal(expected, actual)
}

func (t *IterativeVerifierTestSuite) TestRowsWithSameDataButDifferentPKs() {
	t.InsertRow(42, "foo")
	t.InsertRow(43, "foo")

	hashes := t.GetHashes([]uint64{42, 43})
	t.Require().NotEqual(hashes[0], hashes[1])
}

func (t *IterativeVerifierTestSuite) TestPositiveAndNegativeZeroFloat() {
	_, err := t.db.Exec("ALTER TABLE gftest.test_table_1 MODIFY data float")
	t.Require().Nil(err)
	t.reloadTables()

	_, err = t.db.Exec("INSERT INTO gftest.test_table_1 VALUES (42, \"0.0\")")
	t.Require().Nil(err)

	expected := t.GetHashes([]uint64{42})[0]

	_, err = t.db.Exec("UPDATE gftest.test_table_1 SET data=\"-0.0\" WHERE id=42")
	t.Require().Nil(err)

	actual := t.GetHashes([]uint64{42})[0]

	t.Require().Equal(expected, actual)
}

func (t *IterativeVerifierTestSuite) TestChangingNumberValueChangesHash() {
	_, err := t.db.Exec("ALTER TABLE gftest.test_table_1 MODIFY data bigint(20)")
	t.Require().Nil(err)
	t.reloadTables()

	_, err = t.db.Exec("INSERT INTO gftest.test_table_1 VALUES (42, -100)")
	t.Require().Nil(err)

	neg := t.GetHashes([]uint64{42})[0]

	_, err = t.db.Exec("UPDATE gftest.test_table_1 SET data=100 WHERE id=42")
	t.Require().Nil(err)

	pos := t.GetHashes([]uint64{42})[0]

	t.Require().NotEqual(neg, pos)
}

func (t *IterativeVerifierTestSuite) TestNULLValues() {
	_, err := t.db.Exec("INSERT INTO gftest.test_table_1 VALUES (42, NULL)")
	t.Require().Nil(err)
	null := t.GetHashes([]uint64{42})[0]

	t.UpdateRow(42, "")
	empty := t.GetHashes([]uint64{42})[0]

	t.UpdateRow(42, "foo")
	foo := t.GetHashes([]uint64{42})[0]

	t.Require().NotEqual(null, empty)
	t.Require().NotEqual(foo, empty)
	t.Require().NotEqual(foo, null)
}

func (t *IterativeVerifierTestSuite) InsertRow(id int, data string) {
	_, err := t.db.Exec(fmt.Sprintf("INSERT INTO %s.%s VALUES (%d,\"%s\")", testhelpers.TestSchemaName, testhelpers.TestTable1Name, id, data))
	t.Require().Nil(err)
}

func (t *IterativeVerifierTestSuite) UpdateRow(id int, data string) {
	_, err := t.db.Exec(fmt.Sprintf("UPDATE %s.%s SET data=\"%s\" WHERE id=%d", testhelpers.TestSchemaName, testhelpers.TestTable1Name, data, id))
	t.Require().Nil(err)
}

func (t *IterativeVerifierTestSuite) DeleteRow(id int) {
	_, err := t.db.Exec(fmt.Sprintf("DELETE FROM %s.%s WHERE id=%d", testhelpers.TestSchemaName, testhelpers.TestTable1Name, id))
	t.Require().Nil(err)
}

func (t *IterativeVerifierTestSuite) GetHashes(ids []uint64) []string {
	hashes, err := t.verifier.GetHashes(t.db, t.table.Schema, t.table.Name, t.table.GetPKColumn(0).Name, t.table.Columns, ids)
	t.Require().Nil(err)
	t.Require().Equal(len(hashes), len(ids))

	res := make([]string, len(ids))

	for idx, id := range ids {
		hash, ok := hashes[id]
		t.Require().True(ok)
		t.Require().True(len(hash) > 0)

		res[idx] = string(hash)
	}

	return res
}

func (t *IterativeVerifierTestSuite) reloadTables() {
	tableFilter := &testhelpers.TestTableFilter{
		DbsFunc:    testhelpers.DbApplicabilityFilter([]string{testhelpers.TestSchemaName}),
		TablesFunc: nil,
	}

	tables, err := ghostferry.LoadTables(t.db, tableFilter)
	t.Require().Nil(err)

	t.table = tables.Get(testhelpers.TestSchemaName, testhelpers.TestTable1Name)
	t.Require().NotNil(t.table)
}

func TestIterativeVerifierTestSuite(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &IterativeVerifierTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
