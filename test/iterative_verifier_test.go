package test

import (
	"database/sql"
	"fmt"
	"sort"
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
			ReadRetries: t.Ferry.Config.DBReadRetries,
		},
		BinlogStreamer: t.Ferry.BinlogStreamer,
		SourceDB:       t.Ferry.SourceDB,
		TargetDB:       t.Ferry.TargetDB,

		Concurrency: 1,
	}

	t.db = t.Ferry.SourceDB
	t.reloadTables()

	err := t.verifier.Initialize()
	testhelpers.PanicIfError(err)
}

func (t *IterativeVerifierTestSuite) TearDownTest() {
	t.GhostferryUnitTestSuite.TearDownTest()
}

func (t *IterativeVerifierTestSuite) TestNothingToVerify() {
	err := t.verifier.VerifyBeforeCutover()
	t.Require().Nil(err)

	result, err := t.verifier.VerifyDuringCutover()
	t.Require().Nil(err)
	t.Require().True(result.DataCorrect)
	t.Require().Equal("", result.Message)
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

	t.Ferry.Tables = tables
	t.verifier.Tables = tables.AsSlice()
	t.verifier.TableSchemaCache = tables

	t.table = tables.Get(testhelpers.TestSchemaName, testhelpers.TestTable1Name)
	t.Require().NotNil(t.table)
}

type ReverifyStoreTestSuite struct {
	suite.Suite

	store *ghostferry.ReverifyStore
}

func (t *ReverifyStoreTestSuite) SetupTest() {
	t.store = ghostferry.NewReverifyStore()
}

func (t *ReverifyStoreTestSuite) TestAddEntryIntoReverifyStoreWillDeduplicate() {
	pk1 := uint64(100)
	pk2 := uint64(101)
	// different references to schema.Table shouldn't cause an issue.
	t.store.Add(ghostferry.ReverifyEntry{Pk: pk1, Table: &schema.Table{Schema: "gftest", Name: "table1"}})
	t.store.Add(ghostferry.ReverifyEntry{Pk: pk1, Table: &schema.Table{Schema: "gftest", Name: "table1"}})
	t.store.Add(ghostferry.ReverifyEntry{Pk: pk1, Table: &schema.Table{Schema: "gftest", Name: "table1"}})
	t.store.Add(ghostferry.ReverifyEntry{Pk: pk2, Table: &schema.Table{Schema: "gftest", Name: "table1"}})
	t.store.Add(ghostferry.ReverifyEntry{Pk: pk2, Table: &schema.Table{Schema: "gftest", Name: "table1"}})

	t.Require().Equal(uint64(2), t.store.RowCount)
	t.Require().Equal(1, len(t.store.MapStore))
	t.Require().Equal(
		map[uint64]struct{}{
			pk1: struct{}{},
			pk2: struct{}{},
		},
		t.store.MapStore[ghostferry.TableIdentifier{"gftest", "table1"}],
	)
}

func (t *ReverifyStoreTestSuite) TestFreezeAndBatchByTableWillCreateReverifyBatchesAndDeleteMapStore() {
	expectedTable1Pks := make([]uint64, 0, 55)
	for i := uint64(100); i < 155; i++ {
		t.store.Add(ghostferry.ReverifyEntry{Pk: i, Table: &schema.Table{Schema: "gftest", Name: "table1"}})
		expectedTable1Pks = append(expectedTable1Pks, i)
	}

	expectedTable2Pks := make([]uint64, 0, 45)
	for i := uint64(200); i < 245; i++ {
		t.store.Add(ghostferry.ReverifyEntry{Pk: i, Table: &schema.Table{Schema: "gftest", Name: "table2"}})
		expectedTable2Pks = append(expectedTable2Pks, i)
	}

	batches := t.store.FreezeAndBatchByTable(10)
	t.Require().Equal(11, len(batches))
	table1Batches := make([]ghostferry.ReverifyBatch, 0)
	table2Batches := make([]ghostferry.ReverifyBatch, 0)

	for _, batch := range batches {
		switch batch.Table.TableName {
		case "table1":
			table1Batches = append(table1Batches, batch)
		case "table2":
			table2Batches = append(table2Batches, batch)
		}
	}

	t.Require().Equal(6, len(table1Batches))
	t.Require().Equal(5, len(table2Batches))

	actualTable1Pks := make([]uint64, 0)
	for _, batch := range table1Batches {
		for _, pk := range batch.Pks {
			actualTable1Pks = append(actualTable1Pks, pk)
		}
	}

	sort.Slice(actualTable1Pks, func(i, j int) bool { return actualTable1Pks[i] < actualTable1Pks[j] })
	t.Require().Equal(expectedTable1Pks, actualTable1Pks)

	actualTable2Pks := make([]uint64, 0)
	for _, batch := range table2Batches {
		for _, pk := range batch.Pks {
			actualTable2Pks = append(actualTable2Pks, pk)
		}
	}

	sort.Slice(actualTable2Pks, func(i, j int) bool { return actualTable2Pks[i] < actualTable2Pks[j] })
	t.Require().Equal(expectedTable2Pks, actualTable2Pks)
}

func TestIterativeVerifierTestSuite(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &IterativeVerifierTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}

func TestReverifyStoreTestSuite(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &ReverifyStoreTestSuite{})
}
