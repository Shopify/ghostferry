package test

import (
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/suite"
)

type RowBatchTestSuite struct {
	suite.Suite

	tableMapEvent    *replication.TableMapEvent
	tableSchemaCache ghostferry.TableSchemaCache
	sourceTable      *ghostferry.TableSchema
	targetTable      *ghostferry.TableSchema
}

func (this *RowBatchTestSuite) SetupTest() {
	this.tableMapEvent = &replication.TableMapEvent{
		Schema: []byte("test_schema"),
		Table:  []byte("test_table"),
	}

	columns := []schema.TableColumn{
		{Name: "col1"},
		{Name: "col2"},
		{Name: "col3"},
	}

	this.sourceTable = &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:  "test_schema",
			Name:    "test_table",
			Columns: columns,
		},
	}

	this.targetTable = &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:  "target_schema",
			Name:    "target_table",
			Columns: columns,
		},
	}

	this.tableSchemaCache = map[string]*ghostferry.TableSchema{
		"test_schema.test_table": this.sourceTable,
	}
}

func (this *RowBatchTestSuite) TestRowBatchGeneratesInsertQuery() {
	vals := []ghostferry.RowData{
		ghostferry.RowData{1000, []byte("val1"), true},
		ghostferry.RowData{1001, []byte("val2"), true},
		ghostferry.RowData{1002, []byte("val3"), true},
	}
	batch := ghostferry.NewRowBatch(this.sourceTable, vals, 0)
	this.Require().Equal(vals, batch.Values())

	q1, v1, err := batch.AsSQLQuery(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("INSERT IGNORE INTO `target_schema`.`target_table` (`col1`,`col2`,`col3`) VALUES (?,?,?),(?,?,?),(?,?,?)", q1)

	expected := []interface{}{
		1000, []byte("val1"), true,
		1001, []byte("val2"), true,
		1002, []byte("val3"), true,
	}

	this.Require().Equal(expected, v1)
}

// TestRowBatchReorderedColumnsGeneratesCorrectInsert pins the INSERT column
// list to query-result order, never schema order.  The sharding copy filter's
// JOIN ... USING moves the join column to the front of the result set, so
// schema-ordered names against result-ordered values would silently write
// every value into the wrong column (the gh-285 corruption pattern).
func (this *RowBatchTestSuite) TestRowBatchReorderedColumnsGeneratesCorrectInsert() {
	// Schema order: tenant_id(0), col1(1), id(2), d(3)
	schemaColumns := []schema.TableColumn{
		{Name: "tenant_id"},
		{Name: "col1"},
		{Name: "id"},
		{Name: "d"},
	}
	table := &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:  "test_schema",
			Name:    "test_table",
			Columns: schemaColumns,
		},
	}

	// Query result order after USING(id): id, tenant_id, col1, d
	resultColumns := []string{"id", "tenant_id", "col1", "d"}
	rowInResultOrder := ghostferry.RowData{int64(2), int64(1), "z", "2021-01-01"}

	batch := ghostferry.NewRowBatchWithColumns(table, []ghostferry.RowData{rowInResultOrder}, resultColumns, 0)

	q, args, err := batch.AsSQLQuery("test_schema", "test_table")
	this.Require().Nil(err)

	// Column list must follow result order, not schema order.
	this.Require().Equal(
		"INSERT IGNORE INTO `test_schema`.`test_table` (`id`,`tenant_id`,`col1`,`d`) VALUES (?,?,?,?)",
		q,
	)
	this.Require().Equal([]interface{}{int64(2), int64(1), "z", "2021-01-01"}, args)
}

// TestRowBatchReorderedColumnsWithGeneratedColumnFiltersCorrectly combines the
// gh-285 reordering scenario with generated column filtering: the USING join
// moves 'id' first, and a VIRTUAL column 'gen' must be excluded from the
// INSERT while column/value alignment is still preserved for the others.
func (this *RowBatchTestSuite) TestRowBatchReorderedColumnsWithGeneratedColumnFiltersCorrectly() {
	// Schema order: tenant_id(0), gen VIRTUAL(1), col1(2), id(3)
	schemaColumns := []schema.TableColumn{
		{Name: "tenant_id"},
		{Name: "gen", IsVirtual: true},
		{Name: "col1"},
		{Name: "id"},
	}
	table := &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:  "test_schema",
			Name:    "test_table",
			Columns: schemaColumns,
		},
	}

	// Query result order after USING(id): id, tenant_id, gen, col1
	resultColumns := []string{"id", "tenant_id", "gen", "col1"}
	rowInResultOrder := ghostferry.RowData{int64(2), int64(1), nil, "z"}

	batch := ghostferry.NewRowBatchWithColumns(table, []ghostferry.RowData{rowInResultOrder}, resultColumns, 0)

	q, args, err := batch.AsSQLQuery("test_schema", "test_table")
	this.Require().Nil(err)

	// 'gen' must be absent from both column list and args.
	this.Require().Equal(
		"INSERT IGNORE INTO `test_schema`.`test_table` (`id`,`tenant_id`,`col1`) VALUES (?,?,?)",
		q,
	)
	this.Require().Equal([]interface{}{int64(2), int64(1), "z"}, args)
}

// TestRowBatchWithOnlyGeneratedColumnsReturnsError covers the case LoadTables
// cannot: a CopyFilter narrowing ColumnsToSelect, or an embedder populating
// Ferry.Tables directly, can reach AsSQLQuery with nothing writable.  That
// must be an error, not a panic inside strings.Repeat.
func (this *RowBatchTestSuite) TestRowBatchWithOnlyGeneratedColumnsReturnsError() {
	table := &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema: "test_schema",
			Name:   "test_table",
			Columns: []schema.TableColumn{
				{Name: "gen_a", IsStored: true},
				{Name: "gen_b", IsVirtual: true},
			},
		},
	}

	batch := ghostferry.NewRowBatch(table, []ghostferry.RowData{{int64(1), int64(2)}}, 0)

	q, args, err := batch.AsSQLQuery("test_schema", "test_table")
	this.Require().NotNil(err, "must report an error rather than panicking in strings.Repeat")
	this.Require().Contains(err.Error(), "has no columns to write")
	this.Require().Equal("", q)
	this.Require().Nil(args)
}

func (this *RowBatchTestSuite) TestRowBatchWithWrongColumnsReturnsError() {
	vals := []ghostferry.RowData{
		ghostferry.RowData{1000, []byte("val0"), true},
		ghostferry.RowData{1001},
		ghostferry.RowData{1002, []byte("val2"), true},
	}
	batch := ghostferry.NewRowBatch(this.sourceTable, vals, 0)

	_, _, err := batch.AsSQLQuery(this.targetTable.Schema, this.targetTable.Name)
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "test_table has 3 columns but event has 1 column")
}

func (this *RowBatchTestSuite) TestRowBatchMetadata() {
	vals := []ghostferry.RowData{
		ghostferry.RowData{1000},
	}
	batch := ghostferry.NewRowBatch(this.sourceTable, vals, 0)

	this.Require().Equal("test_schema", batch.TableSchema().Schema)
	this.Require().Equal("test_table", batch.TableSchema().Name)
	this.Require().Equal(true, batch.ValuesContainPaginationKey())
	this.Require().Equal(0, batch.PaginationKeyIndex())
	this.Require().Equal(1000, batch.Values()[0][batch.PaginationKeyIndex()])
}

func (this *RowBatchTestSuite) TestRowBatchNoPaginationKeyIndex() {
	vals := []ghostferry.RowData{
		ghostferry.RowData{"hello"},
	}
	batch := ghostferry.NewRowBatch(this.sourceTable, vals, -1)

	this.Require().Equal(false, batch.ValuesContainPaginationKey())
}

func (this *RowBatchTestSuite) TestEstimateBytesSize() {
	vals := []ghostferry.RowData{
		ghostferry.RowData{make([]byte, 100)},
		ghostferry.RowData{make([]byte, 100)},
	}

	batch := ghostferry.NewRowBatch(this.sourceTable, vals, 0)

	this.Require().GreaterOrEqual(batch.EstimateByteSize(), uint64(200), "estimated batch size should be greater then 200")
}

func TestRowBatchTestSuite(t *testing.T) {
	suite.Run(t, new(RowBatchTestSuite))
}
