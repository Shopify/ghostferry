package test

import (
	"testing"

	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/suite"

	"github.com/Shopify/ghostferry/v2"
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
	this.Require().Equal(true, batch.ValuesContainPk())
	this.Require().Equal(0, batch.PkIndex())
	this.Require().Equal(1000, batch.Values()[0][batch.PkIndex()])
}

func (this *RowBatchTestSuite) TestRowBatchNoPkIndex() {
	vals := []ghostferry.RowData{
		ghostferry.RowData{"hello"},
	}
	batch := ghostferry.NewRowBatch(this.sourceTable, vals, -1)

	this.Require().Equal(false, batch.ValuesContainPk())
}

func TestRowBatchTestSuite(t *testing.T) {
	suite.Run(t, new(RowBatchTestSuite))
}
