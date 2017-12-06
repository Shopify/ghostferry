package test

import (
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/suite"
)

type RowBatchTestSuite struct {
	suite.Suite

	tableMapEvent    *replication.TableMapEvent
	tableSchemaCache ghostferry.TableSchemaCache
	sourceTable      *schema.Table
	targetTable      *schema.Table
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

	this.sourceTable = &schema.Table{
		Schema:  "test_schema",
		Name:    "test_table",
		Columns: columns,
	}

	this.targetTable = &schema.Table{
		Schema:  "target_schema",
		Name:    "target_table",
		Columns: columns,
	}

	this.tableSchemaCache = map[string]*schema.Table{
		"test_schema.test_table": this.sourceTable,
	}
}

func (this *RowBatchTestSuite) TestRowBatchGeneratesInsertQuery() {
	vals := []ghostferry.RowData{
		ghostferry.RowData{1000, []byte("val1"), true},
		ghostferry.RowData{1001, []byte("val2"), true},
		ghostferry.RowData{1002, []byte("val3"), true},
	}
	batch, err := ghostferry.NewRowBatch(this.sourceTable, vals)
	this.Require().Nil(err)
	this.Require().Equal(vals, batch.Values())

	q1, v1, err := batch.AsSQLQuery(this.targetTable)
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
	batch, err := ghostferry.NewRowBatch(this.sourceTable, vals)
	this.Require().Nil(err)

	_, _, err = batch.AsSQLQuery(this.targetTable)
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "test_table has 3 columns but event has 1 column")
}

func (this *RowBatchTestSuite) TestRowBatchMetadata() {
	vals := []ghostferry.RowData{
		ghostferry.RowData{1000},
	}
	batch, err := ghostferry.NewRowBatch(this.sourceTable, vals)
	this.Require().Nil(err)

	this.Require().Equal("test_schema", batch.Database())
	this.Require().Equal("test_table", batch.Table())
}

func TestRowBatchTestSuite(t *testing.T) {
	suite.Run(t, new(RowBatchTestSuite))
}
