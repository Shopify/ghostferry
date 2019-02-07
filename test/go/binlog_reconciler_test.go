package test

import (
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/suite"
)

type BinlogReconcilerTestSuite struct {
	suite.Suite
}

func (s *BinlogReconcilerTestSuite) TestReconciliationDMLEventGeneratesReplaceQuery() {
	table := &schema.Table{
		Schema: "source_schema",
		Name:   "test_table",
		Columns: []schema.TableColumn{
			{Name: "id"},
			{Name: "col1"},
			{Name: "col2"},
		},
	}

	targetTable := &schema.Table{
		Schema: "target_schema",
		Name:   "test_table",
	}

	rowData := ghostferry.RowData{
		1,
		"test1",
		10,
	}

	event := ghostferry.NewReconciliationDMLEvent(table, 1, rowData)
	query, err := event.AsSQLString(targetTable)
	s.Require().Nil(err)
	s.Require().Equal("REPLACE INTO `target_schema`.`test_table` (`id`,`col1`,`col2`) VALUES (1,'test1',10)", query)
}

func (s *BinlogReconcilerTestSuite) TestReconciliationDMLEventGeneratesDeleteQuery() {
	table := &schema.Table{
		Schema: "source_schema",
		Name:   "test_table",
		Columns: []schema.TableColumn{
			{Name: "id"},
			{Name: "col1"},
			{Name: "col2"},
		},
		PKColumns: []int{0},
	}

	targetTable := &schema.Table{
		Schema: "target_schema",
		Name:   "test_table",
	}

	event := ghostferry.NewReconciliationDMLEvent(table, 1, nil)
	query, err := event.AsSQLString(targetTable)
	s.Require().Nil(err)
	s.Require().Equal("DELETE FROM `target_schema`.`test_table` WHERE `id`=1", query)
}

func TestBinlogReconciler(t *testing.T) {
	suite.Run(t, new(BinlogReconcilerTestSuite))
}
