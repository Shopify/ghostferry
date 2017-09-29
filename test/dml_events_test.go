package test

import (
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/suite"
)

type DMLEventsTestSuite struct {
	suite.Suite

	tableMapEvent    *replication.TableMapEvent
	tableSchemaCache ghostferry.TableSchemaCache
}

func (this *DMLEventsTestSuite) SetupTest() {
	this.tableMapEvent = &replication.TableMapEvent{
		Schema: []byte("test_schema"),
		Table:  []byte("test_table"),
	}

	this.tableSchemaCache = map[string]*schema.Table{
		"test_schema.test_table": &schema.Table{
			Schema: "test_schema",
			Name:   "test_table",
			Columns: []schema.TableColumn{
				{Name: "col1"},
				{Name: "col2"},
				{Name: "col3"},
			},
		},
	}
}

func (this *DMLEventsTestSuite) TestBinlogInsertEventGeneratesInsertQuery() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1000, []byte("val1"), true},
			{1001, []byte("val2"), false},
		},
	}

	dmlEvents := ghostferry.NewBinlogInsertEvents(rowsEvent)
	this.Require().Equal(2, len(dmlEvents))

	q1, v1, err := dmlEvents[0].AsSQLQuery(this.tableSchemaCache)
	this.Require().Nil(err)
	this.Require().Equal("INSERT IGNORE INTO `test_schema`.`test_table` (`col1`,`col2`,`col3`) VALUES (?,?,?)", q1)
	this.Require().Equal(rowsEvent.Rows[0], v1)

	q2, v2, err := dmlEvents[1].AsSQLQuery(this.tableSchemaCache)
	this.Require().Nil(err)
	this.Require().Equal("INSERT IGNORE INTO `test_schema`.`test_table` (`col1`,`col2`,`col3`) VALUES (?,?,?)", q2)
	this.Require().Equal(rowsEvent.Rows[1], v2)
}

func (this *DMLEventsTestSuite) TestBinlogInsertEventWithWrongColumnsReturnsError() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}},
	}

	dmlEvents := ghostferry.NewBinlogInsertEvents(rowsEvent)
	this.Require().Equal(1, len(dmlEvents))
	_, _, err := dmlEvents[0].AsSQLQuery(this.tableSchemaCache)
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "test_table has 3 columns but event has 1 column")
}

func (this *DMLEventsTestSuite) TestBinlogInsertEventMetadata() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}},
	}

	dmlEvents := ghostferry.NewBinlogInsertEvents(rowsEvent)
	this.Require().Equal(1, len(dmlEvents))
	this.Require().Equal("test_schema", dmlEvents[0].Database())
	this.Require().Equal("test_table", dmlEvents[0].Table())
	this.Require().Equal([]interface{}{nil}, dmlEvents[0].OldValues())
	this.Require().Equal([]interface{}{1000}, dmlEvents[0].NewValues())
}

func (this *DMLEventsTestSuite) TestBinlogUpdateEventGeneratesUpdateQuery() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1000, []byte("val1"), true},
			{1000, []byte("val2"), false},
			{1001, []byte("val3"), false},
			{1001, []byte("val4"), true},
		},
	}

	dmlEvents := ghostferry.NewBinlogUpdateEvents(rowsEvent)
	this.Require().Equal(2, len(dmlEvents))

	q1, v1, err := dmlEvents[0].AsSQLQuery(this.tableSchemaCache)
	this.Require().Nil(err)
	this.Require().Equal("UPDATE `test_schema`.`test_table` SET `col1` = ?, `col2` = ?, `col3` = ? WHERE `col1` = ? AND `col2` = ? AND `col3` = ?", q1)
	this.Require().Equal(append(rowsEvent.Rows[1], rowsEvent.Rows[0]...), v1)

	q2, v2, err := dmlEvents[1].AsSQLQuery(this.tableSchemaCache)
	this.Require().Nil(err)
	this.Require().Equal("UPDATE `test_schema`.`test_table` SET `col1` = ?, `col2` = ?, `col3` = ? WHERE `col1` = ? AND `col2` = ? AND `col3` = ?", q2)
	this.Require().Equal(append(rowsEvent.Rows[3], rowsEvent.Rows[2]...), v2)
}

func (this *DMLEventsTestSuite) TestBinlogUpdateEventWithWrongColumnsReturnsError() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}, {1000}},
	}

	dmlEvents := ghostferry.NewBinlogUpdateEvents(rowsEvent)
	this.Require().Equal(1, len(dmlEvents))
	_, _, err := dmlEvents[0].AsSQLQuery(this.tableSchemaCache)
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "test_table has 3 columns but event has 1 column")
}

func (this *DMLEventsTestSuite) TestBinlogUpdateEventMetadata() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}, {1001}},
	}

	dmlEvents := ghostferry.NewBinlogUpdateEvents(rowsEvent)
	this.Require().Equal(1, len(dmlEvents))
	this.Require().Equal("test_schema", dmlEvents[0].Database())
	this.Require().Equal("test_table", dmlEvents[0].Table())
	this.Require().Equal([]interface{}{1000}, dmlEvents[0].OldValues())
	this.Require().Equal([]interface{}{1001}, dmlEvents[0].NewValues())
}

func (this *DMLEventsTestSuite) TestBinlogDeleteEventGeneratesDeleteQuery() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1000, []byte("val1"), true},
			{1001, []byte("val2"), false},
		},
	}

	dmlEvents := ghostferry.NewBinlogDeleteEvents(rowsEvent)
	this.Require().Equal(2, len(dmlEvents))

	q1, v1, err := dmlEvents[0].AsSQLQuery(this.tableSchemaCache)
	this.Require().Nil(err)
	this.Require().Equal("DELETE FROM `test_schema`.`test_table` WHERE `col1` = ? AND `col2` = ? AND `col3` = ?", q1)
	this.Require().Equal(rowsEvent.Rows[0], v1)

	q2, v2, err := dmlEvents[1].AsSQLQuery(this.tableSchemaCache)
	this.Require().Nil(err)
	this.Require().Equal("DELETE FROM `test_schema`.`test_table` WHERE `col1` = ? AND `col2` = ? AND `col3` = ?", q2)
	this.Require().Equal(rowsEvent.Rows[1], v2)
}

func (this *DMLEventsTestSuite) TestBinlogDeleteEventWithWrongColumnsReturnsError() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}},
	}

	dmlEvents := ghostferry.NewBinlogDeleteEvents(rowsEvent)
	this.Require().Equal(1, len(dmlEvents))
	_, _, err := dmlEvents[0].AsSQLQuery(this.tableSchemaCache)
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "test_table has 3 columns but event has 1 column")
}

func (this *DMLEventsTestSuite) TestBinlogDeleteEventMetadata() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}},
	}

	dmlEvents := ghostferry.NewBinlogDeleteEvents(rowsEvent)
	this.Require().Equal(1, len(dmlEvents))
	this.Require().Equal("test_schema", dmlEvents[0].Database())
	this.Require().Equal("test_table", dmlEvents[0].Table())
	this.Require().Equal([]interface{}{1000}, dmlEvents[0].OldValues())
	this.Require().Equal([]interface{}{nil}, dmlEvents[0].NewValues())
}

func (this *DMLEventsTestSuite) TestExistingRowEventGeneratesInsertQuery() {
	vals := []interface{}{1000, []byte("val1"), true}
	dmlEvent := ghostferry.NewExistingRowEvent("test_schema", "test_table", vals)

	q1, v1, err := dmlEvent.AsSQLQuery(this.tableSchemaCache)
	this.Require().Nil(err)
	this.Require().Equal("INSERT IGNORE INTO `test_schema`.`test_table` (`col1`,`col2`,`col3`) VALUES (?,?,?)", q1)
	this.Require().Equal(vals, v1)
}

func (this *DMLEventsTestSuite) TestExistingRowEventWithWrongColumnsReturnsError() {
	vals := []interface{}{1000}
	dmlEvent := ghostferry.NewExistingRowEvent("test_schema", "test_table", vals)

	_, _, err := dmlEvent.AsSQLQuery(this.tableSchemaCache)
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "test_table has 3 columns but event has 1 column")
}

func (this *DMLEventsTestSuite) TestExistingRowEventMetadata() {
	vals := []interface{}{1000}
	dmlEvent := ghostferry.NewExistingRowEvent("test_schema", "test_table", vals)

	this.Require().Equal("test_schema", dmlEvent.Database())
	this.Require().Equal("test_table", dmlEvent.Table())
	this.Require().Equal([]interface{}{1000}, dmlEvent.OldValues())
	this.Require().Equal([]interface{}{1000}, dmlEvent.NewValues())
}

func TestDMLEventsTestSuite(t *testing.T) {
	suite.Run(t, new(DMLEventsTestSuite))
}
