package test

import (
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/suite"
)

type DMLEventsTestSuite struct {
	suite.Suite

	eventBase        *ghostferry.DMLEventBase
	tableMapEvent    *replication.TableMapEvent
	tableSchemaCache ghostferry.TableSchemaCache
	sourceTable      *ghostferry.TableSchema
	targetTable      *ghostferry.TableSchema
}

func (this *DMLEventsTestSuite) SetupTest() {
	this.tableMapEvent = &replication.TableMapEvent{
		Schema: []byte("test_schema"),
		Table:  []byte("test_table"),
	}

	columns := []schema.TableColumn{
		{Name: "col1"},
		{Name: "col2", Type: schema.TYPE_JSON},
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

	this.eventBase = ghostferry.NewDMLEventBase(this.sourceTable, mysql.Position{}, mysql.Position{}, nil, time.Unix(1618318965, 0))
}

func (this *DMLEventsTestSuite) TestBinlogInsertEventGeneratesInsertQuery() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1000, []byte("val1"), true},
			{1001, []byte("val2"), false},
			{1002, "{\"val\": 42.0}", false},
		},
	}

	dmlEvents, err := ghostferry.NewBinlogInsertEvents(this.eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(3, len(dmlEvents))

	q1, err := dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("INSERT IGNORE INTO `target_schema`.`target_table` (`col1`,`col2`,`col3`) VALUES (1000,CAST('val1' AS JSON),1)", q1)

	q2, err := dmlEvents[1].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("INSERT IGNORE INTO `target_schema`.`target_table` (`col1`,`col2`,`col3`) VALUES (1001,CAST('val2' AS JSON),0)", q2)

	q3, err := dmlEvents[2].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("INSERT IGNORE INTO `target_schema`.`target_table` (`col1`,`col2`,`col3`) VALUES (1002,CAST('{\"val\": 42.0}' AS JSON),0)", q3)
}

func (this *DMLEventsTestSuite) TestBinlogInsertEventWithWrongColumnsReturnsError() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}},
	}

	dmlEvents, err := ghostferry.NewBinlogInsertEvents(this.eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))

	_, err = dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "test_table has 3 columns but event has 1 column")
}

func (this *DMLEventsTestSuite) TestBinlogInsertEventMetadata() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}},
	}

	dmlEvents, err := ghostferry.NewBinlogInsertEvents(this.eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))
	this.Require().Equal("test_schema", dmlEvents[0].Database())
	this.Require().Equal("test_table", dmlEvents[0].Table())
	this.Require().Nil(dmlEvents[0].OldValues())
	this.Require().Equal(ghostferry.RowData{1000}, dmlEvents[0].NewValues())
	this.Require().Equal(time.Unix(1618318965, 0), dmlEvents[0].Timestamp())
}

func (this *DMLEventsTestSuite) TestBinlogUpdateEventGeneratesUpdateQuery() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1000, []byte("val1"), true},
			{1000, []byte("val2"), false},
			{1001, []byte("val3"), false},
			{1001, []byte("val4"), true},
			{1002, "{\"val\": 42.0}", false},
			{1002, "{\"val\": 43.0}", false},
		},
	}

	dmlEvents, err := ghostferry.NewBinlogUpdateEvents(this.eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(3, len(dmlEvents))

	q1, err := dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("UPDATE `target_schema`.`target_table` SET `col1`=1000,`col2`=CAST('val2' AS JSON),`col3`=0 WHERE `col1`=1000 AND `col2`=CAST('val1' AS JSON) AND `col3`=1", q1)

	q2, err := dmlEvents[1].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("UPDATE `target_schema`.`target_table` SET `col1`=1001,`col2`=CAST('val4' AS JSON),`col3`=1 WHERE `col1`=1001 AND `col2`=CAST('val3' AS JSON) AND `col3`=0", q2)

	q3, err := dmlEvents[2].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("UPDATE `target_schema`.`target_table` SET `col1`=1002,`col2`=CAST('{\"val\": 43.0}' AS JSON),`col3`=0 WHERE `col1`=1002 AND `col2`=CAST('{\"val\": 42.0}' AS JSON) AND `col3`=0", q3)
}

func (this *DMLEventsTestSuite) TestBinlogUpdateEventWithWrongColumnsReturnsError() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}, {1000}},
	}

	dmlEvents, err := ghostferry.NewBinlogUpdateEvents(this.eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))

	_, err = dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "test_table has 3 columns but event has 1 column")
}

func (this *DMLEventsTestSuite) TestBinlogUpdateEventWithNull() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1000, []byte("val1"), nil},
			{1000, []byte("val2"), nil},
		},
	}

	dmlEvents, err := ghostferry.NewBinlogUpdateEvents(this.eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))

	q1, err := dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("UPDATE `target_schema`.`target_table` SET `col1`=1000,`col2`=CAST('val2' AS JSON),`col3`=NULL WHERE `col1`=1000 AND `col2`=CAST('val1' AS JSON) AND `col3` IS NULL", q1)
}

func (this *DMLEventsTestSuite) TestBinlogUpdateEventMetadata() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}, {1001}},
	}

	dmlEvents, err := ghostferry.NewBinlogUpdateEvents(this.eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))
	this.Require().Equal("test_schema", dmlEvents[0].Database())
	this.Require().Equal("test_table", dmlEvents[0].Table())
	this.Require().Equal(ghostferry.RowData{1000}, dmlEvents[0].OldValues())
	this.Require().Equal(ghostferry.RowData{1001}, dmlEvents[0].NewValues())
	this.Require().Equal(time.Unix(1618318965, 0), dmlEvents[0].Timestamp())
}

func (this *DMLEventsTestSuite) TestBinlogDeleteEventGeneratesDeleteQuery() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1000, []byte("val1"), true},
			{1001, []byte("val2"), false},
		},
	}

	dmlEvents, err := ghostferry.NewBinlogDeleteEvents(this.eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(2, len(dmlEvents))

	q1, err := dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("DELETE FROM `target_schema`.`target_table` WHERE `col1`=1000 AND `col2`=CAST('val1' AS JSON) AND `col3`=1", q1)

	q2, err := dmlEvents[1].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("DELETE FROM `target_schema`.`target_table` WHERE `col1`=1001 AND `col2`=CAST('val2' AS JSON) AND `col3`=0", q2)
}

func (this *DMLEventsTestSuite) TestBinlogDeleteEventWithNull() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1000, []byte("val1"), nil},
		},
	}

	dmlEvents, err := ghostferry.NewBinlogDeleteEvents(this.eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))

	q1, err := dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("DELETE FROM `target_schema`.`target_table` WHERE `col1`=1000 AND `col2`=CAST('val1' AS JSON) AND `col3` IS NULL", q1)
}

func (this *DMLEventsTestSuite) TestBinlogDeleteEventWithWrongColumnsReturnsError() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}},
	}

	dmlEvents, err := ghostferry.NewBinlogDeleteEvents(this.eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))

	_, err = dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "test_table has 3 columns but event has 1 column")
}

func (this *DMLEventsTestSuite) TestBinlogDeleteEventMetadata() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}},
	}

	dmlEvents, err := ghostferry.NewBinlogDeleteEvents(this.eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))
	this.Require().Equal("test_schema", dmlEvents[0].Database())
	this.Require().Equal("test_table", dmlEvents[0].Table())
	this.Require().Equal(ghostferry.RowData{1000}, dmlEvents[0].OldValues())
	this.Require().Nil(dmlEvents[0].NewValues())
	this.Require().Equal(time.Unix(1618318965, 0), dmlEvents[0].Timestamp())
}

func (this *DMLEventsTestSuite) TestAnnotations() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1, []byte("val1"), true},
		},
	}

	eventBase := ghostferry.NewDMLEventBase(
		this.sourceTable,
		mysql.Position{},
		mysql.Position{},
		[]byte("/*application:ghostferry*/ INSERT IGNORE INTO `target_schema`.`target_table` (`col1`,`col2`) VALUES (1, val1)"),
		time.Unix(1618318965, 0),
	)

	dmlEvents, err := ghostferry.NewBinlogInsertEvents(eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))

	annotation, err := dmlEvents[0].Annotation()
	this.Require().Nil(err)
	this.Require().Equal(annotation, "application:ghostferry")

}

func (this *DMLEventsTestSuite) TestNoAnnotations() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1, []byte("val1"), true},
		},
	}

	eventBase := ghostferry.NewDMLEventBase(
		this.sourceTable,
		mysql.Position{},
		mysql.Position{},
		[]byte("INSERT IGNORE INTO `target_schema`.`target_table` (`col1`,`col2`) VALUES (1, val1)"),
		time.Unix(1618318965, 0),
	)

	dmlEvents, err := ghostferry.NewBinlogInsertEvents(eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))

	annotation, err := dmlEvents[0].Annotation()
	this.Require().Nil(err)
	this.Require().Equal("", annotation)
}

func (this *DMLEventsTestSuite) TestMultipleAnnotations() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1, []byte("val1"), true},
		},
	}

	eventBase := ghostferry.NewDMLEventBase(
		this.sourceTable,
		mysql.Position{},
		mysql.Position{},
		[]byte("/*application:ghostferry*/ /*request_id:d8e8fca2dc0f896fd7cb4cb0031ba249*/ /*myannotation*/ INSERT IGNORE INTO `target_schema`.`target_table` (`col1`,`col2`) VALUES (1, val1)"),
		time.Unix(1618318965, 0),
	)

	dmlEvents, err := ghostferry.NewBinlogInsertEvents(eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))

	annotation, err := dmlEvents[0].Annotation()
	this.Require().Nil(err)
	this.Require().Equal(annotation, "application:ghostferry")
}

func (this *DMLEventsTestSuite) TestSeparatedAnnotations() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1, []byte("val1"), true},
		},
	}

	eventBase := ghostferry.NewDMLEventBase(
		this.sourceTable,
		mysql.Position{},
		mysql.Position{},
		[]byte("/*application:ghostferry*/ /*request_id:d8e8fca2dc0f896fd7cb4cb0031ba249;other:annotation*/ INSERT IGNORE INTO `target_schema`.`target_table` (`col1`,`col2`) VALUES (1, val1)"),
		time.Unix(1618318965, 0),
	)

	dmlEvents, err := ghostferry.NewBinlogInsertEvents(eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))

	annotation, err := dmlEvents[0].Annotation()
	this.Require().Nil(err)
	this.Require().Equal(annotation, "application:ghostferry")
}

func (this *DMLEventsTestSuite) TestNoRowsQueryEvent() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1, []byte("val1"), true},
		},
	}

	eventBase := ghostferry.NewDMLEventBase(
		this.sourceTable,
		mysql.Position{},
		mysql.Position{},
		nil,
		time.Unix(1618318965, 0),
	)

	dmlEvents, err := ghostferry.NewBinlogInsertEvents(eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))

	annotation, err := dmlEvents[0].Annotation()
	this.Require().NotNil(err)
	this.Require().Equal(err.Error(), "could not get query from DML event")
	this.Require().Equal("", annotation)
}

// TestNewBinlogDMLEventsUnsignedConversionWithGeneratedColumn pins the index
// space that unsigned normalisation runs in.
//
// Binlog row images are always full width, so the normalisation loop indexes
// the full schema.  Compacting the row first — dropping generated values to
// match what INSERT and SET emit — would leave the loop reading the wrong
// positions: here it would either run off the end of the shortened slice or
// leave int8(-1) serialised as -1 rather than uint8(255).  Filtering happens
// at SQL construction, never before.
func (this *DMLEventsTestSuite) TestNewBinlogDMLEventsUnsignedConversionWithGeneratedColumn() {
	// Do not reorder these into a more natural shape.  'gen' has to sit BEFORE
	// 'u8' for this test to detect anything: the two index spaces only diverge
	// after the first generated column, so with 'gen' last every index would
	// coincide and the test would pass against the very bug it exists to catch.
	columns := []schema.TableColumn{
		{Name: "id"},
		{Name: "gen", IsVirtual: true},
		{Name: "u8", IsUnsigned: true},
	}
	table := &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:  "test_schema",
			Name:    "test_table",
			Columns: columns,
		},
	}

	ev := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2},
		Event: &replication.RowsEvent{
			Rows: [][]interface{}{
				{int64(1000), "gen_val", int8(-1)},
			},
		},
	}

	dmlEvents, err := ghostferry.NewBinlogDMLEvents(table, ev, mysql.Position{}, mysql.Position{}, nil)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))

	q, err := dmlEvents[0].AsSQLString("test_schema", "test_table")
	this.Require().Nil(err)
	this.Require().Equal(
		"INSERT IGNORE INTO `test_schema`.`test_table` (`id`,`u8`) VALUES (1000,255)",
		q,
	)
}

// TestBinlogInsertEventGeneratedColumnBeforeJSONPreservesJSONCasting pins the
// pairing between a value and the column metadata used to escape it.
//
// buildStringListForValues receives values with the generated ones already
// removed, so its value index and its schema index diverge as soon as a
// generated column appears.  Using the value index to look up column metadata
// would escape this JSON payload against the virtual column's metadata and
// emit a plain quoted string instead of CAST(... AS JSON).
func (this *DMLEventsTestSuite) TestBinlogInsertEventGeneratedColumnBeforeJSONPreservesJSONCasting() {
	// Do not reorder these into a more natural shape.  'gen' has to sit BEFORE
	// 'payload' for this test to detect anything: the two index spaces only
	// diverge after the first generated column, so with 'gen' last every index
	// would coincide and the test would pass against the very bug it exists to
	// catch.  The name of the test says "GeneratedColumnBeforeJSON" for this
	// reason and not as a description of the fixture.
	columns := []schema.TableColumn{
		{Name: "gen", IsVirtual: true},
		{Name: "payload", Type: schema.TYPE_JSON},
	}
	table := &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:  "test_schema",
			Name:    "test_table",
			Columns: columns,
		},
	}
	eventBase := ghostferry.NewDMLEventBase(table, mysql.Position{}, mysql.Position{}, nil, time.Unix(1618318965, 0))

	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{"gen_val", []byte("payload_data")}},
	}

	dmlEvents, err := ghostferry.NewBinlogInsertEvents(eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))

	q, err := dmlEvents[0].AsSQLString("test_schema", "test_table")
	this.Require().Nil(err)
	this.Require().Equal(
		"INSERT IGNORE INTO `test_schema`.`test_table` (`payload`) VALUES (CAST('payload_data' AS JSON))",
		q,
	)
}

// TestBinlogUpdateEventExcludesGeneratedColumnFromSetOnly verifies the
// asymmetry between the two clauses of a replayed UPDATE: the SET clause must
// omit the generated column, because MySQL rejects the assignment, while the
// WHERE clause must keep it, because it may be the only thing distinguishing
// this row from its neighbours.  See buildStringMapForWhere in dml_events.go.
func (this *DMLEventsTestSuite) TestBinlogUpdateEventExcludesGeneratedColumnFromSetOnly() {
	columns := []schema.TableColumn{
		{Name: "id"},
		{Name: "gen", IsVirtual: true},
		{Name: "data"},
	}
	table := &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:  "test_schema",
			Name:    "test_table",
			Columns: columns,
		},
	}
	eventBase := ghostferry.NewDMLEventBase(table, mysql.Position{}, mysql.Position{}, nil, time.Unix(1618318965, 0))

	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{int64(1000), "gen_old", "old_data"},
			{int64(1000), "gen_new", "new_data"},
		},
	}

	dmlEvents, err := ghostferry.NewBinlogUpdateEvents(eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))

	q, err := dmlEvents[0].AsSQLString("test_schema", "test_table")
	this.Require().Nil(err)
	this.Require().Equal(
		"UPDATE `test_schema`.`test_table` SET `id`=1000,`data`='new_data' WHERE `id`=1000 AND `gen`='gen_old' AND `data`='old_data'",
		q,
	)
}

// TestBinlogDeleteEventKeepsStoredGeneratedColumnInWhere pins the narrowest
// possible DELETE predicate.  A STORED generated column is allowed to be the
// primary key, so dropping it from the WHERE clause both loses the index and,
// where the remaining columns only compare equal under a collation, deletes
// rows that were never deleted on the source.
func (this *DMLEventsTestSuite) TestBinlogDeleteEventKeepsStoredGeneratedColumnInWhere() {
	columns := []schema.TableColumn{
		{Name: "id"},
		{Name: "data"},
		{Name: "summary", IsStored: true},
	}
	table := &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:  "test_schema",
			Name:    "test_table",
			Columns: columns,
		},
	}
	eventBase := ghostferry.NewDMLEventBase(table, mysql.Position{}, mysql.Position{}, nil, time.Unix(1618318965, 0))

	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{int64(1000), "hello", "abc123"}},
	}

	dmlEvents, err := ghostferry.NewBinlogDeleteEvents(eventBase, rowsEvent)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))

	q, err := dmlEvents[0].AsSQLString("test_schema", "test_table")
	this.Require().Nil(err)
	this.Require().Equal(
		"DELETE FROM `test_schema`.`test_table` WHERE `id`=1000 AND `data`='hello' AND `summary`='abc123'",
		q,
	)
}

func TestDMLEventsTestSuite(t *testing.T) {
	suite.Run(t, new(DMLEventsTestSuite))
}
