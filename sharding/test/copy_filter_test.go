package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/sharding"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/suite"
)

type CopyFilterTestSuite struct {
	suite.Suite

	shardingValue       int64
	paginationKeyCursor uint64

	normalTable, normalTable2, joinedTable, primaryKeyTable *ghostferry.TableSchema

	filter *sharding.ShardedCopyFilter
}

func (t *CopyFilterTestSuite) SetupTest() {
	t.shardingValue = int64(1)
	t.paginationKeyCursor = uint64(12345)

	columns := []schema.TableColumn{{Name: "id"}, {Name: "tenant_id"}, {Name: "data"}}
	t.normalTable = &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:    "shard_1",
			Name:      "normaltable",
			Columns:   columns,
			PKColumns: []int{0},
			Indexes: []*schema.Index{
				{Name: "unrelated_index", Columns: []string{"tenant_id", "data"}, Visible: true},
				{Name: "less_good_sharding_index", Columns: []string{"tenant_id"}, Visible: true},
				{Name: "good_sharding_index", Columns: []string{"tenant_id", "id"}, Visible: true},
				{Name: "unrelated_index2", Columns: []string{"data"}, Visible: true},
			},
		},
		PaginationKeyColumn: &columns[0],
	}

	columns = []schema.TableColumn{{Name: "id"}, {Name: "tenant_id"}, {Name: "more_data"}}
	t.normalTable2 = &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:    "shard_1",
			Name:      "normaltable2",
			Columns:   columns,
			PKColumns: []int{0},
			Indexes: []*schema.Index{
				{Name: "good_sharding_index", Columns: []string{"tenant_id", "id"}, Visible: true},
			},
		},
		PaginationKeyColumn: &columns[0],
	}

	columns = []schema.TableColumn{{Name: "joined_paginationKey"}}
	t.joinedTable = &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:    "shard_1",
			Name:      "joinedtable",
			Columns:   columns,
			PKColumns: []int{0},
		},
		PaginationKeyColumn: &columns[0],
	}

	columns = []schema.TableColumn{{Name: "tenant_id"}}
	t.primaryKeyTable = &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:    "shard_1",
			Name:      "pkTable",
			Columns:   columns,
			PKColumns: []int{0},
		},
		PaginationKeyColumn: &columns[0],
	}

	t.filter = &sharding.ShardedCopyFilter{
		ShardingKey:   "tenant_id",
		ShardingValue: t.shardingValue,

		JoinedTables: map[string][]sharding.JoinTable{
			"joinedtable": []sharding.JoinTable{
				{TableName: "join1", JoinColumn: "joined_paginationKey1"},
				{TableName: "join2", JoinColumn: "joined_paginationKey2"},
			},
		},

		PrimaryKeyTables: map[string]struct{}{"pkTable": struct{}{}},
		IndexHint:        "use",
	}
}

func (t *CopyFilterTestSuite) TestSelectsRegularTables() {
	selectBuilder, err := t.filter.BuildSelect([]string{"*"}, t.normalTable, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` JOIN (SELECT `id` FROM `shard_1`.`normaltable` USE INDEX (`good_sharding_index`) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)
}

func (t *CopyFilterTestSuite) TestFallsBackToLessGoodIndex() {
	t.normalTable.Indexes[2].Columns = []string{"data"} // Remove good index.
	selectBuilder, err := t.filter.BuildSelect([]string{"*"}, t.normalTable, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` JOIN (SELECT `id` FROM `shard_1`.`normaltable` USE INDEX (`less_good_sharding_index`) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)
}

func (t *CopyFilterTestSuite) TestFallsBackToIgnoredPrimaryIndex() {
	t.normalTable.Indexes[1].Columns = []string{"data"} // Remove less good index.
	t.normalTable.Indexes[2].Columns = []string{"data"} // Remove good index.
	selectBuilder, err := t.filter.BuildSelect([]string{"*"}, t.normalTable, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` JOIN (SELECT `id` FROM `shard_1`.`normaltable` IGNORE INDEX (PRIMARY) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)
}

func (t *CopyFilterTestSuite) TestRemovesIndexHint() {
	t.filter.IndexHint = "none"
	selectBuilder, err := t.filter.BuildSelect([]string{"*"}, t.normalTable, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` JOIN (SELECT `id` FROM `shard_1`.`normaltable` WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)
}

func (t *CopyFilterTestSuite) TestUsesForceIndex() {
	t.filter.IndexHint = "force"
	selectBuilder, err := t.filter.BuildSelect([]string{"*"}, t.normalTable, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` JOIN (SELECT `id` FROM `shard_1`.`normaltable` FORCE INDEX (`good_sharding_index`) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)
}

func (t *CopyFilterTestSuite) TestUsesIndexHintThatIsNotLowercased() {
	t.filter.IndexHint = "Force"
	selectBuilder, err := t.filter.BuildSelect([]string{"*"}, t.normalTable, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` JOIN (SELECT `id` FROM `shard_1`.`normaltable` FORCE INDEX (`good_sharding_index`) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)
}

func (t *CopyFilterTestSuite) TestHigherSpecificityOfIndexHintingPerTable() {
	t.filter.IndexConfigPerTable = map[string]sharding.IndexConfigPerTable{
		"normaltable": {
			IndexHint: "USE", // also testing it should work when the index hint is not lowercase.
		},
	}

	t.filter.IndexHint = "none"

	selectBuilder1, err := t.filter.BuildSelect([]string{"*"}, t.normalTable, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder1.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` JOIN (SELECT `id` FROM `shard_1`.`normaltable` USE INDEX (`good_sharding_index`) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)

	selectBuilder2, err := t.filter.BuildSelect([]string{"*"}, t.normalTable2, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err = selectBuilder2.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable2` JOIN (SELECT `id` FROM `shard_1`.`normaltable2` WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)
}

func (t *CopyFilterTestSuite) TestHigherSpecificityOfIndexHintingPerTable2() {
	t.filter.IndexConfigPerTable = map[string]sharding.IndexConfigPerTable{
		"normaltable": {
			IndexHint: "none",
		},
	}

	t.filter.IndexHint = "force"

	selectBuilder1, err := t.filter.BuildSelect([]string{"*"}, t.normalTable, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder1.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` JOIN (SELECT `id` FROM `shard_1`.`normaltable` WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)

	selectBuilder2, err := t.filter.BuildSelect([]string{"*"}, t.normalTable2, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err = selectBuilder2.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable2` JOIN (SELECT `id` FROM `shard_1`.`normaltable2` FORCE INDEX (`good_sharding_index`) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)
}

func (t *CopyFilterTestSuite) TestIndexHintingPerTableWithNonExistentIndex() {
	t.filter.IndexConfigPerTable = map[string]sharding.IndexConfigPerTable{
		"normaltable": {
			IndexHint: "none",                   // will override IndexHint from the higher level i.e. "force"
			IndexName: "another_sharding_index", // will be ignored when IndexHint is "none"
		},
		"normaltable2": {
			IndexName: "another_sharding_index", // ignored because it doesn't exist on the table
			// will inherit IndexHint from the higher level i.e. "force"
		},
	}

	t.filter.IndexHint = "force"

	selectBuilder1, err := t.filter.BuildSelect([]string{"*"}, t.normalTable, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder1.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` JOIN (SELECT `id` FROM `shard_1`.`normaltable` WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)

	selectBuilder2, err := t.filter.BuildSelect([]string{"*"}, t.normalTable2, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err = selectBuilder2.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable2` JOIN (SELECT `id` FROM `shard_1`.`normaltable2` FORCE INDEX (`good_sharding_index`) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)
}

func (t *CopyFilterTestSuite) TestIndexHintingPerTableWithIndexOnTable() {
	t.filter.IndexConfigPerTable = map[string]sharding.IndexConfigPerTable{
		"normaltable": {
			IndexName: "less_good_sharding_index",
		},
	}

	t.filter.IndexHint = "force"

	selectBuilder1, err := t.filter.BuildSelect([]string{"*"}, t.normalTable, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder1.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` JOIN (SELECT `id` FROM `shard_1`.`normaltable` FORCE INDEX (`less_good_sharding_index`) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)
}

func (t *CopyFilterTestSuite) TestSelectsJoinedTables() {
	selectBuilder, err := t.filter.BuildSelect([]string{"*"}, t.joinedTable, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`joinedtable` WHERE `joined_paginationKey` IN (SELECT * FROM (SELECT `joined_paginationKey1` AS sharding_join_alias FROM `shard_1`.`join1` WHERE `tenant_id` = ? AND `joined_paginationKey1` > ? UNION DISTINCT SELECT `joined_paginationKey2` AS sharding_join_alias FROM `shard_1`.`join2` WHERE `tenant_id` = ? AND `joined_paginationKey2` > ? ORDER BY sharding_join_alias LIMIT 1024) AS sharding_join_table) ORDER BY `joined_paginationKey`", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor, t.shardingValue, t.paginationKeyCursor}, args)
}

func (t *CopyFilterTestSuite) TestSelectsPrimaryKeyTables() {
	selectBuilder, err := t.filter.BuildSelect([]string{"*"}, t.primaryKeyTable, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`pkTable` USE INDEX (PRIMARY) WHERE `tenant_id` = ? AND `tenant_id` > ?", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)
}

func (t *CopyFilterTestSuite) TestSkipsInvisibleIndexes() {
	// Make the good index invisible, should fall back to less good index
	t.normalTable.Indexes[2].Visible = false
	selectBuilder, err := t.filter.BuildSelect([]string{"*"}, t.normalTable, t.paginationKeyCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` JOIN (SELECT `id` FROM `shard_1`.`normaltable` USE INDEX (`less_good_sharding_index`) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.paginationKeyCursor}, args)
}

func (t *CopyFilterTestSuite) TestShardingValueTypes() {
	tenantIds := []interface{}{
		uint64(1), uint32(1), uint16(1), uint8(1), uint(1),
		int64(1), int32(1), int16(1), int8(1), int(1),
	}

	eventBase := ghostferry.NewDMLEventBase(
		t.normalTable,
		mysql.Position{},
		mysql.Position{},
		nil,
		time.Unix(1618318965, 0),
	)

	for _, tenantId := range tenantIds {
		dmlEvents, _ := ghostferry.NewBinlogInsertEvents(eventBase, t.newRowsEvent([]interface{}{1001, tenantId, "data"}))
		applicable, err := t.filter.ApplicableEvent(dmlEvents[0])
		t.Require().Nil(err)
		t.Require().True(applicable, fmt.Sprintf("value %t wasn't applicable", tenantId))
	}
}

func (t *CopyFilterTestSuite) TestInvalidShardingValueTypesErrors() {
	eventBase := ghostferry.NewDMLEventBase(
		t.normalTable,
		mysql.Position{},
		mysql.Position{},
		nil,
		time.Unix(1618318965, 0),
	)

	dmlEvents, err := ghostferry.NewBinlogInsertEvents(eventBase, t.newRowsEvent([]interface{}{1001, string("1"), "data"}))
	_, err = t.filter.ApplicableEvent(dmlEvents[0])
	t.Require().Equal("parsing new sharding key: invalid type %!t(string=1)", err.Error())
}

func (t *CopyFilterTestSuite) newRowsEvent(rowData []interface{}) *replication.RowsEvent {
	normalTableMapEvent := &replication.TableMapEvent{
		Schema: []byte(t.normalTable.Schema),
		Table:  []byte(t.normalTable.Name),
	}

	rowsEvent := &replication.RowsEvent{
		Table: normalTableMapEvent,
		Rows:  [][]interface{}{rowData},
	}

	return rowsEvent
}

func TestCopyFilterTestSuite(t *testing.T) {
	suite.Run(t, &CopyFilterTestSuite{})
}
