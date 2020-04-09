package test

import (
	"fmt"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/sharding"

	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/suite"
)

type CopyFilterTestSuite struct {
	suite.Suite

	shardingValue       int64
	paginationKeyCursor uint64

	normalTable, joinedTable, primaryKeyTable *ghostferry.TableSchema

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
				{Name: "unrelated_index", Columns: []string{"tenant_id", "data"}},
				{Name: "less_good_sharding_index", Columns: []string{"tenant_id"}},
				{Name: "good_sharding_index", Columns: []string{"tenant_id", "id"}},
				{Name: "unrelated_index2", Columns: []string{"data"}},
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

func (t *CopyFilterTestSuite) TestShardingValueTypes() {
	tenantIds := []interface{}{
		uint64(1), uint32(1), uint16(1), uint8(1), uint(1),
		int64(1), int32(1), int16(1), int8(1), int(1),
	}

	for _, tenantId := range tenantIds {
		dmlEvents, _ := ghostferry.NewBinlogInsertEvents(t.normalTable, t.newRowsEvent([]interface{}{1001, tenantId, "data"}), ghostferry.BinlogPosition{})
		applicable, err := t.filter.ApplicableEvent(dmlEvents[0])
		t.Require().Nil(err)
		t.Require().True(applicable, fmt.Sprintf("value %t wasn't applicable", tenantId))
	}
}

func (t *CopyFilterTestSuite) TestInvalidShardingValueTypesErrors() {
	dmlEvents, err := ghostferry.NewBinlogInsertEvents(t.normalTable, t.newRowsEvent([]interface{}{1001, string("1"), "data"}), ghostferry.BinlogPosition{})
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
