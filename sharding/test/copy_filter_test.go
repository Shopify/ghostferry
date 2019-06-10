package test

import (
	"fmt"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/sharding"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/suite"
)

type CopyFilterTestSuite struct {
	suite.Suite

	shardingValue int64
	pkCursor      uint64

	normalTable, joinedTable, pkTable *ghostferry.TableSchema

	filter *sharding.ShardedCopyFilter
}

func (t *CopyFilterTestSuite) SetupTest() {
	t.shardingValue = int64(1)
	t.pkCursor = uint64(12345)

	t.normalTable = &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:    "shard_1",
			Name:      "normaltable",
			Columns:   []schema.TableColumn{{Name: "id"}, {Name: "tenant_id"}, {Name: "data"}},
			PKColumns: []int{0},
			Indexes: []*schema.Index{
				{Name: "unrelated_index", Columns: []string{"tenant_id", "data"}},
				{Name: "less_good_sharding_index", Columns: []string{"tenant_id"}},
				{Name: "good_sharding_index", Columns: []string{"tenant_id", "id"}},
				{Name: "unrelated_index2", Columns: []string{"data"}},
			},
		},
	}

	t.joinedTable = &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:    "shard_1",
			Name:      "joinedtable",
			Columns:   []schema.TableColumn{{Name: "joined_pk"}},
			PKColumns: []int{0},
		},
	}

	t.pkTable = &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:    "shard_1",
			Name:      "pktable",
			Columns:   []schema.TableColumn{{Name: "tenant_id"}},
			PKColumns: []int{0},
		},
	}

	t.filter = &sharding.ShardedCopyFilter{
		ShardingKey:   "tenant_id",
		ShardingValue: t.shardingValue,

		JoinedTables: map[string][]sharding.JoinTable{
			"joinedtable": []sharding.JoinTable{
				{TableName: "join1", JoinColumn: "joined_pk1"},
				{TableName: "join2", JoinColumn: "joined_pk2"},
			},
		},

		PrimaryKeyTables: map[string]struct{}{"pktable": struct{}{}},
	}
}

func (t *CopyFilterTestSuite) TestSelectsRegularTables() {
	selectBuilder, err := t.filter.BuildSelect([]string{"*"}, t.normalTable, t.pkCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` JOIN (SELECT `id` FROM `shard_1`.`normaltable` USE INDEX (`good_sharding_index`) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.pkCursor}, args)
}

func (t *CopyFilterTestSuite) TestFallsBackToLessGoodIndex() {
	t.normalTable.Indexes[2].Columns = []string{"data"} // Remove good index.
	selectBuilder, err := t.filter.BuildSelect([]string{"*"}, t.normalTable, t.pkCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` JOIN (SELECT `id` FROM `shard_1`.`normaltable` USE INDEX (`less_good_sharding_index`) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.pkCursor}, args)
}

func (t *CopyFilterTestSuite) TestFallsBackToIgnoredPrimaryIndex() {
	t.normalTable.Indexes[1].Columns = []string{"data"} // Remove less good index.
	t.normalTable.Indexes[2].Columns = []string{"data"} // Remove good index.
	selectBuilder, err := t.filter.BuildSelect([]string{"*"}, t.normalTable, t.pkCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` JOIN (SELECT `id` FROM `shard_1`.`normaltable` IGNORE INDEX (PRIMARY) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024) AS `batch` USING(`id`)", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.pkCursor}, args)
}

func (t *CopyFilterTestSuite) TestSelectsJoinedTables() {
	selectBuilder, err := t.filter.BuildSelect([]string{"*"}, t.joinedTable, t.pkCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`joinedtable` WHERE `joined_pk` IN (SELECT * FROM (SELECT `joined_pk1` AS sharding_join_alias FROM `shard_1`.`join1` WHERE `tenant_id` = ? AND `joined_pk1` > ? UNION DISTINCT SELECT `joined_pk2` AS sharding_join_alias FROM `shard_1`.`join2` WHERE `tenant_id` = ? AND `joined_pk2` > ? ORDER BY sharding_join_alias LIMIT 1024) AS sharding_join_table) ORDER BY `joined_pk`", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.pkCursor, t.shardingValue, t.pkCursor}, args)
}

func (t *CopyFilterTestSuite) TestSelectsPrimaryKeyTables() {
	selectBuilder, err := t.filter.BuildSelect([]string{"*"}, t.pkTable, t.pkCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`pktable` USE INDEX (PRIMARY) WHERE `tenant_id` = ? AND `tenant_id` > ?", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.pkCursor}, args)
}

func (t *CopyFilterTestSuite) TestShardingValueTypes() {
	tenantIds := []interface{}{
		uint64(1), uint32(1), uint16(1), uint8(1), uint(1),
		int64(1), int32(1), int16(1), int8(1), int(1),
	}

	for _, tenantId := range tenantIds {
		dmlEvents, _ := ghostferry.NewBinlogInsertEvents(t.normalTable, t.newRowsEvent([]interface{}{1001, tenantId, "data"}), mysql.Position{})
		applicable, err := t.filter.ApplicableEvent(dmlEvents[0])
		t.Require().Nil(err)
		t.Require().True(applicable, fmt.Sprintf("value %t wasn't applicable", tenantId))
	}
}

func (t *CopyFilterTestSuite) TestInvalidShardingValueTypesErrors() {
	dmlEvents, err := ghostferry.NewBinlogInsertEvents(t.normalTable, t.newRowsEvent([]interface{}{1001, string("1"), "data"}), mysql.Position{})
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
