package test

import (
	"testing"

	"github.com/Shopify/ghostferry/reloc"

	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/suite"
)

type CopyFilterTestSuite struct {
	suite.Suite

	shardingValue int64
	pkCursor      uint64

	normalTable, joinedTable, pkTable *schema.Table

	filter *reloc.ShardedCopyFilter
}

func (t *CopyFilterTestSuite) SetupTest() {
	t.shardingValue = int64(1)
	t.pkCursor = uint64(12345)

	t.normalTable = &schema.Table{
		Schema:    "shard_1",
		Name:      "normaltable",
		Columns:   []schema.TableColumn{{Name: "id"}, {Name: "tenant_id"}},
		PKColumns: []int{0},
		Indexes:   []*schema.Index{{Name: "sharding_index", Columns: []string{"tenant_id", "id"}}},
	}

	t.joinedTable = &schema.Table{
		Schema:    "shard_1",
		Name:      "joinedtable",
		Columns:   []schema.TableColumn{{Name: "joined_pk"}},
		PKColumns: []int{0},
	}

	t.pkTable = &schema.Table{
		Schema:    "shard_1",
		Name:      "pktable",
		Columns:   []schema.TableColumn{{Name: "tenant_id"}},
		PKColumns: []int{0},
	}

	t.filter = &reloc.ShardedCopyFilter{
		ShardingKey:   "tenant_id",
		ShardingValue: t.shardingValue,

		JoinedTables: map[string][]reloc.JoinTable{
			"joinedtable": []reloc.JoinTable{
				{TableName: "join1", JoinColumn: "joined_pk1"},
				{TableName: "join2", JoinColumn: "joined_pk2"},
			},
		},

		PrimaryKeyTables: map[string]struct{}{"pktable": struct{}{}},
	}
}

func (t *CopyFilterTestSuite) TestSelectsRegularTables() {
	selectBuilder, err := t.filter.BuildSelect(t.normalTable, t.pkCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` USE INDEX (`sharding_index`) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.pkCursor}, args)
}

func (t *CopyFilterTestSuite) TestFallsBackToIgnoredPrimaryIndex() {
	t.normalTable.Indexes = nil
	selectBuilder, err := t.filter.BuildSelect(t.normalTable, t.pkCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`normaltable` IGNORE INDEX (PRIMARY) WHERE `tenant_id` = ? AND `id` > ? ORDER BY `id` LIMIT 1024", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.pkCursor}, args)
}

func (t *CopyFilterTestSuite) TestSelectsJoinedTables() {
	selectBuilder, err := t.filter.BuildSelect(t.joinedTable, t.pkCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`joinedtable` WHERE `joined_pk` IN (SELECT * FROM (SELECT `joined_pk1` AS reloc_join_alias FROM `shard_1`.`join1` WHERE `tenant_id` = ? AND `joined_pk1` > ? UNION DISTINCT SELECT `joined_pk2` AS reloc_join_alias FROM `shard_1`.`join2` WHERE `tenant_id` = ? AND `joined_pk2` > ? ORDER BY reloc_join_alias LIMIT 1024) AS reloc_join_table) ORDER BY `joined_pk`", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.pkCursor, t.shardingValue, t.pkCursor}, args)
}

func (t *CopyFilterTestSuite) TestSelectsPrimaryKeyTables() {
	selectBuilder, err := t.filter.BuildSelect(t.pkTable, t.pkCursor, 1024)
	t.Require().Nil(err)

	sql, args, err := selectBuilder.ToSql()
	t.Require().Nil(err)
	t.Require().Equal("SELECT * FROM `shard_1`.`pktable` USE INDEX (PRIMARY) WHERE `tenant_id` = ? AND `tenant_id` > ?", sql)
	t.Require().Equal([]interface{}{t.shardingValue, t.pkCursor}, args)
}

func TestCopyFilterTestSuite(t *testing.T) {
	suite.Run(t, &CopyFilterTestSuite{})
}
