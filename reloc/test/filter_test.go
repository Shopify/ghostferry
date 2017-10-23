package test

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/reloc"
	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/assert"
)

func TestShardedTableFilterSelectsSingleDatabase(t *testing.T) {
	filter := &reloc.ShardedTableFilter{SourceShard: "shard_42", ShardingKey: "tenant_id"}
	applicable := filter.ApplicableDatabases([]string{"shard_41", "shard_42", "shard_43"})
	assert.Equal(t, []string{"shard_42"}, applicable)

	applicable = filter.ApplicableDatabases(nil)
	assert.Equal(t, []string{"shard_42"}, applicable)
}

func TestShardedTableFilterSelectsTablesWithShardingKey(t *testing.T) {
	filter := &reloc.ShardedTableFilter{SourceShard: "shard_42", ShardingKey: "tenant_id"}

	tables := []*schema.Table{
		{Schema: "shard_42", Name: "table1", Columns: []schema.TableColumn{{Name: "id"}}},
		{Schema: "shard_42", Name: "table2", Columns: []schema.TableColumn{{Name: "id"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "table3", Columns: []schema.TableColumn{{Name: "foo"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "table4", Columns: []schema.TableColumn{{Name: "bar"}}},
	}

	applicable := filter.ApplicableTables(tables)
	assert.Equal(t, tables[1:3], applicable)
}

func TestShardedRowFilterSupportsJoinedTables(t *testing.T) {
	shardingValue := int64(1)
	pkCursor := uint64(12345)

	filter := &reloc.ShardedRowFilter{
		ShardingKey:   "tenant_id",
		ShardingValue: shardingValue,

		JoinedTables: map[string][]reloc.JoinTable{
			"joined": []reloc.JoinTable{
				{Name: "join1", Column: "joined_pk1"},
				{Name: "join2", Column: "joined_pk2"},
			},
		},
	}

	table := &schema.Table{
		Schema:    "shard_1",
		Name:      "joined",
		Columns:   []schema.TableColumn{{Name: "joined_pk"}},
		PKColumns: []int{0},
	}

	cond, err := filter.ConstrainSelect(table, pkCursor, 1024)
	assert.Nil(t, err)

	selectBuilder := sq.Select("*").
		From(ghostferry.QuotedTableName(table)).
		Limit(1024).
		OrderBy("`joined_pk`").
		Suffix("FOR UPDATE")

	sql, args, err := selectBuilder.Where(cond).ToSql()
	assert.Nil(t, err)
	assert.Equal(t, "SELECT * FROM `shard_1`.`joined` WHERE `joined_pk` IN (SELECT * FROM (SELECT `joined_pk1` AS reloc_join_alias FROM `shard_1`.`join1` WHERE `tenant_id` = ? AND `joined_pk1` > ? UNION DISTINCT SELECT `joined_pk2` AS reloc_join_alias FROM `shard_1`.`join2` WHERE `tenant_id` = ? AND `joined_pk2` > ? ORDER BY reloc_join_alias LIMIT 1024) AS reloc_join_table) ORDER BY `joined_pk` LIMIT 1024 FOR UPDATE", sql)
	assert.Equal(t, []interface{}{shardingValue, pkCursor, shardingValue, pkCursor}, args)
}
