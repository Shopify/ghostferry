package test

import (
	"regexp"
	"testing"

	"github.com/Shopify/ghostferry/reloc"
	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/assert"
)

func TestShardedTableFilterSelectsSingleDatabase(t *testing.T) {
	filter := &reloc.ShardedTableFilter{SourceShard: "shard_42", ShardingKey: "tenant_id"}
	applicable, err := filter.ApplicableDatabases([]string{"shard_41", "shard_42", "shard_43"})
	assert.Nil(t, err)
	assert.Equal(t, []string{"shard_42"}, applicable)

	applicable, err = filter.ApplicableDatabases(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"shard_42"}, applicable)
}

func TestShardedTableFilterRejectsIgnoredTables(t *testing.T) {
	filter := &reloc.ShardedTableFilter{
		SourceShard: "shard_42",
		ShardingKey: "tenant_id",
		IgnoredTables: []*regexp.Regexp{
			regexp.MustCompile("^_(.*)_new$"),
			regexp.MustCompile("^_(.*)_old$"),
			regexp.MustCompile("^lhm._(.*)"),
			regexp.MustCompile("^_(.*)_gho$"),
		},
	}

	tables := []*schema.Table{
		{Schema: "shard_42", Name: "_table_name_new", Columns: []schema.TableColumn{{Name: "id"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "_table_name_old", Columns: []schema.TableColumn{{Name: "id"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "_table_name_gho", Columns: []schema.TableColumn{{Name: "id"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "lhma_1234_table_name", Columns: []schema.TableColumn{{Name: "id"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "lhmn_1234_table_name", Columns: []schema.TableColumn{{Name: "id"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "new", Columns: []schema.TableColumn{{Name: "id"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "old", Columns: []schema.TableColumn{{Name: "id"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "table_new", Columns: []schema.TableColumn{{Name: "foo"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "ghost", Columns: []schema.TableColumn{{Name: "foo"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "lhm_test", Columns: []schema.TableColumn{{Name: "foo"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "x_lhmn_table_name", Columns: []schema.TableColumn{{Name: "bar"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "table_name", Columns: []schema.TableColumn{{Name: "bar"}, {Name: "tenant_id"}}},
	}

	applicable, err := filter.ApplicableTables(tables)
	assert.Nil(t, err)
	assert.Equal(t, tables[5:], applicable)
}

func TestShardedTableFilterSelectsTablesWithShardingKey(t *testing.T) {
	filter := &reloc.ShardedTableFilter{SourceShard: "shard_42", ShardingKey: "tenant_id"}

	tables := []*schema.Table{
		{Schema: "shard_42", Name: "table1", Columns: []schema.TableColumn{{Name: "id"}}},
		{Schema: "shard_42", Name: "table2", Columns: []schema.TableColumn{{Name: "id"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "table3", Columns: []schema.TableColumn{{Name: "foo"}, {Name: "tenant_id"}}},
		{Schema: "shard_42", Name: "table4", Columns: []schema.TableColumn{{Name: "bar"}}},
	}

	applicable, err := filter.ApplicableTables(tables)
	assert.Nil(t, err)
	assert.Equal(t, tables[1:3], applicable)
}

func TestShardedTableFilterSelectsJoinedTables(t *testing.T) {
	filter := &reloc.ShardedTableFilter{
		SourceShard:  "shard_42",
		ShardingKey:  "tenant_id",
		JoinedTables: map[string][]reloc.JoinTable{"table2": nil},
	}

	tables := []*schema.Table{
		{Schema: "shard_42", Name: "table1", Columns: []schema.TableColumn{{Name: "id"}}},
		{Schema: "shard_42", Name: "table2", Columns: []schema.TableColumn{{Name: "id"}}},
	}

	applicable, err := filter.ApplicableTables(tables)
	assert.Nil(t, err)
	assert.Equal(t, tables[1:], applicable)
}

func TestShardedRowFilterSupportsJoinedTables(t *testing.T) {
	shardingValue := int64(1)
	pkCursor := uint64(12345)

	filter := &reloc.ShardedRowFilter{
		ShardingKey:   "tenant_id",
		ShardingValue: shardingValue,

		JoinedTables: map[string][]reloc.JoinTable{
			"joined": []reloc.JoinTable{
				{TableName: "join1", JoinColumn: "joined_pk1"},
				{TableName: "join2", JoinColumn: "joined_pk2"},
			},
		},
	}

	table := &schema.Table{
		Schema:    "shard_1",
		Name:      "joined",
		Columns:   []schema.TableColumn{{Name: "joined_pk"}},
		PKColumns: []int{0},
	}

	selectBuilder, err := filter.BuildSelect(table, pkCursor, 1024)
	assert.Nil(t, err)

	sql, args, err := selectBuilder.ToSql()
	assert.Nil(t, err)
	assert.Equal(t, "SELECT * FROM `shard_1`.`joined` WHERE `joined_pk` IN (SELECT * FROM (SELECT `joined_pk1` AS reloc_join_alias FROM `shard_1`.`join1` WHERE `tenant_id` = ? AND `joined_pk1` > ? UNION DISTINCT SELECT `joined_pk2` AS reloc_join_alias FROM `shard_1`.`join2` WHERE `tenant_id` = ? AND `joined_pk2` > ? ORDER BY reloc_join_alias LIMIT 1024) AS reloc_join_table) ORDER BY `joined_pk`", sql)
	assert.Equal(t, []interface{}{shardingValue, pkCursor, shardingValue, pkCursor}, args)
}
