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
