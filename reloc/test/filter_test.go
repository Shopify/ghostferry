package test

import (
	"testing"

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
