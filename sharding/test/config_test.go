package test

import (
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/sharding"
	"github.com/stretchr/testify/assert"
)

func TestConfigWithBothIncludedAndIgnoredTablesIsInvalid(t *testing.T) {
	config := sharding.Config{
		IncludedTables: []string{"table_one", "table_two"},
		IgnoredTables:  []string{"table_one", "table_two"},
	}

	err := config.ValidateConfig()
	assert.EqualError(t, err, "IgnoredTables and IncludedTables cannot be defined at the same time.")
}

func TestConfigWithRunFerryFromReplicaWithoutValidSourceReplicationMasterIsInvalid(t *testing.T) {
	config := sharding.Config{
		RunFerryFromReplica:     true,
		SourceReplicationMaster: &ghostferry.DatabaseConfig{},
	}

	err := config.ValidateConfig()
	assert.EqualError(t, err, "host is empty")
}

func TestConfigWithInvalidIndexHintIsInvalid(t *testing.T) {
	config := sharding.Config{
		ShardedCopyFilterConfig: &sharding.ShardedCopyFilterConfig{
			IndexHint: "invalid",
		},
	}

	err := config.ValidateConfig()
	assert.EqualError(t, err, "IndexHint must be one of: none, force, use")
}

func TestConfigWithInvalidIndexHintForTableIsInvalid(t *testing.T) {
	config := sharding.Config{
		ShardedCopyFilterConfig: &sharding.ShardedCopyFilterConfig{
			IndexHintingPerTable: map[string]map[string]sharding.IndexConfigPerTable{
				"schema_one": {
					"table_one": {
						IndexHint: "invalid",
					},
				},
			},
		},
	}

	err := config.ValidateConfig()
	assert.EqualError(t, err, "For table table_one, IndexHint must be one of: none, force, use")
}

func TestIndexConfigPerTableWithValues(t *testing.T) {
	config := sharding.Config{
		ShardedCopyFilterConfig: &sharding.ShardedCopyFilterConfig{
			IndexHintingPerTable: map[string]map[string]sharding.IndexConfigPerTable{
				"schema_one": {
					"table_one": {
						IndexHint: "force",
						IndexName: "ix_table_one_id",
					},
				},
			},
		},
	}

	indexConfig := config.ShardedCopyFilterConfig.IndexConfigForTables("schema_one")
	assert.Equal(t, indexConfig["table_one"].IndexHint, "force")
	assert.Equal(t, indexConfig["table_one"].IndexName, "ix_table_one_id")
}

func TestIndexConfigPerTableWithNoValues(t *testing.T) {
	config := sharding.Config{
		ShardedCopyFilterConfig: &sharding.ShardedCopyFilterConfig{
			IndexHintingPerTable: map[string]map[string]sharding.IndexConfigPerTable{},
		},
	}

	indexConfig := config.ShardedCopyFilterConfig.IndexConfigForTables("schema_one")
	assert.Equal(t, indexConfig["table_one"].IndexHint, "")
	assert.Equal(t, indexConfig["table_one"].IndexName, "")
}
