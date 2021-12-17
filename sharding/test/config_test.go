package test

import (
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/sharding"
	"github.com/stretchr/testify/assert"
)

func TestConfigWithBothIncludedAndIgnoredTablesIsInvalid(t *testing.T)  {
	config := sharding.Config{
		IncludedTables: []string{"table_one", "table_two"},
		IgnoredTables:  []string{"table_one", "table_two"},
	}

	err := config.ValidateConfig()
	assert.EqualError(t, err, "IgnoredTables and IncludedTables cannot be defined at the same time.")
}


func TestConfigWithRunFerryFromReplicaWithoutValidSourceReplicationMasterIsInvalid(t *testing.T)  {
	config := sharding.Config{
		RunFerryFromReplica: true,
		SourceReplicationMaster: &ghostferry.DatabaseConfig{},
	}

	err := config.ValidateConfig()
	assert.EqualError(t, err, "host is empty")
}
