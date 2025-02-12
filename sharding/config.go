package sharding

import (
	"fmt"
	"strings"

	"github.com/Shopify/ghostferry"
)

type Config struct {
	*ghostferry.Config

	ShardingKey   string
	ShardingValue int64
	SourceDB      string
	TargetDB      string

	SourceReplicationMaster       *ghostferry.DatabaseConfig
	ReplicatedMasterPositionQuery string
	RunFerryFromReplica           bool

	StatsDAddress string

	JoinedTables map[string][]JoinTable

	// IgnoredTables and IncludedTables are mutually exclusive. Specifying both is an error.
	IgnoredTables  []string
	IncludedTables []string

	PrimaryKeyTables []string

	Throttle *ghostferry.LagThrottlerConfig

	// ShardedCopyFilterConfig is used to configure the sharded copy filter query.
	ShardedCopyFilterConfig *ShardedCopyFilterConfig
}

func (c *Config) ValidateConfig() error {
	if len(c.IgnoredTables) != 0 && len(c.IncludedTables) != 0 {
		return fmt.Errorf("IgnoredTables and IncludedTables cannot be defined at the same time.")
	}

	if c.RunFerryFromReplica && c.SourceReplicationMaster != nil {
		if err := c.SourceReplicationMaster.Validate(); err != nil {
			return err
		}
	}

	if err := c.ShardedCopyFilterConfig.Validate(); err != nil {
		return err
	}

	return c.Config.ValidateConfig()
}

type ShardedCopyFilterConfig struct {
	// Ghostferry requires an index to be present on sharding key to improve the performance of the data iterator's query.

	// `use` is used by default as part of the index hint i.e. `USE INDEX`.
	// `USE INDEX` is taken as a suggestion and optimizer can still opt in to use full table scan if it thinks it's faster.
	//
	// `force` replaces `USE INDEX` with `FORCE INDEX` on the query.
	// `FORCE INDEX` will force the optimizer to use the index and will not consider other options like full table scan.
	//
	// `none` will not use any index hint.

	// See https://dev.mysql.com/doc/refman/8.0/en/index-hints.html for more information on index hints.
	IndexHint string // none or force or use

	// IndexHintingPerTable has greatest specificity and takes precedence over the other options if specified.
	// Otherwise it will inherit the higher level IndexHint.
	// IndexName option is ignored if IndexHint is "none".
	// IndexName option is ignored if it is not an index on the table.
	//
	// example:
	// IndexHintingPerTable: {
	//   "blog": {
	//     "users": {
	//       "IndexHint": "force",
	//       "IndexName": "ix_users_some_id"
	//     }
	//   }
	// }
	IndexHintingPerTable map[string]map[string]IndexConfigPerTable
}

// SchemaName => TableName => IndexConfig
func (c *ShardedCopyFilterConfig) IndexConfigForTables(schemaName string) map[string]IndexConfigPerTable {
	tableConfig, found := c.IndexHintingPerTable[schemaName]
	if !found {
		return nil
	}

	return tableConfig
}

func (c *ShardedCopyFilterConfig) Validate() error {
	if c.IndexHint == "" {
		c.IndexHint = "use"
	}

	indexHint := strings.ToLower(c.IndexHint)

	if indexHint != "none" && indexHint != "force" && indexHint != "use" {
		return fmt.Errorf("IndexHint must be one of: none, force, use")
	}

	if c.IndexHintingPerTable != nil {
		for _, tableConfig := range c.IndexHintingPerTable {
			for tableName, indexConfig := range tableConfig {
				tableIndexHint := strings.ToLower(indexConfig.IndexHint)
				if tableIndexHint != "" && tableIndexHint != "none" && tableIndexHint != "force" && tableIndexHint != "use" {
					return fmt.Errorf("For table %s, IndexHint must be one of: none, force, use", tableName)
				}
			}
		}
	}

	return nil
}
