package copydb

import (
	"fmt"
	"time"

	"github.com/Shopify/ghostferry"
)

// Whitelisting and blacklisting databases/tables to copy.
// Also allows to rename databases/tables during the copy process.
//
// If this is empty for a filter, it means to not filter for anything and thus
// whitelist everything.
type FilterAndRewriteConfigs struct {
	// Whitelisted databases/tables. Mutually exclusive with Blacklist as it will
	// result in an error.
	Whitelist []string

	// Blacklisted databases/tables. Mutually exclusive with Whitelist as it will
	// result in an error.
	Blacklist []string

	// Allows database/tables to be renamed from source to the target, where the
	// key of this map is the database/table names on the source database and the
	// value of the map is on the database/table names target database.
	Rewrites map[string]string
}

func (f FilterAndRewriteConfigs) Validate() error {
	if len(f.Whitelist) > 0 && len(f.Blacklist) > 0 {
		return fmt.Errorf("Whitelist and Blacklist cannot both be specified")
	}

	return nil
}

type Config struct {
	*ghostferry.Config

	// Filter configuration for databases to copy
	Databases FilterAndRewriteConfigs

	// Filter configuration for tables to copy
	Tables FilterAndRewriteConfigs

	// Specifies the order in which to create database tables as <db>.<table> .
	// Names refer to original databases and tables (that is, before renaming
	// occurs).
	// If a table is to be created on start and appears in this list, it is
	// created before any other table, and is created in the order listed here.
	// All tables not specified in this list are created in arbitrary order.
	TablesToBeCreatedFirst []string

	// If you're running Ghostferry from a read only replica, turn this option
	// on and specify SourceReplicationMaster and ReplicatedMasterPositionQuery.
	RunFerryFromReplica bool

	// This is the configuration to connect to the master writer of the source DB.
	// This is only used if the source db is a replica and RunFerryFromReplica
	// is on.
	SourceReplicationMaster *ghostferry.DatabaseConfig

	// This is the SQL query used to read the position of the master binlog that
	// has been replicated to the Source. As an example, you can query the
	// pt-heartbeat table:
	//
	// SELECT file, position FROM meta.ptheartbeat WHERE server_id = master_server_id
	ReplicatedMasterPositionQuery string

	// The duration to wait for the replication to catchup before aborting. Only use if RunFerryFromReplica is true.
	WaitForReplicationTimeout string
}

func (c *Config) InitializeAndValidateConfig() error {
	if err := c.Databases.Validate(); err != nil {
		return err
	}

	if err := c.Tables.Validate(); err != nil {
		return err
	}

	c.TableFilter = NewStaticTableFilter(
		c.Databases,
		c.Tables,
	)

	c.DatabaseRewrites = c.Databases.Rewrites
	c.TableRewrites = c.Tables.Rewrites

	if c.WaitForReplicationTimeout != "" {
		_, err := time.ParseDuration(c.WaitForReplicationTimeout)
		if err != nil {
			return err
		}
	}

	if err := c.Config.ValidateConfig(); err != nil {
		return err
	}

	return nil
}
