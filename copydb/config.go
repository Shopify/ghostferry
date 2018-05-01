package copydb

import (
	"fmt"

	"github.com/Shopify/ghostferry"
)

// Whitelist and blacklisting things to copy.
// Also allows to rename things during the copy process.
//
// If this is empty for a filter, it means to not filter for anything and thus
// whitelist everything.
type FilterAndRewriteConfigs struct {
	// Whitelisted databases/tables. Mutually exclusive with Blacklist.
	Whitelist []string

	// Blacklisted databases/tables. Mutually exclusive with Whitelist.
	Blacklist []string

	// Allows database/tables to be renamed from source to the target, where they
	// key of this struct is the name on the source database and the value of the
	// struct is on the target database.
	Rewrites map[string]string
}

func (f FilterAndRewriteConfigs) Validate() error {
	if len(f.Whitelist) > 0 && len(f.Blacklist) > 0 {
		return fmt.Errorf("Whitelist and Blacklist cannot both be specified")
	}

	return nil
}

const (
	VerifierTypeChecksumTable  = "ChecksumTable"
	VerifierTypeIterative      = "Iterative"
	VerifierTypeNoVerification = "NoVerification"
)

var validVerifierTypes map[string]struct{} = map[string]struct{}{
	VerifierTypeChecksumTable:  struct{}{},
	VerifierTypeIterative:      struct{}{},
	VerifierTypeNoVerification: struct{}{},
}

type Config struct {
	*ghostferry.Config

	// Filter configuration for databases to copy
	Databases FilterAndRewriteConfigs

	// Filter configuration for tables to copy
	Tables FilterAndRewriteConfigs

	// The verifier to use during the run. Valid choices are:
	// ChecksumTable
	// Iterative
	// NoVerification
	VerifierType string
}

func (c *Config) InitializeAndValidateConfig() error {
	if _, valid := validVerifierTypes[c.VerifierType]; !valid {
		return fmt.Errorf("'%s' is not a valid VerifierType", c.VerifierType)
	}

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

	if err := c.Config.ValidateConfig(); err != nil {
		return err
	}

	return nil
}
