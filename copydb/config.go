package copydb

import (
	"fmt"

	"github.com/Shopify/ghostferry"
)

// With nothing specified, it assumes that everything is applicable.
type FilterAndRewriteConfigs struct {
	Whitelist []string
	Blacklist []string
	Rewrites  map[string]string
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

	Databases    FilterAndRewriteConfigs
	Tables       FilterAndRewriteConfigs
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
