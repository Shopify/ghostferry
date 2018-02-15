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

type Config struct {
	*ghostferry.Config

	Databases FilterAndRewriteConfigs
	Tables    FilterAndRewriteConfigs
	EnableIterativeVerifier bool
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

	if err := c.Config.ValidateConfig(); err != nil {
		return err
	}

	return nil
}
