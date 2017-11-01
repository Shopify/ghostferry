package copydb

import (
	"fmt"

	"github.com/Shopify/ghostferry"
)

// With nothing specified, it assumes that everything is applicable.
type FiltersRenames struct {
	Whitelist []string
	Blacklist []string
	Renames   map[string]string
}

func (f FiltersRenames) Validate() error {
	if len(f.Whitelist) > 0 && len(f.Blacklist) > 0 {
		return fmt.Errorf("Whitelist and Blacklist cannot both be specified")
	}

	return nil
}

type Config struct {
	ghostferry.Config

	Databases FiltersRenames
	Tables    FiltersRenames
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

	c.DatabaseRenames = c.Databases.Renames
	c.TableRenames = c.Tables.Renames

	if err := c.Config.ValidateConfig(); err != nil {
		return err
	}

	return nil
}
