package copydb

import (
	"fmt"

	"github.com/Shopify/ghostferry"
)

type Config struct {
	ghostferry.Config

	ApplicableDatabases map[string]bool
	ApplicableTables    map[string]bool
}

func (c *Config) ValidateConfig() error {
	if len(c.ApplicableDatabases) == 0 {
		return fmt.Errorf("failed to validate config: no applicable databases specified")
	}

	c.TableFilter = NewStaticTableFilter(
		c.ApplicableDatabases,
		c.ApplicableTables,
	)

	if err := c.Config.ValidateConfig(); err != nil {
		return err
	}

	return nil
}
