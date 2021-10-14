package testhelpers

import (
	"github.com/Shopify/ghostferry/copydb"
	ghostferryHelpers "github.com/Shopify/ghostferry/testhelpers"
)

func NewTestConfig() *copydb.Config {
	config := &copydb.Config{
		Config: ghostferryHelpers.NewTestConfig(),
	}

	err := config.InitializeAndValidateConfig()
	ghostferryHelpers.PanicIfError(err)

	return config
}
