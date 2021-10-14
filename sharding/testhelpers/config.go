package testhelpers

import (
	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/sharding"
	ghostferryHelpers "github.com/Shopify/ghostferry/testhelpers"
)

func NewTestConfig() *sharding.Config {
	config := &sharding.Config{
		Config:        &ghostferry.Config{AutomaticCutover: true},
		ShardingValue: 1,
		ShardingKey:   "tenant_id",
		SourceDB:      "source_pod_10",
		TargetDB:      "target_pod_20",
	}

	err := config.InitializeAndValidateConfig()
	ghostferryHelpers.PanicIfError(err)

	return config
}
