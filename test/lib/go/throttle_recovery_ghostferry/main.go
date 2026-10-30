package main

import (
	tf "github.com/Shopify/ghostferry/test/lib/go/integrationferry"
)

// A Ghostferry with one copy worker, a 30s throttle recovery interval and the
// control server on 127.0.0.1:18000, so tests can pause and unpause it.
func main() {
	c := tf.RunCallbacks{
		BeforeInitialize: func(f *tf.IntegrationFerry) error {
			f.Config.DataIterationConcurrency = 1
			f.Config.UpdatableConfig.DataIterationBatchSize = 200
			f.Config.DataIterationThrottleRecovery.RecoveryInterval = "30s"
			f.Config.ControlServerConfig.Enabled = true
			f.Config.ControlServerConfig.ServerBindAddr = "127.0.0.1:18000"
			return nil
		},
	}
	f := tf.Setup(&c)

	err := tf.Run(f)
	if err != nil {
		panic(err)
	}
}
