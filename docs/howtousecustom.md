<a name="howtousecustom"></a>

# Using Ghostferry in Custom Applications

For an example application, see [ghostferry-copydb](https://github.com/Shopify/ghostferry/tree/main/copydb).

## Consuming Ghostferry Metrics

Ghostferry provides optional metrics to your application. The following is a
complete program that consumes them and emits one metric of its own:

```go
package main

import (
	"fmt"

	"github.com/Shopify/ghostferry"
)

func main() {
	sink := make(chan interface{}, 512)
	metrics := ghostferry.SetGlobalMetrics("myApp", sink)

	metrics.AddConsumer()
	go func() {
		defer metrics.DoneConsumer()

		for m := range sink {
			switch metric := m.(type) {
			case ghostferry.CountMetric:
				fmt.Printf("count %s=%d\n", metric.Key, metric.Value)
			case ghostferry.GaugeMetric:
				fmt.Printf("gauge %s=%g\n", metric.Key, metric.Value)
			case ghostferry.TimerMetric:
				fmt.Printf("timer %s=%s\n", metric.Key, metric.Value)
			}
		}
	}()

	metrics.Count("myOwnCustomMetrics", 42, nil, 1.0)

	metrics.StopAndFlush()
}
```

Metric keys are prefixed with the name passed to `SetGlobalMetrics`, so this
prints `count myApp.myOwnCustomMetrics=42`. `StopAndFlush` closes the sink and
waits for every consumer registered with `AddConsumer` to call `DoneConsumer`.
In a real application, stop everything that emits metrics (including the
Ferry) before calling `StopAndFlush`, because sending to the closed sink
panics. Metrics are sent without blocking: when the sink is full, a metric is
dropped and a warning is logged, so size the channel for your consumer's
throughput.
