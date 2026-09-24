<a name="howtousecustom"></a>

# Using Ghostferry in Custom Applications

For an example application, see [ghostferry-copydb](https://github.com/Shopify/ghostferry/tree/main/copydb).

## Consuming Ghostferry Metrics

Ghostferry provides optional metrics to your application.

Start consuming the metrics:

```go
sink := make(chan interface{}, 512)
metrics := ghostferry.SetGlobalMetrics("myApp", sink)

go func(){
  for {
    switch metric := (<-sink).(type) {
    case ghostferry.CountMetric:
      // Do something with the metric
    case ghostferry.GaugeMetric:
      // Do something with the metric
    case ghostferry.TimerMetric:
      // Do something with the metric
    }
  }
}()
```

Emit additional metrics:

```go
metrics.Count("myOwnCustomMetrics", 42, nil, 1.0)
```
