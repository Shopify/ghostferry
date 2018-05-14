.. _howtousecustom:

=======================================
Using Ghostferry in Custom Applications
=======================================

TODO. For the time being, see the ghostferry-copydb project.

Consuming Ghostferry Metrics
----------------------------

Ghostferry provides optional metrics to your application.

Start consuming the metrics::

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

Emit additional metrics::

  metrics.Count("myOwnCustomMetrics", 42, nil, 1.0)
