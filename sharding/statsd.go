package sharding

import (
	"fmt"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
)

var (
	metrics = &ghostferry.Metrics{
		Prefix: "ghostferry.sharding",
		Sink:   nil,
	}
)

func InitializeMetrics(config *Config) error {
	if config.StatsdAddress == "" {
		logrus.Debug("statsd metrics not configured")
		return nil
	}

	client, err := statsd.New(config.StatsdAddress)
	if err != nil {
		return err
	}

	metrics.Sink = make(chan interface{}, config.StatsdQueueSize)
	metrics.DefaultTags = []ghostferry.MetricTag{
		{Name: "SourceDB", Value: config.SourceDB},
		{Name: "TargetDB", Value: config.TargetDB},
	}

	metrics = ghostferry.SetGlobalMetrics(metrics)

	go consumeMetrics(client, metrics.Sink)

	return nil
}

func Metrics() *ghostferry.Metrics {
	return metrics
}

func SetMetricSink(sink chan interface{}) {
	metrics.Sink = sink
}

func StopAndFlushMetrics() {
	metrics.StopAndFlush()
}

func consumeMetrics(client *statsd.Client, sink chan interface{}) {
	defer metrics.DoneConsumer()
	metrics.AddConsumer()
	for {
		switch metric := (<-sink).(type) {
		case ghostferry.CountMetric:
			handleErr(client.Count(metric.Key, metric.Value, tagsToStrings(metric.Tags), metric.SampleRate), metric)
		case ghostferry.GaugeMetric:
			handleErr(client.Gauge(metric.Key, metric.Value, tagsToStrings(metric.Tags), metric.SampleRate), metric)
		case ghostferry.TimerMetric:
			handleErr(client.Timing(metric.Key, metric.Value, tagsToStrings(metric.Tags), metric.SampleRate), metric)
		case nil:
			return
		}
	}
}

func tagsToStrings(tags []ghostferry.MetricTag) []string {
	strs := make([]string, len(tags))
	for i, tag := range tags {
		if tag.Value != "" {
			strs[i] = fmt.Sprintf("%s:%s", tag.Name, tag.Value)
		} else {
			strs[i] = tag.Name
		}
	}
	return strs
}

func handleErr(err error, metric interface{}) {
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error":  err,
			"metric": metric,
		}).Warnln("could not emit statsd metric")
	}
}
