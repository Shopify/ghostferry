package sharding

import (
	"fmt"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/go-dogstatsd"
)

var (
	metrics = &ghostferry.Metrics{
		Prefix: "ghostferry",
		Sink:   nil,
	}
)

func InitializeMetrics(prefix string, config *Config) error {
	address := config.StatsDAddress

	client, err := dogstatsd.New(address, &dogstatsd.Context{})
	if err != nil {
		return err
	}

	metricsChan := make(chan interface{}, 1024)
	SetGlobalMetrics(prefix, metricsChan)

	metrics.DefaultTags = []ghostferry.MetricTag{
		{Name: "SourceDB", Value: config.SourceDB},
		{Name: "TargetDB", Value: config.TargetDB},
	}

	metrics.AddConsumer()
	go consumeMetrics(client, metricsChan)

	return nil
}

func SetGlobalMetrics(prefix string, metricsChan chan interface{}) {
	metrics = ghostferry.SetGlobalMetrics(prefix, metricsChan)
}

func StopAndFlushMetrics() {
	metrics.StopAndFlush()
}

func consumeMetrics(client *dogstatsd.Client, metricsChan chan interface{}) {
	defer metrics.DoneConsumer()
	for {
		switch metric := (<-metricsChan).(type) {
		case ghostferry.CountMetric:
			handleErr(client.Count(metric.Key, metric.Value, tagsToStrings(metric.Tags), metric.SampleRate), metric)
		case ghostferry.GaugeMetric:
			handleErr(client.Gauge(metric.Key, metric.Value, tagsToStrings(metric.Tags), metric.SampleRate), metric)
		case ghostferry.TimerMetric:
			handleErr(client.Timer(metric.Key, metric.Value, tagsToStrings(metric.Tags), metric.SampleRate), metric)
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
		fmt.Println("ghostferry-sharding could not emit statsd metric ", metric)
	}
}
