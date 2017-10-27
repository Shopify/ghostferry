package reloc

import (
	"fmt"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/go-dogstatsd"
)

var metrics *ghostferry.Metrics

func InitializeMetrics(prefix, address string) error {
	client, err := dogstatsd.New(address, &dogstatsd.Context{})
	if err != nil {
		return err
	}

	metricsChan := make(chan interface{}, 1024)

	metrics = ghostferry.SetGlobalMetrics(prefix, metricsChan)

	go consumeMetrics(client, metricsChan)

	return nil
}

func consumeMetrics(client *dogstatsd.Client, metricsChan chan interface{}) {
	for {
		switch metric := (<-metricsChan).(type) {
		case ghostferry.CountMetric:
			handleErr(client.Count(metric.Key, metric.Value, tagsToStrings(metric.Tags), metric.SampleRate), metric)
		case ghostferry.GaugeMetric:
			handleErr(client.Gauge(metric.Key, metric.Value, tagsToStrings(metric.Tags), metric.SampleRate), metric)
		case ghostferry.TimerMetric:
			handleErr(client.Timer(metric.Key, metric.Value, tagsToStrings(metric.Tags), metric.SampleRate), metric)
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
		fmt.Println("reloc could not emit statsd metric ", metric)
	}
}
