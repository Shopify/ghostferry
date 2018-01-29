package ghostferry

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	metrics = &Metrics{
		Prefix: "ghostferry",
		Sink:   nil,
	}
)

type MetricTag struct {
	Name  string
	Value string
}

type MetricBase struct {
	Key        string
	Tags       []MetricTag
	SampleRate float64
}

type Metrics struct {
	Prefix      string
	DefaultTags []MetricTag
	Sink        chan interface{}
}

func SetGlobalMetrics(prefix string, sink chan interface{}) *Metrics {
	metrics = &Metrics{
		Prefix: prefix,
		Sink:   sink,
	}
	return metrics
}

type CountMetric struct {
	MetricBase
	Value int64
}

func (m *Metrics) Count(key string, value int64, tags []MetricTag, sampleRate float64) {
	m.sendMetric(CountMetric{
		MetricBase: MetricBase{
			Key:        m.applyPrefix(key),
			Tags:       m.mergeWithDefaultTags(tags),
			SampleRate: sampleRate,
		},
		Value: value,
	})
}

type GaugeMetric struct {
	MetricBase
	Value float64
}

func (m *Metrics) Gauge(key string, value float64, tags []MetricTag, sampleRate float64) {
	m.sendMetric(GaugeMetric{
		MetricBase: MetricBase{
			Key:        m.applyPrefix(key),
			Tags:       m.mergeWithDefaultTags(tags),
			SampleRate: sampleRate,
		},
		Value: value,
	})
}

type TimerMetric struct {
	MetricBase
	Value time.Duration
}

func (m *Metrics) Timer(key string, duration time.Duration, tags []MetricTag, sampleRate float64) {
	m.sendMetric(TimerMetric{
		MetricBase: MetricBase{
			Key:        m.applyPrefix(key),
			Tags:       m.mergeWithDefaultTags(tags),
			SampleRate: sampleRate,
		},
		Value: duration,
	})
}

func (m *Metrics) Measure(key string, tags []MetricTag, sampleRate float64, f func()) {
	start := time.Now()
	f()
	m.Timer(key, time.Since(start), m.mergeWithDefaultTags(tags), sampleRate)
}

func (m *Metrics) sendMetric(metric interface{}) {
	if m.Sink == nil {
		return
	}

	select {
	case m.Sink <- metric:
	default:
		log.WithField("tag", "metrics").
			WithField("metric", metric).
			Warn("Metrics sink full, dropping metric")
	}
}

func (m *Metrics) applyPrefix(key string) string {
	return fmt.Sprintf("%s.%s", m.Prefix, key)
}

func (m *Metrics) mergeWithDefaultTags(tags []MetricTag) []MetricTag {
	mergedTags := make([]MetricTag, 0, len(tags)+len(m.DefaultTags))

	for _, tag := range tags {
		mergedTags = append(mergedTags, tag)
	}

	for _, tag := range m.DefaultTags {
		exists := false
		for _, existingTag := range mergedTags {
			if tag.Name == existingTag.Name {
				exists = true
				break
			}
		}

		if !exists {
			mergedTags = append(mergedTags, tag)
		}
	}

	return mergedTags
}
