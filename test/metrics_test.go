package test

import (
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/stretchr/testify/suite"
)

type MetricsTestSuite struct {
	suite.Suite

	sink    chan interface{}
	metrics *ghostferry.Metrics
	tags    []ghostferry.MetricTag
}

func (this *MetricsTestSuite) SetupTest() {
	this.sink = make(chan interface{}, 50)

	this.metrics = &ghostferry.Metrics{
		Prefix: "test",
		Sink:   this.sink,
	}

	this.tags = []ghostferry.MetricTag{
		ghostferry.MetricTag{"test", "true"},
		ghostferry.MetricTag{"4", "2"},
	}
}

func (this *MetricsTestSuite) TestPrefix() {
	this.metrics.Prefix = "test42"
	this.metrics.Count("test_key", 42, nil, 1.0)

	expected := ghostferry.CountMetric{
		MetricBase: ghostferry.MetricBase{
			Key:        "test42.test_key",
			Tags:       nil,
			SampleRate: 1.0,
		},
		Value: 42,
	}

	this.Require().Equal(expected, <-this.sink)
}

func (this *MetricsTestSuite) TestMeasure() {
	this.metrics.Measure("test_run", this.tags, 1.0, func() {
		time.Sleep(100 * time.Millisecond)
	})

	actual := (<-this.sink).(ghostferry.TimerMetric)

	this.Require().True(actual.Value > 100*time.Millisecond)
	this.Require().True(actual.Value < 200*time.Millisecond)

	this.Require().Equal("test.test_run", actual.Key)
	this.Require().Equal(this.tags, actual.Tags)
	this.Require().Equal(1.0, actual.SampleRate)
}

func (this *MetricsTestSuite) TestCount() {
	this.metrics.Count("test_key", 42, this.tags, 1.0)

	expected := ghostferry.CountMetric{
		MetricBase: ghostferry.MetricBase{
			Key:        "test.test_key",
			Tags:       this.tags,
			SampleRate: 1.0,
		},
		Value: int64(42),
	}

	this.Require().Equal(expected, <-this.sink)
}

func (this *MetricsTestSuite) TestGauge() {
	this.metrics.Gauge("test_key", 0.42, this.tags, 0.5)

	expected := ghostferry.GaugeMetric{
		MetricBase: ghostferry.MetricBase{
			Key:        "test.test_key",
			Tags:       this.tags,
			SampleRate: 0.5,
		},
		Value: float64(0.42),
	}

	this.Require().Equal(expected, <-this.sink)
}

func (this *MetricsTestSuite) TestTimer() {
	this.metrics.Timer("test_key", 42*time.Second, this.tags, 1.0)

	expected := ghostferry.TimerMetric{
		MetricBase: ghostferry.MetricBase{
			Key:        "test.test_key",
			Tags:       this.tags,
			SampleRate: 1.0,
		},
		Value: 42 * time.Second,
	}

	this.Require().Equal(expected, <-this.sink)
}

func (this *MetricsTestSuite) TestDropMetricIfSinkFull() {
	sink := make(chan interface{}, 1)

	this.metrics.Sink = sink

	this.metrics.Timer("test_key", 42*time.Second, this.tags, 1.0)
	this.metrics.Timer("test_key", 42*time.Second, this.tags, 1.0)

	expected := ghostferry.TimerMetric{
		MetricBase: ghostferry.MetricBase{
			Key:        "test.test_key",
			Tags:       this.tags,
			SampleRate: 1.0,
		},
		Value: 42 * time.Second,
	}

	this.Require().Equal(expected, <-sink)

	select {
	case <-sink:
		this.Require().Fail("should not have got a second MetricTag")
	default:
	}
}

func TestMetricsTestSuite(t *testing.T) {
	suite.Run(t, new(MetricsTestSuite))
}
