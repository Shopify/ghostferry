package test

import (
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/copydb"
	"github.com/Shopify/ghostferry/copydb/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type StatsdTestSuite struct {
	suite.Suite

	sink    chan interface{}
	metrics *ghostferry.Metrics
	tags    []ghostferry.MetricTag
	config  *copydb.Config
}

func (t *StatsdTestSuite) SetupTest() {
	t.sink = make(chan interface{}, 50)

	t.tags = []ghostferry.MetricTag{
		ghostferry.MetricTag{
			Name:  "test",
			Value: "true",
		},
		ghostferry.MetricTag{
			Name:  "4",
			Value: "2",
		},
	}

	t.config = testhelpers.NewTestConfig()
	t.config.StatsdAddress = "127.0.0.1:8125"
}

func (t *StatsdTestSuite) TearDownTest() {
	copydb.SetMetricSink(nil)
}

func (t *StatsdTestSuite) TestEmptyInitializeMetrics() {
	t.config.StatsdAddress = ""
	err := copydb.InitializeMetrics(t.config)
	t.Require().Nil(err)
	t.Require().Nil(copydb.Metrics().Sink)
}

func (t *StatsdTestSuite) TestMetrics() {
	metrics := copydb.Metrics()
	metrics.Sink = t.sink

	metricBase := ghostferry.MetricBase{
		Key:        "ghostferry.copydb.RowEvent",
		Tags:       t.tags,
		SampleRate: 1.0,
	}

	metrics.Count("RowEvent", 1, t.tags, 1.0)
	t.Require().Equal(ghostferry.CountMetric{
		MetricBase: metricBase,
		Value:      1,
	}, <-t.sink)

	metrics.Gauge("RowEvent", 1, t.tags, 1.0)
	t.Require().Equal(ghostferry.GaugeMetric{
		MetricBase: metricBase,
		Value:      1,
	}, <-t.sink)

	metrics.Timer("RowEvent", 1, t.tags, 1.0)
	t.Require().Equal(ghostferry.TimerMetric{
		MetricBase: metricBase,
		Value:      1,
	}, <-t.sink)

	metrics.Measure("RowEvent", t.tags, 1.0, func() {})
	consumedMetric := <-t.sink
	consumedMetricValue := consumedMetric.(ghostferry.TimerMetric).Value
	t.Require().Equal(ghostferry.TimerMetric{
		MetricBase: metricBase,
		Value:      consumedMetricValue,
	}, consumedMetric)
	t.Require().GreaterOrEqual(consumedMetricValue, time.Duration(0))
}

func (t *StatsdTestSuite) TestStopAndFlushMetrics() {
	err := copydb.InitializeMetrics(t.config)
	t.Require().Nil(err)

	copydb.StopAndFlushMetrics()

	assert.PanicsWithError(t.Suite.T(), "close of closed channel", func() {
		copydb.StopAndFlushMetrics()
	})
}

func TestStatsdTestSuite(t *testing.T) {
	suite.Run(t, new(StatsdTestSuite))
}
