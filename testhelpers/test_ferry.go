package testhelpers

import (
	"os"
	"strconv"

	"github.com/Shopify/ghostferry"
)

type TestFerry struct {
	*ghostferry.Ferry

	BeforeBatchCopyListener   func(batch *ghostferry.RowBatch) error
	BeforeBinlogApplyListener func(events []ghostferry.DMLEvent) error
	BeforeRowCopyDoneListener func() error

	AfterBatchCopyListener   func(batch *ghostferry.RowBatch) error
	AfterBinlogApplyListener func(events []ghostferry.DMLEvent) error
	AfterRowCopyDoneListener func() error
}

var (
	TestSourcePort = getPortFromEnv("N1_PORT", 29291)
	TestTargetPort = getPortFromEnv("N2_PORT", 29292)

	ApplicableTestDbs = []string{"gftest", "gftest1", "gftest2"}
)

func NewTestConfig() *ghostferry.Config {
	config := &ghostferry.Config{
		Source: &ghostferry.DatabaseConfig{
			Host:      "127.0.0.1",
			Port:      uint16(TestSourcePort),
			User:      "root",
			Pass:      "",
			Collation: "utf8mb4_unicode_ci",
			Params: map[string]string{
				"charset": "utf8mb4",
			},
		},

		Target: &ghostferry.DatabaseConfig{
			Host:      "127.0.0.1",
			Port:      uint16(TestTargetPort),
			User:      "root",
			Pass:      "",
			Collation: "utf8mb4_unicode_ci",
			Params: map[string]string{
				"charset": "utf8mb4",
			},
		},

		MyServerId:       91919,
		AutomaticCutover: true,

		TableFilter: &TestTableFilter{
			DbsFunc:    DbApplicabilityFilter(ApplicableTestDbs),
			TablesFunc: nil,
		},
	}

	err := config.ValidateConfig()
	PanicIfError(err)

	return config
}

func NewTestFerry() *TestFerry {
	return &TestFerry{
		Ferry: &ghostferry.Ferry{
			Config: NewTestConfig(),
		},
	}
}

func (this *TestFerry) Initialize() error {
	return this.Ferry.Initialize()
}

func (this *TestFerry) Start() error {
	if this.BeforeBatchCopyListener != nil {
		this.Ferry.DataIterator.AddBatchListener(this.BeforeBatchCopyListener)
	}

	if this.BeforeBinlogApplyListener != nil {
		this.Ferry.SourceBinlogStreamer.AddEventListener(this.BeforeBinlogApplyListener)
	}

	if this.BeforeRowCopyDoneListener != nil {
		this.Ferry.DataIterator.AddDoneListener(this.BeforeRowCopyDoneListener)
	}

	err := this.Ferry.Start()
	if err != nil {
		return err
	}

	if this.AfterBatchCopyListener != nil {
		this.Ferry.DataIterator.AddBatchListener(this.AfterBatchCopyListener)
	}

	if this.AfterBinlogApplyListener != nil {
		this.Ferry.SourceBinlogStreamer.AddEventListener(this.AfterBinlogApplyListener)
	}

	if this.AfterRowCopyDoneListener != nil {
		this.Ferry.DataIterator.AddDoneListener(this.AfterRowCopyDoneListener)
	}

	return nil
}

func (this *TestFerry) Run() {
	this.Ferry.Run()
}

func getPortFromEnv(env string, defaultVal uint64) uint64 {
	portStr := os.Getenv(env)
	if portStr == "" {
		return defaultVal
	} else {
		port, err := strconv.ParseUint(portStr, 10, 16)
		PanicIfError(err)
		return port
	}
}
