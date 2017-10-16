package testhelpers

import (
	"os"
	"strconv"

	"github.com/Shopify/ghostferry"
)

type TestFerry struct {
	*ghostferry.Ferry

	BeforeRowCopyListener     func(events []ghostferry.DMLEvent) error
	BeforeBinlogApplyListener func(events []ghostferry.DMLEvent) error
	BeforeRowCopyDoneListener func() error

	AfterRowCopyListener     func(events []ghostferry.DMLEvent) error
	AfterBinlogApplyListener func(events []ghostferry.DMLEvent) error
	AfterRowCopyDoneListener func() error

	ApplicableDatabases map[string]bool
}

var (
	TestSourcePort = getPortFromEnv("N1_PORT", 29291)
	TestTargetPort = getPortFromEnv("N2_PORT", 29292)

	TestApplicableDatabases = map[string]bool{
		"gftest":  true,
		"gftest1": true,
		"gftest2": true,
	}
)

func NewTestConfig() *ghostferry.Config {
	config := &ghostferry.Config{
		SourceHost: "127.0.0.1",
		SourcePort: uint16(TestSourcePort),
		SourceUser: "root",
		SourcePass: "",

		TargetHost: "127.0.0.1",
		TargetPort: uint16(TestTargetPort),
		TargetUser: "root",
		TargetPass: "",

		MyServerId:       91919,
		AutomaticCutover: true,

		Applicability: NewTestApplicability(TestApplicableDatabases),
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

		ApplicableDatabases: TestApplicableDatabases,
	}
}

func (this *TestFerry) Initialize() error {
	return this.Ferry.Initialize()
}

func (this *TestFerry) Start() error {
	if this.BeforeRowCopyListener != nil {
		this.Ferry.DataIterator.AddEventListener(this.BeforeRowCopyListener)
	}

	if this.BeforeBinlogApplyListener != nil {
		this.Ferry.BinlogStreamer.AddEventListener(this.BeforeBinlogApplyListener)
	}

	if this.BeforeRowCopyDoneListener != nil {
		this.Ferry.DataIterator.AddDoneListener(this.BeforeRowCopyDoneListener)
	}

	err := this.Ferry.Start()
	if err != nil {
		return err
	}

	this.Ferry.Verifier = &ghostferry.ChecksumTableVerifier{
		TablesToCheck: this.Ferry.Tables.AsSlice(),
	}

	if this.AfterRowCopyListener != nil {
		this.Ferry.DataIterator.AddEventListener(this.AfterRowCopyListener)
	}

	if this.AfterBinlogApplyListener != nil {
		this.Ferry.BinlogStreamer.AddEventListener(this.AfterBinlogApplyListener)
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
