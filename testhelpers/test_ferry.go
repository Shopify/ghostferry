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
}

func NewTestFerry() *TestFerry {
	sourcePortStr := os.Getenv("N1_PORT")
	targetPortStr := os.Getenv("N2_PORT")
	var sourcePort, targetPort uint64
	var err error
	if sourcePortStr == "" {
		sourcePort = 29291
	} else {
		sourcePort, err = strconv.ParseUint(sourcePortStr, 10, 16)
		PanicIfError(err)
	}

	if targetPortStr == "" {
		targetPort = 29292
	} else {
		targetPort, err = strconv.ParseUint(targetPortStr, 10, 16)
		PanicIfError(err)
	}

	config := &ghostferry.Config{
		SourceHost: "127.0.0.1",
		SourcePort: uint16(sourcePort),
		SourceUser: "root",
		SourcePass: "",

		TargetHost: "127.0.0.1",
		TargetPort: uint16(targetPort),
		TargetUser: "root",
		TargetPass: "",

		ApplicableDatabases: map[string]bool{
			"gftest":  true,
			"gftest1": true,
			"gftest2": true,
		},

		MyServerId:       91919,
		AutomaticCutover: true,
		WebBasedir:       "..",
	}

	err = config.ValidateConfig()
	PanicIfError(err)

	return &TestFerry{
		Ferry: &ghostferry.Ferry{
			Config: config,
		},
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

	tables := make([]string, 0)
	for table := range this.Ferry.Tables {
		tables = append(tables, table)
	}
	this.Ferry.Verifier = &ghostferry.ChecksumTableVerifier{
		TablesToCheck: tables,
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
