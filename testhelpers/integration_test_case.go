package testhelpers

import (
	"fmt"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
)

type IntegrationTestCase struct {
	T *testing.T

	SetupAction                   func(*TestFerry)
	AfterStartBinlogStreaming     func(*TestFerry)
	BeforeStoppingBinlogStreaming func(*TestFerry)
	AfterStoppedBinlogStreaming   func(*TestFerry)
	CustomVerifyAction            func(*TestFerry)

	DataWriter DataWriter
	Ferry      *TestFerry

	wg *sync.WaitGroup
}

func (this *IntegrationTestCase) Run() {
	defer this.Teardown()
	this.CopyData()
	this.VerifyData()
}

func (this *IntegrationTestCase) CopyData() {
	this.Setup()
	this.StartFerryAndDataWriter()
	this.WaitUntilRowCopyIsComplete()
	this.SetReadonlyOnSourceDbAndStopDataWriter()
	this.StopStreamingAndWaitForGhostferryFinish()
}

func (this *IntegrationTestCase) Setup() {
	SetupTest()

	PanicIfError(this.Ferry.Initialize())

	_, err := this.Ferry.SourceDB.Exec("SET GLOBAL read_only = OFF")
	PanicIfError(err)

	this.callCustomAction(this.SetupAction)
}

func (this *IntegrationTestCase) StartFerryAndDataWriter() {
	PanicIfError(this.Ferry.Start())

	this.callCustomAction(this.AfterStartBinlogStreaming)

	if this.DataWriter != nil {
		this.DataWriter.SetDB(this.Ferry.SourceDB)
		go this.DataWriter.Run()
	}

	this.wg = &sync.WaitGroup{}
	this.wg.Add(1)
	go func() {
		defer this.wg.Done()
		this.Ferry.Run()
	}()
}

func (this *IntegrationTestCase) WaitUntilRowCopyIsComplete() {
	this.Ferry.WaitUntilRowCopyIsComplete()
}

func (this *IntegrationTestCase) SetReadonlyOnSourceDbAndStopDataWriter() {
	_, err := this.Ferry.SourceDB.Exec("SET GLOBAL read_only = ON")
	PanicIfError(err)

	if this.DataWriter != nil {
		this.DataWriter.Stop()
		this.DataWriter.Wait()
	}
}

func (this *IntegrationTestCase) StopStreamingAndWaitForGhostferryFinish() {
	this.callCustomAction(this.BeforeStoppingBinlogStreaming)

	this.Ferry.FlushBinlogAndStopStreaming()
	this.wg.Wait()

	this.callCustomAction(this.AfterStoppedBinlogStreaming)
}

func (this *IntegrationTestCase) VerifyData() {
	tablesMismatched, err := this.verifyTableChecksum()
	if err != nil {
		this.T.Fatalf("error while verifying data: %v", err)
	}

	if len(tablesMismatched) > 0 {
		this.T.Fatalf("%d tables mismatched: %v", len(tablesMismatched), tablesMismatched)
	}

	this.callCustomAction(this.CustomVerifyAction)
}

func (this *IntegrationTestCase) Teardown() {
	for _, dbname := range ApplicableTestDbs {
		if this.Ferry.SourceDB != nil {
			_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
			if err != nil {
				logrus.WithError(err).Errorf("failed to drop database %s on the source db as a part of the test cleanup", dbname)
			}
		}

		if this.Ferry.TargetDB != nil {
			_, err := this.Ferry.TargetDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
			if err != nil {
				logrus.WithError(err).Errorf("failed to drop database %s on the target db as a part of the test cleanup", dbname)
			}
		}
	}
}

func (this *IntegrationTestCase) verifyTableChecksum() ([]string, error) {
	this.Ferry.Verifier.StartVerification(this.Ferry.Ferry)
	this.Ferry.Verifier.Wait()
	return this.Ferry.Verifier.MismatchedTables()
}

func (this *IntegrationTestCase) callCustomAction(f func(*TestFerry)) {
	if f != nil {
		f(this.Ferry)
	}
}

func (this *IntegrationTestCase) AssertOnlyDataOnSourceAndTargetIs(data string) {
	row := this.Ferry.SourceDB.QueryRow("SELECT id, data FROM gftest.table1")
	var id int64
	var d string
	PanicIfError(row.Scan(&id, &d))

	if d != data {
		this.T.Fatalf("source row data is not '%s', but '%s'", data, d)
	}

	d = ""
	row = this.Ferry.TargetDB.QueryRow("SELECT id, data FROM gftest.table1")
	PanicIfError(row.Scan(&id, &d))

	if d != data {
		this.T.Fatalf("target row data is not '%s', but '%s'", data, d)
	}
}

func (this *IntegrationTestCase) AssertQueriesHaveEqualResult(query string, args ...interface{}) []map[string]interface{} {
	return AssertQueriesHaveEqualResult(this.T, this.Ferry.Ferry, query)
}
