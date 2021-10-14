package testhelpers

import (
	"fmt"
	"sync"
	"testing"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
)

type IntegrationTestCase struct {
	T *testing.T

	SetupAction                   func(*TestFerry, *sql.DB, *sql.DB)
	AfterStartBinlogStreaming     func(*TestFerry, *sql.DB, *sql.DB)
	AfterRowCopyIsComplete        func(*TestFerry, *sql.DB, *sql.DB)
	BeforeStoppingBinlogStreaming func(*TestFerry, *sql.DB, *sql.DB)
	AfterStoppedBinlogStreaming   func(*TestFerry, *sql.DB, *sql.DB)
	CustomVerifyAction            func(*TestFerry, *sql.DB, *sql.DB)

	DisableChecksumVerifier bool

	SourceDB   *sql.DB
	TargetDB   *sql.DB
	DataWriter DataWriter
	Ferry      *TestFerry
	Verifier   *ghostferry.ChecksumTableVerifier

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

	var err error
	this.SourceDB, err = this.Ferry.Source.SqlDB(nil)
	PanicIfError(err)

	this.TargetDB, err = this.Ferry.Target.SqlDB(nil)
	PanicIfError(err)

	this.callCustomAction(this.SetupAction)
	PanicIfError(this.Ferry.Initialize())
}

func (this *IntegrationTestCase) StartFerryAndDataWriter() {
	PanicIfError(this.Ferry.Start())

	this.Verifier = &ghostferry.ChecksumTableVerifier{
		Tables:   this.Ferry.Tables.AsSlice(),
		SourceDB: this.Ferry.SourceDB,
		TargetDB: this.Ferry.TargetDB,
	}

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
	this.callCustomAction(this.AfterRowCopyIsComplete)
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
	this.Ferry.StopTargetVerifier()
}

func (this *IntegrationTestCase) VerifyData() {
	if !this.DisableChecksumVerifier {

		if len(this.Verifier.Tables) == 0 {
			this.T.Fatalf("the integration test verifier is instructed to verify no tables, this doesn't seem correct")
		}

		verificationResult, err := this.verifyTableChecksum()
		if err != nil {
			this.T.Fatalf("error while verifying data: %v", err)
		}

		if !verificationResult.DataCorrect {
			this.T.Fatalf(verificationResult.Message)
		}
	}

	this.callCustomAction(this.CustomVerifyAction)
}

func (this *IntegrationTestCase) Teardown() {
	r := recover()
	if r != nil {
		logrus.Errorf("panic detected in integration test: %v", r)
		logrus.Error("cleaning up now...")
		logrus.Error("you might see an unrelated panic as we delete the db, if there are background processes operating on the db")
	}

	_, err := this.SourceDB.Exec("SET GLOBAL read_only = OFF")
	if err != nil {
		logrus.WithError(err).Error("cannot set global read_only = OFF at the source db")
	}

	for _, dbname := range ApplicableTestDbs {
		_, err = this.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
		if err != nil {
			logrus.WithError(err).Errorf("failed to drop database %s on the source db as a part of the test cleanup", dbname)
		}

		_, err = this.TargetDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
		if err != nil {
			logrus.WithError(err).Errorf("failed to drop database %s on the target db as a part of the test cleanup", dbname)
		}
	}

	if r != nil {
		panic(r)
	}

	this.SourceDB.Close()
	this.TargetDB.Close()
}

func (this *IntegrationTestCase) verifyTableChecksum() (ghostferry.VerificationResult, error) {
	return this.Verifier.VerifyDuringCutover()
}

func (this *IntegrationTestCase) callCustomAction(f func(*TestFerry, *sql.DB, *sql.DB)) {
	if f != nil {
		f(this.Ferry, this.SourceDB, this.TargetDB)
	}
}

func (this *IntegrationTestCase) AssertOnlyDataOnSourceAndTargetIs(data string) {
	row := this.SourceDB.QueryRow("SELECT id, data FROM gftest.table1")
	var id int64
	var d string
	PanicIfError(row.Scan(&id, &d))

	if d != data {
		this.T.Fatalf("source row data is not '%s', but '%s'", data, d)
	}

	d = ""
	row = this.TargetDB.QueryRow("SELECT id, data FROM gftest.table1")
	PanicIfError(row.Scan(&id, &d))

	if d != data {
		this.T.Fatalf("target row data is not '%s', but '%s'", data, d)
	}
}

func (this *IntegrationTestCase) AssertQueriesHaveEqualResult(query string, args ...interface{}) []map[string]interface{} {
	return AssertQueriesHaveEqualResult(this.T, this.Ferry.Ferry, query)
}
