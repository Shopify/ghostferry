package testhelpers

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/Shopify/ghostferry"
)

// integrationTestTimeout is the per-test deadline enforced by startWatchdog.
// It is intentionally shorter than the -timeout flag passed to go test so
// that the goroutine dump produced here is visible in CI logs before the
// test binary is killed by the Go test runner.
const integrationTestTimeout = 4 * time.Minute

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
	watchdogDone := this.startWatchdog()
	defer close(watchdogDone)
	defer this.Teardown()
	this.CopyData()
	this.VerifyData()
}

// startWatchdog starts a background goroutine that fires after
// integrationTestTimeout.  When it fires it writes a full goroutine dump to
// stderr — the primary diagnostic for a stuck / deadlocked test — then
// terminates the process with exit code 1 so that CI sees a failure instead
// of silently waiting for the job-level timeout.
//
// The caller must close the returned channel when the test finishes normally
// so the watchdog goroutine exits cleanly.
func (this *IntegrationTestCase) startWatchdog() chan struct{} {
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			return
		case <-time.After(integrationTestTimeout):
		}

		// runtime.Stack with true collects stacks for all goroutines, not
		// just the calling one.  1 MB is enough for hundreds of goroutines.
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)

		// Write directly to stderr so the output is not swallowed by the
		// test runner's log-capture buffer.
		fmt.Fprintf(os.Stderr,
			"\n=== GHOSTFERRY INTEGRATION TEST WATCHDOG FIRED ===\n"+
				"Test %q has been running for more than %s.\n"+
				"All goroutines at the time of the hang:\n\n"+
				"%s\n"+
				"===================================================\n",
			this.T.Name(), integrationTestTimeout, buf[:n])
		os.Stderr.Sync()

		// os.Exit terminates immediately, bypassing deferred teardown.
		// This is intentional: the test is hung and we want fast CI
		// failure with the diagnostic above rather than waiting for the
		// GitHub Actions job timeout.
		os.Exit(1)
	}()
	return done
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
		ghostferry.LogWithField("tag", "integration_test").Errorf("panic detected in integration test: %v", r)
		ghostferry.LogWithField("tag", "integration_test").Error("cleaning up now...")
		ghostferry.LogWithField("tag", "integration_test").Error("you might see an unrelated panic as we delete the db, if there are background processes operating on the db")
	}

	_, err := this.SourceDB.Exec("SET GLOBAL read_only = OFF")
	if err != nil {
		ghostferry.LogWithError(err).Error("cannot set global read_only = OFF at the source db")
	}

	for _, dbname := range ApplicableTestDbs {
		_, err = this.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
		if err != nil {
			ghostferry.LogWithError(err).Errorf("failed to drop database %s on the source db as a part of the test cleanup", dbname)
		}

		_, err = this.TargetDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
		if err != nil {
			ghostferry.LogWithError(err).Errorf("failed to drop database %s on the target db as a part of the test cleanup", dbname)
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
