package testhelpers

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

type IntegrationTestCase struct {
	T *testing.T

	SetupAction                   func(*TestFerry) error
	AfterStartBinlogStreaming     func(*TestFerry) error
	BeforeStoppingBinlogStreaming func(*TestFerry) error
	AfterStoppedBinlogStreaming   func(*TestFerry) error
	CustomVerifyAction            func(*TestFerry) error

	DataWriter DataWriter
	Ferry      *TestFerry

	wg *sync.WaitGroup
}

func (this *IntegrationTestCase) Run() {
	defer this.Teardown()
	this.Setup()
	this.StartFerryAndDataWriter()
	this.WaitUntilRowCopyIsComplete()
	// we must shutdown the control server otherwise it'll leak a socket
	this.ShutdownControlServer()
	this.SetReadonlyOnSourceDbAndStopDataWriter()
	this.StopStreamingAndWaitForGhostferryFinish()
	this.VerifyData()
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

func (this *IntegrationTestCase) ShutdownControlServer() {
	err := this.Ferry.ShutdownControlServer()
	if err != nil {
		logrus.WithError(err).Error("failed to shutdown control server")
	}
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
	this.Ferry.WaitForControlServer()

	this.callCustomAction(this.AfterStoppedBinlogStreaming)
}

func (this *IntegrationTestCase) VerifyData() {
	tablesMismatched, err := this.verifyTableChecksum()
	if err != nil {
		this.T.Fatalf("error while verifying data: %v", err)
	}

	if len(tablesMismatched) > 0 {
		logrus.Error("error")
		time.Sleep(10 * time.Hour)
		this.T.Fatalf("%d tables mismatched: %v", len(tablesMismatched), tablesMismatched)
	}

	this.callCustomAction(this.CustomVerifyAction)
}

func (this *IntegrationTestCase) Teardown() {
	for dbname, _ := range this.Ferry.ApplicableDatabases {
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
	tables := make([]string, 0)
	tablesMismatched := make([]string, 0)
	for table, _ := range this.Ferry.Tables {
		tables = append(tables, table)
	}

	// Cache the source table checksums in a map
	sourceTableChecksums := make(map[string]int64)
	query := fmt.Sprintf("CHECKSUM TABLE %s EXTENDED", strings.Join(tables, ", "))
	rows, err := this.Ferry.SourceDB.Query(query)
	if err != nil {
		return tablesMismatched, err
	}

	for rows.Next() {
		var tablename string
		var checksum int64

		err = rows.Scan(&tablename, &checksum)
		if err != nil {
			rows.Close()
			return tablesMismatched, err
		}
		sourceTableChecksums[tablename] = checksum
	}

	err = rows.Err()
	if err != nil {
		rows.Close()
		return tablesMismatched, err
	}

	rows.Close()

	// Compared the target table checksums to the source table checksum map
	rows, err = this.Ferry.TargetDB.Query(query)
	if err != nil {
		return tablesMismatched, err
	}

	defer rows.Close()

	for rows.Next() {
		var tablename string
		var checksum int64

		err = rows.Scan(&tablename, &checksum)
		if err != nil {
			return tablesMismatched, err
		}

		if checksum != sourceTableChecksums[tablename] {
			tablesMismatched = append(tablesMismatched, tablename)
		}
	}

	return tablesMismatched, rows.Err()
}

func (this *IntegrationTestCase) callCustomAction(f func(*TestFerry) error) {
	if f != nil {
		PanicIfError(f(this.Ferry))
	}
}

func (this *IntegrationTestCase) AssertOnlyDataOnSourceAndTargetIs(data string) {
	row := this.Ferry.SourceDB.QueryRow("SELECT * FROM gftest.table1")
	var id int64
	var d string
	PanicIfError(row.Scan(&id, &d))

	if d != data {
		this.T.Fatalf("source row data is not '%s', but '%s'", data, d)
	}

	d = ""
	row = this.Ferry.TargetDB.QueryRow("SELECT * FROM gftest.table1")
	PanicIfError(row.Scan(&id, &d))

	if d != data {
		this.T.Fatalf("target row data is not '%s', but '%s'", data, d)
	}
}
