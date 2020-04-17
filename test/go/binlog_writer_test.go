package test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/siddontang/go-mysql/replication"
	"github.com/stretchr/testify/suite"
)

type BinlogWriterTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite
}

func (t *BinlogWriterTestSuite) TestAnnotatesAllItsDMLStatements() {
	testhelpers.SeedInitialData(t.Ferry.SourceDB, "gftest", "test_table_1", 0)
	testhelpers.SeedInitialData(t.Ferry.TargetDB, "gftest", "test_table_1", 0)
	t.Require().Nil(t.Ferry.Initialize())

	writer, stopWriterAndWait := t.startNewWriter()

	events := t.getRandomDMLEventsFromWrites()
	t.Require().Nil(writer.BufferBinlogEvents(events))

	fmt.Printf("len(events) = %+v\n", len(events))

	getRecorded := t.startRecordingTargetQueryEvents()

	writer.BufferBinlogEvents(events)
	stopWriterAndWait()

	recordedEvents := getRecorded()
	numQueries := 0

	for _, ev := range recordedEvents {
		switch e := ev.Event.(type) {
		case *replication.RowsQueryEvent:
			numQueries = numQueries + 1
			q := string(e.Query)
			t.Require().Regexp("\\/\\*application:ghostferry\\*\\/", q)
			fmt.Printf("q = %+v\n", q)
		}
	}

	// This fails... should it?
	t.Require().Equal(len(events), numQueries)
}
func (t *BinlogWriterTestSuite) startRecordingTargetQueryEvents() (getRecordedAndStop func() []*replication.BinlogEvent) {
	syncerConfig := replication.BinlogSyncerConfig{
		ServerID:                42342,
		Host:                    t.TestFerry.Target.Host,
		Port:                    t.TestFerry.Target.Port,
		User:                    t.TestFerry.Target.User,
		Password:                t.TestFerry.Target.Pass,
		UseDecimal:              true,
		TimestampStringLocation: time.UTC,
	}

	currentPosition, err := ghostferry.ShowMasterStatusBinlogPosition(t.TestFerry.TargetDB)
	t.Require().Nil(err)

	binlogSyncer := replication.NewBinlogSyncer(syncerConfig)

	streamer, err := binlogSyncer.StartSync(currentPosition)
	t.Require().Nil(err)

	return func() []*replication.BinlogEvent {
		evs := streamer.DumpEvents()
		for {
			newEvs := streamer.DumpEvents()
			if len(newEvs) == 0 {
				break
			}

			evs = append(evs, newEvs...)
			time.Sleep(1 * time.Second)
		}

		fmt.Printf("len(evs) = %+v\n", len(evs))
		binlogSyncer.Close()
		return evs
	}
}

func (t *BinlogWriterTestSuite) startNewWriter() (writer *ghostferry.BinlogWriter, stopAndWait func()) {
	writer = t.TestFerry.NewBinlogWriter()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		writer.Run()
		wg.Done()
	}()

	stopAndWait = func() {
		writer.Stop()
		wg.Wait()
	}

	return
}

func (t *BinlogWriterTestSuite) getRandomDMLEventsFromWrites() []ghostferry.DMLEvent {
	events := make([]ghostferry.DMLEvent, 0)

	streamer := t.TestFerry.NewBinlogStreamer()
	streamer.AddEventListener(func(ev []ghostferry.DMLEvent) error {
		events = append(events, ev...)
		return nil
	})

	_, err := streamer.ConnectBinlogStreamerToMysql()
	t.Require().Nil(err)

	streamerWg := sync.WaitGroup{}
	streamerWg.Add(1)
	go func() {
		streamer.Run()
		streamerWg.Done()
	}()

	randomDataWriter := &testhelpers.MixedActionDataWriter{
		ProbabilityOfInsert: 0.3,
		ProbabilityOfUpdate: 0,
		ProbabilityOfDelete: 0,
		NumberOfWriters:     2,
		Db:                  t.TestFerry.SourceDB,
		Tables:              []string{"gftest.test_table_1"},
	}

	writeWg := sync.WaitGroup{}
	writeWg.Add(1)
	go func() {
		randomDataWriter.Run()
		writeWg.Done()
	}()

	for {
		if len(events) > 200 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	randomDataWriter.Stop()
	writeWg.Wait()

	streamer.FlushAndStop()
	streamerWg.Wait()

	return events
}

func TestBinlogWriterTestSuite(t *testing.T) {
	suite.Run(t, &BinlogWriterTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
