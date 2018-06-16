package ghostferry

import (
	"database/sql"
	"fmt"

	"github.com/sirupsen/logrus"
)

type BinlogWriter struct {
	DB               *sql.DB
	DatabaseRewrites map[string]string
	TableRewrites    map[string]string
	Throttler        Throttler
	Schema           *Schema

	BatchSize    int
	WriteRetries int

	ErrorHandler ErrorHandler

	binlogEventBuffer chan DMLEvent
	logger            *logrus.Entry
}

func (b *BinlogWriter) Initialize() error {
	b.logger = logrus.WithField("tag", "binlog_writer")
	b.binlogEventBuffer = make(chan DMLEvent, b.BatchSize)
	return nil
}

func (b *BinlogWriter) Run() {
	batch := make([]DMLEvent, 0, b.BatchSize)
	for {
		firstEvent := <-b.binlogEventBuffer
		if firstEvent == nil {
			// Channel is closed, no more events to write
			break
		}

		batch = append(batch, firstEvent)
		wantMoreEvents := true
		for wantMoreEvents && len(batch) < b.BatchSize {
			select {
			case event := <-b.binlogEventBuffer:
				if event != nil {
					batch = append(batch, event)
				} else {
					// Channel is closed, finish writing batch.
					wantMoreEvents = false
				}
			default: // Nothing in the buffer so just write it
				wantMoreEvents = false
			}
		}

		err := WithRetries(b.WriteRetries, 0, b.logger, "write events to target", func() error {
			return b.writeEvents(batch)
		})
		if err != nil {
			b.ErrorHandler.Fatal("binlog_writer", err)
			return
		}

		batch = make([]DMLEvent, 0, b.BatchSize)
	}
}

func (b *BinlogWriter) Stop() {
	close(b.binlogEventBuffer)
}

func (b *BinlogWriter) BufferBinlogEvents(events []DMLEvent) error {
	for _, event := range events {
		b.binlogEventBuffer <- event
	}

	return nil
}

func (b *BinlogWriter) writeEvents(events []DMLEvent) error {
	WaitForThrottle(b.Throttler)

	queryBuffer := []byte("BEGIN;\n")

	for _, ev := range events {
		eventDatabaseName := ev.Database()
		if targetDatabaseName, exists := b.DatabaseRewrites[eventDatabaseName]; exists {
			eventDatabaseName = targetDatabaseName
		}

		eventTableName := ev.Table()
		if targetTableName, exists := b.TableRewrites[eventTableName]; exists {
			eventTableName = targetTableName
		}

		intersectedTable := b.Schema.GetIntersectedSchema().Get(eventDatabaseName, eventTableName)
		if intersectedTable == nil {
			return fmt.Errorf("have event for table that doesnt exist in intersection schema")
		}

		sql, err := ev.AsSQLString(intersectedTable)
		if err != nil {
			return fmt.Errorf("generating sql query: %v", err)
		}

		queryBuffer = append(queryBuffer, sql...)
		queryBuffer = append(queryBuffer, ";\n"...)
	}

	queryBuffer = append(queryBuffer, "COMMIT"...)

	query := string(queryBuffer)
	_, err := b.DB.Exec(query)
	if err != nil {
		return fmt.Errorf("exec query (%d bytes): %v", len(query), err)
	}
	return nil
}
