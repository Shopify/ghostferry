package ghostferry

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

type BinlogWriter struct {
	DB               *sql.DB
	DatabaseRewrites map[string]string
	TableRewrites    map[string]string
	Throttler        Throttler

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

func (b *BinlogWriter) Run(ctx context.Context) {
	batch := make([]DMLEvent, 0, b.BatchSize)
	for {
		var firstEvent DMLEvent
		select {
		case firstEvent = <-b.binlogEventBuffer:
		case <-ctx.Done():
			b.logger.Info("cancellation received, terminating goroutine abruptly")
			return
		}

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
			case <-ctx.Done():
				b.logger.Info("cancellation received, terminating goroutine abruptly")
				return
			default: // Nothing in the buffer so just write it
				wantMoreEvents = false
			}
		}

		err := WithRetriesContext(ctx, b.WriteRetries, 0, b.logger, "write events to target", func() error {
			return b.writeEvents(ctx, batch)
		})

		if err == context.Canceled {
			b.logger.Info("cancellation received, terminating goroutine abruptly")
			return
		}

		if err != nil {
			b.ErrorHandler.Fatal("binlog_writer", err)
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

func (b *BinlogWriter) writeEvents(ctx context.Context, events []DMLEvent) error {
	WaitForThrottle(ctx, b.Throttler)

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

		sql, err := ev.AsSQLString(&schema.Table{Schema: eventDatabaseName, Name: eventTableName})
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
