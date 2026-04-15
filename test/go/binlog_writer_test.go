package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

// stubDMLEvent satisfies the DMLEvent interface with no-op implementations.
// It is only used to exercise the BinlogWriter buffer without needing a real
// MySQL connection or a fully populated TableSchema.
type stubDMLEvent struct{}

func (e *stubDMLEvent) Database() string                        { return "db" }
func (e *stubDMLEvent) Table() string                           { return "tbl" }
func (e *stubDMLEvent) TableSchema() *ghostferry.TableSchema    { return nil }
func (e *stubDMLEvent) AsSQLString(_, _ string) (string, error) { return "", nil }
func (e *stubDMLEvent) OldValues() ghostferry.RowData           { return nil }
func (e *stubDMLEvent) NewValues() ghostferry.RowData           { return nil }
func (e *stubDMLEvent) PaginationKey() (string, error)          { return "", nil }
func (e *stubDMLEvent) BinlogPosition() mysql.Position          { return mysql.Position{} }
func (e *stubDMLEvent) ResumableBinlogPosition() mysql.Position { return mysql.Position{} }
func (e *stubDMLEvent) Annotation() (string, error)             { return "", nil }
func (e *stubDMLEvent) Timestamp() time.Time                    { return time.Time{} }

// TestBinlogWriterBufferBinlogEventsBeforeRun verifies that BufferBinlogEvents
// does not block when called before Run() has started in its own goroutine.
//
// Regression: BinlogWriter.Run() used to be the sole place that created the
// binlogEventBuffer channel.  Ferry.Run() launches the writer and streamer
// goroutines concurrently (ferry.go:733-745).  If the BinlogStreamer receives
// a row event and calls BufferBinlogEvents before the writer goroutine
// schedules and reaches the make() call, the send blocks forever on a nil
// channel, hanging the test (and in production, the entire run).
//
// The fix: BinlogWriter.Initialize() creates the channel eagerly, and
// Ferry.NewBinlogWriter() calls it before any goroutine is started.
func TestBinlogWriterBufferBinlogEventsBeforeRun(t *testing.T) {
	w := &ghostferry.BinlogWriter{
		BatchSize: 10,
	}
	w.Initialize()

	done := make(chan error, 1)
	go func() {
		done <- w.BufferBinlogEvents([]ghostferry.DMLEvent{&stubDMLEvent{}})
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal(fmt.Sprintf(
			"BufferBinlogEvents blocked after Initialize() — nil channel race is not fixed",
		))
	}

	// Drain the buffer so the goroutine above can exit cleanly.
	w.Stop()
}
