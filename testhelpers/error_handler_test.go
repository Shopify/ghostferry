package testhelpers

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// Mirrors Ghostferry: goroutines report while the test goroutine polls. Run with -race.
func TestErrorHandlerConcurrentReportAndRead(t *testing.T) {
	handler := &ErrorHandler{}

	if err := handler.Err(); err != nil {
		t.Fatalf("a fresh handler must report no error, got %v", err)
	}

	const writers = 4
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(writers + 1)

	for w := range writers {
		go func() {
			defer wg.Done()
			for i := range iterations {
				handler.ReportError("test", fmt.Errorf("failure %d from writer %d", i, w))
			}
		}()
	}

	observed := false
	go func() {
		defer wg.Done()
		for range writers * iterations {
			if handler.Err() != nil {
				observed = true
			}
		}
	}()

	wg.Wait()

	if !observed {
		t.Error("a concurrent reader never observed a reported error")
	}

	final := handler.Err()
	if final == nil {
		t.Fatal("the last reported error must be retained")
	}

	last := errors.New("the final failure")
	handler.Fatal("test", last)
	if got := handler.Err(); got != last {
		t.Errorf("last error wins: got %v, want %v", got, last)
	}
}
