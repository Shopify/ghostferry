package testhelpers

import "sync"

// Ghostferry reports errors from its goroutines while tests read from the
// test goroutine, so access is synchronized.
type ErrorHandler struct {
	mu        sync.RWMutex
	lastError error
}

func (this *ErrorHandler) ReportError(from string, err error) {
	this.Fatal(from, err)
}

func (this *ErrorHandler) Fatal(from string, err error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.lastError = err
}

func (this *ErrorHandler) Err() error {
	this.mu.RLock()
	defer this.mu.RUnlock()
	return this.lastError
}
