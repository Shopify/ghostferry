package testhelpers

import (
	"context"
	"sync"
)

type ErrorHandler struct {
	LastError error
}

func (this *ErrorHandler) Initialize() {}

func (this *ErrorHandler) Run(wg *sync.WaitGroup, ctx context.Context) {
	wg.Done()
}

func (this *ErrorHandler) Fatal(from string, err error) {
	this.LastError = err
}
