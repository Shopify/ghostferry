package testhelpers

import (
	"sync"
)

type ErrorHandler struct {
	LastError error
}

func (this *ErrorHandler) Initialize() {}

func (this *ErrorHandler) Run(wg *sync.WaitGroup) { wg.Done() }

func (this *ErrorHandler) Stop() {}

func (this *ErrorHandler) Fatal(from string, err error) {
	this.LastError = err
}
