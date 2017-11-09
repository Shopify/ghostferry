package testhelpers

import (
	"context"
)

type ErrorHandler struct {
	LastError error
}

func (this *ErrorHandler) Initialize() {}

func (this *ErrorHandler) Run(ctx context.Context) {
}

func (this *ErrorHandler) Fatal(from string, err error) {
	this.LastError = err
}
