package ghostferry

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
)

type ErrorHandler interface {
	Initialize()
	Run(context.Context)
	Fatal(from string, err error)
}

type FerryError struct {
	err  error
	from string
}

type PanicErrorHandler struct {
	Ferry *Ferry
	errCh chan *FerryError
}

func (this *PanicErrorHandler) Initialize() {
	this.errCh = make(chan *FerryError)
}

func (this *PanicErrorHandler) Run(ctx context.Context) {
	logger := logrus.WithField("tag", "error_handler")
	logger.Info("started error handler")

	var ferryErr *FerryError

	select {
	case <-ctx.Done():
		logger.Info("no errors detected during run")
		return
	case ferryErr = <-this.errCh:
		close(this.errCh)
	}

	state := make(map[string]interface{})
	state["LastSuccessfulBinlogPos"] = this.Ferry.BinlogStreamer.GetLastStreamedBinlogPosition()
	state["LastSuccessfulPrimaryKeys"] = this.Ferry.DataIterator.CurrentState.LastSuccessfulPrimaryKeys()
	state["CompletedTables"] = this.Ferry.DataIterator.CurrentState.CompletedTables()

	logger.WithError(ferryErr.err).WithField("errfrom", ferryErr.from).Error("fatal error detected, state dump coming in stdout")
	stateBytes, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		logger.WithError(err).Error("failed to dump state, trying dump via logger")
		logger.WithFields(logrus.Fields(state)).Error("are the states kinda visible?")
		goto goodbye
	}

	fmt.Fprintln(os.Stdout, string(stateBytes))

goodbye:
	panic("fatal error detected, see logs for details")
}

func (this *PanicErrorHandler) Fatal(from string, err error) {
	this.errCh <- &FerryError{
		err:  err,
		from: from,
	}
}
