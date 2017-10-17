package ghostferry

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

type ErrorHandler interface {
	Initialize()
	Run(*sync.WaitGroup)
	Stop()
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

func (this *PanicErrorHandler) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	logger := logrus.WithField("tag", "error_handler")
	logger.Info("started error handler")
	ferryErr, ok := <-this.errCh
	if !ok {
		logger.Info("no errors detected during run, quitting as errCh is closed")
		return
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

func (this *PanicErrorHandler) Stop() {
	close(this.errCh)
}
