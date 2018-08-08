package ghostferry

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

type ErrorHandler interface {
	Fatal(from string, err error)
}

type PanicErrorHandler struct {
	Ferry         *Ferry
	errorCount    int32
	ErrorCallback HTTPCallback
}

func (this *PanicErrorHandler) Fatal(from string, err error) {
	logger := logrus.WithField("tag", "error_handler")

	if atomic.AddInt32(&this.errorCount, 1) > 1 {
		logger.WithError(err).WithField("errfrom", from).Error("multiple fatal errors detected")
		return
	}

	// Invoke ErrorCallback if defined
	if this.ErrorCallback != (HTTPCallback{}) {
		client := &http.Client{}

		errorData := make(map[string]string)
		errorData["ErrFrom"] = from
		errorData["ErrMessage"] = err.Error()

		errorDataBytes, jsonErr := json.MarshalIndent(errorData, "", "  ")
		if jsonErr != nil {
			logger.WithField("error", jsonErr).Errorf("ghostferry-sharding failed to marshal error data")
		} else {
			this.ErrorCallback.Payload = string(errorDataBytes)

			postErr := this.ErrorCallback.Post(client)
			if postErr != nil {
				logger.WithField("error", postErr).Errorf("ghostferry-sharding failed to notify error")
			}
		}
	}

	// Print error to STDERR
	logger.WithError(err).WithField("errfrom", from).Error("fatal error detected, state dump coming in stdout")

	// Save current state to STDOUT
	state := make(map[string]interface{})
	state["LastSuccessfulBinlogPos"] = this.Ferry.BinlogStreamer.GetLastStreamedBinlogPosition()
	state["LastSuccessfulPrimaryKeys"] = this.Ferry.DataIterator.CurrentState.LastSuccessfulPrimaryKeys()
	state["CompletedTables"] = this.Ferry.DataIterator.CurrentState.CompletedTables()

	stateBytes, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		logger.WithError(err).Error("failed to dump state, trying dump via logger")
		logger.WithFields(logrus.Fields(state)).Error("are the states kinda visible?")
	} else {
		fmt.Fprintln(os.Stdout, string(stateBytes))
	}

	// Panic
	panic("fatal error detected, see logs for details")
}
