package ghostferry

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

type ErrorHandler interface {
	// Usually called from Fatal. When called from Fatal, if this method returns
	// true, Fatal should panic, otherwise it should not.
	ReportError(from string, err error)
	Fatal(from string, err error)
}

type PanicErrorHandler struct {
	Ferry             *Ferry
	ErrorCallback     HTTPCallback
	DumpState         bool
	DumpStateFilename string

	errorCount int32
}

func (this *PanicErrorHandler) ReportError(from string, err error) {
	logger := logrus.WithField("tag", "error_handler")

	stateFilename := this.DumpStateFilename
	stateJSON, jsonErr := this.Ferry.SerializeStateToJSON()
	if jsonErr != nil {
		logger.WithError(jsonErr).Error("failed to dump state to JSON...")
	} else if this.DumpState {
		if stateFilename != "" {
			logger.Infof("writing state to %s", stateFilename)
			writeErr := ioutil.WriteFile(stateFilename, []byte(stateJSON), 0640)
			if writeErr != nil {
				logger.WithError(writeErr).Errorf("failed to write state to %s. Dumping to stdout", stateFilename)
				// if the write to file failed, write to stdout as last resort
				// so the data is not lost entirely
				stateFilename = ""
			}
		}
		if stateFilename == "" {
			logger.Info("writing state to stdout")
			fmt.Fprintln(os.Stdout, stateJSON)
		}
	}

	// Invoke ErrorCallback if defined
	if this.ErrorCallback != (HTTPCallback{}) {
		client := &http.Client{}

		errorData := make(map[string]string)
		errorData["ErrFrom"] = from
		errorData["ErrMessage"] = err.Error()
		errorData["StateDump"] = stateJSON

		errorDataBytes, jsonErr := json.MarshalIndent(errorData, "", "  ")
		if jsonErr != nil {
			logger.WithField("error", jsonErr).Errorf("ghostferry failed to marshal error data")
		} else {
			this.ErrorCallback.Payload = string(errorDataBytes)

			postErr := this.ErrorCallback.Post(client)
			if postErr != nil {
				logger.WithField("error", postErr).Errorf("ghostferry failed to notify error")
			}
		}
	}

	errmsg := "fatal error detected"
	if this.DumpState {
		errmsg += ", state dump "
		if jsonErr != nil {
			errmsg += "missing"
		} else if stateFilename == "" {
			errmsg += "in <stdout>"
		} else {
			errmsg += "written to " + stateFilename
		}
	}
	// Print error to STDERR
	logger.WithError(err).WithField("errfrom", from).Error(errmsg)
}

func (this *PanicErrorHandler) Fatal(from string, err error) {
	if atomic.AddInt32(&this.errorCount, 1) > 1 {
		logrus.WithField("tag", "error_handler").WithError(err).WithField("errfrom", from).Error("multiple fatal errors detected, not reporting again")
		return
	}

	this.ReportError(from, err)
	panic("fatal error detected, see logs for details")
}
