package ghostferry

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

type ErrorHandler interface {
	ReportError(from string, err error)
	Fatal(from string, err error)
}

type PanicErrorHandler struct {
	Ferry         *Ferry
	errorCount    int32
	ErrorCallback HTTPCallback
}

func (this *PanicErrorHandler) ReportError(from string, err error) {
	logger := logrus.WithField("tag", "error_handler")

	if atomic.AddInt32(&this.errorCount, 1) > 1 {
		logger.WithError(err).WithField("errfrom", from).Error("multiple fatal errors detected, not reporting again")
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
}

func (this *PanicErrorHandler) Fatal(from string, err error) {
	this.ReportError(from, err)
	panic("fatal error detected, see logs for details")
}
