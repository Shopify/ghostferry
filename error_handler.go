package ghostferry

import (
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

type ErrorHandler interface {
	Fatal(from string, err error)
}

type PanicErrorHandler struct {
	Ferry      *Ferry
	errorCount int32
}

func (this *PanicErrorHandler) Fatal(from string, err error) {
	logger := logrus.WithField("tag", "error_handler")

	if atomic.AddInt32(&this.errorCount, 1) > 1 {
		logger.WithError(err).WithField("errfrom", from).Error("multiple fatal errors detected")
		return
	}

	logger.WithError(err).WithField("errfrom", from).Error("fatal error detected, state dump coming in stdout")

	serializedState := &SerializableState{
		GhostferryVersion:         VersionString,
		LastKnownTableSchemaCache: this.Ferry.Tables,
	}
	this.Ferry.StateTracker.Serialize(serializedState)

	stateBytes, err := json.MarshalIndent(serializedState, "", "  ")
	if err != nil {
		logger.WithError(err).Error("failed to dump state to JSON...")
	} else {
		fmt.Fprintln(os.Stdout, string(stateBytes))
	}

	panic("fatal error detected, see logs for details")
}
