package reloc

import (
	"encoding/json"
	"net/http"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
)

type RelocErrorHandler struct {
	ghostferry.ErrorHandler
	PanicCallback HTTPCallback
	Logger        *logrus.Entry
}

func (this *RelocErrorHandler) Fatal(from string, err error) {
	client := &http.Client{}

	panicData := make(map[string]string)
	panicData["ErrFrom"] = from
	if err != nil {
		panicData["ErrMessage"] = err.Error()
	} else {
		panicData["ErrMessage"] = ""
	}

	panicDataBytes, jsonErr := json.MarshalIndent(panicData, "", "  ")
	if jsonErr != nil {
		this.Logger.WithField("error", jsonErr).Errorf("reloc failed to marshal panic data")
		err = jsonErr
	} else {
		this.PanicCallback.Payload = string(panicDataBytes)

		postErr := this.PanicCallback.Post(client)
		if postErr != nil {
			this.Logger.WithField("error", postErr).Errorf("reloc failed to notify panic error")
			err = postErr
		}
	}

	this.ErrorHandler.Fatal(from, err)
}
