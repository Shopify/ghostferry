package reloc

import (
	"encoding/json"
	"net/http"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
)

type RelocErrorHandler struct {
	ghostferry.ErrorHandler
	ErrorCallback HTTPCallback
	Logger        *logrus.Entry
}

func (this *RelocErrorHandler) Fatal(from string, err error) {
	client := &http.Client{}

	errorData := make(map[string]string)
	errorData["ErrFrom"] = from
	if err != nil {
		errorData["ErrMessage"] = err.Error()
	}

	errorDataBytes, jsonErr := json.MarshalIndent(errorData, "", "  ")
	if jsonErr != nil {
		this.Logger.WithField("error", jsonErr).Errorf("reloc failed to marshal error data")
		err = jsonErr
	} else {
		this.ErrorCallback.Payload = string(errorDataBytes)

		postErr := this.ErrorCallback.Post(client)
		if postErr != nil {
			this.Logger.WithField("error", postErr).Errorf("reloc failed to notify error")
			err = postErr
		}
	}

	this.ErrorHandler.Fatal(from, err)
}
