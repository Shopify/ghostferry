package sharding

import (
	"encoding/json"
	"net/http"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
)

type ShardingErrorHandler struct {
	ghostferry.ErrorHandler
	ErrorCallback HTTPCallback
	Logger        *logrus.Entry
}

func (this *ShardingErrorHandler) Fatal(from string, err error) {
	client := &http.Client{}

	errorData := make(map[string]string)
	errorData["ErrFrom"] = from
	errorData["ErrMessage"] = err.Error()

	errorDataBytes, jsonErr := json.MarshalIndent(errorData, "", "  ")
	if jsonErr != nil {
		this.Logger.WithField("error", jsonErr).Errorf("ghostferry-sharding failed to marshal error data")
	} else {
		this.ErrorCallback.Payload = string(errorDataBytes)

		postErr := this.ErrorCallback.Post(client)
		if postErr != nil {
			this.Logger.WithField("error", postErr).Errorf("ghostferry-sharding failed to notify error")
		}
	}

	this.ErrorHandler.Fatal(from, err)
}
