package reloc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
)

type HTTPCallback struct {
	URI     string
	Payload string
}

func (h *HTTPCallback) Post(client *http.Client) error {
	if h.URI == "" {
		return nil
	}

	payload := map[string]interface{}{"Payload": h.Payload}
	return postCallback(client, h.URI, payload)
}

type Config struct {
	ghostferry.Config

	ShardingKey   string
	ShardingValue int64
	SourceDB      string
	TargetDB      string

	StatsDAddress string
	CutoverLock   HTTPCallback
	CutoverUnlock HTTPCallback

	JoinedTables  map[string][]JoinTable
	IgnoredTables []string

	Throttle *ThrottlerConfig
}

func postCallback(client *http.Client, uri string, body interface{}) error {
	buf := bytes.Buffer{}

	err := json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return err
	}

	logger := logrus.WithFields(logrus.Fields{
		"tag": "reloc-http",
		"uri": uri,
	})

	logger.Infof("sending callback")

	res, err := client.Post(uri, "application/json", &buf)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		resBody, err := ioutil.ReadAll(res.Body)

		if err != nil {
			logger.WithField("error", err).Errorf("error reading callback body")
		}

		logger.WithFields(logrus.Fields{
			"status": res.StatusCode,
			"body":   string(resBody),
		}).Errorf("callback not ok")

		return fmt.Errorf("callback returned %s", res.Status)
	}

	return nil
}
