package ghostferry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/sirupsen/logrus"
)

type HTTPCallback struct {
	URI     string
	Payload string
}

func (h HTTPCallback) Post(client *http.Client) error {
	if h.URI == "" {
		return nil
	}

	payload := map[string]interface{}{"Payload": h.Payload}
	return postCallback(client, h.URI, payload)
}

func postCallback(client *http.Client, uri string, body interface{}) error {
	buf := bytes.Buffer{}

	err := json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return err
	}

	logger := logrus.WithFields(logrus.Fields{
		"tag": "sharding-http",
		"uri": uri,
	})

	logger.Infof("sending callback")

	res, err := client.Post(uri, "application/json", &buf)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		io.Copy(ioutil.Discard, res.Body)
		return nil
	}

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
