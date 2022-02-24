package integrationferry

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/sirupsen/logrus"
)

const (
	// These should be kept in sync with ghostferry.rb
	portEnvName string        = "GHOSTFERRY_INTEGRATION_PORT"
	timeout     time.Duration = 30 * time.Second
)

const (
	// These should be kept in sync with ghostferry.rb

	// Could only be sent once by the main thread
	StatusReady                  string = "READY"
	StatusBinlogStreamingStarted string = "BINLOG_STREAMING_STARTED"
	StatusRowCopyCompleted       string = "ROW_COPY_COMPLETED"
	StatusVerifyDuringCutover    string = "VERIFY_DURING_CUTOVER"
	StatusVerified               string = "VERIFIED"
	StatusDone                   string = "DONE"

	// Could be sent by multiple goroutines in parallel
	StatusBeforeRowCopy     string = "BEFORE_ROW_COPY"
	StatusAfterRowCopy      string = "AFTER_ROW_COPY"
	StatusBeforeBinlogApply string = "BEFORE_BINLOG_APPLY"
	StatusAfterBinlogApply  string = "AFTER_BINLOG_APPLY"
)

type RunCallbacks struct {
	BeforeInitialize func(*IntegrationFerry) error
	AfterInitialize  func(*IntegrationFerry) error
}

type IntegrationFerry struct {
	*ghostferry.Ferry
	callbacks *RunCallbacks
}

// =========================================
// Code for integration server communication
// =========================================

func (f *IntegrationFerry) SendStatusAndWaitUntilContinue(status string, data ...string) error {
	integrationPort := os.Getenv(portEnvName)
	if integrationPort == "" {
		return fmt.Errorf("environment variable %s must be specified", portEnvName)
	}

	client := &http.Client{
		Timeout: timeout,
	}

	resp, err := client.PostForm(fmt.Sprintf("http://localhost:%s", integrationPort), url.Values{
		"status": []string{status},
		"data":   data,
	})

	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("server returned invalid status: %d", resp.StatusCode)
	}

	return nil
}

// Method override for Start in order to send status to the integration
// server.
func (f *IntegrationFerry) Start() error {
	f.Ferry.DataIterator.AddBatchListener(func(rowBatch *ghostferry.RowBatch) error {
		return f.SendStatusAndWaitUntilContinue(StatusBeforeRowCopy, rowBatch.TableSchema().Name)
	})

	f.Ferry.BinlogStreamer.AddEventListener(func(events []ghostferry.DMLEvent) error {
		return f.SendStatusAndWaitUntilContinue(StatusBeforeBinlogApply)
	})

	err := f.Ferry.Start()
	if err != nil {
		return err
	}

	f.Ferry.DataIterator.AddBatchListener(func(rowBatch *ghostferry.RowBatch) error {
		return f.SendStatusAndWaitUntilContinue(StatusAfterRowCopy, rowBatch.TableSchema().Name)
	})

	f.Ferry.BinlogStreamer.AddEventListener(func(events []ghostferry.DMLEvent) error {
		return f.SendStatusAndWaitUntilContinue(StatusAfterBinlogApply)
	})

	return nil
}

// ===========================================
// Code to handle an almost standard Ferry run
// ===========================================
func (f *IntegrationFerry) Main() error {
	var err error

	err = f.SendStatusAndWaitUntilContinue(StatusReady)
	if err != nil {
		return err
	}

	err = f.Initialize()
	if err != nil {
		return err
	}

	err = f.Start()
	if err != nil {
		return err
	}

	defer f.StopTargetVerifier()

	err = f.SendStatusAndWaitUntilContinue(StatusBinlogStreamingStarted)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		f.Run()
	}()

	f.WaitUntilRowCopyIsComplete()
	err = f.SendStatusAndWaitUntilContinue(StatusRowCopyCompleted)
	if err != nil {
		return err
	}

	// TODO: this method should return errors rather than calling
	// the error handler to panic directly.
	f.FlushBinlogAndStopStreaming()
	wg.Wait()

	if f.Verifier != nil {
		err := f.SendStatusAndWaitUntilContinue(StatusVerifyDuringCutover)
		if err != nil {
			return err
		}

		result, err := f.Verifier.VerifyDuringCutover()
		if err != nil {
			return err
		}

		// We now send the results back to the integration server as each verifier
		// might log them differently, making it difficult to assert that the
		// incorrect table was caught from the logs
		err = f.SendStatusAndWaitUntilContinue(StatusVerified, result.IncorrectTables...)
		if err != nil {
			return err
		}
	}

	return f.SendStatusAndWaitUntilContinue(StatusDone)
}

func NewStandardConfig() (*ghostferry.Config, error) {
	config := &ghostferry.Config{
		Source: &ghostferry.DatabaseConfig{
			Host:      "127.0.0.1",
			Port:      uint16(29291),
			User:      "root",
			Pass:      "",
			Collation: "utf8mb4_unicode_ci",
			Params: map[string]string{
				"charset": "utf8mb4",
			},
			Marginalia: os.Getenv("GHOSTFERRY_MARGINALIA"),
		},

		Target: &ghostferry.DatabaseConfig{
			Host:      "127.0.0.1",
			Port:      uint16(29292),
			User:      "root",
			Pass:      "",
			Collation: "utf8mb4_unicode_ci",
			Params: map[string]string{
				"charset": "utf8mb4",
			},
			Marginalia: os.Getenv("GHOSTFERRY_MARGINALIA"),
		},

		AutomaticCutover: true,
		TableFilter: &testhelpers.TestTableFilter{
			DbsFunc:    testhelpers.DbApplicabilityFilter([]string{"gftest"}),
			TablesFunc: nil,
		},

		DumpStateOnSignal:        true,
		SkipTargetVerification:   (os.Getenv("GHOSTFERRY_SKIP_TARGET_VERIFICATION") == "true"),
		EnableRowBatchSize:       true,
		DumpStateToStdoutOnError: true,
	}

	integrationPort := os.Getenv(portEnvName)
	if integrationPort == "" {
		return nil, fmt.Errorf("environment variable %s must be specified", portEnvName)
	}

	config.ProgressCallback = ghostferry.HTTPCallback{
		URI: fmt.Sprintf("http://localhost:%s/callbacks/progress", integrationPort),
	}
	config.ProgressReportFrequency = 500

	config.StateCallback = ghostferry.HTTPCallback{
		URI: fmt.Sprintf("http://localhost:%s/callbacks/state", integrationPort),
	}
	config.StateReportFrequency = 500

	resumeStateJSON, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return nil, err
	}

	if len(resumeStateJSON) > 0 {
		config.StateToResumeFrom = &ghostferry.SerializableState{}
		err = json.Unmarshal(resumeStateJSON, config.StateToResumeFrom)
		if err != nil {
			return nil, err
		}
	}

	verifierType := os.Getenv("GHOSTFERRY_VERIFIER_TYPE")
	if verifierType == ghostferry.VerifierTypeIterative {
		config.VerifierType = ghostferry.VerifierTypeIterative
		config.IterativeVerifierConfig = ghostferry.IterativeVerifierConfig{
			Concurrency: 2,
		}
	} else if verifierType != "" {
		config.VerifierType = verifierType
	}

	if cascadingPaginationKeyColumnConfig := os.Getenv("GHOSTFERRY_CASCADING_PAGINATION_COLUMN_CONFIG"); cascadingPaginationKeyColumnConfig != "" {
		config.CascadingPaginationColumnConfig = &ghostferry.CascadingPaginationColumnConfig{}
		err = json.Unmarshal([]byte(cascadingPaginationKeyColumnConfig), config.CascadingPaginationColumnConfig)
		if err != nil {
			return nil, err
		}
	}

	return config, config.ValidateConfig()
}

func Run(f *IntegrationFerry) error {
	err := f.SendStatusAndWaitUntilContinue(StatusReady)
	if err != nil {
		return err
	}

	if f.callbacks.BeforeInitialize != nil {
		if err := f.callbacks.BeforeInitialize(f); err != nil {
			return err
		}
	}

	// run before callback
	err = f.Initialize()
	if err != nil {
		return err
	}

	// run after callback
	if f.callbacks.AfterInitialize != nil {
		if err := f.callbacks.AfterInitialize(f); err != nil {
			return err
		}
	}

	err = f.Start()
	if err != nil {
		return err
	}

	defer f.StopTargetVerifier()

	err = f.SendStatusAndWaitUntilContinue(StatusBinlogStreamingStarted)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		f.Run()
	}()

	f.WaitUntilRowCopyIsComplete()
	err = f.SendStatusAndWaitUntilContinue(StatusRowCopyCompleted)
	if err != nil {
		return err
	}

	// TODO: this method should return errors rather than calling
	// the error handler to panic directly.
	f.FlushBinlogAndStopStreaming()
	wg.Wait()

	if f.Verifier != nil {
		err := f.SendStatusAndWaitUntilContinue(StatusVerifyDuringCutover)
		if err != nil {
			return err
		}

		result, err := f.Verifier.VerifyDuringCutover()
		if err != nil {
			return err
		}

		// We now send the results back to the integration server as each verifier
		// might log them differently, making it difficult to assert that the
		// incorrect table was caught from the logs
		err = f.SendStatusAndWaitUntilContinue(StatusVerified, result.IncorrectTables...)
		if err != nil {
			return err
		}

	}
	return f.SendStatusAndWaitUntilContinue(StatusDone)
}

func Setup(c *RunCallbacks) *IntegrationFerry {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.DebugLevel)
	if os.Getenv("CI") == "true" {
		logrus.SetLevel(logrus.ErrorLevel)
	}

	config, err := NewStandardConfig()
	if err != nil {
		panic(err)
	}

	// This is currently a hack to customize the Ghostferry configuration.
	// TODO: allow Ghostferry config to be specified by the ruby test directly.
	compressedDataColumn := os.Getenv("GHOSTFERRY_DATA_COLUMN_SNAPPY")
	if compressedDataColumn != "" {
		config.CompressedColumnsForVerification = map[string]map[string]map[string]string{
			"gftest": map[string]map[string]string{
				"test_table_1": map[string]string{
					"data": "SNAPPY",
				},
			},
		}
	}

	ignoredColumn := os.Getenv("GHOSTFERRY_IGNORED_COLUMN")
	if ignoredColumn != "" {
		config.IgnoredColumnsForVerification = map[string]map[string]map[string]struct{}{
			"gftest": map[string]map[string]struct{}{
				"test_table_1": map[string]struct{}{
					ignoredColumn: struct{}{},
				},
			},
		}
	}

	f := &IntegrationFerry{
		Ferry: &ghostferry.Ferry{
			Config: config,
		},
		callbacks: c,
	}

	integrationPort := os.Getenv(portEnvName)
	if integrationPort == "" {
		panic(fmt.Sprintf("environment variable %s must be specified", portEnvName))
	}

	f.ErrorHandler = &ghostferry.PanicErrorHandler{
		Ferry: f.Ferry,
		ErrorCallback: ghostferry.HTTPCallback{
			URI: fmt.Sprintf("http://localhost:%s/callbacks/error", integrationPort),
		},
		DumpStateToStdoutOnError: true,
	}

	return f
}
