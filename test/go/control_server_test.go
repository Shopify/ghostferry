package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func postBatchSize(client *http.Client, url string, batchSize uint64) error {
	body, err := json.Marshal(ghostferry.UpdatableConfig{DataIterationBatchSize: batchSize})
	if err != nil {
		return err
	}

	resp, err := client.Post(url+"/api/config", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("POST /api/config returned %d", resp.StatusCode)
	}
	return nil
}

func getBatchSize(client *http.Client, url string) (uint64, error) {
	req, err := http.NewRequest(http.MethodGet, url+"/api/config", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("GET /api/config returned %d", resp.StatusCode)
	}

	var config ghostferry.UpdatableConfig
	if err := json.NewDecoder(resp.Body).Decode(&config); err != nil {
		return 0, err
	}
	return config.DataIterationBatchSize, nil
}

func TestConcurrentBatchSizeUpdates(t *testing.T) {
	const (
		writers           = 4
		readers           = 4
		iterations        = 50
		overrideBatchSize = uint64(80)
	)

	config := testhelpers.NewTestConfig()
	config.UpdatableConfig.DataIterationBatchSize = 200

	server := &ghostferry.ControlServer{
		Config: &ghostferry.ControlServerConfig{WebBasedir: "../.."},
		F:      &ghostferry.Ferry{Config: config},
	}
	require.NoError(t, server.Initialize())

	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	client := httpServer.Client()

	cursorConfig := &ghostferry.CursorConfig{
		BatchSize: &config.UpdatableConfig.DataIterationBatchSize,
		BatchSizePerTableOverride: &ghostferry.DataIterationBatchSizePerTableOverride{
			TableOverride: map[string]map[string]uint64{
				testhelpers.TestSchemaName: {"overridden": overrideBatchSize},
			},
		},
	}

	allowed := map[uint64]bool{104: true, 200: true}
	start := make(chan struct{})
	wg := &sync.WaitGroup{}

	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := range iterations {
				batchSize := uint64(104)
				if (w+i)%2 == 1 {
					batchSize = 200
				}
				assert.NoError(t, postBatchSize(client, httpServer.URL, batchSize))
			}
		}()
	}

	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				base := cursorConfig.GetBatchSize(testhelpers.TestSchemaName, "not_overridden")
				assert.True(t, allowed[base], "cursor observed unexpected batch size %d", base)
				assert.Equal(t, overrideBatchSize, cursorConfig.GetBatchSize(testhelpers.TestSchemaName, "overridden"))

				snapshot, err := getBatchSize(client, httpServer.URL)
				if assert.NoError(t, err) {
					assert.True(t, allowed[snapshot], "HTTP snapshot observed unexpected batch size %d", snapshot)
				}
			}
		}()
	}

	close(start)
	wg.Wait()

	require.NoError(t, postBatchSize(client, httpServer.URL, 104))

	snapshot, err := getBatchSize(client, httpServer.URL)
	require.NoError(t, err)
	require.Equal(t, uint64(104), snapshot)
	require.Equal(t, uint64(104), cursorConfig.GetBatchSize(testhelpers.TestSchemaName, "not_overridden"))
	require.Equal(t, overrideBatchSize, cursorConfig.GetBatchSize(testhelpers.TestSchemaName, "overridden"))
}
