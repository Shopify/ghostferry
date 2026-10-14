package test

import (
	"encoding/json"
	"testing"

	"github.com/Shopify/ghostferry/copydb"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func copydbConfigFromJSON(t *testing.T, data string) *copydb.Config {
	config := &copydb.Config{Config: testhelpers.NewTestConfig()}
	require.NoError(t, json.Unmarshal([]byte(data), config))
	return config
}

func TestTopLevelRewritesAreKept(t *testing.T) {
	config := copydbConfigFromJSON(t, `{
		"DatabaseRewrites": {"gftest": "gftest_renamed"},
		"TableRewrites": {"test_table_1": "test_table_1_renamed"}
	}`)

	require.NoError(t, config.InitializeAndValidateConfig())
	assert.Equal(t, map[string]string{"gftest": "gftest_renamed"}, config.DatabaseRewrites)
	assert.Equal(t, map[string]string{"test_table_1": "test_table_1_renamed"}, config.TableRewrites)
}

func TestTopLevelAndNestedRewritesCannotBothBeSet(t *testing.T) {
	config := copydbConfigFromJSON(t, `{
		"DatabaseRewrites": {"gftest": "a"},
		"Databases": {"Rewrites": {"gftest": "b"}}
	}`)
	assert.EqualError(t, config.InitializeAndValidateConfig(), "DatabaseRewrites and Databases.Rewrites cannot both be specified")

	config = copydbConfigFromJSON(t, `{
		"TableRewrites": {"test_table_1": "a"},
		"Tables": {"Rewrites": {"test_table_1": "b"}}
	}`)
	assert.EqualError(t, config.InitializeAndValidateConfig(), "TableRewrites and Tables.Rewrites cannot both be specified")
}
