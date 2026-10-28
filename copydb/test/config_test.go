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

func TestConflictingRewritesAreRejected(t *testing.T) {
	config := copydbConfigFromJSON(t, `{
		"DatabaseRewrites": {"gftest": "a"},
		"Databases": {"Rewrites": {"gftest": "b"}}
	}`)
	assert.Error(t, config.InitializeAndValidateConfig())

	config = copydbConfigFromJSON(t, `{
		"TableRewrites": {"test_table_1": "a"},
		"Tables": {"Rewrites": {"test_table_1": "b"}}
	}`)
	assert.Error(t, config.InitializeAndValidateConfig())
}

func TestIdenticalRewriteFormsAreAccepted(t *testing.T) {
	config := copydbConfigFromJSON(t, `{
		"DatabaseRewrites": {"gftest": "gftest_renamed"},
		"Databases": {"Rewrites": {"gftest": "gftest_renamed"}},
		"TableRewrites": {"test_table_1": "test_table_1_renamed"},
		"Tables": {"Rewrites": {"test_table_1": "test_table_1_renamed"}}
	}`)

	require.NoError(t, config.InitializeAndValidateConfig())
	assert.Equal(t, map[string]string{"gftest": "gftest_renamed"}, config.DatabaseRewrites)
	assert.Equal(t, map[string]string{"test_table_1": "test_table_1_renamed"}, config.TableRewrites)
}

func TestNestedRewritesCanBeInitializedRepeatedly(t *testing.T) {
	config := copydbConfigFromJSON(t, `{
		"Databases": {"Rewrites": {"gftest": "gftest_renamed"}},
		"Tables": {"Rewrites": {"test_table_1": "test_table_1_renamed"}}
	}`)

	require.NoError(t, config.InitializeAndValidateConfig())
	require.NoError(t, config.InitializeAndValidateConfig())
	assert.Equal(t, map[string]string{"gftest": "gftest_renamed"}, config.DatabaseRewrites)
	assert.Equal(t, map[string]string{"test_table_1": "test_table_1_renamed"}, config.TableRewrites)
}
