package test

import (
	"encoding/json"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializableState_MarshalJSON_EmptyState(t *testing.T) {
	state := &ghostferry.SerializableState{
		GhostferryVersion:            "test-version",
		LastSuccessfulPaginationKeys: make(map[string]ghostferry.PaginationKey),
		CompletedTables:              make(map[string]bool),
	}

	data, err := json.Marshal(state)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	var decoded ghostferry.SerializableState
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, "test-version", decoded.GhostferryVersion)
	assert.Empty(t, decoded.LastSuccessfulPaginationKeys)
	assert.Empty(t, decoded.CompletedTables)
}

func TestSerializableState_MarshalJSON_WithUint64Keys(t *testing.T) {
	state := &ghostferry.SerializableState{
		GhostferryVersion: "test-version",
		LastSuccessfulPaginationKeys: map[string]ghostferry.PaginationKey{
			"db.table1": ghostferry.NewUint64Key(100),
			"db.table2": ghostferry.NewUint64Key(200),
			"db.table3": ghostferry.NewUint64Key(300),
		},
		CompletedTables: map[string]bool{
			"db.table4": true,
		},
	}

	data, err := json.Marshal(state)
	require.NoError(t, err)

	var decoded ghostferry.SerializableState
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, "test-version", decoded.GhostferryVersion)
	assert.Len(t, decoded.LastSuccessfulPaginationKeys, 3)

	key1, ok := decoded.LastSuccessfulPaginationKeys["db.table1"].(ghostferry.Uint64Key)
	require.True(t, ok)
	assert.Equal(t, uint64(100), uint64(key1))

	key2, ok := decoded.LastSuccessfulPaginationKeys["db.table2"].(ghostferry.Uint64Key)
	require.True(t, ok)
	assert.Equal(t, uint64(200), uint64(key2))

	key3, ok := decoded.LastSuccessfulPaginationKeys["db.table3"].(ghostferry.Uint64Key)
	require.True(t, ok)
	assert.Equal(t, uint64(300), uint64(key3))

	assert.True(t, decoded.CompletedTables["db.table4"])
}

func TestSerializableState_MarshalJSON_WithBinaryKeys(t *testing.T) {
	uuid1 := []byte{0x01, 0x8f, 0x3e, 0x4c, 0x5a, 0x6b, 0x7c, 0x8d, 0x9e, 0xaf, 0xb0, 0xc1, 0xd2, 0xe3, 0xf4, 0x01}
	uuid2 := []byte{0x01, 0x8f, 0x3e, 0x4c, 0x5a, 0x6b, 0x7c, 0x8d, 0x9e, 0xaf, 0xb0, 0xc1, 0xd2, 0xe3, 0xf4, 0x02}

	state := &ghostferry.SerializableState{
		GhostferryVersion: "test-version",
		LastSuccessfulPaginationKeys: map[string]ghostferry.PaginationKey{
			"db.uuid_table1": ghostferry.NewBinaryKey(uuid1),
			"db.uuid_table2": ghostferry.NewBinaryKey(uuid2),
		},
		CompletedTables: make(map[string]bool),
	}

	data, err := json.Marshal(state)
	require.NoError(t, err)

	var decoded ghostferry.SerializableState
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, "test-version", decoded.GhostferryVersion)
	assert.Len(t, decoded.LastSuccessfulPaginationKeys, 2)

	key1, ok := decoded.LastSuccessfulPaginationKeys["db.uuid_table1"].(ghostferry.BinaryKey)
	require.True(t, ok)
	assert.Equal(t, uuid1, []byte(key1))

	key2, ok := decoded.LastSuccessfulPaginationKeys["db.uuid_table2"].(ghostferry.BinaryKey)
	require.True(t, ok)
	assert.Equal(t, uuid2, []byte(key2))
}

func TestSerializableState_MarshalJSON_WithMixedKeys(t *testing.T) {
	uuid := []byte{0x01, 0x8f, 0x3e, 0x4c, 0x5a, 0x6b, 0x7c, 0x8d, 0x9e, 0xaf, 0xb0, 0xc1, 0xd2, 0xe3, 0xf4, 0x01}

	state := &ghostferry.SerializableState{
		GhostferryVersion: "test-version",
		LastSuccessfulPaginationKeys: map[string]ghostferry.PaginationKey{
			"db.numeric_table":   ghostferry.NewUint64Key(12345),
			"db.uuid_table":      ghostferry.NewBinaryKey(uuid),
			"db.varchar_table":   ghostferry.NewBinaryKey([]byte("some_key")),
			"db.bigint_table":    ghostferry.NewUint64Key(999999999),
		},
		CompletedTables: map[string]bool{
			"db.completed_table": true,
		},
	}

	data, err := json.Marshal(state)
	require.NoError(t, err)

	var decoded ghostferry.SerializableState
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, "test-version", decoded.GhostferryVersion)
	assert.Len(t, decoded.LastSuccessfulPaginationKeys, 4)

	numericKey, ok := decoded.LastSuccessfulPaginationKeys["db.numeric_table"].(ghostferry.Uint64Key)
	require.True(t, ok)
	assert.Equal(t, uint64(12345), uint64(numericKey))

	uuidKey, ok := decoded.LastSuccessfulPaginationKeys["db.uuid_table"].(ghostferry.BinaryKey)
	require.True(t, ok)
	assert.Equal(t, uuid, []byte(uuidKey))

	varcharKey, ok := decoded.LastSuccessfulPaginationKeys["db.varchar_table"].(ghostferry.BinaryKey)
	require.True(t, ok)
	assert.Equal(t, []byte("some_key"), []byte(varcharKey))

	bigintKey, ok := decoded.LastSuccessfulPaginationKeys["db.bigint_table"].(ghostferry.Uint64Key)
	require.True(t, ok)
	assert.Equal(t, uint64(999999999), uint64(bigintKey))

	assert.True(t, decoded.CompletedTables["db.completed_table"])
}

func TestSerializableState_MarshalJSON_WithBinlogPosition(t *testing.T) {
	state := &ghostferry.SerializableState{
		GhostferryVersion: "test-version",
		LastSuccessfulPaginationKeys: map[string]ghostferry.PaginationKey{
			"db.table1": ghostferry.NewUint64Key(100),
		},
		CompletedTables: make(map[string]bool),
		LastWrittenBinlogPosition: mysql.Position{
			Name: "mysql-bin.000123",
			Pos:  456789,
		},
		LastStoredBinlogPositionForInlineVerifier: mysql.Position{
			Name: "mysql-bin.000122",
			Pos:  123456,
		},
		LastStoredBinlogPositionForTargetVerifier: mysql.Position{
			Name: "mysql-bin.000121",
			Pos:  987654,
		},
	}

	data, err := json.Marshal(state)
	require.NoError(t, err)

	var decoded ghostferry.SerializableState
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, "mysql-bin.000123", decoded.LastWrittenBinlogPosition.Name)
	assert.Equal(t, uint32(456789), decoded.LastWrittenBinlogPosition.Pos)

	assert.Equal(t, "mysql-bin.000122", decoded.LastStoredBinlogPositionForInlineVerifier.Name)
	assert.Equal(t, uint32(123456), decoded.LastStoredBinlogPositionForInlineVerifier.Pos)

	assert.Equal(t, "mysql-bin.000121", decoded.LastStoredBinlogPositionForTargetVerifier.Name)
	assert.Equal(t, uint32(987654), decoded.LastStoredBinlogPositionForTargetVerifier.Pos)
}

func TestSerializableState_UnmarshalJSON_CorruptedData(t *testing.T) {
	corruptedJSON := `{
		"GhostferryVersion": "test-version",
		"LastSuccessfulPaginationKeys": {
			"db.table1": {"type": "invalid_type", "value": 123}
		}
	}`

	var decoded ghostferry.SerializableState
	err := json.Unmarshal([]byte(corruptedJSON), &decoded)
	assert.Error(t, err)
}

func TestSerializableState_RoundTrip_LargeState(t *testing.T) {
	uuid1 := []byte{0x01, 0x8f, 0x3e, 0x4c, 0x5a, 0x6b, 0x7c, 0x8d, 0x9e, 0xaf, 0xb0, 0xc1, 0xd2, 0xe3, 0xf4, 0x01}
	uuid2 := []byte{0x01, 0x8f, 0x3e, 0x4c, 0x5a, 0x6b, 0x7c, 0x8d, 0x9e, 0xaf, 0xb0, 0xc1, 0xd2, 0xe3, 0xf4, 0x02}

	state := &ghostferry.SerializableState{
		GhostferryVersion: "test-version-1.2.3",
		LastSuccessfulPaginationKeys: map[string]ghostferry.PaginationKey{
			"prod.users":         ghostferry.NewUint64Key(1000000),
			"prod.orders":        ghostferry.NewUint64Key(5000000),
			"prod.products":      ghostferry.NewUint64Key(250000),
			"prod.sessions":      ghostferry.NewBinaryKey(uuid1),
			"prod.api_keys":      ghostferry.NewBinaryKey(uuid2),
			"staging.users":      ghostferry.NewUint64Key(500),
			"staging.orders":     ghostferry.NewUint64Key(1000),
		},
		CompletedTables: map[string]bool{
			"prod.old_table1":     true,
			"prod.old_table2":     true,
			"staging.old_table":   true,
		},
		LastWrittenBinlogPosition: mysql.Position{
			Name: "mysql-bin.001234",
			Pos:  987654321,
		},
		LastStoredBinlogPositionForInlineVerifier: mysql.Position{
			Name: "mysql-bin.001233",
			Pos:  123456789,
		},
		LastStoredBinlogPositionForTargetVerifier: mysql.Position{
			Name: "mysql-bin.001232",
			Pos:  111222333,
		},
	}

	data, err := json.Marshal(state)
	require.NoError(t, err)

	var decoded ghostferry.SerializableState
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, state.GhostferryVersion, decoded.GhostferryVersion)
	assert.Len(t, decoded.LastSuccessfulPaginationKeys, 7)
	assert.Len(t, decoded.CompletedTables, 3)

	usersKey, ok := decoded.LastSuccessfulPaginationKeys["prod.users"].(ghostferry.Uint64Key)
	require.True(t, ok)
	assert.Equal(t, uint64(1000000), uint64(usersKey))

	sessionsKey, ok := decoded.LastSuccessfulPaginationKeys["prod.sessions"].(ghostferry.BinaryKey)
	require.True(t, ok)
	assert.Equal(t, uuid1, []byte(sessionsKey))

	assert.Equal(t, state.LastWrittenBinlogPosition, decoded.LastWrittenBinlogPosition)
	assert.Equal(t, state.LastStoredBinlogPositionForInlineVerifier, decoded.LastStoredBinlogPositionForInlineVerifier)
	assert.Equal(t, state.LastStoredBinlogPositionForTargetVerifier, decoded.LastStoredBinlogPositionForTargetVerifier)

	for tableName := range state.CompletedTables {
		assert.True(t, decoded.CompletedTables[tableName])
	}
}

func TestSerializableState_JSONStructure(t *testing.T) {
	uuid := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	state := &ghostferry.SerializableState{
		GhostferryVersion: "test",
		LastSuccessfulPaginationKeys: map[string]ghostferry.PaginationKey{
			"db.table1": ghostferry.NewUint64Key(123),
			"db.table2": ghostferry.NewBinaryKey(uuid),
		},
		CompletedTables: make(map[string]bool),
	}

	data, err := json.Marshal(state)
	require.NoError(t, err)

	var raw map[string]interface{}
	err = json.Unmarshal(data, &raw)
	require.NoError(t, err)

	keys, ok := raw["LastSuccessfulPaginationKeys"].(map[string]interface{})
	require.True(t, ok)

	table1Data := keys["db.table1"].(map[string]interface{})
	assert.Equal(t, "uint64", table1Data["type"])
	assert.Equal(t, float64(123), table1Data["value"])

	table2Data := keys["db.table2"].(map[string]interface{})
	assert.Equal(t, "binary", table2Data["type"])
	assert.Equal(t, "deadbeef", table2Data["value"])
}

func TestSerializableState_EmptyBinaryKey(t *testing.T) {
	state := &ghostferry.SerializableState{
		GhostferryVersion: "test",
		LastSuccessfulPaginationKeys: map[string]ghostferry.PaginationKey{
			"db.table": ghostferry.NewBinaryKey([]byte{}),
		},
		CompletedTables: make(map[string]bool),
	}

	data, err := json.Marshal(state)
	require.NoError(t, err)

	var decoded ghostferry.SerializableState
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	key, ok := decoded.LastSuccessfulPaginationKeys["db.table"].(ghostferry.BinaryKey)
	require.True(t, ok)
	assert.Equal(t, []byte{}, []byte(key))
}

func TestSerializableState_ZeroUint64Key(t *testing.T) {
	state := &ghostferry.SerializableState{
		GhostferryVersion: "test",
		LastSuccessfulPaginationKeys: map[string]ghostferry.PaginationKey{
			"db.table": ghostferry.NewUint64Key(0),
		},
		CompletedTables: make(map[string]bool),
	}

	data, err := json.Marshal(state)
	require.NoError(t, err)

	var decoded ghostferry.SerializableState
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	key, ok := decoded.LastSuccessfulPaginationKeys["db.table"].(ghostferry.Uint64Key)
	require.True(t, ok)
	assert.Equal(t, uint64(0), uint64(key))
}
