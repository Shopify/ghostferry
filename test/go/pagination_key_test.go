package test

import (
	"encoding/hex"
	"encoding/json"
	"math"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUint64Key_SQLValue(t *testing.T) {
	key := ghostferry.NewUint64Key(12345)
	assert.Equal(t, uint64(12345), key.SQLValue())
}

func TestUint64Key_Compare(t *testing.T) {
	tests := []struct {
		name     string
		key1     ghostferry.Uint64Key
		key2     ghostferry.Uint64Key
		expected int
	}{
		{"less than", ghostferry.NewUint64Key(100), ghostferry.NewUint64Key(200), -1},
		{"equal", ghostferry.NewUint64Key(100), ghostferry.NewUint64Key(100), 0},
		{"greater than", ghostferry.NewUint64Key(200), ghostferry.NewUint64Key(100), 1},
		{"zero vs non-zero", ghostferry.NewUint64Key(0), ghostferry.NewUint64Key(1), -1},
		{"max uint64", ghostferry.NewUint64Key(math.MaxUint64), ghostferry.NewUint64Key(math.MaxUint64-1), 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.key1.Compare(tt.key2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestUint64Key_ComparePanicsOnTypeMismatch(t *testing.T) {
	key1 := ghostferry.NewUint64Key(100)
	key2 := ghostferry.NewBinaryKey([]byte{0x01, 0x02})

	assert.Panics(t, func() {
		key1.Compare(key2)
	})
}

func TestUint64Key_NumericPosition(t *testing.T) {
	tests := []struct {
		value    uint64
		expected float64
	}{
		{0, 0.0},
		{100, 100.0},
		{math.MaxUint64, float64(math.MaxUint64)},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			key := ghostferry.NewUint64Key(tt.value)
			assert.Equal(t, tt.expected, key.NumericPosition())
		})
	}
}

func TestUint64Key_String(t *testing.T) {
	tests := []struct {
		value    uint64
		expected string
	}{
		{0, "0"},
		{12345, "12345"},
		{math.MaxUint64, "18446744073709551615"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			key := ghostferry.NewUint64Key(tt.value)
			assert.Equal(t, tt.expected, key.String())
		})
	}
}

func TestUint64Key_IsMax(t *testing.T) {
	assert.True(t, ghostferry.NewUint64Key(math.MaxUint64).IsMax())
	assert.False(t, ghostferry.NewUint64Key(math.MaxUint64-1).IsMax())
	assert.False(t, ghostferry.NewUint64Key(0).IsMax())
}

func TestUint64Key_MarshalJSON(t *testing.T) {
	key := ghostferry.NewUint64Key(12345)
	data, err := key.MarshalJSON()
	require.NoError(t, err)
	assert.Equal(t, "12345", string(data))
}

func TestBinaryKey_NewBinaryKeyClones(t *testing.T) {
	original := []byte{0x01, 0x02, 0x03}
	key := ghostferry.NewBinaryKey(original)

	original[0] = 0xFF

	assert.Equal(t, []byte{0x01, 0x02, 0x03}, []byte(key))
}

func TestBinaryKey_SQLValue(t *testing.T) {
	original := []byte{0x01, 0x02, 0x03}
	key := ghostferry.NewBinaryKey(original)
	assert.Equal(t, original, key.SQLValue())
}

func TestBinaryKey_Compare(t *testing.T) {
	tests := []struct {
		name     string
		key1     ghostferry.BinaryKey
		key2     ghostferry.BinaryKey
		expected int
	}{
		{
			"less than",
			ghostferry.NewBinaryKey([]byte{0x01, 0x02}),
			ghostferry.NewBinaryKey([]byte{0x01, 0x03}),
			-1,
		},
		{
			"equal",
			ghostferry.NewBinaryKey([]byte{0x01, 0x02}),
			ghostferry.NewBinaryKey([]byte{0x01, 0x02}),
			0,
		},
		{
			"greater than",
			ghostferry.NewBinaryKey([]byte{0x02, 0x01}),
			ghostferry.NewBinaryKey([]byte{0x01, 0x02}),
			1,
		},
		{
			"empty vs non-empty",
			ghostferry.NewBinaryKey([]byte{}),
			ghostferry.NewBinaryKey([]byte{0x01}),
			-1,
		},
		{
			"different lengths",
			ghostferry.NewBinaryKey([]byte{0x01}),
			ghostferry.NewBinaryKey([]byte{0x01, 0x00}),
			-1,
		},
		{
			"UUID comparison",
			ghostferry.NewBinaryKey([]byte{
				0x01, 0x8f, 0x3e, 0x4c, 0x5a, 0x6b, 0x7c, 0x8d,
				0x9e, 0xaf, 0xb0, 0xc1, 0xd2, 0xe3, 0xf4, 0x05,
			}),
			ghostferry.NewBinaryKey([]byte{
				0x01, 0x8f, 0x3e, 0x4c, 0x5a, 0x6b, 0x7c, 0x8d,
				0x9e, 0xaf, 0xb0, 0xc1, 0xd2, 0xe3, 0xf4, 0x06,
			}),
			-1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.key1.Compare(tt.key2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBinaryKey_ComparePanicsOnTypeMismatch(t *testing.T) {
	key1 := ghostferry.NewBinaryKey([]byte{0x01, 0x02})
	key2 := ghostferry.NewUint64Key(100)

	assert.Panics(t, func() {
		key1.Compare(key2)
	})
}

func TestBinaryKey_NumericPosition(t *testing.T) {
	tests := []struct {
		name     string
		bytes    []byte
		expected float64
	}{
		{
			"empty",
			[]byte{},
			0.0,
		},
		{
			"single byte",
			[]byte{0x01},
			float64(0x0100000000000000),
		},
		{
			"8 bytes",
			[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
			float64(0x0102030405060708),
		},
		{
			"more than 8 bytes uses first 8",
			[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a},
			float64(0x0102030405060708),
		},
		{
			"UUIDv7 timestamp ordering",
			[]byte{0x01, 0x8f, 0x3e, 0x4c, 0x5a, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			float64(0x018f3e4c5a6b0000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := ghostferry.NewBinaryKey(tt.bytes)
			assert.Equal(t, tt.expected, key.NumericPosition())
		})
	}
}

func TestBinaryKey_NumericPosition_Monotonic(t *testing.T) {
	key1 := ghostferry.NewBinaryKey([]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	key2 := ghostferry.NewBinaryKey([]byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

	assert.True(t, key1.NumericPosition() < key2.NumericPosition())
}

func TestBinaryKey_String(t *testing.T) {
	tests := []struct {
		name     string
		bytes    []byte
		expected string
	}{
		{"empty", []byte{}, ""},
		{"single byte", []byte{0x01}, "01"},
		{"multiple bytes", []byte{0x01, 0x02, 0x03}, "010203"},
		{"UUID", []byte{
			0x01, 0x8f, 0x3e, 0x4c, 0x5a, 0x6b, 0x7c, 0x8d,
			0x9e, 0xaf, 0xb0, 0xc1, 0xd2, 0xe3, 0xf4, 0x05,
		}, "018f3e4c5a6b7c8d9eafb0c1d2e3f405"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := ghostferry.NewBinaryKey(tt.bytes)
			assert.Equal(t, tt.expected, key.String())
		})
	}
}

func TestBinaryKey_IsMax(t *testing.T) {
	tests := []struct {
		name     string
		bytes    []byte
		expected bool
	}{
		{"empty is not max", []byte{}, false},
		{"all FF is max", []byte{0xFF, 0xFF, 0xFF, 0xFF}, true},
		{"UUID(16) all FF is max", []byte{
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		}, true},
		{"one non-FF byte is not max", []byte{0xFF, 0xFE, 0xFF, 0xFF}, false},
		{"zero is not max", []byte{0x00}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := ghostferry.NewBinaryKey(tt.bytes)
			assert.Equal(t, tt.expected, key.IsMax())
		})
	}
}

func TestBinaryKey_MarshalJSON(t *testing.T) {
	key := ghostferry.NewBinaryKey([]byte{0x01, 0x02, 0x03})
	data, err := key.MarshalJSON()
	require.NoError(t, err)
	assert.Equal(t, `"010203"`, string(data))
}

func TestMarshalPaginationKey_Uint64(t *testing.T) {
	key := ghostferry.NewUint64Key(12345)
	data, err := ghostferry.MarshalPaginationKey(key)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	assert.Equal(t, "uint64", result["type"])
	assert.Equal(t, float64(12345), result["value"])
}

func TestMarshalPaginationKey_Binary(t *testing.T) {
	key := ghostferry.NewBinaryKey([]byte{0x01, 0x02, 0x03})
	data, err := ghostferry.MarshalPaginationKey(key)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	assert.Equal(t, "binary", result["type"])
	assert.Equal(t, "010203", result["value"])
}

func TestUnmarshalPaginationKey_Uint64(t *testing.T) {
	data := []byte(`{"type":"uint64","value":12345}`)
	key, err := ghostferry.UnmarshalPaginationKey(data)
	require.NoError(t, err)

	uint64Key, ok := key.(ghostferry.Uint64Key)
	require.True(t, ok)
	assert.Equal(t, uint64(12345), uint64(uint64Key))
}

func TestUnmarshalPaginationKey_Binary(t *testing.T) {
	data := []byte(`{"type":"binary","value":"010203"}`)
	key, err := ghostferry.UnmarshalPaginationKey(data)
	require.NoError(t, err)

	binaryKey, ok := key.(ghostferry.BinaryKey)
	require.True(t, ok)
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, []byte(binaryKey))
}

func TestUnmarshalPaginationKey_InvalidType(t *testing.T) {
	data := []byte(`{"type":"invalid","value":"something"}`)
	_, err := ghostferry.UnmarshalPaginationKey(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown key type")
}

func TestUnmarshalPaginationKey_InvalidJSON(t *testing.T) {
	data := []byte(`{invalid json}`)
	_, err := ghostferry.UnmarshalPaginationKey(data)
	assert.Error(t, err)
}

func TestUnmarshalPaginationKey_InvalidBinaryHex(t *testing.T) {
	data := []byte(`{"type":"binary","value":"ZZZZ"}`)
	_, err := ghostferry.UnmarshalPaginationKey(data)
	assert.Error(t, err)
}

func TestPaginationKey_RoundTrip_Uint64(t *testing.T) {
	original := ghostferry.NewUint64Key(98765)

	marshaled, err := ghostferry.MarshalPaginationKey(original)
	require.NoError(t, err)

	unmarshaled, err := ghostferry.UnmarshalPaginationKey(marshaled)
	require.NoError(t, err)

	assert.Equal(t, original, unmarshaled)
}

func TestPaginationKey_RoundTrip_Binary(t *testing.T) {
	original := ghostferry.NewBinaryKey([]byte{0xDE, 0xAD, 0xBE, 0xEF})

	marshaled, err := ghostferry.MarshalPaginationKey(original)
	require.NoError(t, err)

	unmarshaled, err := ghostferry.UnmarshalPaginationKey(marshaled)
	require.NoError(t, err)

	assert.Equal(t, original, unmarshaled)
}

func TestMinPaginationKey_Numeric(t *testing.T) {
	column := &schema.TableColumn{
		Name: "id",
		Type: schema.TYPE_NUMBER,
	}

	minKey := ghostferry.MinPaginationKey(column)
	uint64Key, ok := minKey.(ghostferry.Uint64Key)
	require.True(t, ok)
	assert.Equal(t, uint64(0), uint64(uint64Key))
}

func TestMinPaginationKey_MediumInt(t *testing.T) {
	column := &schema.TableColumn{
		Name: "id",
		Type: schema.TYPE_MEDIUM_INT,
	}

	minKey := ghostferry.MinPaginationKey(column)
	uint64Key, ok := minKey.(ghostferry.Uint64Key)
	require.True(t, ok)
	assert.Equal(t, uint64(0), uint64(uint64Key))
}

func TestMinPaginationKey_Binary(t *testing.T) {
	column := &schema.TableColumn{
		Name: "uuid",
		Type: schema.TYPE_BINARY,
	}

	minKey := ghostferry.MinPaginationKey(column)
	binaryKey, ok := minKey.(ghostferry.BinaryKey)
	require.True(t, ok)
	assert.Equal(t, []byte{}, []byte(binaryKey))
}

func TestMinPaginationKey_String(t *testing.T) {
	column := &schema.TableColumn{
		Name: "key",
		Type: schema.TYPE_STRING,
	}

	minKey := ghostferry.MinPaginationKey(column)
	binaryKey, ok := minKey.(ghostferry.BinaryKey)
	require.True(t, ok)
	assert.Equal(t, []byte{}, []byte(binaryKey))
}


func TestMaxPaginationKey_Numeric(t *testing.T) {
	column := &schema.TableColumn{
		Name: "id",
		Type: schema.TYPE_NUMBER,
	}

	maxKey := ghostferry.MaxPaginationKey(column)
	uint64Key, ok := maxKey.(ghostferry.Uint64Key)
	require.True(t, ok)
	assert.Equal(t, uint64(math.MaxUint64), uint64(uint64Key))
}

func TestMaxPaginationKey_MediumInt(t *testing.T) {
	column := &schema.TableColumn{
		Name: "id",
		Type: schema.TYPE_MEDIUM_INT,
	}

	maxKey := ghostferry.MaxPaginationKey(column)
	uint64Key, ok := maxKey.(ghostferry.Uint64Key)
	require.True(t, ok)
	assert.Equal(t, uint64(math.MaxUint64), uint64(uint64Key))
}

func TestMaxPaginationKey_Binary_UUID16(t *testing.T) {
	column := &schema.TableColumn{
		Name:    "uuid",
		Type:    schema.TYPE_BINARY,
		MaxSize: 16,
	}

	maxKey := ghostferry.MaxPaginationKey(column)
	binaryKey, ok := maxKey.(ghostferry.BinaryKey)
	require.True(t, ok)
	assert.Equal(t, 16, len(binaryKey))

	for _, b := range binaryKey {
		assert.Equal(t, byte(0xFF), b)
	}
	assert.True(t, binaryKey.IsMax())
}

func TestMaxPaginationKey_Binary_LargeSize(t *testing.T) {
	column := &schema.TableColumn{
		Name:    "large",
		Type:    schema.TYPE_STRING,
		MaxSize: 100000,
	}

	maxKey := ghostferry.MaxPaginationKey(column)
	binaryKey, ok := maxKey.(ghostferry.BinaryKey)
	require.True(t, ok)
	assert.Equal(t, 4096, len(binaryKey))
}

func TestMaxPaginationKey_DefaultToNumeric(t *testing.T) {
	column := &schema.TableColumn{
		Name: "id",
		Type: 999,
	}

	maxKey := ghostferry.MaxPaginationKey(column)
	uint64Key, ok := maxKey.(ghostferry.Uint64Key)
	require.True(t, ok)
	assert.Equal(t, uint64(math.MaxUint64), uint64(uint64Key))
}

func TestPaginationKey_CrossTypeComparison_UUIDv7Ordering(t *testing.T) {
	uuidBytes1, _ := hex.DecodeString("018f3e4c5a6b7c8d9eafb0c1d2e3f405")
	uuidBytes2, _ := hex.DecodeString("018f3e4c5a6c7c8d9eafb0c1d2e3f405")
	uuidBytes3, _ := hex.DecodeString("018f3e4c5b6b7c8d9eafb0c1d2e3f405")

	key1 := ghostferry.NewBinaryKey(uuidBytes1)
	key2 := ghostferry.NewBinaryKey(uuidBytes2)
	key3 := ghostferry.NewBinaryKey(uuidBytes3)

	assert.Equal(t, -1, key1.Compare(key2))
	assert.Equal(t, -1, key1.Compare(key3))
	assert.Equal(t, -1, key2.Compare(key3))

	assert.True(t, key1.NumericPosition() < key2.NumericPosition())
	assert.True(t, key2.NumericPosition() < key3.NumericPosition())
}
