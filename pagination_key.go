package ghostferry

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"

	"github.com/go-mysql-org/go-mysql/schema"
)

// PaginationKey represents a cursor position for paginating through table rows.
// It abstracts over different primary key types (integers, UUIDs, binary data)
// to enable consistent batched iteration through tables.
type PaginationKey interface {
	// SQLValue returns the value to use in SQL WHERE clauses (e.g., WHERE id > ?).
	SQLValue() interface{}
	// ColumnName returns the column name this key belongs to, if known.
	ColumnName() string
	// Compare returns -1, 0, or 1 if this key is less than, equal to, or greater than other.
	Compare(other PaginationKey) int
	// NumericPosition returns a float64 approximation for progress tracking and estimation.
	NumericPosition() float64
	// String returns a human-readable representation for logging and debugging.
	String() string
	// MarshalJSON serializes the key for state persistence and checkpointing.
	MarshalJSON() ([]byte, error)
	// IsMax returns true if this key represents the maximum possible value for its type.
	IsMax() bool
}

type Uint64Key struct {
	Column string
	Value  uint64
}

func NewUint64Key(value uint64) Uint64Key {
	return Uint64Key{Value: value}
}

func NewUint64KeyWithColumn(column string, value uint64) Uint64Key {
	return Uint64Key{Column: column, Value: value}
}

func (k Uint64Key) SQLValue() interface{} {
	return k.Value
}

func (k Uint64Key) ColumnName() string {
	return k.Column
}

func (k Uint64Key) Compare(other PaginationKey) int {
	otherKey, ok := other.(Uint64Key)
	if !ok {
		panic(fmt.Sprintf("cannot compare Uint64Key with %T", other))
	}

	if k.Value < otherKey.Value {
		return -1
	} else if k.Value > otherKey.Value {
		return 1
	}
	return 0
}

func (k Uint64Key) NumericPosition() float64 {
	return float64(k.Value)
}

func (k Uint64Key) String() string {
	return fmt.Sprintf("%d", k.Value)
}

func (k Uint64Key) IsMax() bool {
	return k.Value == math.MaxUint64
}

func (k Uint64Key) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.Value)
}

type BinaryKey struct {
	Column string
	Value  []byte
}

func NewBinaryKey(value []byte) BinaryKey {
	clone := make([]byte, len(value))
	copy(clone, value)
	return BinaryKey{Value: clone}
}

func NewBinaryKeyWithColumn(column string, value []byte) BinaryKey {
	clone := make([]byte, len(value))
	copy(clone, value)
	return BinaryKey{Column: column, Value: clone}
}

func (k BinaryKey) SQLValue() interface{} {
	return k.Value
}

func (k BinaryKey) ColumnName() string {
	return k.Column
}

func (k BinaryKey) Compare(other PaginationKey) int {
	otherKey, ok := other.(BinaryKey)
	if !ok {
		panic(fmt.Sprintf("type mismatch: cannot compare BinaryKey with %T", other))
	}
	return bytes.Compare(k.Value, otherKey.Value)
}

// NumericPosition calculates a rough float position for progress tracking.
//
// Note: This method only uses the first 8 bytes of the binary key for progress calculation.
// This works well for timestamp-based keys like UUID v7 (where the first 48 bits are a timestamp),
// but progress may appear frozen when processing rows that differ only in bytes 9+.
// For random binary keys (like UUID v4), progress will be unpredictable.
//
// The core pagination algorithm (using Compare()) is unaffected and works correctly with any binary data.
func (k BinaryKey) NumericPosition() float64 {
	if len(k.Value) == 0 {
		return 0.0
	}

	// Take up to the first 8 bytes to form a uint64 for estimation
	var buf [8]byte
	copy(buf[:], k.Value)

	val := binary.BigEndian.Uint64(buf[:])
	return float64(val)
}

func (k BinaryKey) String() string {
	return hex.EncodeToString(k.Value)
}

func (k BinaryKey) IsMax() bool {
	// We cannot know the true "Max" of a VARBINARY without knowing the length.
	// However, for UUID(16), we can check for FF...
	if len(k.Value) == 0 {
		return false
	}
	for _, b := range k.Value {
		if b != 0xFF {
			return false
		}
	}
	return true
}

func (k BinaryKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(k.Value))
}

type encodedKey struct {
	Type   string          `json:"type"`
	Value  json.RawMessage `json:"value"`
	Column string          `json:"column,omitempty"`
}

func MarshalPaginationKey(k PaginationKey) ([]byte, error) {
	var typeName string
	var valBytes []byte
	var err error

	switch t := k.(type) {
	case Uint64Key:
		typeName = "uint64"
		valBytes, err = t.MarshalJSON()
	case BinaryKey:
		typeName = "binary"
		valBytes, err = t.MarshalJSON()
	default:
		return nil, fmt.Errorf("unknown pagination key type: %T", k)
	}

	if err != nil {
		return nil, err
	}

	return json.Marshal(encodedKey{
		Type:   typeName,
		Value:  valBytes,
		Column: k.ColumnName(),
	})
}

func UnmarshalPaginationKey(data []byte) (PaginationKey, error) {
	var wrapper encodedKey
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return nil, err
	}

	switch wrapper.Type {
	case "uint64":
		var i uint64
		if err := json.Unmarshal(wrapper.Value, &i); err != nil {
			return nil, err
		}
		key := NewUint64Key(i)
		key.Column = wrapper.Column
		return key, nil
	case "binary":
		var s string
		if err := json.Unmarshal(wrapper.Value, &s); err != nil {
			return nil, err
		}
		b, err := hex.DecodeString(s)
		if err != nil {
			return nil, err
		}
		key := NewBinaryKey(b)
		key.Column = wrapper.Column
		return key, nil
	default:
		return nil, fmt.Errorf("unknown key type: %s", wrapper.Type)
	}
}

func MinPaginationKey(column *schema.TableColumn) PaginationKey {
	switch column.Type {
	case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
		return NewUint64KeyWithColumn(column.Name, 0)
	// Handle all potential binary/string types
	case schema.TYPE_BINARY, schema.TYPE_STRING:
		// The smallest value for any binary/string type is an empty slice.
		// Even for fixed BINARY(N), starting at empty ensures we catch [0x00, ...]
		return NewBinaryKeyWithColumn(column.Name, []byte{})
	default:
		return NewUint64KeyWithColumn(column.Name, 0)
	}
}

func MaxPaginationKey(column *schema.TableColumn) PaginationKey {
	switch column.Type {
	case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
		return NewUint64KeyWithColumn(column.Name, math.MaxUint64)
	case schema.TYPE_BINARY, schema.TYPE_STRING:
		// SAFETY: Cap the size to prevent OOM on LONGBLOB (4GB).
		// InnoDB index limit is 3072 bytes. 4KB is a safe upper bound for a PK.
		size := column.MaxSize
		if size > 4096 {
			size = 4096
		}

		maxBytes := make([]byte, size)
		for i := range maxBytes {
			maxBytes[i] = 0xFF
		}
		return NewBinaryKeyWithColumn(column.Name, maxBytes)
	default:
		return NewUint64KeyWithColumn(column.Name, math.MaxUint64)
	}
}

// NewPaginationKeyFromRow extracts a pagination key from a row at the given index.
// It determines the appropriate key type based on the column schema.
func NewPaginationKeyFromRow(rowData RowData, index int, column *schema.TableColumn) (PaginationKey, error) {
	switch column.Type {
	case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
		value, err := rowData.GetUint64(index)
		if err != nil {
			return nil, fmt.Errorf("failed to get uint64 pagination key: %w", err)
		}
		return NewUint64KeyWithColumn(column.Name, value), nil

	case schema.TYPE_BINARY, schema.TYPE_STRING:
		valueInterface := rowData[index]
		var valueBytes []byte
		switch v := valueInterface.(type) {
		case []byte:
			valueBytes = v
		case string:
			valueBytes = []byte(v)
		default:
			return nil, fmt.Errorf("expected binary pagination key to be []byte or string, got %T", valueInterface)
		}
		return NewBinaryKeyWithColumn(column.Name, valueBytes), nil

	default:
		// Fallback for other integer types
		value, err := rowData.GetUint64(index)
		if err != nil {
			return nil, fmt.Errorf("failed to get pagination key: %w", err)
		}
		return NewUint64KeyWithColumn(column.Name, value), nil
	}
}
