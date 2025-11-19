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

type PaginationKey interface {
	SQLValue() interface{}
	Compare(other PaginationKey) int
	NumericPosition() float64
	String() string
	MarshalJSON() ([]byte, error)
	IsMax() bool
}

type Uint64Key uint64

func NewUint64Key(value uint64) Uint64Key {
	return Uint64Key(value)
}

func (k Uint64Key) SQLValue() interface{} {
	return uint64(k)
}

func (k Uint64Key) Compare(other PaginationKey) int {
	otherKey, ok := other.(Uint64Key)
	if !ok {
		panic(fmt.Sprintf("cannot compare Uint64Key with %T", other))
	}

	if k < otherKey {
		return -1
	} else if k > otherKey {
		return 1
	}
	return 0
}

func (k Uint64Key) NumericPosition() float64 {
	return float64(k)
}

func (k Uint64Key) String() string {
	return fmt.Sprintf("%d", uint64(k))
}

func (k Uint64Key) IsMax() bool {
	return k == Uint64Key(math.MaxUint64)
}

func (k Uint64Key) MarshalJSON() ([]byte, error) {
	return json.Marshal(uint64(k))
}

type BinaryKey []byte

func NewBinaryKey(value []byte) BinaryKey {
	clone := make([]byte, len(value))
	copy(clone, value)
	return BinaryKey(clone)
}

func (k BinaryKey) SQLValue() interface{} {
	return []byte(k)
}

func (k BinaryKey) Compare(other PaginationKey) int {
	otherKey, ok := other.(BinaryKey)
	if !ok {
		panic(fmt.Sprintf("type mismatch: cannot compare BinaryKey with %T", other))
	}
	return bytes.Compare(k, otherKey)
}

// NumericPosition calculates a rough float position.
func (k BinaryKey) NumericPosition() float64 {
	if len(k) == 0 {
		return 0.0
	}

	// Take up to the first 8 bytes to form a uint64 for estimation
	var buf [8]byte
	copy(buf[:], k)

	val := binary.BigEndian.Uint64(buf[:])
	return float64(val)
}

func (k BinaryKey) String() string {
	return hex.EncodeToString(k)
}

func (k BinaryKey) IsMax() bool {
	// We cannot know the true "Max" of a VARBINARY without knowing the length.
	// However, for UUID(16), we can check for FF...
	if len(k) == 0 {
		return false
	}
	for _, b := range k {
		if b != 0xFF {
			return false
		}
	}
	return true
}

func (k BinaryKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(k))
}

type encodedKey struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
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
		Type:  typeName,
		Value: valBytes,
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
		return NewUint64Key(i), nil
	case "binary":
		var s string
		if err := json.Unmarshal(wrapper.Value, &s); err != nil {
			return nil, err
		}
		b, err := hex.DecodeString(s)
		if err != nil {
			return nil, err
		}
		return NewBinaryKey(b), nil
	default:
		return nil, fmt.Errorf("unknown key type: %s", wrapper.Type)
	}
}

func MinPaginationKey(column *schema.TableColumn) PaginationKey {
	switch column.Type {
	case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
		return NewUint64Key(0)
	// Handle all potential binary/string types
	case schema.TYPE_BINARY, schema.TYPE_STRING:
		// The smallest value for any binary/string type is an empty slice.
		// Even for fixed BINARY(N), starting at empty ensures we catch [0x00, ...]
		return NewBinaryKey([]byte{})
	default:
		return NewUint64Key(0)
	}
}

func MaxPaginationKey(column *schema.TableColumn) PaginationKey {
	switch column.Type {
	case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
		return NewUint64Key(math.MaxUint64)
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
		return NewBinaryKey(maxBytes)
	default:
		return NewUint64Key(math.MaxUint64)
	}
}
