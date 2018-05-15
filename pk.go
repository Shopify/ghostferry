package ghostferry

import (
	"fmt"
	"math"

	"github.com/siddontang/go-mysql/schema"
)

type PK struct {
	Value interface{}
}

func (pk PK) Compare(other PK) int {
	switch v1 := pk.Value.(type) {
	case uint64:
		v2 := other.Value.(uint64)

		if v1 < v2 {
			return -1
		} else if v1 == v2 {
			return 0
		} else {
			return 1
		}

	default:
		panic(fmt.Sprintf("pk type %T are current not supported", v1))
	}
}

// Could possibly upstream these functions in the future.
func MinPK(column *schema.TableColumn) PK {
	switch column.Type {
	case schema.TYPE_NUMBER:
		return PK{Value: uint64(0)}
	default:
		panic(fmt.Sprintf("non integer type is currently not supported"))
	}
}

func MaxPK(column *schema.TableColumn) PK {
	switch column.Type {
	case schema.TYPE_NUMBER:
		return PK{Value: uint64(math.MaxUint64)}
	default:
		panic(fmt.Sprintf("non integer type is currently not supported"))
	}
}
