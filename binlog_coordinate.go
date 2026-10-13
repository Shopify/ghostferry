package ghostferry

import (
	"encoding/json"
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// BinlogCoordinateType identifies how a binlog position is expressed.
type BinlogCoordinateType string

const (
	BinlogCoordinateFilePosition BinlogCoordinateType = "file_position"

	// BinlogCoordinateGTID is reserved for the future GTID coordinate.
	BinlogCoordinateGTID BinlogCoordinateType = "gtid"
)

// BinlogCoordinate is a representation-agnostic replication coordinate.
type BinlogCoordinate struct {
	Type         BinlogCoordinateType
	FilePosition mysql.Position
}

// NewFilePositionCoordinate wraps a mysql.Position in a BinlogCoordinate.
func NewFilePositionCoordinate(pos mysql.Position) BinlogCoordinate {
	return BinlogCoordinate{
		Type:         BinlogCoordinateFilePosition,
		FilePosition: pos,
	}
}

// resolvedType treats an empty Type as file/position for backwards compatibility.
func (c BinlogCoordinate) resolvedType() BinlogCoordinateType {
	if c.Type == "" {
		return BinlogCoordinateFilePosition
	}
	return c.Type
}

func (c BinlogCoordinate) IsFilePosition() bool {
	return c.resolvedType() == BinlogCoordinateFilePosition
}

// Position returns the underlying mysql.Position; valid only for file/position coordinates.
func (c BinlogCoordinate) Position() mysql.Position {
	return c.FilePosition
}

func (c BinlogCoordinate) IsZero() bool {
	switch c.resolvedType() {
	case BinlogCoordinateFilePosition:
		return c.FilePosition == (mysql.Position{})
	default:
		return false
	}
}

// Compare orders two coordinates of the same type. Comparing coordinates of
// different types is a programmer error and panics.
func (c BinlogCoordinate) Compare(other BinlogCoordinate) int {
	if c.resolvedType() != other.resolvedType() {
		panic(fmt.Sprintf(
			"cannot compare binlog coordinates of different types: %q vs %q",
			c.resolvedType(), other.resolvedType(),
		))
	}

	switch c.resolvedType() {
	case BinlogCoordinateFilePosition:
		return c.FilePosition.Compare(other.FilePosition)
	default:
		panic(fmt.Sprintf("comparison not implemented for binlog coordinate type %q", c.resolvedType()))
	}
}

func (c BinlogCoordinate) String() string {
	switch c.resolvedType() {
	case BinlogCoordinateFilePosition:
		return fmt.Sprintf("%s:%d", c.FilePosition.Name, c.FilePosition.Pos)
	default:
		return fmt.Sprintf("<unknown binlog coordinate type %q>", c.resolvedType())
	}
}

// serializedBinlogCoordinate is the serialized shape of a BinlogCoordinate.
type serializedBinlogCoordinate struct {
	Type         BinlogCoordinateType `json:"Type"`
	FilePosition *mysql.Position      `json:"FilePosition,omitempty"`
}

// MarshalJSON implements json.Marshaler.
func (c BinlogCoordinate) MarshalJSON() ([]byte, error) {
	out := serializedBinlogCoordinate{Type: c.resolvedType()}

	switch c.resolvedType() {
	case BinlogCoordinateFilePosition:
		pos := c.FilePosition
		out.FilePosition = &pos
	default:
		return nil, fmt.Errorf("cannot marshal binlog coordinate of type %q", c.resolvedType())
	}

	return json.Marshal(out)
}

// UnmarshalJSON implements json.Unmarshaler. It also accepts a bare
// mysql.Position object for backwards compatibility.
func (c *BinlogCoordinate) UnmarshalJSON(data []byte) error {
	var typed serializedBinlogCoordinate
	if err := json.Unmarshal(data, &typed); err != nil {
		return err
	}
	if typed.Type != "" {
		c.Type = typed.Type
		switch typed.Type {
		case BinlogCoordinateFilePosition:
			if typed.FilePosition != nil {
				c.FilePosition = *typed.FilePosition
			} else {
				c.FilePosition = mysql.Position{}
			}
			return nil
		default:
			return fmt.Errorf("cannot unmarshal binlog coordinate of type %q", typed.Type)
		}
	}

	var pos mysql.Position
	if err := json.Unmarshal(data, &pos); err != nil {
		return err
	}
	c.Type = BinlogCoordinateFilePosition
	c.FilePosition = pos
	return nil
}
