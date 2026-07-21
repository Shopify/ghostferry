package ghostferry

import (
	"encoding/json"
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// BinlogCoordinateType identifies how a binlog position is expressed.
//
// This is the seam that lets Ghostferry evolve from file/position based
// replication coordinates towards GTID based coordinates without forcing every
// consumer to know which representation is in use. In this first iteration only
// the file/position representation is implemented; the GTID representation is
// reserved so that state serialized today remains forward compatible.
type BinlogCoordinateType string

const (
	// BinlogCoordinateFilePosition is the classic (file, position) coordinate.
	BinlogCoordinateFilePosition BinlogCoordinateType = "file_position"

	// BinlogCoordinateGTID is reserved for the future GTID based coordinate.
	// It is intentionally defined now so that the serialization format and the
	// strategy dispatch have a stable name to target.
	BinlogCoordinateGTID BinlogCoordinateType = "gtid"
)

// BinlogCoordinate is a representation-agnostic replication coordinate.
//
// Today it only wraps a file/position (mysql.Position). It exists so that the
// rest of Ghostferry can be migrated to talk in terms of "a coordinate" rather
// than "a file and a position", which is a prerequisite for adding a GTID mode
// behind a feature flag without a second invasive refactor.
//
// The zero value is a zero file/position coordinate, matching the previous
// behavior where an empty mysql.Position was used as the "no coordinate"
// sentinel.
type BinlogCoordinate struct {
	// Type selects the active representation. An empty Type is treated as
	// BinlogCoordinateFilePosition for backwards compatibility.
	Type BinlogCoordinateType

	// FilePosition holds the coordinate when Type is BinlogCoordinateFilePosition.
	FilePosition mysql.Position
}

// NewFilePositionCoordinate wraps a mysql.Position in a BinlogCoordinate.
func NewFilePositionCoordinate(pos mysql.Position) BinlogCoordinate {
	return BinlogCoordinate{
		Type:         BinlogCoordinateFilePosition,
		FilePosition: pos,
	}
}

// resolvedType returns the effective type, treating the empty string as
// file/position for backwards compatibility with older serialized state and
// zero-valued coordinates.
func (c BinlogCoordinate) resolvedType() BinlogCoordinateType {
	if c.Type == "" {
		return BinlogCoordinateFilePosition
	}
	return c.Type
}

// IsFilePosition reports whether this coordinate is a file/position coordinate.
func (c BinlogCoordinate) IsFilePosition() bool {
	return c.resolvedType() == BinlogCoordinateFilePosition
}

// Position returns the underlying mysql.Position.
//
// It is valid to call this only for file/position coordinates. This accessor
// exists to keep the migration incremental: consumers that still require a
// concrete mysql.Position can obtain it here while the surrounding plumbing is
// converted to BinlogCoordinate.
func (c BinlogCoordinate) Position() mysql.Position {
	return c.FilePosition
}

// IsZero reports whether the coordinate carries no meaningful position. This
// mirrors the previous use of an empty mysql.Position as the "unset" sentinel.
func (c BinlogCoordinate) IsZero() bool {
	switch c.resolvedType() {
	case BinlogCoordinateFilePosition:
		return c.FilePosition == (mysql.Position{})
	default:
		return false
	}
}

// Compare orders two coordinates of the same type.
//
// It returns -1, 0 or 1 following the semantics of mysql.Position.Compare for
// file/position coordinates. Comparing coordinates of differing types is a
// programmer error and panics, because there is no meaningful total ordering
// across representations. Callers that may receive mixed types should branch on
// the coordinate type first.
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

// String returns a human readable representation for logs and status output.
func (c BinlogCoordinate) String() string {
	switch c.resolvedType() {
	case BinlogCoordinateFilePosition:
		return fmt.Sprintf("%s:%d", c.FilePosition.Name, c.FilePosition.Pos)
	default:
		return fmt.Sprintf("<unknown binlog coordinate type %q>", c.resolvedType())
	}
}

// serializedBinlogCoordinate is the on-disk / on-wire shape of a
// BinlogCoordinate. It is deliberately explicit and self-describing so that a
// future GTID variant can be added as additional fields without breaking
// existing readers.
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

// UnmarshalJSON implements json.Unmarshaler.
//
// It accepts both the new self-describing form and, for defensive
// compatibility, a bare mysql.Position object of the form {"Name":...,"Pos":...}.
func (c *BinlogCoordinate) UnmarshalJSON(data []byte) error {
	var typed serializedBinlogCoordinate
	if err := json.Unmarshal(data, &typed); err == nil && typed.Type != "" {
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

	// Fall back to a bare mysql.Position for older/simpler payloads.
	var pos mysql.Position
	if err := json.Unmarshal(data, &pos); err != nil {
		return err
	}
	c.Type = BinlogCoordinateFilePosition
	c.FilePosition = pos
	return nil
}
