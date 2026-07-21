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
// It can express either a file/position (mysql.Position) or a GTID set. It
// exists so that the rest of Ghostferry can be migrated to talk in terms of "a
// coordinate" rather than "a file and a position", which is a prerequisite for
// adding a GTID mode behind a feature flag without a second invasive refactor.
//
// The zero value is a zero file/position coordinate, matching the previous
// behavior where an empty mysql.Position was used as the "no coordinate"
// sentinel.
//
// GTID coordinates are stored canonically as the GTID set string. The parsed
// mysql.GTIDSet is derived on demand; it is intentionally not stored so that
// the value type stays trivially copyable and comparable-by-value-free (GTID
// sets are mutable and must be cloned before mutation).
type BinlogCoordinate struct {
	// Type selects the active representation. An empty Type is treated as
	// BinlogCoordinateFilePosition for backwards compatibility.
	Type BinlogCoordinateType

	// FilePosition holds the coordinate when Type is BinlogCoordinateFilePosition.
	FilePosition mysql.Position

	// GTIDSet holds the coordinate as a canonical MySQL GTID set string when
	// Type is BinlogCoordinateGTID, e.g.
	// "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-57". An empty string means "no
	// GTIDs" which is distinct from a nil/unset coordinate; callers that need
	// that distinction should check Type and IsZero together.
	GTIDSet string
}

// NewFilePositionCoordinate wraps a mysql.Position in a BinlogCoordinate.
func NewFilePositionCoordinate(pos mysql.Position) BinlogCoordinate {
	return BinlogCoordinate{
		Type:         BinlogCoordinateFilePosition,
		FilePosition: pos,
	}
}

// NewGTIDCoordinate builds a GTID coordinate from a canonical GTID set string.
// The string is not validated here; use ParsedGTIDSet or the DB read helpers
// when a parsed/validated set is required.
func NewGTIDCoordinate(gtidSet string) BinlogCoordinate {
	return BinlogCoordinate{
		Type:    BinlogCoordinateGTID,
		GTIDSet: gtidSet,
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

// IsGTID reports whether this coordinate is a GTID coordinate.
func (c BinlogCoordinate) IsGTID() bool {
	return c.resolvedType() == BinlogCoordinateGTID
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

// ParsedGTIDSet parses and returns the underlying GTID set.
//
// It is valid to call this only for GTID coordinates. A fresh mysql.GTIDSet is
// returned on each call so callers may mutate it freely without affecting the
// coordinate.
func (c BinlogCoordinate) ParsedGTIDSet() (mysql.GTIDSet, error) {
	if c.resolvedType() != BinlogCoordinateGTID {
		return nil, fmt.Errorf("ParsedGTIDSet called on non-GTID coordinate of type %q", c.resolvedType())
	}
	return mysql.ParseMysqlGTIDSet(c.GTIDSet)
}

// IsZero reports whether the coordinate carries no meaningful position. This
// mirrors the previous use of an empty mysql.Position as the "unset" sentinel.
//
// For GTID coordinates, an empty GTID set string is considered zero. Note that
// an empty GTID set is semantically "earlier than everything"; callers that
// must distinguish "no usable coordinate" from "genuinely empty GTID set"
// should track that distinction separately (e.g. a nil coordinate pointer).
func (c BinlogCoordinate) IsZero() bool {
	switch c.resolvedType() {
	case BinlogCoordinateFilePosition:
		return c.FilePosition == (mysql.Position{})
	case BinlogCoordinateGTID:
		return c.GTIDSet == ""
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
//
// GTID coordinates do not form a total order (a set can contain another, be
// contained by it, both, or neither). Compare therefore does not support GTID
// coordinates; use Contains for GTID reachability checks instead. This mirrors
// the reality that "which GTID set is further ahead" is not well defined.
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
		panic(fmt.Sprintf("Compare not supported for binlog coordinate type %q; use Contains for GTID", c.resolvedType()))
	}
}

// Contains reports whether this GTID coordinate's set fully contains the other
// GTID coordinate's set. This is the correct "have we reached/passed" check for
// GTID based stop and catchup conditions.
//
// Both coordinates must be GTID coordinates. Calling Contains on file/position
// coordinates returns an error, since containment is not the file/position
// reachability model (Compare is).
func (c BinlogCoordinate) Contains(other BinlogCoordinate) (bool, error) {
	if c.resolvedType() != BinlogCoordinateGTID || other.resolvedType() != BinlogCoordinateGTID {
		return false, fmt.Errorf(
			"Contains is only defined for GTID coordinates, got %q and %q",
			c.resolvedType(), other.resolvedType(),
		)
	}

	mine, err := c.ParsedGTIDSet()
	if err != nil {
		return false, fmt.Errorf("parsing GTID set %q: %w", c.GTIDSet, err)
	}
	theirs, err := other.ParsedGTIDSet()
	if err != nil {
		return false, fmt.Errorf("parsing GTID set %q: %w", other.GTIDSet, err)
	}

	return mine.Contain(theirs), nil
}

// String returns a human readable representation for logs and status output.
func (c BinlogCoordinate) String() string {
	switch c.resolvedType() {
	case BinlogCoordinateFilePosition:
		return fmt.Sprintf("%s:%d", c.FilePosition.Name, c.FilePosition.Pos)
	case BinlogCoordinateGTID:
		return fmt.Sprintf("gtid:%s", c.GTIDSet)
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
	GTIDSet      string               `json:"GTIDSet,omitempty"`
}

// MarshalJSON implements json.Marshaler.
func (c BinlogCoordinate) MarshalJSON() ([]byte, error) {
	out := serializedBinlogCoordinate{Type: c.resolvedType()}

	switch c.resolvedType() {
	case BinlogCoordinateFilePosition:
		pos := c.FilePosition
		out.FilePosition = &pos
	case BinlogCoordinateGTID:
		out.GTIDSet = c.GTIDSet
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
		case BinlogCoordinateGTID:
			c.GTIDSet = typed.GTIDSet
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
