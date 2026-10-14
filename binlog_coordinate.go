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

	BinlogCoordinateGTID BinlogCoordinateType = "gtid"
)

// BinlogCoordinate is a representation-agnostic replication coordinate.
type BinlogCoordinate struct {
	Type         BinlogCoordinateType
	FilePosition mysql.Position

	GTIDSet string

	// parsedGTIDSet is a read-only cache (accessors clone); not serialized, reset on decode.
	parsedGTIDSet mysql.GTIDSet
}

// NewFilePositionCoordinate wraps a mysql.Position in a BinlogCoordinate.
func NewFilePositionCoordinate(pos mysql.Position) BinlogCoordinate {
	return BinlogCoordinate{
		Type:         BinlogCoordinateFilePosition,
		FilePosition: pos,
	}
}

// NewGTIDCoordinate builds a GTID coordinate from a canonical GTID set string.
func NewGTIDCoordinate(gtidSet string) BinlogCoordinate {
	return BinlogCoordinate{
		Type:    BinlogCoordinateGTID,
		GTIDSet: gtidSet,
	}
}

// NewGTIDCoordinateFromSet builds a GTID coordinate from a parsed set, cloning
// it so the coordinate does not alias the caller's set.
func NewGTIDCoordinateFromSet(set mysql.GTIDSet) BinlogCoordinate {
	if set == nil {
		return BinlogCoordinate{Type: BinlogCoordinateGTID}
	}
	cloned := set.Clone()
	return BinlogCoordinate{
		Type:          BinlogCoordinateGTID,
		GTIDSet:       cloned.String(),
		parsedGTIDSet: cloned,
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

func (c BinlogCoordinate) IsGTID() bool {
	return c.resolvedType() == BinlogCoordinateGTID
}

// Position returns the underlying mysql.Position; valid only for file/position coordinates.
func (c BinlogCoordinate) Position() mysql.Position {
	return c.FilePosition
}

// ParsedGTIDSet returns a fresh copy of the parsed GTID set; callers may mutate it freely.
func (c BinlogCoordinate) ParsedGTIDSet() (mysql.GTIDSet, error) {
	set, err := c.parsedSet()
	if err != nil {
		return nil, err
	}
	return set.Clone(), nil
}

// parsedSet returns the (possibly cached) set; callers must not mutate it.
func (c BinlogCoordinate) parsedSet() (mysql.GTIDSet, error) {
	if c.resolvedType() != BinlogCoordinateGTID {
		return nil, fmt.Errorf("parsedSet called on non-GTID coordinate of type %q", c.resolvedType())
	}
	if c.parsedGTIDSet != nil {
		return c.parsedGTIDSet, nil
	}
	return mysql.ParseMysqlGTIDSet(c.GTIDSet)
}

// IsZero treats an empty GTID set as zero, even though it semantically means "earlier than everything".
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

// HasReached reports whether a stream at c has covered everything up to target:
// position comparison for file/position, set containment for GTID (GTID sets
// have no total order). Mismatched types return an error.
func (c BinlogCoordinate) HasReached(target BinlogCoordinate) (bool, error) {
	if c.resolvedType() != target.resolvedType() {
		return false, fmt.Errorf(
			"cannot compare binlog coordinates of different types: %q vs %q",
			c.resolvedType(), target.resolvedType(),
		)
	}

	switch c.resolvedType() {
	case BinlogCoordinateFilePosition:
		return c.FilePosition.Compare(target.FilePosition) >= 0, nil
	case BinlogCoordinateGTID:
		sourceSet, err := c.parsedSet()
		if err != nil {
			return false, fmt.Errorf("parsing GTID set %q: %w", c.GTIDSet, err)
		}
		targetSet, err := target.parsedSet()
		if err != nil {
			return false, fmt.Errorf("parsing GTID set %q: %w", target.GTIDSet, err)
		}
		return sourceSet.Contain(targetSet), nil
	default:
		return false, fmt.Errorf("HasReached not supported for binlog coordinate type %q", c.resolvedType())
	}
}

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

// intersectGTIDSets computes A ∩ B = A - (A - B) on clones; go-mysql exposes no
// intersection primitive. Fail-closed: an error lets the caller refuse to resume
// rather than advancing the floor past what a consumer durably processed.
func intersectGTIDSets(a, b mysql.GTIDSet) (mysql.GTIDSet, error) {
	aMysql, aOK := a.(*mysql.MysqlGTIDSet)
	bMysql, bOK := b.(*mysql.MysqlGTIDSet)
	if !aOK || !bOK {
		return nil, fmt.Errorf("GTID intersection requires MySQL GTID sets, got %T and %T", a, b)
	}

	diff, ok := aMysql.Clone().(*mysql.MysqlGTIDSet)
	if !ok {
		return nil, fmt.Errorf("GTID intersection: unexpected clone type %T", aMysql.Clone())
	}
	if err := diff.Minus(*bMysql); err != nil {
		return nil, fmt.Errorf("GTID intersection (A - B): %w", err)
	}

	result, ok := aMysql.Clone().(*mysql.MysqlGTIDSet)
	if !ok {
		return nil, fmt.Errorf("GTID intersection: unexpected clone type %T", aMysql.Clone())
	}
	if err := result.Minus(*diff); err != nil {
		return nil, fmt.Errorf("GTID intersection (A - diff): %w", err)
	}

	return result, nil
}

// serializedBinlogCoordinate is the serialized shape of a BinlogCoordinate.
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

// UnmarshalJSON implements json.Unmarshaler. It also accepts a bare
// mysql.Position object for backwards compatibility.
func (c *BinlogCoordinate) UnmarshalJSON(data []byte) error {
	var typed serializedBinlogCoordinate
	if err := json.Unmarshal(data, &typed); err != nil {
		return err
	}
	if typed.Type != "" {
		switch typed.Type {
		case BinlogCoordinateFilePosition:
			var pos mysql.Position
			if typed.FilePosition != nil {
				pos = *typed.FilePosition
			}
			*c = NewFilePositionCoordinate(pos)
			return nil
		case BinlogCoordinateGTID:
			*c = NewGTIDCoordinate(typed.GTIDSet)
			return nil
		default:
			return fmt.Errorf("cannot unmarshal binlog coordinate of type %q", typed.Type)
		}
	}

	var pos mysql.Position
	if err := json.Unmarshal(data, &pos); err != nil {
		return err
	}
	*c = NewFilePositionCoordinate(pos)
	return nil
}
