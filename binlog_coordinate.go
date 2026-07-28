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

	// parsedGTIDSet is an optional cache of the parsed GTIDSet. It is populated
	// by NewGTIDCoordinateFromSet (and lazily by parsedSet) so that hot paths
	// such as the stop-condition check do not re-parse the canonical string on
	// every binlog event. It is never mutated in place; accessors that hand a
	// set to callers clone it. It is intentionally excluded from JSON.
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
// The string is not validated here; use ParsedGTIDSet or the DB read helpers
// when a parsed/validated set is required.
func NewGTIDCoordinate(gtidSet string) BinlogCoordinate {
	return BinlogCoordinate{
		Type:    BinlogCoordinateGTID,
		GTIDSet: gtidSet,
	}
}

// NewGTIDCoordinateFromSet builds a GTID coordinate from an already-parsed
// mysql.GTIDSet. The set is cloned so the coordinate does not alias (or later
// mutate) the caller's set, and the clone is cached to avoid re-parsing on hot
// paths. Prefer this over NewGTIDCoordinate when a parsed set is already in
// hand (e.g. from go-mysql event tracking).
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
// coordinate (the internal cache, if any, is cloned before returning).
func (c BinlogCoordinate) ParsedGTIDSet() (mysql.GTIDSet, error) {
	set, err := c.parsedSet()
	if err != nil {
		return nil, err
	}
	return set.Clone(), nil
}

// parsedSet returns the parsed GTID set, using the cache when present. The
// returned set MUST NOT be mutated by callers; use ParsedGTIDSet for a
// mutable clone. This exists so hot paths (HasReached) avoid re-parsing.
func (c BinlogCoordinate) parsedSet() (mysql.GTIDSet, error) {
	if c.resolvedType() != BinlogCoordinateGTID {
		return nil, fmt.Errorf("parsedSet called on non-GTID coordinate of type %q", c.resolvedType())
	}
	if c.parsedGTIDSet != nil {
		return c.parsedGTIDSet, nil
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

// HasReached reports whether this coordinate has reached or passed target,
// i.e. whether a stream positioned at c has already covered everything up to
// target. This is the single "have we crossed the finish line?" question used
// for stop and catchup conditions; it hides the representation-specific
// mechanics from callers.
//
//   - For file/position coordinates it is a position comparison (c >= target).
//   - For GTID coordinates it is set containment (c's set contains target's
//     set), because GTID sets do not form a total order.
//
// Both coordinates must be the same type; a mismatch returns an error rather
// than guessing across representations.
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
		// Use the cached parsed sets (read-only) to avoid re-parsing on hot
		// paths such as the per-event stop check. Contain does not mutate.
		mine, err := c.parsedSet()
		if err != nil {
			return false, fmt.Errorf("parsing GTID set %q: %w", c.GTIDSet, err)
		}
		theirs, err := target.parsedSet()
		if err != nil {
			return false, fmt.Errorf("parsing GTID set %q: %w", target.GTIDSet, err)
		}
		return mine.Contain(theirs), nil
	default:
		return false, fmt.Errorf("HasReached not supported for binlog coordinate type %q", c.resolvedType())
	}
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

// intersectGTIDSets returns the intersection of two MySQL GTID sets, i.e. the
// GTIDs present in both. go-mysql does not expose an intersection primitive, so
// this computes A ∩ B = A - (A - B).
//
// It operates on clones and does not mutate its inputs. It is fail-closed:
// rather than silently falling back to one side (which could advance a resume
// floor past what a consumer durably processed and skip events), it returns an
// error so the caller can refuse to resume.
func intersectGTIDSets(a, b mysql.GTIDSet) (mysql.GTIDSet, error) {
	aMysql, aOK := a.(*mysql.MysqlGTIDSet)
	bMysql, bOK := b.(*mysql.MysqlGTIDSet)
	if !aOK || !bOK {
		return nil, fmt.Errorf("GTID intersection requires MySQL GTID sets, got %T and %T", a, b)
	}

	// diff = A - B
	diff, ok := aMysql.Clone().(*mysql.MysqlGTIDSet)
	if !ok {
		return nil, fmt.Errorf("GTID intersection: unexpected clone type %T", aMysql.Clone())
	}
	if err := diff.Minus(*bMysql); err != nil {
		return nil, fmt.Errorf("GTID intersection (A - B): %w", err)
	}

	// result = A - diff = A ∩ B
	result, ok := aMysql.Clone().(*mysql.MysqlGTIDSet)
	if !ok {
		return nil, fmt.Errorf("GTID intersection: unexpected clone type %T", aMysql.Clone())
	}
	if err := result.Minus(*diff); err != nil {
		return nil, fmt.Errorf("GTID intersection (A - diff): %w", err)
	}

	return result, nil
}

// unionGTIDStringInto returns a new GTID set equal to base with the GTID(s) in
// add merged in. base may be nil (treated as empty). It never mutates base. It
// is used by failover recovery to fold an in-flight transaction's GTID into the
// already-applied set before validating a candidate master.
func unionGTIDStringInto(base mysql.GTIDSet, add string) (mysql.GTIDSet, error) {
	var result *mysql.MysqlGTIDSet
	if base == nil {
		empty, err := mysql.ParseMysqlGTIDSet("")
		if err != nil {
			return nil, err
		}
		result = empty.(*mysql.MysqlGTIDSet)
	} else {
		clone, ok := base.Clone().(*mysql.MysqlGTIDSet)
		if !ok {
			return nil, fmt.Errorf("GTID union requires a MySQL GTID set, got %T", base)
		}
		result = clone
	}

	if err := result.Update(add); err != nil {
		return nil, fmt.Errorf("GTID union: updating with %q: %w", add, err)
	}
	return result, nil
}

// unionGTIDSets returns a new GTID set equal to a with b merged in. Either may
// be nil (treated as empty). It never mutates its inputs. It is used by failover
// recovery to require a candidate master to contain both the applied set and the
// cutover stop target.
func unionGTIDSets(a, b mysql.GTIDSet) (mysql.GTIDSet, error) {
	var result *mysql.MysqlGTIDSet
	if a == nil {
		empty, err := mysql.ParseMysqlGTIDSet("")
		if err != nil {
			return nil, err
		}
		result = empty.(*mysql.MysqlGTIDSet)
	} else {
		clone, ok := a.Clone().(*mysql.MysqlGTIDSet)
		if !ok {
			return nil, fmt.Errorf("GTID union requires MySQL GTID sets, got %T", a)
		}
		result = clone
	}

	if b != nil {
		bMysql, ok := b.(*mysql.MysqlGTIDSet)
		if !ok {
			return nil, fmt.Errorf("GTID union requires MySQL GTID sets, got %T", b)
		}
		for _, uuidSet := range bMysql.Sets {
			// Clone so result never aliases b's internal UUIDSet (AddSet stores
			// the pointer directly when the SID is not already present).
			result.AddSet(uuidSet.Clone())
		}
	}

	return result, nil
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
