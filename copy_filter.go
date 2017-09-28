package ghostferry

import (
	sq "github.com/Masterminds/squirrel"
)

// CopyFilter provides an interface for restricting the copying to a subset of
// data. This typically involves adding a WHERE condition in the ConstrainSelect
// function, and returning false for unwanted rows in ApplicableEvent.
type CopyFilter interface {
	ConstrainSelect(sq.SelectBuilder) sq.SelectBuilder
	ApplicableEvent(DMLEvent) bool
}
