package ghostferry

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/siddontang/go-mysql/schema"
)

// CopyFilter provides an interface for restricting the copying to a subset of
// data. This typically involves adding a WHERE condition in the ConstrainSelect
// function, and returning false for unwanted rows in ApplicableEvent.
type CopyFilter interface {
	// ConstrainSelect is used to augment the query used for batch data
	// copying, allowing for restricting copying to a subset of data.
	// Returning an error here will cause the query to be retried, until the
	// retry limit is reached, at which point the ferry will be aborted.
	ConstrainSelect(sq.SelectBuilder) (sq.SelectBuilder, error)

	// ApplicableEvent is used to filter events for rows that have been
	// filtered in ConstrainSelect. ApplicableEvent should return true if the
	// event is for a row that would be selected by ConstrainSelect, and false
	// otherwise.
	// Returning an error here will cause the ferry to be aborted.
	ApplicableEvent(DMLEvent) (bool, error)
}

type ApplicableFilter interface {
	ApplicableTables([]*schema.Table) []*schema.Table
	ApplicableDatabases([]string) []string
}
