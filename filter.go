package ghostferry

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/siddontang/go-mysql/schema"
)

// CopyFilter provides an interface for restricting the copying to a subset of
// data. This typically involves adding a WHERE condition in the ConstrainSelect
// function, and returning false for unwanted rows in ApplicableEvent.
type CopyFilter interface {
	// BuildSelect is used to set up the query used for batch data copying,
	// allowing for restricting copying to a subset of data. Returning an error
	// here will cause the query to be retried, until the retry limit is
	// reached, at which point the ferry will be aborted. BuildSelect is passed
	// the columns to be selected, table being copied, the last primary key value
	// from the previous batch, and the batch size. Call DefaultBuildSelect to
	// generate the default query, which may be used as a starting point.
	BuildSelect([]string, *schema.Table, uint64, uint64) (sq.SelectBuilder, error)

	// ApplicableEvent is used to filter events for rows that have been
	// filtered in ConstrainSelect. ApplicableEvent should return true if the
	// event is for a row that would be selected by ConstrainSelect, and false
	// otherwise.
	// Returning an error here will cause the ferry to be aborted.
	ApplicableEvent(DMLEvent) (bool, error)
}

type TableFilter interface {
	ApplicableTables([]*schema.Table) ([]*schema.Table, error)
	ApplicableDatabases([]string) ([]string, error)
}
