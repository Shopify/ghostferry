package testhelpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAssertQueriesHaveEqualResultReturnsRows(t *testing.T) {
	ferry := NewTestFerry()
	ferry.Initialize()

	rows := AssertQueriesHaveEqualResult(t, ferry.Ferry, "SELECT 1 as value UNION ALL SELECT 2 as value")
	assert.Equal(t, 2, len(rows))
	assert.Equal(t, []byte("1"), rows[0]["value"])
	assert.Equal(t, []byte("2"), rows[1]["value"])
}

func TestAssertQueriesHaveEqualResultFailsIfDifferent(t *testing.T) {
	ferry := NewTestFerry()
	ferry.Initialize()

	mockT := &testing.T{}
	AssertQueriesHaveEqualResult(mockT, ferry.Ferry, "SELECT @@server_id as server_id")
	assert.True(t, mockT.Failed())
}
