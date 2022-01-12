package testhelpers

import (
	sqlorig "database/sql"
	"strings"
	"testing"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/Shopify/ghostferry"
	"github.com/stretchr/testify/assert"
)

func PanicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func ProcessListContainsQueries(db *sql.DB, queries []string) bool {
	rows, err := db.Query("SHOW FULL PROCESSLIST")
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	queriesFound := make(map[string]bool)
	for _, query := range queries {
		queriesFound[query] = false
	}

	for rows.Next() {
		//columns, _ := rows.Columns()
		//data, err := ghostferry.ScanGenericRow(rows, len(columns))
		data, err := ghostferry.ScanGenericRow(rows, 8)
		if err != nil {
			panic(err)
		}

		if data[7] == nil {
			continue
		}

		info := data[7].([]byte)

		for query, found := range queriesFound {
			if !found && strings.HasSuffix(strings.TrimSpace(string(info)), query) {
				queriesFound[query] = true
				break
			}
		}
	}

	for _, found := range queriesFound {
		if !found {
			return false
		}
	}

	return true
}

func AssertTwoQueriesHaveEqualResult(t *testing.T, ferry *ghostferry.Ferry, sourceQuery string, targetQuery string, args ...interface{}) []map[string]interface{} {
	rows1, err := ferry.SourceDB.Query(sourceQuery, args...)
	assert.Nil(t, err)
	defer rows1.Close()

	rows2, err := ferry.TargetDB.Query(targetQuery, args...)
	assert.Nil(t, err)
	defer rows2.Close()

	results1, err := LoadResults(rows1)
	assert.Nil(t, err)

	results2, err := LoadResults(rows2)
	assert.Nil(t, err)

	assert.Equal(t, results1, results2)
	assert.True(t, len(results1) > 0)

	return results1
}

func AssertQueriesHaveEqualResult(t *testing.T, ferry *ghostferry.Ferry, query string, args ...interface{}) []map[string]interface{} {
	rows1, err := ferry.SourceDB.Query(query, args...)
	assert.Nil(t, err)
	defer rows1.Close()

	rows2, err := ferry.TargetDB.Query(query, args...)
	assert.Nil(t, err)
	defer rows2.Close()

	results1, err := LoadResults(rows1)
	assert.Nil(t, err)

	results2, err := LoadResults(rows2)
	assert.Nil(t, err)

	assert.Equal(t, results1, results2)
	assert.True(t, len(results1) > 0)

	return results1
}

func LoadResults(rows *sqlorig.Rows) (out []map[string]interface{}, err error) {
	var columns []string
	var row []interface{}

	columns, err = rows.Columns()
	if err != nil {
		return
	}

	for rows.Next() {
		row, err = ghostferry.ScanGenericRow(rows, len(columns))
		if err != nil {
			return
		}

		rowMap := make(map[string]interface{})
		for idx, val := range row {
			rowMap[columns[idx]] = val
		}
		out = append(out, rowMap)
	}
	return
}
