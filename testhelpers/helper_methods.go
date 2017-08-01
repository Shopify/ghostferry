package testhelpers

import (
	"database/sql"
	"strings"
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
		data := make([]interface{}, 10)
		dataPtrs := make([]interface{}, 10)
		for i, _ := range data {
			dataPtrs[i] = &data[i]
		}

		err = rows.Scan(dataPtrs...)
		if err != nil {
			panic(err)
		}

		if data[7] == nil {
			continue
		}

		info := data[7].([]byte)

		for query, found := range queriesFound {
			if !found && strings.TrimSpace(string(info)) == query {
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
