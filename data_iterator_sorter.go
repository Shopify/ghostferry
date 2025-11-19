package ghostferry

import (
	"fmt"
	"sort"
	"strings"
)

// DataIteratorSorter is an interface for the DataIterator to choose which order it will process table
type DataIteratorSorter interface {
	Sort(unorderedTables map[*TableSchema]PaginationKey) ([]TableMaxPaginationKey, error)
}

// MaxPaginationKeySorter arranges table based on the MaxPaginationKey in DESC order
type MaxPaginationKeySorter struct{}

func (s *MaxPaginationKeySorter) Sort(unorderedTables map[*TableSchema]PaginationKey) ([]TableMaxPaginationKey, error) {
	orderedTables := make([]TableMaxPaginationKey, len(unorderedTables))
	i := 0

	for k, v := range unorderedTables {
		orderedTables[i] = TableMaxPaginationKey{k, v}
		i++
	}

	sort.Slice(orderedTables, func(i, j int) bool {
		return orderedTables[i].MaxPaginationKey.Compare(orderedTables[j].MaxPaginationKey) > 0
	})

	return orderedTables, nil
}

// MaxTableSizeSorter uses `information_schema.tables` to estimate the size of the DB and sorts tables in DESC order
type MaxTableSizeSorter struct {
	DataIterator *DataIterator
}

func (s *MaxTableSizeSorter) Sort(unorderedTables map[*TableSchema]PaginationKey) ([]TableMaxPaginationKey, error) {
	orderedTables := []TableMaxPaginationKey{}
	tableNames := []string{}
	databaseSchemasSet := map[string]struct{}{}
	databaseSchemas := []string{}

	for tableSchema, maxPK := range unorderedTables {
		orderedTables = append(orderedTables, TableMaxPaginationKey{tableSchema, maxPK})
		tableNames = append(tableNames, tableSchema.Name)

		if _, exists := databaseSchemasSet[tableSchema.Schema]; !exists {
			databaseSchemasSet[tableSchema.Schema] = struct{}{}
			databaseSchemas = append(databaseSchemas, tableSchema.Schema)
		}
	}

	query := fmt.Sprintf(`
		SELECT TABLE_NAME, TABLE_SCHEMA
		FROM information_schema.tables
		WHERE TABLE_SCHEMA IN ("%s")
			AND TABLE_NAME IN ("%s")
		ORDER BY DATA_LENGTH DESC`,
		strings.Join(databaseSchemas, `", "`),
		strings.Join(tableNames, `", "`),
	)
	rows, err := s.DataIterator.DB.Query(query)

	if err != nil {
		return orderedTables, err
	}

	defer rows.Close()

	databaseOrder := make(map[string]int, len(unorderedTables))
	i := 0
	for rows.Next() {
		var tableName, schemaName string
		err = rows.Scan(&tableName, &schemaName)

		if err != nil {
			return orderedTables, err
		}

		databaseOrder[fmt.Sprintf("%s %s", tableName, schemaName)] = i
		i++
	}

	sort.Slice(orderedTables, func(i, j int) bool {
		iName := fmt.Sprintf("%s %s", orderedTables[i].Table.Name, orderedTables[i].Table.Schema)
		jName := fmt.Sprintf("%s %s", orderedTables[j].Table.Name, orderedTables[j].Table.Schema)
		return databaseOrder[iName] < databaseOrder[jName]
	})

	return orderedTables, nil
}
