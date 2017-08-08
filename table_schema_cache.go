package ghostferry

import (
	"database/sql"
	"fmt"

	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

var ignoredDatabases = map[string]bool{
	"mysql":              true,
	"information_schema": true,
	"performance_schema": true,
	"sys":                true,
}

type TableSchemaCache map[string]*schema.Table

func quotedTableName(table *schema.Table) string {
	return quotedTableNameFromString(table.Schema, table.Name)
}

func quotedTableNameFromString(database, table string) string {
	return fmt.Sprintf("`%s`.`%s`", database, table)
}

func loadTables(db *sql.DB, applicableDatabases, applicableTables map[string]bool) (TableSchemaCache, error) {
	logger := logrus.WithField("tag", "table_schema_cache")

	tableSchemaCache := make(TableSchemaCache)

	dbnames, err := showDatabases(db)
	if err != nil {
		logger.WithError(err).Error("failed to show databases")
		return tableSchemaCache, err
	}

	// For each database, get a list of tables from it and cache the table's schema
	for _, dbname := range dbnames {
		dbLog := logger.WithField("database", dbname)

		if applicableDatabases != nil {
			if _, exists := applicableDatabases[dbname]; !exists {
				dbLog.Debug("skipping database as it is not applicable")
				continue
			}
		}

		dbLog.Debug("loading tables from database")
		tableNames, err := showTablesFrom(db, dbname)
		if err != nil {
			dbLog.WithError(err).Error("failed to show tables")
			return tableSchemaCache, err
		}

		for _, table := range tableNames {
			tableLog := dbLog.WithField("table", table)
			if applicableTables != nil {
				if _, exists := applicableTables[table]; !exists {
					tableLog.Debug("skipping table as it is not applicable")
					continue
				}
			}

			tableLog.Debug("caching table schema")
			tableSchema, err := schema.NewTableFromSqlDB(db, dbname, table)
			if err != nil {
				tableLog.WithError(err).Error("cannot fetch table schema from source db")
				return tableSchemaCache, err
			}

			// Sanity check
			if len(tableSchema.PKColumns) != 1 {
				err = fmt.Errorf("table %s has %d primary key columns and this is not supported", table, len(tableSchema.PKColumns))
				logger.WithError(err).Error("invalid table")
				return tableSchemaCache, err
			}

			if tableSchema.GetPKColumn(0).Type != schema.TYPE_NUMBER {
				err = fmt.Errorf("table %s is using a non-numeric primary key column and this is not supported", table)
				logger.WithError(err).Error("invalid table")
				return tableSchemaCache, err
			}

			tableSchemaCache[tableSchema.String()] = tableSchema
		}
	}

	logger.WithField("tables", tableSchemaCache.AllTableNames()).Info("table schemas cached")

	return tableSchemaCache, nil
}

func (c TableSchemaCache) AsSlice() (tables []*schema.Table) {
	for _, tableSchema := range c {
		tables = append(tables, tableSchema)
	}

	return
}

func (c TableSchemaCache) AllTableNames() (tableNames []string) {
	for tableName, _ := range c {
		tableNames = append(tableNames, tableName)
	}

	return
}

func (c TableSchemaCache) TableColumns(database, table string) ([]schema.TableColumn, error) {
	fullTableName := fmt.Sprintf("%s.%s", database, table)
	tableSchema, exists := c[fullTableName]
	if !exists {
		return []schema.TableColumn{}, fmt.Errorf("table %s does not exists", fullTableName)
	}

	return tableSchema.Columns, nil
}

func (c TableSchemaCache) ValuesMap(database, table string, values []interface{}) (map[string]interface{}, error) {
	tableColumns, err := c.TableColumns(database, table)
	if err != nil {
		return nil, err
	}

	err = verifyValuesHasTheSameLengthAsColumns(tableColumns, values, database, table)
	if err != nil {
		return nil, err
	}

	v := make(map[string]interface{})

	for i, column := range tableColumns {
		value := values[i]
		v[quoteField(column.Name)] = value
	}

	return v, nil
}

func verifyValuesHasTheSameLengthAsColumns(tableColumns []schema.TableColumn, values []interface{}, databaseHint, tableHint string) error {
	if len(tableColumns) != len(values) {
		return fmt.Errorf(
			"table %s.%s has %d columns but binlog has %d columns instead",
			databaseHint,
			tableHint,
			len(tableColumns),
			len(values),
		)
	}
	return nil
}

func showDatabases(c *sql.DB) ([]string, error) {
	rows, err := c.Query("show databases")
	if err != nil {
		return []string{}, err
	}

	defer rows.Close()

	databases := make([]string, 0)
	for rows.Next() {
		var database string
		err = rows.Scan(&database)
		if err != nil {
			return databases, err
		}

		if _, ignored := ignoredDatabases[database]; ignored {
			continue
		}

		databases = append(databases, database)
	}

	return databases, nil
}

func showTablesFrom(c *sql.DB, dbname string) ([]string, error) {
	rows, err := c.Query(fmt.Sprintf("show tables from %s", quoteField(dbname)))
	if err != nil {
		return []string{}, err
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return tables, err
		}

		tables = append(tables, table)
	}

	return tables, nil
}
