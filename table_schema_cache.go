package ghostferry

import (
	"fmt"

	"github.com/siddontang/go-mysql/client"
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

func loadTables(host string, port uint16, user, pass string, applicableDatabases, applicableTables map[string]bool) (TableSchemaCache, error) {
	// Have to connect directly to the database via the client from siddongtang
	// as the schema package requires a client rather than a sql.Db instance.
	// TODO: this is possibly something we can improve in the upstream library
	// so we don't have to use this hack.
	logger := logrus.WithFields(logrus.Fields{
		"tag":  "table_schema_cache",
		"host": host,
		"port": port,
		"user": user,
		"pass": pass,
	})

	tableSchemaCache := make(TableSchemaCache)

	logger.Info("loading table schemas from database")
	dbClient, err := client.Connect(fmt.Sprintf("%s:%d", host, port), user, pass, "")
	if err != nil {
		logger.WithError(err).Error("failed to connect to database to list tables")
		return tableSchemaCache, err
	}
	defer dbClient.Close()

	dbnames, err := showDatabases(dbClient)
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
		tableNames, err := showTablesFrom(dbClient, dbname)
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
			tableSchema, err := schema.NewTable(dbClient, dbname, table)
			if err != nil {
				tableLog.WithError(err).Error("cannot fetch table schema from source db")
				return tableSchemaCache, err
			}

			// Sanity check
			if len(tableSchema.PKColumns) != 1 {
				err = fmt.Errorf("table %s has multiple primary key columns and this is not supported", table)
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

func (c TableSchemaCache) AsStringSlice() (tables []string) {
	for tableName, _ := range c {
		tables = append(tables, tableName)
	}

	return
}

func (c TableSchemaCache) AllTableNames() (tableNames []string) {
	for tableName, _ := range c {
		tableNames = append(tableNames, tableName)
	}

	return
}

func (c TableSchemaCache) TableColumnNames(database, table string) ([]string, error) {
	fullTableName := fmt.Sprintf("%s.%s", database, table)
	tableSchema, exists := c[fullTableName]
	if !exists {
		return []string{}, fmt.Errorf("table %s does not exists", fullTableName)
	}

	tableColumns := make([]string, len(tableSchema.Columns))
	for i, col := range tableSchema.Columns {
		tableColumns[i] = col.Name
	}

	return tableColumns, nil
}

func showDatabases(c *client.Conn) ([]string, error) {
	r, err := c.Execute("show databases")
	if err != nil {
		return []string{}, err
	}

	databases := make([]string, 0)
	for i := 0; i < r.RowNumber(); i++ {
		database, err := r.GetString(i, 0)
		if err != nil {
			return []string{}, err
		}

		if _, ignored := ignoredDatabases[database]; ignored {
			continue
		}

		databases = append(databases, database)
	}

	return databases, nil
}

func showTablesFrom(c *client.Conn, dbname string) ([]string, error) {
	r, err := c.Execute(fmt.Sprintf("show tables from %s", dbname))
	if err != nil {
		return []string{}, err
	}

	tables := make([]string, r.RowNumber())
	for i := 0; i < r.RowNumber(); i++ {
		tables[i], err = r.GetString(i, 0)
		if err != nil {
			return []string{}, err
		}
	}

	return tables, nil
}
