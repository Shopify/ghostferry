package ghostferry

import (
	sqlorig "database/sql"
	"fmt"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

var ignoredDatabases = map[string]bool{
	"mysql":              true,
	"information_schema": true,
	"performance_schema": true,
	"sys":                true,
}

// A comparable and lightweight type that stores the schema and table name.
type TableIdentifier struct {
	SchemaName string
	TableName  string
}

func NewTableIdentifierFromSchemaTable(table *TableSchema) TableIdentifier {
	return TableIdentifier{
		SchemaName: table.Schema,
		TableName:  table.Name,
	}
}

// This is a wrapper on schema.Table with some custom information we need.
type TableSchema struct {
	*schema.Table

	CompressedColumnsForVerification map[string]string   // Map of column name => compression type
	IgnoredColumnsForVerification    map[string]struct{} // Set of column name
	PaginationKeyColumn              *schema.TableColumn
	PaginationKeyIndex               int

	rowMd5Query string
}

// This query returns the MD5 hash for a row on this table. This query is valid
// for both the source and the target shard.
//
// Any compressed columns specified via CompressedColumnsForVerification are
// excluded in this checksum and the raw data is returned directly.
//
// Any columns specified in IgnoredColumnsForVerification are excluded from the
// checksum and the raw data will not be returned.
//
// Note that the MD5 hash should consists of at least 1 column: the paginationKey column.
// This is to say that there should never be a case where the MD5 hash is
// derived from an empty string.
func (t *TableSchema) FingerprintQuery(schemaName, tableName string, numRows int) string {
	columnsToSelect := make([]string, 2+len(t.CompressedColumnsForVerification))
	columnsToSelect[0] = quoteField(t.GetPaginationColumn().Name)
	columnsToSelect[1] = t.RowMd5Query()
	i := 2
	for columnName, _ := range t.CompressedColumnsForVerification {
		columnsToSelect[i] = quoteField(columnName)
		i += 1
	}

	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s IN (%s)",
		strings.Join(columnsToSelect, ","),
		QuotedTableNameFromString(schemaName, tableName),
		columnsToSelect[0],
		strings.Repeat("?,", numRows-1)+"?",
	)
}

func (t *TableSchema) RowMd5Query() string {
	if t.rowMd5Query != "" {
		return t.rowMd5Query
	}

	columns := make([]schema.TableColumn, 0, len(t.Columns))
	for _, column := range t.Columns {
		_, isCompressed := t.CompressedColumnsForVerification[column.Name]
		_, isIgnored := t.IgnoredColumnsForVerification[column.Name]

		if isCompressed || isIgnored {
			continue
		}

		columns = append(columns, column)
	}

	hashStrs := make([]string, len(columns))
	for i, column := range columns {
		// Magic string that's unlikely to be a real record. For a history of this
		// issue, refer to https://github.com/Shopify/ghostferry/pull/137
		hashStrs[i] = fmt.Sprintf("MD5(COALESCE(%s, 'NULL_PBj}b]74P@JTo$5G_null'))", normalizeAndQuoteColumn(column))
	}

	t.rowMd5Query = fmt.Sprintf("MD5(CONCAT(%s)) AS __ghostferry_row_md5", strings.Join(hashStrs, ","))
	return t.rowMd5Query
}

type TableSchemaCache map[string]*TableSchema

func fullTableName(schemaName, tableName string) string {
	return fmt.Sprintf("%s.%s", schemaName, tableName)
}

func QuotedTableName(table *TableSchema) string {
	return QuotedTableNameFromString(table.Schema, table.Name)
}

func QuotedTableNameFromString(database, table string) string {
	return fmt.Sprintf("`%s`.`%s`", database, table)
}

func MaxPaginationKeys(db *sql.DB, tables []*TableSchema, logger *logrus.Entry) (map[*TableSchema]uint64, []*TableSchema, error) {
	tablesWithData := make(map[*TableSchema]uint64)
	emptyTables := make([]*TableSchema, 0, len(tables))

	for _, table := range tables {
		logger := logger.WithField("table", table.String())

		maxPaginationKey, maxPaginationKeyExists, err := maxPaginationKey(db, table)
		if err != nil {
			logger.WithError(err).Errorf("failed to get max primary key %s", table.GetPaginationColumn().Name)
			return tablesWithData, emptyTables, err
		}

		if !maxPaginationKeyExists {
			emptyTables = append(emptyTables, table)
			logger.Warn("no data in this table, skipping")
			continue
		}

		tablesWithData[table] = maxPaginationKey
	}

	return tablesWithData, emptyTables, nil
}

func LoadTables(db *sql.DB, tableFilter TableFilter, columnCompressionConfig ColumnCompressionConfig, columnIgnoreConfig ColumnIgnoreConfig, cascadingPaginationColumnConfig *CascadingPaginationColumnConfig) (TableSchemaCache, error) {
	logger := logrus.WithField("tag", "table_schema_cache")

	tableSchemaCache := make(TableSchemaCache)

	dbnames, err := showDatabases(db)
	if err != nil {
		logger.WithError(err).Error("failed to show databases")
		return tableSchemaCache, err
	}

	dbnames, err = tableFilter.ApplicableDatabases(dbnames)
	if err != nil {
		logger.WithError(err).Error("could not apply database filter")
		return tableSchemaCache, err
	}

	// For each database, get a list of tables from it and cache the table's schema
	for _, dbname := range dbnames {
		dbLog := logger.WithField("database", dbname)
		dbLog.Debug("loading tables from database")
		tableNames, err := showTablesFrom(db, dbname)
		if err != nil {
			dbLog.WithError(err).Error("failed to show tables")
			return tableSchemaCache, err
		}

		var tableSchemas []*TableSchema

		for _, table := range tableNames {
			tableLog := dbLog.WithField("table", table)
			tableLog.Debug("fetching table schema")
			tableSchema, err := schema.NewTableFromSqlDB(db.DB, dbname, table)
			if err != nil {
				tableLog.WithError(err).Error("cannot fetch table schema from source db")
				return tableSchemaCache, err
			}

			tableSchemas = append(tableSchemas, &TableSchema{
				Table:                            tableSchema,
				CompressedColumnsForVerification: columnCompressionConfig.CompressedColumnsFor(dbname, table),
				IgnoredColumnsForVerification:    columnIgnoreConfig.IgnoredColumnsFor(dbname, table),
			})
		}

		tableSchemas, err = tableFilter.ApplicableTables(tableSchemas)
		if err != nil {
			return tableSchemaCache, nil
		}

		for _, tableSchema := range tableSchemas {
			tableName := tableSchema.Name
			tableLog := dbLog.WithField("table", tableName)
			tableLog.Debug("caching table schema")

			paginationKeyColumn, paginationKeyIndex, err := tableSchema.paginationKeyColumn(cascadingPaginationColumnConfig)
			if err != nil {
				logger.WithError(err).Error("invalid table")
				return tableSchemaCache, err
			}
			tableSchema.PaginationKeyColumn = paginationKeyColumn
			tableSchema.PaginationKeyIndex = paginationKeyIndex

			tableSchemaCache[tableSchema.String()] = tableSchema
		}
	}

	logger.WithField("tables", tableSchemaCache.AllTableNames()).Info("table schemas cached")

	return tableSchemaCache, nil
}

func (t *TableSchema) findColumnByName(name string) (*schema.TableColumn, int, error) {
	for i, column := range t.Columns {
		if column.Name == name {
			return &column, i, nil
		}
	}
	return nil, -1, NonExistingPaginationKeyColumnError(t.Schema, t.Name, name)
}

// NonExistingPaginationKeyColumnError exported to facilitate black box testing
func NonExistingPaginationKeyColumnError(schema, table, paginationKey string) error {
	return fmt.Errorf("Pagination Key `%s` for %s non existent", paginationKey, QuotedTableNameFromString(schema, table))
}

// NonExistingPaginationKeyError exported to facilitate black box testing
func NonExistingPaginationKeyError(schema, table string) error {
	return fmt.Errorf("%s has no Primary Key to default to for Pagination purposes. Kindly specify a Pagination Key for this table in the CascadingPaginationColumnConfig", QuotedTableNameFromString(schema, table))
}

// NonNumericPaginationKeyError exported to facilitate black box testing
func NonNumericPaginationKeyError(schema, table, paginationKey string) error {
	return fmt.Errorf("Pagination Key `%s` for %s is non-numeric", paginationKey, QuotedTableNameFromString(schema, table))
}

func (t *TableSchema) paginationKeyColumn(cascadingPaginationColumnConfig *CascadingPaginationColumnConfig) (*schema.TableColumn, int, error) {
	var err error
	var paginationKeyColumn *schema.TableColumn
	var paginationKeyIndex int

	if paginationColumn, found := cascadingPaginationColumnConfig.PaginationColumnFor(t.Schema, t.Name); found {
		// Use per-schema, per-table pagination key from config
		paginationKeyColumn, paginationKeyIndex, err = t.findColumnByName(paginationColumn)
	} else if len(t.PKColumns) == 1 {
		// Use Primary Key
		paginationKeyIndex = t.PKColumns[0]
		paginationKeyColumn = &t.Columns[paginationKeyIndex]
	} else if fallbackColumnName, found := cascadingPaginationColumnConfig.FallbackPaginationColumnName(); found {
		// Try fallback from config
		paginationKeyColumn, paginationKeyIndex, err = t.findColumnByName(fallbackColumnName)
	} else {
		// No usable pagination key found
		err = NonExistingPaginationKeyError(t.Schema, t.Name)
	}

	if paginationKeyColumn != nil && paginationKeyColumn.Type != schema.TYPE_NUMBER {
		return nil, -1, NonNumericPaginationKeyError(t.Schema, t.Name, paginationKeyColumn.Name)
	}

	return paginationKeyColumn, paginationKeyIndex, err
}

// GetPaginationColumn retrieves PaginationKeyColumn
func (t *TableSchema) GetPaginationColumn() *schema.TableColumn {
	return t.PaginationKeyColumn
}

func (t *TableSchema) GetPaginationKeyIndex() int {
	return t.PaginationKeyIndex
}

func (c TableSchemaCache) AsSlice() (tables []*TableSchema) {
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

func (c TableSchemaCache) Get(database, table string) *TableSchema {
	return c[fullTableName(database, table)]
}

// Helper to sort a given map of tables with a second list giving a priority.
// If an element is present in the input and the priority lists, the item will
// appear first (in the order of the priority list), all other items appear in
// the order given in the input
func (c TableSchemaCache) GetTableListWithPriority(priorityList []string) (prioritzedTableNames []string) {
	// just a fast lookup if the list contains items already
	contains := map[string]struct{}{}
	if len(priorityList) >= 0 {
		for _, tableName := range priorityList {
			// ignore tables given in the priority list that we don't know
			if _, found := c[tableName]; found {
				contains[tableName] = struct{}{}
				prioritzedTableNames = append(prioritzedTableNames, tableName)
			}
		}
	}
	for tableName, _ := range c {
		if _, found := contains[tableName]; !found {
			prioritzedTableNames = append(prioritzedTableNames, tableName)
		}
	}

	return
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

func maxPaginationKey(db *sql.DB, table *TableSchema) (uint64, bool, error) {
	primaryKeyColumn := table.GetPaginationColumn()
	paginationKeyName := quoteField(primaryKeyColumn.Name)
	query, args, err := sq.
		Select(paginationKeyName).
		From(QuotedTableName(table)).
		OrderBy(fmt.Sprintf("%s DESC", paginationKeyName)).
		Limit(1).
		ToSql()

	if err != nil {
		return 0, false, err
	}

	var maxPaginationKey uint64
	err = db.QueryRow(query, args...).Scan(&maxPaginationKey)

	switch {
	case err == sqlorig.ErrNoRows:
		return 0, false, nil
	case err != nil:
		return 0, false, err
	default:
		return maxPaginationKey, true, nil
	}
}
