package ghostferry

import (
	sqlorig "database/sql"
	"errors"
	"fmt"
	"strings"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	sq "github.com/Masterminds/squirrel"
	"github.com/go-mysql-org/go-mysql/schema"
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
	ForcedIndexForVerification       string              // Forced index name
	PaginationKeyColumn              *schema.TableColumn // Deprecated: Use PaginationKeyColumns
	PaginationKeyIndex               int                 // Deprecated: Use PaginationKeyIndexes
	PaginationKeyColumns             []*schema.TableColumn
	PaginationKeyIndexes             []int

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
	var forceIndex string

	// Construct the column list.
	// Start with pagination key columns.
	paginationCols := t.GetPaginationColumns()
	columnsToSelect := make([]string, 0, len(paginationCols)+1+len(t.CompressedColumnsForVerification))
	for _, col := range paginationCols {
		columnsToSelect = append(columnsToSelect, QuoteField(col.Name))
	}

	// Add the MD5 hash column
	columnsToSelect = append(columnsToSelect, t.RowMd5Query())

	// Add compressed columns
	for columnName := range t.CompressedColumnsForVerification {
		columnsToSelect = append(columnsToSelect, QuoteField(columnName))
	}

	if t.ForcedIndexForVerification != "" {
		forceIndex = fmt.Sprintf(" FORCE INDEX (%s)", t.ForcedIndexForVerification)
	}

	// Build the WHERE clause
	var whereClause string
	if len(paginationCols) == 1 {
		// Single column: WHERE `id` IN (?,?,?)
		colName := QuoteField(paginationCols[0].Name)
		placeholders := make([]string, numRows)
		for i := range placeholders {
			placeholders[i] = "?"
		}
		whereClause = fmt.Sprintf("WHERE %s IN (%s)", colName, strings.Join(placeholders, ","))
	} else {
		// Composite key: WHERE (col1, col2) IN ((?,?), (?,?))
		pkColsQuoted := make([]string, len(paginationCols))
		for i, col := range paginationCols {
			pkColsQuoted[i] = QuoteField(col.Name)
		}
		pkTuple := fmt.Sprintf("(%s)", strings.Join(pkColsQuoted, ","))

		// Build placeholders: (?,?)
		placeholders := make([]string, len(paginationCols))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		tuplePlaceholder := fmt.Sprintf("(%s)", strings.Join(placeholders, ","))
		
		// Repeat tuple placeholders for numRows
		allPlaceholders := make([]string, numRows)
		for i := range allPlaceholders {
			allPlaceholders[i] = tuplePlaceholder
		}
		
		whereClause = fmt.Sprintf("WHERE %s IN (%s)", pkTuple, strings.Join(allPlaceholders, ","))
	}

	return fmt.Sprintf(
		"SELECT %s FROM %s%s %s",
		strings.Join(columnsToSelect, ","),
		QuotedTableNameFromString(schemaName, tableName),
		forceIndex,
		whereClause,
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

func MaxPaginationKeys(db *sql.DB, tables []*TableSchema, logger *logrus.Entry) (map[*TableSchema]PaginationKey, []*TableSchema, error) {
	tablesWithData := make(map[*TableSchema]PaginationKey)
	emptyTables := make([]*TableSchema, 0, len(tables))

	for _, table := range tables {
		logger := logger.WithField("table", table.String())

		maxPaginationKey, maxPaginationKeyExists, err := maxPaginationKey(db, table)
		if err != nil {
			logger.WithError(err).Errorf("failed to get max primary key for %s", table.String())
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

func LoadTables(db *sql.DB, tableFilter TableFilter, columnCompressionConfig ColumnCompressionConfig, columnIgnoreConfig ColumnIgnoreConfig, forceIndexConfig ForceIndexConfig, cascadingPaginationColumnConfig *CascadingPaginationColumnConfig) (TableSchemaCache, error) {
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

			// Filter out invisible indexes
			visibleIndexes := make([]*schema.Index, 0, len(tableSchema.Indexes))
			for _, index := range tableSchema.Indexes {
				if index.Visible {
					visibleIndexes = append(visibleIndexes, index)
				}
			}
			tableSchema.Indexes = visibleIndexes

			tableSchemas = append(tableSchemas, &TableSchema{
				Table:                            tableSchema,
				CompressedColumnsForVerification: columnCompressionConfig.CompressedColumnsFor(dbname, table),
				IgnoredColumnsForVerification:    columnIgnoreConfig.IgnoredColumnsFor(dbname, table),
				ForcedIndexForVerification:       forceIndexConfig.IndexFor(dbname, table),
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

			paginationKeyColumns, paginationKeyIndexes, err := tableSchema.getPaginationKeyColumns(cascadingPaginationColumnConfig)
			if err != nil {
				logger.WithError(err).Error("invalid table")
				return tableSchemaCache, err
			}
			tableSchema.PaginationKeyColumns = paginationKeyColumns
			tableSchema.PaginationKeyIndexes = paginationKeyIndexes
			
			// Backwards compatibility
			if len(paginationKeyColumns) > 0 {
				tableSchema.PaginationKeyColumn = paginationKeyColumns[0]
				tableSchema.PaginationKeyIndex = paginationKeyIndexes[0]
			}

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

func (t *TableSchema) getPaginationKeyColumns(cascadingPaginationColumnConfig *CascadingPaginationColumnConfig) ([]*schema.TableColumn, []int, error) {
	var err error
	var paginationKeyColumns []*schema.TableColumn
	var paginationKeyIndexes []int

	var paginationColumnStr string
	found := false

	if cascadingPaginationColumnConfig != nil {
		paginationColumnStr, found = cascadingPaginationColumnConfig.PaginationColumnFor(t.Schema, t.Name)
	}

	if found {
		// Configured
		cols := strings.Split(paginationColumnStr, ",")
		for _, colName := range cols {
			colName = strings.TrimSpace(colName)
			col, idx, e := t.findColumnByName(colName)
			if e != nil {
				return nil, nil, e
			}
			paginationKeyColumns = append(paginationKeyColumns, col)
			paginationKeyIndexes = append(paginationKeyIndexes, idx)
		}
	} else if len(t.PKColumns) > 0 {
		// Default to PK
		for _, idx := range t.PKColumns {
			paginationKeyIndexes = append(paginationKeyIndexes, idx)
			paginationKeyColumns = append(paginationKeyColumns, &t.Columns[idx])
		}
	} else if cascadingPaginationColumnConfig != nil {
		// Fallback
		if fallbackColumnName, ok := cascadingPaginationColumnConfig.FallbackPaginationColumnName(); ok {
			cols := strings.Split(fallbackColumnName, ",")
			for _, colName := range cols {
				colName = strings.TrimSpace(colName)
				col, idx, e := t.findColumnByName(colName)
				if e != nil {
					return nil, nil, e
				}
				paginationKeyColumns = append(paginationKeyColumns, col)
				paginationKeyIndexes = append(paginationKeyIndexes, idx)
			}
		} else {
			err = NonExistingPaginationKeyError(t.Schema, t.Name)
		}
	} else {
		err = NonExistingPaginationKeyError(t.Schema, t.Name)
	}
	
	if err != nil {
		return nil, nil, err
	}

	// Validate types
	for _, col := range paginationKeyColumns {
		isNumber := col.Type == schema.TYPE_NUMBER || col.Type == schema.TYPE_MEDIUM_INT
		isBinary := col.Type == schema.TYPE_BINARY || col.Type == schema.TYPE_STRING

		if !isNumber && !isBinary {
			return nil, nil, NonNumericPaginationKeyError(t.Schema, t.Name, col.Name)
		}
	}

	return paginationKeyColumns, paginationKeyIndexes, nil
}

// Deprecated: Use getPaginationKeyColumns
func (t *TableSchema) paginationKeyColumn(cascadingPaginationColumnConfig *CascadingPaginationColumnConfig) (*schema.TableColumn, int, error) {
	cols, idxs, err := t.getPaginationKeyColumns(cascadingPaginationColumnConfig)
	if err != nil {
		return nil, -1, err
	}
	if len(cols) == 0 {
		return nil, -1, NonExistingPaginationKeyError(t.Schema, t.Name)
	}
	return cols[0], idxs[0], nil
}


// GetPaginationColumn retrieves PaginationKeyColumn
// Deprecated: Use GetPaginationColumns
func (t *TableSchema) GetPaginationColumn() *schema.TableColumn {
	return t.PaginationKeyColumn
}

func (t *TableSchema) GetPaginationColumns() []*schema.TableColumn {
	return t.PaginationKeyColumns
}

// Deprecated: Use GetPaginationKeyIndexes
func (t *TableSchema) GetPaginationKeyIndex() int {
	return t.PaginationKeyIndex
}

func (t *TableSchema) GetPaginationKeyIndexes() []int {
	return t.PaginationKeyIndexes
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

func TargetToSourceRewrites(databaseRewrites map[string]string) (map[string]string, error) {
	targetToSourceRewrites := make(map[string]string)

	for sourceVal, targetVal := range databaseRewrites {
		if _, exists := targetToSourceRewrites[targetVal]; exists {
			return nil, errors.New("duplicate target to source rewrite detected")
		}
		targetToSourceRewrites[targetVal] = sourceVal
	}

	return targetToSourceRewrites, nil
}

// Helper to sort a given map of tables with a second list giving a priority.
// If an element is present in the input and the priority lists, the item will
// appear first (in the order of the priority list), all other items appear in
// the order given in the input
func (c TableSchemaCache) GetTableListWithPriority(priorityList []string) (prioritizedTableNames []string) {
	// just a fast lookup if the list contains items already
	contains := map[string]struct{}{}
	if len(priorityList) >= 0 {
		for _, tableName := range priorityList {
			// ignore tables given in the priority list that we don't know
			if _, found := c[tableName]; found {
				contains[tableName] = struct{}{}
				prioritizedTableNames = append(prioritizedTableNames, tableName)
			}
		}
	}
	for tableName, _ := range c {
		if _, found := contains[tableName]; !found {
			prioritizedTableNames = append(prioritizedTableNames, tableName)
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
	rows, err := c.Query(fmt.Sprintf("show tables from %s", QuoteField(dbname)))
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

func maxPaginationKey(db *sql.DB, table *TableSchema) (PaginationKey, bool, error) {
	primaryKeyColumns := table.GetPaginationColumns()
	if len(primaryKeyColumns) == 0 {
		return nil, false, fmt.Errorf("no pagination key columns for table %s", table.String())
	}

	pkNames := make([]string, len(primaryKeyColumns))
	orderByClauses := make([]string, len(primaryKeyColumns))
	for i, col := range primaryKeyColumns {
		quotedName := QuoteField(col.Name)
		pkNames[i] = quotedName
		orderByClauses[i] = fmt.Sprintf("%s DESC", quotedName)
	}

	query, args, err := sq.
		Select(pkNames...).
		From(QuotedTableName(table)).
		OrderBy(strings.Join(orderByClauses, ", ")).
		Limit(1).
		ToSql()

	if err != nil {
		return nil, false, err
	}

	scanArgs := make([]interface{}, len(primaryKeyColumns))
	// We need temp variables to hold scanned values
	values := make([]interface{}, len(primaryKeyColumns))

	for i, col := range primaryKeyColumns {
		switch col.Type {
		case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
			var v uint64
			values[i] = &v
			scanArgs[i] = &v
		case schema.TYPE_BINARY, schema.TYPE_STRING:
			// Use interface{} for flexbility (bytes or string)
			var v interface{}
			values[i] = &v
			scanArgs[i] = &v
		default:
			var v uint64
			values[i] = &v
			scanArgs[i] = &v
		}
	}

	err = db.QueryRow(query, args...).Scan(scanArgs...)
	if err != nil {
		if err == sqlorig.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, err
	}

	// Now convert scanned values to PaginationKey
	keys := make([]PaginationKey, len(primaryKeyColumns))
	for i, col := range primaryKeyColumns {
		switch col.Type {
		case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
			val := *(values[i].(*uint64))
			keys[i] = NewUint64Key(val)
		case schema.TYPE_BINARY, schema.TYPE_STRING:
			val := *(values[i].(*interface{}))
			var binValue []byte
			switch v := val.(type) {
			case []byte:
				binValue = v
			case string:
				binValue = []byte(v)
			default:
				return nil, false, fmt.Errorf("expected binary/string for max key column %s, got %T", col.Name, val)
			}
			keys[i] = NewBinaryKey(binValue)
		default:
			val := *(values[i].(*uint64))
			keys[i] = NewUint64Key(val)
		}
	}

	if len(keys) == 1 {
		return keys[0], true, nil
	}
	return CompositeKey(keys), true, nil
}

