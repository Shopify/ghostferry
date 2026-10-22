package sharding

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/go-sql-driver/mysql"
)

// CutoverDependenciesConfig is an opt-in, bounded copy for dependencies whose
// writers the caller must stop through CutoverLock. These tables are not streamed.
type CutoverDependenciesConfig struct {
	Tables          []CutoverDependency
	MaxRowsPerTable int
	MaxBytes        int64
	TimeoutSeconds  int
}

type CutoverDependency struct {
	Table            string
	ReferenceTable   string
	PaginationColumn string
	IdentityColumns  []string
	JoinColumns      []DependencyJoin
	ReferenceEquals  map[string]interface{}
	Equals           map[string]interface{}
	RequireMatch     bool
}

type DependencyJoin struct {
	Column          string
	ReferenceColumn string
}

var dependencyIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func (c *Config) validateCutoverDependencies() error {
	d := c.CutoverDependencies
	if d == nil {
		return nil
	}
	if len(d.Tables) == 0 || d.MaxRowsPerTable <= 0 || d.MaxRowsPerTable > 100000 || d.MaxBytes <= 0 || d.TimeoutSeconds <= 0 || d.TimeoutSeconds > 300 {
		return fmt.Errorf("CutoverDependencies requires tables, MaxRowsPerTable (1..100000), MaxBytes and TimeoutSeconds (1..300)")
	}
	if c.CutoverLock.URI == "" {
		return fmt.Errorf("CutoverDependencies requires a CutoverLock callback that stops dependency writers")
	}
	names := map[string]bool{}
	for _, t := range d.Tables {
		if names[t.Table] {
			return fmt.Errorf("duplicate cutover dependency %q", t.Table)
		}
		names[t.Table] = true
		if len(t.JoinColumns) == 0 || len(t.IdentityColumns) == 0 || t.Table == t.ReferenceTable {
			return fmt.Errorf("dependency %q requires a distinct reference table, joins and identity columns", t.Table)
		}
		if _, ok := c.JoinedTables[t.Table]; ok {
			return fmt.Errorf("dependency %q cannot also be a JoinedTable", t.Table)
		}
		for _, name := range c.PrimaryKeyTables {
			if name == t.Table {
				return fmt.Errorf("dependency %q cannot also be a PrimaryKeyTable", t.Table)
			}
		}
		identifiers := []string{c.SourceDB, c.TargetDB, c.ShardingKey, t.Table, t.ReferenceTable, t.PaginationColumn}
		seen := map[string]bool{}
		for _, name := range t.IdentityColumns {
			if seen[name] || name == t.PaginationColumn {
				return fmt.Errorf("dependency %q requires distinct logical identity columns separate from pagination", t.Table)
			}
			seen[name] = true
			identifiers = append(identifiers, name)
		}
		for _, join := range t.JoinColumns {
			identifiers = append(identifiers, join.Column, join.ReferenceColumn)
		}
		for _, conditions := range []map[string]interface{}{t.ReferenceEquals, t.Equals} {
			for name, value := range conditions {
				identifiers = append(identifiers, name)
				switch value.(type) {
				case nil, string, bool, float64, int, int64:
				default:
					return fmt.Errorf("dependency %q condition %q must be a scalar", t.Table, name)
				}
			}
		}
		for _, name := range identifiers {
			if !dependencyIdentifier.MatchString(name) {
				return fmt.Errorf("invalid dependency identifier %q", name)
			}
		}
	}
	for _, t := range d.Tables {
		if names[t.ReferenceTable] {
			return fmt.Errorf("dependency chains are not supported: %q", t.ReferenceTable)
		}
	}
	return nil
}

func (r *ShardingFerry) copyCutoverDependencies() error {
	config := r.config.CutoverDependencies
	if config == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.TimeoutSeconds)*time.Second)
	defer cancel()
	source, err := r.Ferry.SourceDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	defer source.Rollback()
	target, err := r.Ferry.TargetDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer target.Rollback()
	var copiedBytes int64
	for _, table := range config.Tables {
		if err := r.copyDependency(ctx, source, target, table, &copiedBytes); err != nil {
			return fmt.Errorf("cutover dependency %s: %w", table.Table, err)
		}
	}
	return target.Commit()
}

type dependencyColumn struct {
	Name      string
	Type      string
	Nullable  string
	Extra     string
	Collation string
}

func dependencyColumns(ctx context.Context, tx *sql.Tx, database, table string) ([]dependencyColumn, error) {
	rows, err := tx.QueryContext(ctx, "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, EXTRA, COALESCE(COLLATION_NAME, '') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION", database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []dependencyColumn
	for rows.Next() {
		var c dependencyColumn
		if err := rows.Scan(&c.Name, &c.Type, &c.Nullable, &c.Extra, &c.Collation); err != nil {
			return nil, err
		}
		columns = append(columns, c)
	}
	return columns, rows.Err()
}

func dependencyHasUniqueKey(ctx context.Context, tx *sql.Tx, database, table string, columns []string, primary bool) (bool, error) {
	rows, err := tx.QueryContext(ctx, "SELECT INDEX_NAME, COLUMN_NAME, SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0 ORDER BY INDEX_NAME, SEQ_IN_INDEX", database, table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	keys := map[string][]string{}
	for rows.Next() {
		var index string
		var column sql.NullString
		var prefix sql.NullInt64
		if err := rows.Scan(&index, &column, &prefix); err != nil {
			return false, err
		}
		if prefix.Valid || !column.Valid {
			keys[index] = append(keys[index], "")
		} else {
			keys[index] = append(keys[index], column.String)
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	for index, key := range keys {
		if (!primary || index == "PRIMARY") && reflect.DeepEqual(key, columns) {
			return true, nil
		}
	}
	return false, nil
}

func (r *ShardingFerry) dependencySchema(ctx context.Context, source, target *sql.Tx, table CutoverDependency) ([]string, error) {
	for _, side := range []struct {
		tx              *sql.Tx
		database, table string
	}{
		{source, r.config.SourceDB, table.Table},
		{source, r.config.SourceDB, table.ReferenceTable},
		{target, r.config.TargetDB, table.Table},
	} {
		var engine string
		if err := side.tx.QueryRowContext(ctx, "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", side.database, side.table).Scan(&engine); err != nil {
			return nil, err
		}
		if engine != "InnoDB" {
			return nil, fmt.Errorf("dependency and reference tables must use InnoDB")
		}
	}
	var triggers int
	if err := target.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?", r.config.TargetDB, table.Table).Scan(&triggers); err != nil {
		return nil, err
	}
	if triggers != 0 {
		return nil, fmt.Errorf("target dependency triggers are not supported")
	}
	sourceColumns, err := dependencyColumns(ctx, source, r.config.SourceDB, table.Table)
	if err != nil {
		return nil, err
	}
	targetColumns, err := dependencyColumns(ctx, target, r.config.TargetDB, table.Table)
	if err != nil {
		return nil, err
	}
	if len(sourceColumns) == 0 || !reflect.DeepEqual(sourceColumns, targetColumns) {
		return nil, fmt.Errorf("source and target column definitions must match")
	}
	names := make([]string, len(sourceColumns))
	for i, col := range sourceColumns {
		names[i] = col.Name
		if strings.Contains(col.Extra, "VIRTUAL GENERATED") || strings.Contains(col.Extra, "STORED GENERATED") {
			return nil, fmt.Errorf("generated columns are not supported")
		}
		if col.Name == table.PaginationColumn && !strings.Contains(col.Type, "int") {
			return nil, fmt.Errorf("pagination column must be an integer")
		}
		for _, identity := range table.IdentityColumns {
			if col.Name == identity && col.Nullable != "NO" {
				return nil, fmt.Errorf("logical identity columns must be non-nullable")
			}
		}
	}
	for _, side := range []struct {
		tx       *sql.Tx
		database string
	}{{source, r.config.SourceDB}, {target, r.config.TargetDB}} {
		for _, key := range []struct {
			columns []string
			primary bool
		}{{[]string{table.PaginationColumn}, true}, {table.IdentityColumns, false}} {
			ok, err := dependencyHasUniqueKey(ctx, side.tx, side.database, table.Table, key.columns, key.primary)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("%s requires a full unique logical key and a single-column pagination primary key", side.database)
			}
		}
	}
	return names, nil
}

func dependencyEquals(alias string, values map[string]interface{}, args *[]interface{}) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	predicates := make([]string, 0, len(names))
	for _, name := range names {
		predicates = append(predicates, alias+"."+ghostferry.QuoteField(name)+" <=> ?")
		*args = append(*args, values[name])
	}
	return predicates
}

func (r *ShardingFerry) dependencyPredicates(table CutoverDependency) ([]string, []interface{}) {
	predicates := []string{"r." + ghostferry.QuoteField(r.config.ShardingKey) + " = ?"}
	args := []interface{}{r.config.ShardingValue}
	for _, join := range table.JoinColumns {
		predicates = append(predicates, "d."+ghostferry.QuoteField(join.Column)+" = r."+ghostferry.QuoteField(join.ReferenceColumn))
	}
	predicates = append(predicates, dependencyEquals("r", table.ReferenceEquals, &args)...)
	predicates = append(predicates, dependencyEquals("d", table.Equals, &args)...)
	return predicates, args
}

func dependencyReadRow(scanner interface{ Scan(...interface{}) error }, count int) ([][]byte, error) {
	values := make([][]byte, count)
	pointers := make([]interface{}, count)
	for i := range values {
		pointers[i] = &values[i]
	}
	err := scanner.Scan(pointers...)
	return values, err
}

func (r *ShardingFerry) copyDependency(ctx context.Context, source, target *sql.Tx, table CutoverDependency, copiedBytes *int64) error {
	columns, err := r.dependencySchema(ctx, source, target, table)
	if err != nil {
		return err
	}
	sourceName := ghostferry.QuotedTableNameFromString(r.config.SourceDB, table.Table)
	referenceName := ghostferry.QuotedTableNameFromString(r.config.SourceDB, table.ReferenceTable)
	targetName := ghostferry.QuotedTableNameFromString(r.config.TargetDB, table.Table)
	predicates, args := r.dependencyPredicates(table)
	if table.RequireMatch {
		// A missing required dependency must not turn an empty SELECT into success.
		refArgs := []interface{}{r.config.ShardingValue}
		refPredicates := []string{"r." + ghostferry.QuoteField(r.config.ShardingKey) + " = ?"}
		refPredicates = append(refPredicates, dependencyEquals("r", table.ReferenceEquals, &refArgs)...)
		inner := make([]string, 0, len(table.JoinColumns))
		for _, join := range table.JoinColumns {
			inner = append(inner, "d."+ghostferry.QuoteField(join.Column)+" = r."+ghostferry.QuoteField(join.ReferenceColumn))
		}
		inner = append(inner, dependencyEquals("d", table.Equals, &refArgs)...)
		query := "SELECT EXISTS (SELECT 1 FROM " + referenceName + " r WHERE " + strings.Join(refPredicates, " AND ") + " AND NOT EXISTS (SELECT 1 FROM " + sourceName + " d WHERE " + strings.Join(inner, " AND ") + "))"
		var missing bool
		if err := source.QueryRowContext(ctx, query, refArgs...).Scan(&missing); err != nil {
			return err
		}
		if missing {
			return fmt.Errorf("required source dependency is missing")
		}
	}
	selectColumns := make([]string, len(columns))
	insertColumns := ghostferry.QuoteFields(columns)
	paginationIndex := 0
	for i, name := range columns {
		selectColumns[i] = "BINARY d." + ghostferry.QuoteField(name)
		if name == table.PaginationColumn {
			paginationIndex = i
		}
	}
	// Bounded keyset iteration uses the source surrogate key, not the logical key.
	var last []byte
	copied := 0
	for {
		query := "SELECT " + strings.Join(selectColumns, ",") + " FROM " + sourceName + " d WHERE EXISTS (SELECT 1 FROM " + referenceName + " r WHERE " + strings.Join(predicates, " AND ") + ")"
		batchArgs := append([]interface{}{}, args...)
		if last != nil {
			query += " AND d." + ghostferry.QuoteField(table.PaginationColumn) + " > ?"
			batchArgs = append(batchArgs, last)
		}
		query += " ORDER BY d." + ghostferry.QuoteField(table.PaginationColumn) + " LIMIT 100"
		rows, err := source.QueryContext(ctx, query, batchArgs...)
		if err != nil {
			return err
		}
		batchCount := 0
		for rows.Next() {
			values, err := dependencyReadRow(rows, len(columns))
			if err != nil {
				rows.Close()
				return err
			}
			copied++
			for _, value := range values {
				*copiedBytes += int64(len(value))
			}
			if copied > r.config.CutoverDependencies.MaxRowsPerTable || *copiedBytes > r.config.CutoverDependencies.MaxBytes {
				rows.Close()
				return fmt.Errorf("dependency copy exceeded row or byte budget")
			}
			if err := r.insertAndVerifyDependency(ctx, target, table, targetName, columns, insertColumns, values); err != nil {
				rows.Close()
				return err
			}
			last = values[paginationIndex]
			batchCount++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if batchCount < 100 {
			r.logger.WithField("table", table.Table).WithField("rows", copied).Info("verified cutover dependency rows; transaction pending")
			return nil
		}
	}
}

func (r *ShardingFerry) insertAndVerifyDependency(ctx context.Context, target *sql.Tx, table CutoverDependency, targetName string, columns, insertColumns []string, values [][]byte) error {
	args := make([]interface{}, len(values))
	for i, value := range values {
		if value != nil {
			args[i] = value
		}
	}
	query := "INSERT INTO " + targetName + " (" + strings.Join(insertColumns, ",") + ") VALUES (" + strings.TrimSuffix(strings.Repeat("?,", len(columns)), ",") + ")"
	_, err := target.ExecContext(ctx, sqlwrapper.AnnotateStmt(query, r.Ferry.TargetDB.Marginalia), args...)
	if err != nil {
		var duplicate *mysql.MySQLError
		if !errors.As(err, &duplicate) || duplicate.Number != 1062 {
			return err
		}
	}
	predicates := make([]string, 0, len(table.IdentityColumns))
	identityArgs := make([]interface{}, 0, len(table.IdentityColumns))
	for _, identity := range table.IdentityColumns {
		predicates = append(predicates, ghostferry.QuoteField(identity)+" = ?")
		for i, name := range columns {
			if name == identity {
				identityArgs = append(identityArgs, args[i])
			}
		}
	}
	selected := make([]string, len(columns))
	for i, name := range columns {
		selected[i] = "BINARY " + ghostferry.QuoteField(name)
	}
	query = "SELECT " + strings.Join(selected, ",") + " FROM " + targetName + " WHERE " + strings.Join(predicates, " AND ") + " FOR UPDATE"
	actual, err := dependencyReadRow(target.QueryRowContext(ctx, query, identityArgs...), len(columns))
	if err != nil {
		return fmt.Errorf("destination logical identity not found after insert (possible surrogate-key collision): %w", err)
	}
	for i, name := range columns {
		if name == table.PaginationColumn {
			continue
		}
		if (actual[i] == nil) != (values[i] == nil) || !bytes.Equal(actual[i], values[i]) {
			return fmt.Errorf("destination payload mismatch in column %s", name)
		}
	}
	return nil
}
