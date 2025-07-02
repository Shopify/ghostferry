package sharding

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	sq "github.com/Masterminds/squirrel"
	"github.com/Shopify/ghostferry"
	log "github.com/sirupsen/logrus"
)

type JoinTable struct {
	TableName, JoinColumn string
}

type IndexConfigPerTable struct {
	IndexHint string
	IndexName string
}

type ShardedCopyFilter struct {
	ShardingKey      string
	ShardingValue    interface{}
	JoinedTables     map[string][]JoinTable
	PrimaryKeyTables map[string]struct{}

	IndexHint           string
	IndexConfigPerTable map[string]IndexConfigPerTable

	missingShardingKeyIndexLogged sync.Map
}

func (f *ShardedCopyFilter) BuildSelect(columns []string, table *ghostferry.TableSchema, lastPaginationKey, batchSize uint64) (sq.SelectBuilder, error) {
	quotedPaginationKey := "`" + table.GetPaginationColumn().Name + "`"
	quotedShardingKey := "`" + f.ShardingKey + "`"
	quotedTable := ghostferry.QuotedTableName(table)

	if _, exists := f.PrimaryKeyTables[table.Name]; exists {
		// This table uses the sharding key as its primary key, and thus contains
		// a single row per sharding key. This can commonly occur when representing
		// the model for a single tenant in a multitenant database, e.g. a shop on
		// a multitenant commerce platform.
		//
		// It is necessary to use two WHERE conditions on quotedPaginationKey so the second batch will be empty.
		// No LIMIT clause is necessary since at most one row is present.
		return sq.Select(columns...).
			From(quotedTable + " USE INDEX (PRIMARY)").
			Where(sq.Eq{quotedPaginationKey: f.ShardingValue}).
			Where(sq.Gt{quotedPaginationKey: lastPaginationKey}), nil
	}

	joinTables, exists := f.JoinedTables[table.Name]
	if !exists {
		// This is a normal sharded table; functionally:
		//   SELECT * FROM x
		//     WHERE ShardingKey = ShardingValue AND PaginationKey > LastPaginationKey
		//     ORDER BY PaginationKey LIMIT BatchSize
		//
		// However, we found that for some tables, MySQL would not use the
		// covering index correctly, indicated in EXPLAIN by used_key_parts
		// including only the sharding key. Copying a sharding key having many
		// rows in an affected table was very slow.
		//
		// To force MySQL to use the index fully, we use:
		//   SELECT * FROM x JOIN (SELECT PaginationKey FROM x ...) USING (PaginationKey)
		//
		// i.e. load the primary keys first, then load the rest of the columns.

		indexHint := f.IndexHint

		if tableSpecificIndexConfig, exists := f.IndexConfigPerTable[table.Name]; exists {
			if tableSpecificIndexConfig.IndexHint != "" {
				indexHint = tableSpecificIndexConfig.IndexHint
			}
		}

		indexHint = strings.ToLower(indexHint)

		selectPaginationKeys := "SELECT " + quotedPaginationKey + " FROM " + quotedTable

		if indexHint != "none" {
			selectPaginationKeys += " " + f.shardingKeyIndexHint(table, indexHint)
		}

		selectPaginationKeys += " WHERE " + quotedShardingKey + " = ? AND " + quotedPaginationKey + " > ?" +
			" ORDER BY " + quotedPaginationKey + " LIMIT " + strconv.Itoa(int(batchSize))

		return sq.Select(columns...).
			From(quotedTable).
			Join("("+selectPaginationKeys+") AS `batch` USING("+quotedPaginationKey+")", f.ShardingValue, lastPaginationKey), nil
	}

	// This is a "joined table". It is the only supported type of table that
	// does not have the sharding key in any column. This only occurs when a
	// row may be shared between multiple sharding values, otherwise a sharding
	// key column can be added.
	//
	// To determine which rows in the joined table are copied, the "join table"
	// is consulted. The join table must contain a sharding key column and a
	// column relating the joined table. There may be multiple join tables for
	// for one joined table.
	//
	// Such tables are typically going to have reference-counted/copy-on-write
	// values, and we require their rows to be immutable (no UPDATE) and
	// uniquely identified by primary key (perhaps the hash of another column).
	//
	// The final query is something like:
	//
	// SELECT * FROM x WHERE PaginationKey IN (
	//   (SELECT PaginationKey FROM JoinTable1 WHERE ShardingKey = ? AND JoinTable1.PaginationKey > ?)
	//   UNION DISTINCT
	//   (SELECT PaginationKey FROM JoinTable2 WHERE ShardingKey = ? AND JoinTable2.PaginationKey > ?)
	//      < ... more UNION DISTINCT for each other join table >
	//   ORDER BY PaginationKey LIMIT BatchSize
	// )
	//
	// i.e. load the primary keys for each join table, take their UNION, limit
	// it to a batch, then select the rest of the columns.
	var clauses []string
	var args []interface{}

	for _, joinTable := range joinTables {
		pattern := "SELECT `%s` AS sharding_join_alias FROM `%s`.`%s` WHERE `%s` = ? AND `%s` > ?"
		sql := fmt.Sprintf(pattern, joinTable.JoinColumn, table.Schema, joinTable.TableName, f.ShardingKey, joinTable.JoinColumn)
		clauses = append(clauses, sql)
		args = append(args, f.ShardingValue, lastPaginationKey)
	}

	subquery := strings.Join(clauses, " UNION DISTINCT ")
	subquery += " ORDER BY sharding_join_alias LIMIT " + strconv.FormatUint(batchSize, 10)

	condition := fmt.Sprintf("%s IN (SELECT * FROM (%s) AS sharding_join_table)", quotedPaginationKey, subquery)

	return sq.Select(columns...).
		From(quotedTable).
		Where(sq.Expr(condition, args...)).
		OrderBy(quotedPaginationKey), nil // LIMIT comes from the subquery.
}

func (f *ShardedCopyFilter) shardingKeyIndexHint(table *ghostferry.TableSchema, indexHint string) string {
	indexName := f.shardingKeyIndexName(table)

	if indexName != "" {
		if indexHint == "force" {
			return "FORCE INDEX (`" + indexName + "`)"
		}

		return "USE INDEX (`" + indexName + "`)"
	} else {
		if _, logged := f.missingShardingKeyIndexLogged.Load(table.Name); !logged {
			log.WithFields(log.Fields{"tag": "sharding", "table": table.Name}).Warnf("missing suitable index")
			metrics.Count("MissingShardingKeyIndex", 1, []ghostferry.MetricTag{{"table", table.Name}}, 1.0)
			f.missingShardingKeyIndexLogged.Store(table.Name, true)
		}
		return "IGNORE INDEX (PRIMARY)"
	}
}

func (f *ShardedCopyFilter) shardingKeyIndexName(table *ghostferry.TableSchema) string {
	if tableSpecificIndexConfig, exists := f.IndexConfigPerTable[table.Name]; exists {
		if tableSpecificIndexConfig.IndexName != "" {
			for _, x := range table.Indexes {
				// ignore the index name passed in via config if it doesn't exist on the table
				if strings.EqualFold(x.Name, tableSpecificIndexConfig.IndexName) {
					return tableSpecificIndexConfig.IndexName
				}
			}

			log.WithFields(log.Fields{"tag": "sharding", "table": table.Name}).
				Warnf("index name %s not found on table %s", tableSpecificIndexConfig.IndexName, table.Name)
		}
	}

	indexName := ""

	paginationKeyName := table.GetPaginationColumn().Name

	for _, x := range table.Indexes {
		if x.Columns[0] == f.ShardingKey {
			if len(x.Columns) == 1 {
				// This index will work in InnoDB, but there may be a more specific one to prefer.
				indexName = x.Name
			} else if x.Columns[1] == paginationKeyName {
				// This index satisfies (sharding key, primary key).
				indexName = x.Name
				break
			}
		}
	}
	return indexName
}

func (f *ShardedCopyFilter) ApplicableEvent(event ghostferry.DMLEvent) (bool, error) {
	shardingKey := f.ShardingKey
	if _, exists := f.PrimaryKeyTables[event.Table()]; exists {
		shardingKey = event.TableSchema().GetPaginationColumn().Name
	}

	columns := event.TableSchema().Columns
	for idx, column := range columns {
		if column.Name == shardingKey {
			oldValues, newValues := event.OldValues(), event.NewValues()

			oldShardingValue, oldExists, err := parseShardingValue(oldValues, idx)
			if err != nil {

				sql, sqlErr := event.AsSQLString(event.Database(), event.Table())
				if sqlErr != nil {
					sql = ""
				}

				log.WithFields(log.Fields{
					"tag":          "sharding",
					"table":        event.Table(),
					"position":     event.BinlogPosition(),
					"sqlStatement": sql,
					"event":        fmt.Sprintf("%T", event),
				}).WithError(err).Error("parsing old sharding key failed")

				return false, fmt.Errorf("parsing old sharding key: %s", err)
			}

			newShardingValue, newExists, err := parseShardingValue(newValues, idx)
			if err != nil {

				sql, sqlErr := event.AsSQLString(event.Database(), event.Table())
				if sqlErr != nil {
					sql = ""
				}

				log.WithFields(log.Fields{
					"tag":          "sharding",
					"table":        event.Table(),
					"position":     event.BinlogPosition(),
					"sqlStatement": sql,
					"event":        fmt.Sprintf("%T", event),
				}).WithError(err).Error("parsing new sharding key failed")

				return false, fmt.Errorf("parsing new sharding key: %s", err)
			}

			oldEqual := oldExists && oldShardingValue == f.ShardingValue
			newEqual := newExists && newShardingValue == f.ShardingValue

			if oldEqual != newEqual && oldExists && newExists {
				// The value of the sharding key for a row was changed - this is unsafe.
				err := fmt.Errorf("sharding key changed from %v to %v", oldValues[idx], newValues[idx])
				return false, err
			}

			return oldEqual || newEqual, nil
		}
	}
	return false, nil
}

type ShardedTableFilterType int64

const (
	IgnoredTablesFilter ShardedTableFilterType = iota
	IncludedTablesFilter
)

type ShardedTableFilter struct {
	SourceShard      string
	ShardingKey      string
	JoinedTables     map[string][]JoinTable
	Type             ShardedTableFilterType
	Tables           []*regexp.Regexp
	PrimaryKeyTables map[string]struct{}
}

func (s *ShardedTableFilter) isIgnoreFilter() bool {
	return s.Type == IgnoredTablesFilter
}

func (s *ShardedTableFilter) isIncludeFilter() bool {
	return s.Type == IncludedTablesFilter
}

func (s *ShardedTableFilter) ApplicableDatabases(dbs []string) ([]string, error) {
	return []string{s.SourceShard}, nil
}

func (s *ShardedTableFilter) ApplicableTables(tables []*ghostferry.TableSchema) (applicable []*ghostferry.TableSchema, err error) {
	for _, table := range tables {
		if (s.isIgnoreFilter() && s.isPresent(table)) || (s.isIncludeFilter() && !s.isPresent(table)) {
			continue
		}

		columns := table.Columns
		for _, column := range columns {
			if column.Name == s.ShardingKey {
				applicable = append(applicable, table)
				break
			}
		}

		if _, exists := s.JoinedTables[table.Name]; exists {
			applicable = append(applicable, table)
		}

		if _, exists := s.PrimaryKeyTables[table.Name]; exists {
			if len(table.PKColumns) != 1 {
				return nil, fmt.Errorf("Multiple PK columns are not supported with the PrimaryKeyTables option")
			}
			applicable = append(applicable, table)
		}
	}
	return
}

func (s *ShardedTableFilter) isPresent(table *ghostferry.TableSchema) bool {
	for _, re := range s.Tables {
		if re.Match([]byte(table.Name)) {
			return true
		}
	}
	return false
}

func parseShardingValue(values []interface{}, index int) (int64, bool, error) {
	if values == nil {
		return 0, false, nil
	}
	if v, ok := ghostferry.Uint64Value(values[index]); ok {
		return int64(v), true, nil
	}
	if v, ok := ghostferry.Int64Value(values[index]); ok {
		return v, true, nil
	}
	return 0, true, fmt.Errorf("invalid type %t", values[index])
}
