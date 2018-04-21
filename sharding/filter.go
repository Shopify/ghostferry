package sharding

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	sq "github.com/Masterminds/squirrel"
	"github.com/Shopify/ghostferry"
	"github.com/siddontang/go-mysql/schema"
	log "github.com/sirupsen/logrus"
)

type JoinTable struct {
	TableName, JoinColumn string
}

type ShardedCopyFilter struct {
	ShardingKey      string
	ShardingValue    interface{}
	JoinedTables     map[string][]JoinTable
	PrimaryKeyTables map[string]struct{}

	missingShardingKeyIndexLogged sync.Map
}

func (f *ShardedCopyFilter) BuildSelect(columns []string, table *schema.Table, lastPk, batchSize uint64) (sq.SelectBuilder, error) {
	quotedPK := "`" + table.GetPKColumn(0).Name + "`"
	quotedShardingKey := "`" + f.ShardingKey + "`"
	quotedTable := ghostferry.QuotedTableName(table)

	if _, exists := f.PrimaryKeyTables[table.Name]; exists {
		// This table uses the sharding key as its primary key, and thus contains
		// a single row per sharding key. This can commonly occur when representing
		// the model for a single tenant in a multitenant database, e.g. a shop on
		// a multitenant commerce platform.
		//
		// It is necessary to use two WHERE conditions on quotedPK so the second batch will be empty.
		// No LIMIT clause is necessary since at most one row is present.
		return sq.Select(columns...).
			From(quotedTable + " USE INDEX (PRIMARY)").
			Where(sq.Eq{quotedPK: f.ShardingValue}).
			Where(sq.Gt{quotedPK: lastPk}), nil
	}

	joinTables, exists := f.JoinedTables[table.Name]
	if !exists {
		// This is a normal sharded table; functionally:
		//   SELECT * FROM x
		//     WHERE ShardingKey = ShardingValue AND PrimaryKey > LastPrimaryKey
		//     ORDER BY PrimaryKey LIMIT BatchSize
		//
		// However, we found that for some tables, MySQL would not use the
		// covering index correctly, indicated in EXPLAIN by used_key_parts
		// including only the sharding key. Copying a sharding key having many
		// rows in an affected table was very slow.
		//
		// To force MySQL to use the index fully, we use:
		//   SELECT * FROM x JOIN (SELECT PrimaryKey FROM x ...) USING (PrimaryKey)
		//
		// i.e. load the primary keys first, then load the rest of the columns.

		selectPrimaryKeys := "SELECT " + quotedPK + " FROM " + quotedTable + " " + f.shardingKeyIndexHint(table) +
			" WHERE " + quotedShardingKey + " = ? AND " + quotedPK + " > ?" +
			" ORDER BY " + quotedPK + " LIMIT " + strconv.Itoa(int(batchSize))

		return sq.Select(columns...).
			From(quotedTable).
			Join("("+selectPrimaryKeys+") AS `batch` USING("+quotedPK+")", f.ShardingValue, lastPk), nil
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
	// SELECT * FROM x WHERE PrimaryKey IN (
	//   (SELECT PrimaryKey FROM JoinTable1 WHERE ShardingKey = ? AND JoinTable1.PrimaryKey > ?)
	//   UNION DISTINCT
	//   (SELECT PrimaryKey FROM JoinTable2 WHERE ShardingKey = ? AND JoinTable2.PrimaryKey > ?)
	//      < ... more UNION DISTINCT for each other join table >
	//   ORDER BY PrimaryKey LIMIT BatchSize
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
		args = append(args, f.ShardingValue, lastPk)
	}

	subquery := strings.Join(clauses, " UNION DISTINCT ")
	subquery += " ORDER BY sharding_join_alias LIMIT " + strconv.FormatUint(batchSize, 10)

	condition := fmt.Sprintf("%s IN (SELECT * FROM (%s) AS sharding_join_table)", quotedPK, subquery)

	return sq.Select(columns...).
		From(quotedTable).
		Where(sq.Expr(condition, args...)).
		OrderBy(quotedPK), nil // LIMIT comes from the subquery.
}

func (f *ShardedCopyFilter) shardingKeyIndexHint(table *schema.Table) string {
	if indexName := f.shardingKeyIndexName(table); indexName != "" {
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

func (f *ShardedCopyFilter) shardingKeyIndexName(table *schema.Table) string {
	indexName := ""
	pkName := table.GetPKColumn(0).Name

	for _, x := range table.Indexes {
		if x.Columns[0] == f.ShardingKey {
			if len(x.Columns) == 1 {
				// This index will work in InnoDB, but there may be a more specific one to prefer.
				indexName = x.Name
			} else if x.Columns[1] == pkName {
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
		shardingKey = event.TableSchema().GetPKColumn(0).Name
	}

	columns := event.TableSchema().Columns
	for idx, column := range columns {
		if column.Name == shardingKey {
			oldValues, newValues := event.OldValues(), event.NewValues()

			oldShardingValue, oldExists, err := parseShardingValue(oldValues, idx)
			if err != nil {
				return false, fmt.Errorf("parsing old sharding key: %s", err)
			}

			newShardingValue, newExists, err := parseShardingValue(newValues, idx)
			if err != nil {
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

type ShardedTableFilter struct {
	SourceShard      string
	ShardingKey      string
	JoinedTables     map[string][]JoinTable
	IgnoredTables    []*regexp.Regexp
	PrimaryKeyTables map[string]struct{}
}

func (s *ShardedTableFilter) ApplicableDatabases(dbs []string) ([]string, error) {
	return []string{s.SourceShard}, nil
}

func (s *ShardedTableFilter) ApplicableTables(tables []*schema.Table) (applicable []*schema.Table, err error) {
	for _, table := range tables {
		if s.isIgnored(table) {
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

func (s *ShardedTableFilter) isIgnored(table *schema.Table) bool {
	for _, re := range s.IgnoredTables {
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
