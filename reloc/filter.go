package reloc

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

func (f *ShardedCopyFilter) BuildSelect(table *schema.Table, lastPk, batchSize uint64) (sq.SelectBuilder, error) {
	quotedPK := "`" + table.GetPKColumn(0).Name + "`"
	quotedShardingKey := "`" + f.ShardingKey + "`"
	quotedTable := ghostferry.QuotedTableName(table)

	if _, exists := f.PrimaryKeyTables[table.Name]; exists {
		return sq.Select("*").
			From(quotedTable + " USE INDEX (PRIMARY)").
			Where(sq.Eq{quotedPK: f.ShardingValue}). // Both WHERE conditions are necessary to prevent infinite iteration.
			Where(sq.Gt{quotedPK: lastPk}), nil      // LIMIT not necessary since we are selecting a single primary key.
	}

	joinTables, exists := f.JoinedTables[table.Name]
	if !exists {
		return sq.Select("*").
			From(quotedTable + " " + f.shardingKeyIndexHint(table)).
			Where(sq.Eq{quotedShardingKey: f.ShardingValue}).
			Where(sq.Gt{quotedPK: lastPk}).
			Limit(batchSize).
			OrderBy(quotedPK), nil
	}

	var clauses []string
	var args []interface{}

	for _, joinTable := range joinTables {
		pattern := "SELECT `%s` AS reloc_join_alias FROM `%s`.`%s` WHERE `%s` = ? AND `%s` > ?"
		sql := fmt.Sprintf(pattern, joinTable.JoinColumn, table.Schema, joinTable.TableName, f.ShardingKey, joinTable.JoinColumn)
		clauses = append(clauses, sql)
		args = append(args, f.ShardingValue, lastPk)
	}

	subquery := strings.Join(clauses, " UNION DISTINCT ")
	subquery += " ORDER BY reloc_join_alias LIMIT " + strconv.FormatUint(batchSize, 10)

	condition := fmt.Sprintf("%s IN (SELECT * FROM (%s) AS reloc_join_table)", quotedPK, subquery)

	return sq.Select("*").
		From(quotedTable).
		Where(sq.Expr(condition, args...)).
		OrderBy(quotedPK), nil // LIMIT comes from the subquery.
}

func (f *ShardedCopyFilter) shardingKeyIndexHint(table *schema.Table) string {
	if indexName := f.shardingKeyIndexName(table); indexName != "" {
		return "USE INDEX (`" + indexName + "`)"
	} else {
		if _, logged := f.missingShardingKeyIndexLogged.Load(table.Name); !logged {
			log.WithFields(log.Fields{"tag": "reloc", "table": table.Name}).Warnf("missing suitable index")
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
				return nil, fmt.Errorf("Multiple PK columns are not supported with the PrimaryKeyTables tables option")
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
