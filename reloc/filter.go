package reloc

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/Shopify/ghostferry"
	"github.com/siddontang/go-mysql/schema"
)

type JoinTable struct {
	Name, Column string
}

type ShardedRowFilter struct {
	ShardingKey   string
	ShardingValue interface{}
	JoinedTables  map[string][]JoinTable
}

func (f *ShardedRowFilter) ConstrainSelect(table *schema.Table, lastPk uint64, batchSize uint64) (sq.Sqlizer, error) {
	joinTables, exists := f.JoinedTables[table.Name]
	if !exists {
		return sq.Eq{f.ShardingKey: f.ShardingValue}, nil
	}

	var clauses []string
	var args []interface{}

	for _, joinTable := range joinTables {
		pattern := "SELECT `%s` AS reloc_join_alias FROM `%s`.`%s` WHERE `%s` = ? AND `%s` > ?"
		sql := fmt.Sprintf(pattern, joinTable.Column, table.Schema, joinTable.Name, f.ShardingKey, joinTable.Column)
		clauses = append(clauses, sql)
		args = append(args, f.ShardingValue, lastPk)
	}

	subquery := strings.Join(clauses, " UNION DISTINCT ")
	subquery += " ORDER BY reloc_join_alias LIMIT " + strconv.FormatUint(batchSize, 10)

	condition := fmt.Sprintf("`%s` IN (SELECT * FROM (%s) AS reloc_join_table)", table.GetPKColumn(0).Name, subquery)
	return sq.Expr(condition, args...), nil
}

func (f *ShardedRowFilter) ApplicableEvent(event ghostferry.DMLEvent) (bool, error) {
	columns := event.TableSchema().Columns
	for idx, column := range columns {
		if column.Name == f.ShardingKey {
			oldValues, newValues := event.OldValues(), event.NewValues()

			oldEqual := oldValues != nil && reflect.DeepEqual(oldValues[idx], f.ShardingValue)
			newEqual := newValues != nil && reflect.DeepEqual(newValues[idx], f.ShardingValue)

			if oldEqual != newEqual && oldValues != nil && newValues != nil {
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
	SourceShard string
	ShardingKey string
}

func (s *ShardedTableFilter) ApplicableDatabases(dbs []string) []string {
	return []string{s.SourceShard}
}

func (s *ShardedTableFilter) ApplicableTables(tables []*schema.Table) (applicable []*schema.Table) {
	for _, table := range tables {
		columns := table.Columns
		for _, column := range columns {
			if column.Name == s.ShardingKey {
				applicable = append(applicable, table)
				break
			}
		}
	}
	return
}
