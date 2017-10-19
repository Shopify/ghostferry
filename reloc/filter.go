package reloc

import (
	"fmt"
	"reflect"

	sq "github.com/Masterminds/squirrel"
	"github.com/Shopify/ghostferry"
	"github.com/siddontang/go-mysql/schema"
)

type ShardedRowFilter struct {
	ShardingKey   string
	ShardingValue interface{}
}

func (f *ShardedRowFilter) ConstrainSelect(builder sq.SelectBuilder) (sq.SelectBuilder, error) {
	return builder.Where(sq.Eq{f.ShardingKey: f.ShardingValue}), nil
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
