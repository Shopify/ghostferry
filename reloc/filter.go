package reloc

import (
	"fmt"
	"reflect"

	sq "github.com/Masterminds/squirrel"
	"github.com/Shopify/ghostferry"
	"github.com/siddontang/go-mysql/schema"
)

type ShardingFilter struct {
	ShardingKey   string
	ShardingValue interface{}
}

func (f *ShardingFilter) ConstrainSelect(builder sq.SelectBuilder) (sq.SelectBuilder, error) {
	return builder.Where(sq.Eq{f.ShardingKey: f.ShardingValue}), nil
}

func (f *ShardingFilter) ApplicableEvent(event ghostferry.DMLEvent) (bool, error) {
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

type ShardedApplicableFilter struct {
	SourceShard string
	ShardingKey string
}

func (s *ShardedApplicableFilter) ApplicableDatabases(dbs []string) []string {
	applicable := []string{}
	for _, db := range dbs {
		if db == s.SourceShard {
			applicable = append(applicable, db)
		}
	}
	return applicable
}

func (s *ShardedApplicableFilter) ApplicableTables(tables []*schema.Table) []*schema.Table {
	applicable := []*schema.Table{}

	for _, table := range tables {
		columns := table.Columns
		for _, column := range columns {
			if column.Name == s.ShardingKey {
				applicable = append(applicable, table)
			}
		}
	}

	return applicable
}
