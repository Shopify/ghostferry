package reloc

import (
	"fmt"
	"reflect"

	sq "github.com/Masterminds/squirrel"
	"github.com/Shopify/ghostferry"
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
