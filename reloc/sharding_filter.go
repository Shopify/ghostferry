package reloc

import (
	"reflect"

	sq "github.com/Masterminds/squirrel"
	"github.com/Shopify/ghostferry"
)

type ShardingFilter struct {
	ShardingKey   string
	ShardingValue interface{}
}

func (f *ShardingFilter) ConstrainSelect(builder sq.SelectBuilder) sq.SelectBuilder {
	return builder.Where(sq.Eq{f.ShardingKey: f.ShardingValue})
}

func (f *ShardingFilter) ApplicableEvent(event ghostferry.DMLEvent) bool {
	columns := event.Schema().Columns
	for idx, column := range columns {
		if column.Name == f.ShardingKey {
			oldValues, newValues := event.OldValues(), event.NewValues()

			oldEqual := oldValues != nil && reflect.DeepEqual(oldValues[idx], f.ShardingValue)
			newEqual := newValues != nil && reflect.DeepEqual(newValues[idx], f.ShardingValue)

			if oldEqual != newEqual && oldValues != nil && newValues != nil {
				// The value of the sharding key for a row was changed - this is unsafe.
				// TODO(pushrax): raise error?
			}

			return oldEqual || newEqual
		}
	}
	return false
}
