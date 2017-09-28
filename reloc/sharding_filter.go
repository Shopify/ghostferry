package reloc

import (
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
	// TODO: implement
	return true
}
