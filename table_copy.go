package ghostferry

import "github.com/siddontang/go-mysql/schema"

type TableCopy struct {
	table schema.Table
}

func (e *TableCopy) Database() string {
	return e.table.Schema
}

func (e *TableCopy) Table() string {
	return e.table.Name
}

func (e *TableCopy) TableSchema() *schema.Table {
	return &e.table
}
