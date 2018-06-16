package ghostferry

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/siddontang/go-mysql/schema"
)

type Schema struct {
	SourceDB    *sql.DB
	TargetDB    *sql.DB
	TableSchema TableSchemaCache
	TableFilter TableFilter
}

func (s *Schema) Init() error {
	return s.FetchSchema()
}

func (s *Schema) GetSourceSchema() TableSchemaCache {
	return s.TableSchema
}

func (s *Schema) SynchronizeTableSchema(stmt string) error {
	if s.illegalStatement(stmt) {
		return fmt.Errorf("schema synchronization triggered by illegal migration: %s", stmt)
	}

	err := s.FetchSchema()
	if err != nil {
		return err
	}

	return nil
}

func (s *Schema) FetchSchema() error {
	err := s.lockSourceSchema()
	if err != nil {
		return err
	}

	err = s.lockTargetSchema()
	if err != nil {
		return err
	}

	sourceSchema, err := s.fetchSourceSchema()
	if err != nil {
		return err
	}

	targetSchema, err := s.fetchTargetSchema()
	if err != nil {
		return err
	}

	err = s.unlockSourceSchema()
	if err != nil {
		return err
	}

	err = s.unlockTargetSchema()
	if err != nil {
		return err
	}

	s.TableSchema = intersectSchema(sourceSchema, targetSchema)

	return nil
}

func (s *Schema) fetchSourceSchema() (TableSchemaCache, error) {
	return LoadTables(s.SourceDB, s.TableFilter)
}

func (s *Schema) fetchTargetSchema() (TableSchemaCache, error) {
	return LoadTables(s.TargetDB, s.TableFilter)
}

func (s *Schema) unlockSourceSchema() error {
	// TODO
	return nil
}

func (s *Schema) unlockTargetSchema() error {
	// TODO
	return nil
}

func (s *Schema) lockSourceSchema() error {
	// TODO
	return nil
}

func (s *Schema) lockTargetSchema() error {
	// TODO
	return nil
}

func (s *Schema) illegalStatement(stmt string) bool {
	// TODO
	return false
}

func intersectSchema(source TableSchemaCache, target TableSchemaCache) TableSchemaCache {
	res := make(TableSchemaCache)

	for tableName, sourceTable := range source {
		targetTable, ok := target[tableName]
		if !ok {
			continue
		}

		res[tableName] = intersectTables(sourceTable, targetTable)
	}

	return res
}

func intersectTables(source, target *schema.Table) *schema.Table {
	if len(source.Columns) == 0 || len(target.Columns) == 0 {
		// TODO rer
		panic(fmt.Sprintf("zero columns: table: %d, target: %d", len(source.Columns), len(target.Columns)))
	}

	if !reflect.DeepEqual(source.Indexes, target.Indexes) {
		// TODO remove?
		panic("indexes suspiciously out of sync")
	}

	if !reflect.DeepEqual(source.PKColumns, target.PKColumns) {
		panic("something bad happened?")
	}

	// source: | a | b     | c(pk) |
	// target: | a | c(pk) |

	intersectedColumns := []schema.TableColumn{}
	for _, sourceCol := range source.Columns {
		for _, targetCol := range target.Columns {
			if sourceCol.Name == targetCol.Name {
				intersectedColumns = append(intersectedColumns, sourceCol)
				break
			}
		}
	}

	return &schema.Table{
		Schema: source.Schema,
		Name:   source.Name,

		Columns:   intersectedColumns,
		Indexes:   source.Indexes,
		PKColumns: source.PKColumns,
	}
}
