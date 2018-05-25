package sharding

import (
	"database/sql"

	sq "github.com/Masterminds/squirrel"
	"github.com/Shopify/ghostferry"
	"github.com/siddontang/go-mysql/schema"
)

type SnappyFingerprinter struct {
	SnappyColumns map[string][]string // table->snappy_compressed_columns
}

func (f *SnappyFingerprinter) GetSnappyHashes(db *sql.DB, schema, table, pkColumn string, columns []schema.TableColumn, pks []uint64) (map[uint64][]byte, error) {
	sql, args, err := f.GetColumnsSql(schema, table, pkColumn, columns, pks)
	if err != nil {
		return nil, err
	}

	// This query must be a prepared query. If it is not, querying will use
	// MySQL's plain text interface, which will scan all values into []uint8
	// if we give it []interface{}.
	stmt, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	resultSet := make(map[uint64][]byte)
	for rows.Next() {
		rowData, err := ghostferry.ScanGenericRow(rows, len(columns)+1) // TODO: find pk better
		if err != nil {
			return nil, err
		}

		pk, err := rowData.GetUint64(0) // TODO
		if err != nil {
			return nil, err
		}

		// TODO: decompressing logic goes here:
		// decompress f.SnappyColumns in rowData
		resultSet[pk] = nil // TODO hash of the rowData
	}
	return resultSet, nil
}

func (f *SnappyFingerprinter) GetColumnsSql(schema, table, pkColumn string, columns []schema.TableColumn, pks []uint64) (string, []interface{}, error) {
	quotedPK := ghostferry.QuoteField(pkColumn)
	return sq.Select(append([]string{quotedPK}, quotedColumnNames(columns)...)...).
		From(ghostferry.QuotedTableNameFromString(schema, table)).
		Where(sq.Eq{quotedPK: pks}).
		OrderBy(quotedPK).
		ToSql()
}

func quotedColumnNames(columns []schema.TableColumn) []string {
	result := make([]string, len(columns))
	for idx, column := range columns {
		result[idx] = ghostferry.QuoteField(column.Name)
	}
	return result
}
