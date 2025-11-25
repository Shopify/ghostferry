package ghostferry

import (
	"crypto/md5"
	sqlorig "database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	sq "github.com/Masterminds/squirrel"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/golang/snappy"
	"github.com/sirupsen/logrus"
)

const (
	// CompressionSnappy is used to identify Snappy (https://google.github.io/snappy/) compressed column data
	CompressionSnappy = "SNAPPY"
)

type (
	// TableColumnCompressionConfig represents compression configuration for a
	// column in a table as table -> column -> compression-type
	// ex: books -> contents -> snappy
	TableColumnCompressionConfig map[string]map[string]string
)

// UnsupportedCompressionError is used to identify errors resulting
// from attempting to decompress unsupported algorithms
type UnsupportedCompressionError struct {
	table     string
	column    string
	algorithm string
}

func (e UnsupportedCompressionError) Error() string {
	return "Compression algorithm: " + e.algorithm +
		" not supported on table: " + e.table +
		" for column: " + e.column
}

// CompressionVerifier provides support for verifying the payload of compressed columns that
// may have different hashes for the same data by first decompressing the compressed
// data before fingerprinting
type CompressionVerifier struct {
	logger *logrus.Entry

	TableSchemaCache        TableSchemaCache
	supportedAlgorithms     map[string]struct{}
	tableColumnCompressions TableColumnCompressionConfig
}

// GetCompressedHashes compares the source data with the target data to ensure the integrity of the
// data being copied.
//
// The GetCompressedHashes method checks if the existing table contains compressed data
// and will apply the decompression algorithm to the applicable columns if necessary.
// After the columns are decompressed, the hashes of the data are used to verify equality
func (c *CompressionVerifier) GetCompressedHashes(db *sql.DB, schemaName, tableName, paginationKeyColumn string, columns []schema.TableColumn, paginationKeys []interface{}) (map[string][]byte, error) {
	c.logger.WithFields(logrus.Fields{
		"tag":   "compression_verifier",
		"table": tableName,
	}).Info("decompressing table data before verification")

	tableCompression := c.tableColumnCompressions[tableName]

	table := c.TableSchemaCache.Get(schemaName, tableName)
	if table == nil {
		return nil, fmt.Errorf("table %s.%s not found in schema cache", schemaName, tableName)
	}
	paginationColumns := table.GetPaginationColumns()

	// Extract the raw rows using SQL to be decompressed
	rows, err := getRows(db, schemaName, tableName, paginationColumns, columns, paginationKeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resultSet := make(map[string][]byte)
	numPaginationCols := len(paginationColumns)

	for rows.Next() {
		// Scan: pagination_col1, pagination_col2, ..., data_cols...
		rowData, err := ScanByteRow(rows, len(columns)+numPaginationCols)
		if err != nil {
			return nil, err
		}

		// Build pagination key from columns (works for both single and composite keys)
		keys := make([]PaginationKey, len(paginationColumns))
		for i, paginationColumn := range paginationColumns {
			switch paginationColumn.Type {
			case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
				paginationKeyUint, err := strconv.ParseUint(string(rowData[i]), 10, 64)
				if err != nil {
					return nil, err
				}
				keys[i] = NewUint64Key(paginationKeyUint)

			case schema.TYPE_BINARY, schema.TYPE_STRING:
				keys[i] = NewBinaryKey(rowData[i])

			default:
				paginationKeyUint, err := strconv.ParseUint(string(rowData[i]), 10, 64)
				if err != nil {
					return nil, err
				}
				keys[i] = NewUint64Key(paginationKeyUint)
			}
		}
		
		// For single column, use the key directly; for composite, wrap in CompositeKey
		var paginationKeyStr string
		if len(keys) == 1 {
			paginationKeyStr = keys[0].String()
		} else {
			paginationKeyStr = CompositeKey(keys).String()
		}

		// Decompress the applicable columns and then hash them together
		// to create a fingerprint. decompressedRowData contains a map of all
		// the non-compressed columns and associated decompressed values by the
		// index of the column
		decompressedRowData := [][]byte{}
		for idx, column := range columns {
			if algorithm, ok := tableCompression[column.Name]; ok {
				// rowData contains the result of "SELECT paginationKeyCols..., * FROM ...", so idx+numPaginationCols to get each data column
				decompressedColData, err := c.Decompress(tableName, column.Name, algorithm, rowData[idx+numPaginationCols])
				if err != nil {
					return nil, err
				}
				decompressedRowData = append(decompressedRowData, decompressedColData)
			} else {
				decompressedRowData = append(decompressedRowData, rowData[idx+numPaginationCols])
			}
		}

		// Hash the data of the row to be added to the result set
		decompressedRowHash, err := c.HashRow(decompressedRowData)
		if err != nil {
			return nil, err
		}

		resultSet[paginationKeyStr] = decompressedRowHash
	}

	metrics.Gauge(
		"compression_verifier_decompress_rows",
		float64(len(resultSet)),
		[]MetricTag{{"table", tableName}},
		1.0,
	)

	logrus.WithFields(logrus.Fields{
		"tag":   "compression_verifier",
		"rows":  len(resultSet),
		"table": tableName,
	}).Debug("decompressed rows will be compared")

	return resultSet, nil
}

// Decompress will apply the configured decompression algorithm to the configured columns data
func (c *CompressionVerifier) Decompress(table, column, algorithm string, compressed []byte) ([]byte, error) {
	var decompressed []byte
	switch strings.ToUpper(algorithm) {
	case CompressionSnappy:
		return snappy.Decode(decompressed, compressed)
	default:
		return nil, UnsupportedCompressionError{
			table:     table,
			column:    column,
			algorithm: algorithm,
		}
	}

}

// HashRow will fingerprint the non-primary columns of the row to verify data equality
func (c *CompressionVerifier) HashRow(decompressedRowData [][]byte) ([]byte, error) {
	if len(decompressedRowData) == 0 {
		return nil, errors.New("Row data to fingerprint must not be empty")
	}

	hash := md5.New()
	var rowFingerprint []byte
	for _, colData := range decompressedRowData {
		rowFingerprint = append(rowFingerprint, colData...)
	}

	_, err := hash.Write(rowFingerprint)
	if err != nil {
		return nil, err
	}

	return []byte(hex.EncodeToString(hash.Sum(nil))), nil
}

// IsCompressedTable will identify whether or not a table is compressed
func (c *CompressionVerifier) IsCompressedTable(table string) bool {
	if _, ok := c.tableColumnCompressions[table]; ok {
		return true
	}
	return false
}

func (c *CompressionVerifier) verifyConfiguredCompression(tableColumnCompressions TableColumnCompressionConfig) error {
	for table, columns := range tableColumnCompressions {
		for column, algorithm := range columns {
			if _, ok := c.supportedAlgorithms[strings.ToUpper(algorithm)]; !ok {
				return &UnsupportedCompressionError{
					table:     table,
					column:    column,
					algorithm: algorithm,
				}
			}
		}
	}

	return nil
}

// NewCompressionVerifier first checks the map for supported compression algorithms before
// initializing and returning the initialized instance.
func NewCompressionVerifier(tableColumnCompressions TableColumnCompressionConfig, tableSchemaCache TableSchemaCache) (*CompressionVerifier, error) {
	supportedAlgorithms := make(map[string]struct{})
	supportedAlgorithms[CompressionSnappy] = struct{}{}

	compressionVerifier := &CompressionVerifier{
		logger:                  logrus.WithField("tag", "compression_verifier"),
		TableSchemaCache:        tableSchemaCache,
		supportedAlgorithms:     supportedAlgorithms,
		tableColumnCompressions: tableColumnCompressions,
	}

	if err := compressionVerifier.verifyConfiguredCompression(tableColumnCompressions); err != nil {
		return nil, err
	}

	return compressionVerifier, nil
}

func getRows(db *sql.DB, schemaName, table string, paginationKeyColumns []*schema.TableColumn, columns []schema.TableColumn, paginationKeys []interface{}) (*sqlorig.Rows, error) {
	builder := rowSelector(columns, paginationKeyColumns).
		From(QuotedTableNameFromString(schemaName, table))
	
	if len(paginationKeyColumns) == 1 {
		// Single column WHERE clause
		quotedPaginationKey := QuoteField(paginationKeyColumns[0].Name)
		builder = builder.Where(sq.Eq{quotedPaginationKey: paginationKeys})
		builder = builder.OrderBy(quotedPaginationKey)
	} else {
		// Composite key WHERE clause: (col1, col2) IN ((?, ?), (?, ?), ...)
		quotedPKCols := make([]string, len(paginationKeyColumns))
		for i, col := range paginationKeyColumns {
			quotedPKCols[i] = QuoteField(col.Name)
		}
		tuple := fmt.Sprintf("(%s)", strings.Join(quotedPKCols, ", "))
		
		// Build placeholder tuples for each pagination key string
		placeholderTuples := make([]string, len(paginationKeys))
		args := make([]interface{}, 0, len(paginationKeys)*len(paginationKeyColumns))
		
		for i, pkInterface := range paginationKeys {
			pkStr, ok := pkInterface.(string)
			if !ok {
				return nil, fmt.Errorf("expected string pagination key for composite key, got %T", pkInterface)
			}
			
			// Parse the composite key string (comma-separated)
			parts := strings.Split(pkStr, ",")
			if len(parts) != len(paginationKeyColumns) {
				return nil, fmt.Errorf("pagination key has %d parts but expected %d", len(parts), len(paginationKeyColumns))
			}
			
			placeholders := make([]string, len(parts))
			for j, part := range parts {
				placeholders[j] = "?"
				// Convert string representation back to appropriate type
				col := paginationKeyColumns[j]
				switch col.Type {
				case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
					val, err := strconv.ParseUint(part, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("failed to parse pagination key part %q as uint64: %w", part, err)
					}
					args = append(args, val)
				case schema.TYPE_BINARY, schema.TYPE_STRING:
					// For binary keys, the string is hex-encoded
					decoded, err := hex.DecodeString(part)
					if err != nil {
						return nil, fmt.Errorf("failed to decode pagination key part %q: %w", part, err)
					}
					args = append(args, decoded)
				default:
					val, err := strconv.ParseUint(part, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("failed to parse pagination key part %q: %w", part, err)
					}
					args = append(args, val)
				}
			}
			placeholderTuples[i] = fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
		}
		
		whereClause := fmt.Sprintf("%s IN (%s)", tuple, strings.Join(placeholderTuples, ", "))
		builder = builder.Where(whereClause, args...)
		builder = builder.OrderBy(strings.Join(quotedPKCols, ", "))
	}
	
	sql, args, err := builder.ToSql()
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

	return rows, nil
}

func rowSelector(columns []schema.TableColumn, paginationKeyColumns []*schema.TableColumn) sq.SelectBuilder {
	// Select all pagination key columns first
	selectParts := make([]string, len(paginationKeyColumns))
	for i, col := range paginationKeyColumns {
		selectParts[i] = QuoteField(col.Name)
	}
	
	columnStrs := make([]string, len(columns))
	for idx, column := range columns {
		columnStrs[idx] = column.Name
	}

	return sq.Select(fmt.Sprintf("%s, %s", strings.Join(selectParts, ", "), strings.Join(columnStrs, ",")))
}
