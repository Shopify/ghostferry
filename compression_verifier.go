package ghostferry

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/golang/snappy"
	"github.com/siddontang/go-mysql/schema"
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

	supportedAlgorithms     map[string]struct{}
	tableColumnCompressions TableColumnCompressionConfig
}

// GetCompressedHashes compares the source data with the target data to ensure the integrity of the
// data being copied.
//
// The GetCompressedHashes method checks if the existing table contains compressed data
// and will apply the decompression algorithm to the applicable columns if necessary.
// After the columns are decompressed, the hashes of the data are used to verify equality
func (c *CompressionVerifier) GetCompressedHashes(db *sql.DB, schema, table, pkColumn string, columns []schema.TableColumn, pks []uint64) (map[uint64][]byte, error) {
	c.logger.WithFields(logrus.Fields{
		"tag":   "compression_verifier",
		"table": table,
	}).Info("decompressing table data before verification")

	tableCompression := c.tableColumnCompressions[table]

	// Extract the raw rows using SQL to be decompressed
	rows, err := getRows(db, schema, table, pkColumn, columns, pks)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Decompress applicable columns and hash the resulting column values for comparison
	resultSet := make(map[uint64][]byte)
	for rows.Next() {
		rowData, err := ScanByteRow(rows, len(columns)+1)
		if err != nil {
			return nil, err
		}

		pk, err := strconv.ParseUint(string(rowData[0]), 10, 64)
		if err != nil {
			return nil, err
		}

		// Decompress the applicable columns and then hash them together
		// to create a fingerprint. decompressedRowData contains a map of all
		// the non-compressed columns and associated decompressed values by the
		// index of the column
		decompressedRowData := [][]byte{}
		for idx, column := range columns {
			if algorithm, ok := tableCompression[column.Name]; ok {
				// rowData contains the result of "SELECT pkColumn, * FROM ...", so idx+1 to get each column
				decompressedColData, err := c.Decompress(table, column.Name, algorithm, rowData[idx+1])
				if err != nil {
					return nil, err
				}
				decompressedRowData = append(decompressedRowData, decompressedColData)
			} else {
				decompressedRowData = append(decompressedRowData, rowData[idx+1])
			}
		}

		// Hash the data of the row to be added to the result set
		decompressedRowHash, err := c.HashRow(decompressedRowData)
		if err != nil {
			return nil, err
		}

		resultSet[pk] = decompressedRowHash
	}

	metrics.Gauge(
		"compression_verifier_decompress_rows",
		float64(len(resultSet)),
		[]MetricTag{{"table", table}},
		1.0,
	)

	logrus.WithFields(logrus.Fields{
		"tag":   "compression_verifier",
		"rows":  len(resultSet),
		"table": table,
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
func NewCompressionVerifier(tableColumnCompressions TableColumnCompressionConfig) (*CompressionVerifier, error) {
	supportedAlgorithms := make(map[string]struct{})
	supportedAlgorithms[CompressionSnappy] = struct{}{}

	compressionVerifier := &CompressionVerifier{
		logger:                  logrus.WithField("tag", "compression_verifier"),
		supportedAlgorithms:     supportedAlgorithms,
		tableColumnCompressions: tableColumnCompressions,
	}

	if err := compressionVerifier.verifyConfiguredCompression(tableColumnCompressions); err != nil {
		return nil, err
	}

	return compressionVerifier, nil
}

func getRows(db *sql.DB, schema, table, pkColumn string, columns []schema.TableColumn, pks []uint64) (*sql.Rows, error) {
	quotedPK := quoteField(pkColumn)
	sql, args, err := rowSelector(columns, pkColumn).
		From(QuotedTableNameFromString(schema, table)).
		Where(sq.Eq{quotedPK: pks}).
		OrderBy(quotedPK).
		ToSql()

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

func rowSelector(columns []schema.TableColumn, pkColumn string) sq.SelectBuilder {
	columnStrs := make([]string, len(columns))
	for idx, column := range columns {
		columnStrs[idx] = column.Name
	}

	return sq.Select(fmt.Sprintf("%s, %s", quoteField(pkColumn), strings.Join(columnStrs, ",")))
}
