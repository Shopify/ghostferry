package ghostferry

import (
	"fmt"
	"os"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

var batchWriteRetryDelay = 5 * time.Millisecond

func init() {
	if os.Getenv("CI") == "true" {
		batchWriteRetryDelay = 500 * time.Millisecond
	}
}

type BatchWriterVerificationFailed struct {
	mismatchedPaginationKeys []InlineVerifierMismatches
	table                    string
}

func (e BatchWriterVerificationFailed) Error() string {
	return fmt.Sprintf("row fingerprints for paginationKeys %v on %v do not match", e.mismatchedPaginationKeys, e.table)
}

type BatchWriter struct {
	DB                        *sql.DB
	InlineVerifier            *InlineVerifier
	StateTracker              *StateTracker
	EnforceInlineVerification bool // Only needed when running the BatchWriter during cutover

	DatabaseRewrites map[string]string
	TableRewrites    map[string]string

	WriteRetries int

	stmtCache *StmtCache
	logger    *logrus.Entry

	enableRowBatchSize bool
}

func (w *BatchWriter) Initialize() {
	w.stmtCache = NewStmtCache()
	w.logger = logrus.WithField("tag", "batch_writer")
}

func (w *BatchWriter) WriteRowBatch(batch *RowBatch) error {
	return WithRetries(w.WriteRetries, batchWriteRetryDelay, w.logger, "write batch to target", func() error {
		values := batch.Values()
		if len(values) == 0 {
			return nil
		}

		var startPaginationKeypos, endPaginationKeypos PaginationKey
		var err error
		
		paginationColumn := batch.TableSchema().GetPaginationColumn()

		switch paginationColumn.Type {
		case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
			var startValue, endValue uint64
			startValue, err = values[0].GetUint64(batch.PaginationKeyIndex())
			if err != nil {
				return err
			}
			endValue, err = values[len(values)-1].GetUint64(batch.PaginationKeyIndex())
			if err != nil {
				return err
			}
			startPaginationKeypos = NewUint64Key(startValue)
			endPaginationKeypos = NewUint64Key(endValue)

		case schema.TYPE_BINARY, schema.TYPE_STRING:
			startValueInterface := values[0][batch.PaginationKeyIndex()]
			endValueInterface := values[len(values)-1][batch.PaginationKeyIndex()]

			getBytes := func(val interface{}) ([]byte, error) {
				switch v := val.(type) {
				case []byte:
					return v, nil
				case string:
					return []byte(v), nil
				default:
					return nil, fmt.Errorf("expected binary/string pagination key, got %T", val)
				}
			}

			startValue, err := getBytes(startValueInterface)
			if err != nil {
				return err
			}
			
			endValue, err := getBytes(endValueInterface)
			if err != nil {
				return err
			}

			startPaginationKeypos = NewBinaryKey(startValue)
			endPaginationKeypos = NewBinaryKey(endValue)

		default:
			var startValue, endValue uint64
			startValue, err = values[0].GetUint64(batch.PaginationKeyIndex())
			if err != nil {
				return err
			}
			endValue, err = values[len(values)-1].GetUint64(batch.PaginationKeyIndex())
			if err != nil {
				return err
			}
			startPaginationKeypos = NewUint64Key(startValue)
			endPaginationKeypos = NewUint64Key(endValue)
		}

		db := batch.TableSchema().Schema
		if targetDbName, exists := w.DatabaseRewrites[db]; exists {
			db = targetDbName
		}

		table := batch.TableSchema().Name
		if targetTableName, exists := w.TableRewrites[table]; exists {
			table = targetTableName
		}

		query, args, err := batch.AsSQLQuery(db, table)
		if err != nil {
			return fmt.Errorf("during generating sql query at paginationKey %s -> %s: %v", startPaginationKeypos.String(), endPaginationKeypos.String(), err)
		}

		stmt, err := w.stmtCache.StmtFor(w.DB, query)
		if err != nil {
			return fmt.Errorf("during prepare query near paginationKey %s -> %s (%s): %v", startPaginationKeypos.String(), endPaginationKeypos.String(), query, err)
		}

		tx, err := w.DB.Begin()
		if err != nil {
			return fmt.Errorf("unable to begin transaction in BatchWriter: %v", err)
		}

		_, err = tx.Stmt(stmt).Exec(args...)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("during exec query near paginationKey %s -> %s (%s): %v", startPaginationKeypos.String(), endPaginationKeypos.String(), query, err)
		}

		if w.InlineVerifier != nil {
			mismatches, err := w.InlineVerifier.CheckFingerprintInline(tx, db, table, batch, w.EnforceInlineVerification)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("during fingerprint checking for paginationKey %s -> %s (%s): %v", startPaginationKeypos.String(), endPaginationKeypos.String(), query, err)
			}

			if w.EnforceInlineVerification {
				// This code should only be active if the InlineVerifier background
				// reverification is not occuring. An example of this would be when you
				// run the BatchWriter as a part of copying the primary table or the delta
				// copying the joined table.
				if len(mismatches) > 0 {
					tx.Rollback()
					return BatchWriterVerificationFailed{mismatches, batch.TableSchema().String()}
				}
			}
		}

		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("during commit near paginationKey %s -> %s (%s): %v", startPaginationKeypos.String(), endPaginationKeypos.String(), query, err)
		}

		// Note that the state tracker expects us the track based on the original
		// database and table names as opposed to the target ones.
		if w.StateTracker != nil {
			bytesWrittenForThisBatch := uint64(0)
			if w.enableRowBatchSize {
				bytesWrittenForThisBatch = batch.EstimateByteSize()
			}
			w.StateTracker.UpdateLastSuccessfulPaginationKey(batch.TableSchema().String(), endPaginationKeypos,
				RowStats{NumBytes: bytesWrittenForThisBatch, NumRows: uint64(batch.Size())})
		}

		return nil
	})
}
