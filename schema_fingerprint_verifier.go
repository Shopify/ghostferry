package ghostferry

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/sirupsen/logrus"
)

type SchemaFingerPrintVerifier struct {
	SourceDB                   *sql.DB
	TableRewrites              map[string]string
	TableSchemaCache           TableSchemaCache
	ErrorHandler               ErrorHandler
	PeriodicallyVerifyInterval time.Duration

	FingerPrints map[string]string

	logger *logrus.Entry
}

func (sf *SchemaFingerPrintVerifier) PeriodicallyVerifySchemaFingerprints(ctx context.Context) {
	sf.logger.Info("starting periodic schema fingerprint verification")
	ticker := time.NewTicker(sf.PeriodicallyVerifyInterval)

	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := sf.VerifySchemaFingerPrint()
			if err != nil {
				sf.ErrorHandler.Fatal("schema_fingerprint_verifier", err)
			}
		case <-ctx.Done():
			sf.logger.Info("shutdown schema_fingerprint_verifier")
			return
		}
	}
}

func (sf *SchemaFingerPrintVerifier) VerifySchemaFingerPrint() error {
	newSchemaFingerPrint, err := sf.GetSchemaFingerPrint()
	if err != nil {
		return err
	}

	oldSchemaFingerPrint := sf.FingerPrints
	if len(oldSchemaFingerPrint) == 0 {
		sf.FingerPrints = newSchemaFingerPrint
		return nil
	}

	for _, table := range sf.TableSchemaCache {
		if newSchemaFingerPrint[table.Schema] != oldSchemaFingerPrint[table.Schema] {
			return fmt.Errorf("failed to verifiy schema fingerprint for %s", table.Schema)
		}
	}

	sf.FingerPrints = newSchemaFingerPrint
	return nil
}

func (sf *SchemaFingerPrintVerifier) GetSchemaFingerPrint() (map[string]string, error) {
	schemaFingerPrints := map[string]string{}
	dbSet := map[string]struct{}{}

	for _, table := range sf.TableSchemaCache {
		if _, found := dbSet[table.Schema]; found {
			continue
		}
		dbSet[table.Schema] = struct{}{}

		query := fmt.Sprintf("SELECT * FROM information_schema.columns WHERE table_schema = '%s' ORDER BY table_name, column_name", table.Schema)
		rows, err := sf.SourceDB.Query(query)
		if err != nil {
			fmt.Println(err)
			return schemaFingerPrints, err
		}

		schemaData := [][]interface{}{}
		for rows.Next() {
			rowData, err := ScanGenericRow(rows, 21)
			if err != nil {
				return schemaFingerPrints, err
			}

			_, isIgnored := table.IgnoredColumnsForVerification[string(rowData[3].([]byte))]
			_, isBlacklisted := sf.TableRewrites[string(rowData[2].([]byte))]

			if !isIgnored && !isBlacklisted {
				schemaData = append(schemaData, rowData)
			}
		}

		schemaDataInBytes, err := json.Marshal(schemaData)
		if err != nil {
			return schemaFingerPrints, err
		}

		hash := md5.Sum([]byte(schemaDataInBytes))
		schemaFingerPrints[table.Schema] = hex.EncodeToString(hash[:])
	}

	return schemaFingerPrints, nil
}
