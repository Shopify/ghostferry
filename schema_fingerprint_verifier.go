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
	TargetDB                   *sql.DB
	DatabaseRewrites           map[string]string
	TableSchemaCache           TableSchemaCache
	ErrorHandler               ErrorHandler
	PeriodicallyVerifyInterval time.Duration

	SourceSchemaFingerprint string
	TargetSchemaFingerprint string

	logger *logrus.Entry
}

func (sf *SchemaFingerPrintVerifier) PeriodicallyVerifySchemaFingerprints(ctx context.Context) {
	sf.logger.Info("starting periodic schema fingerprint verification")
	ticker := time.NewTicker(sf.PeriodicallyVerifyInterval)

	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := sf.VerifySchemaFingerprint()
			if err != nil {
				sf.ErrorHandler.Fatal("schema_fingerprint_verifier", err)
			}
		case <-ctx.Done():
			sf.logger.Info("shutdown schema_fingerprint_verifier")
			return
		}
	}
}

func (sf *SchemaFingerPrintVerifier) VerifySchemaFingerprint() error {
	err := sf.verifySourceSchemaFingerprint()
	if err != nil {
		return err
	}

	err = sf.verifyTargetSchemaFingerprint()
	if err != nil {
		return err
	}

	return nil
}

func (sf *SchemaFingerPrintVerifier) verifySourceSchemaFingerprint() error {
	newSchemaSourceFingerPrint, err := sf.getSchemaFingerPrint(sf.SourceDB, false)
	if err != nil {
		return err
	}

	if len(sf.SourceSchemaFingerprint) != 0 && newSchemaSourceFingerPrint != sf.SourceSchemaFingerprint {
		return fmt.Errorf("failed to verifiy schema fingerprint on source")
	} else {
		sf.SourceSchemaFingerprint = newSchemaSourceFingerPrint
	}

	return nil
}

func (sf *SchemaFingerPrintVerifier) verifyTargetSchemaFingerprint() error {
	newSchemaTargetFingerPrint, err := sf.getSchemaFingerPrint(sf.TargetDB, true)
	if err != nil {
		return err
	}

	if len(sf.TargetSchemaFingerprint) != 0 && newSchemaTargetFingerPrint != sf.TargetSchemaFingerprint {
		return fmt.Errorf("failed to verifiy schema fingerprint on target")
	} else {
		sf.TargetSchemaFingerprint = newSchemaTargetFingerPrint
	}

	return nil
}

func (sf *SchemaFingerPrintVerifier) getSchemaFingerPrint(db *sql.DB, isTargetDB bool) (string, error) {
	dbSet := map[string]struct{}{}
	schemaData := [][]interface{}{}

	for _, table := range sf.TableSchemaCache {
		if _, found := dbSet[table.Schema]; found {
			continue
		}
		dbSet[table.Schema] = struct{}{}

		dbname := table.Schema
		if isTargetDB {
			if targetDbName, exists := sf.DatabaseRewrites[dbname]; exists {
				dbname = targetDbName
			}
		}

		query := fmt.Sprintf("SELECT * FROM information_schema.columns WHERE table_schema = '%s' ORDER BY table_name, column_name", dbname)
		rows, err := db.Query(query)
		if err != nil {
			fmt.Println(err)
			return "", err
		}

		for rows.Next() {
			// `information_schema.columns` table has 21 columns.
			rowData, err := ScanGenericRow(rows, 21)
			if err != nil {
				return "", err
			}
			schemaData = append(schemaData, rowData)
		}
	}

	schemaDataInBytes, err := json.Marshal(schemaData)
	if err != nil {
		return "", err
	}

	hash := md5.Sum([]byte(schemaDataInBytes))
	return hex.EncodeToString(hash[:]), nil
}
