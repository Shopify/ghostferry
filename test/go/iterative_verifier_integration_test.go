package test

import (
	"testing"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/assert"
)

func TestHashesSql(t *testing.T) {
	columns := []schema.TableColumn{schema.TableColumn{Name: "id"}, schema.TableColumn{Name: "data"}, schema.TableColumn{Name: "float_col", Type: schema.TYPE_FLOAT}}
	paginationKeys := []uint64{1, 5, 42}
	paginationKeysInterface := make([]interface{}, len(paginationKeys))
	for i, pk := range paginationKeys {
		paginationKeysInterface[i] = pk
	}

	sql, args, err := ghostferry.GetMd5HashesSql("gftest", "test_table", []*schema.TableColumn{&columns[0]}, columns, paginationKeysInterface)

	assert.Nil(t, err)
	assert.Equal(t, "SELECT `id`, MD5(CONCAT(MD5(COALESCE(`id`, 'NULL')),MD5(COALESCE(`data`, 'NULL')),MD5(COALESCE((if (`float_col` = '-0', 0, `float_col`)), 'NULL')))) "+
		"AS row_fingerprint FROM `gftest`.`test_table` WHERE `id` IN (?,?,?) ORDER BY `id`", sql)
	for idx, arg := range args {
		assert.Equal(t, paginationKeys[idx], arg.(uint64))
	}
}

func TestVerificationFailsDeletedRow(t *testing.T) {
	ferry := testhelpers.NewTestFerry()
	iterativeVerifier := &ghostferry.IterativeVerifier{}
	ran := false

	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleTableDatabase,
		AfterRowCopyIsComplete: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			setupIterativeVerifierFromFerry(iterativeVerifier, ferry.Ferry)

			err := iterativeVerifier.Initialize()
			testhelpers.PanicIfError(err)

			err = iterativeVerifier.VerifyBeforeCutover()
			testhelpers.PanicIfError(err)
		},
		BeforeStoppingBinlogStreaming: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			ensureTestRowsAreReverified(ferry)
		},
		AfterStoppedBinlogStreaming: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			deleteTestRowsToTriggerFailure(ferry)
			result, err := iterativeVerifier.VerifyDuringCutover()
			assert.Nil(t, err)
			assert.False(t, result.DataCorrect)
			assert.Regexp(t, "verification failed.*gftest.table1.*paginationKeys: (43)|(42)|(43,42)|(42,43)", result.Message)
			ran = true
		},
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0 / 3.0,
			ProbabilityOfUpdate: 1.0 / 3.0,
			ProbabilityOfDelete: 1.0 / 3.0,
			NumberOfWriters:     4,
			Tables:              []string{"gftest.table1"},
		},
		Ferry:                   ferry,
		DisableChecksumVerifier: true,
	}

	testcase.Run()
	assert.True(t, ran)
}

func TestVerificationFailsUpdatedRow(t *testing.T) {
	ferry := testhelpers.NewTestFerry()
	iterativeVerifier := &ghostferry.IterativeVerifier{}
	ran := false

	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleTableDatabase,
		AfterRowCopyIsComplete: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			setupIterativeVerifierFromFerry(iterativeVerifier, ferry.Ferry)

			err := iterativeVerifier.Initialize()
			testhelpers.PanicIfError(err)

			err = iterativeVerifier.VerifyBeforeCutover()
			testhelpers.PanicIfError(err)
		},
		BeforeStoppingBinlogStreaming: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			ensureTestRowsAreReverified(ferry)
		},
		AfterStoppedBinlogStreaming: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			modifyDataColumnInSourceDB(ferry)
			result, err := iterativeVerifier.VerifyDuringCutover()
			assert.Nil(t, err)
			assert.False(t, result.DataCorrect)
			assert.Regexp(t, "verification failed.*gftest.table1.*paginationKeys: (42)|(43)|(43,42)|(42,43)", result.Message)
			ran = true
		},
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0 / 3.0,
			ProbabilityOfUpdate: 1.0 / 3.0,
			ProbabilityOfDelete: 1.0 / 3.0,
			NumberOfWriters:     4,
			Tables:              []string{"gftest.table1"},
		},
		Ferry:                   ferry,
		DisableChecksumVerifier: true,
	}

	testcase.Run()
	assert.True(t, ran)
}

func TestIgnoresColumns(t *testing.T) {
	ferry := testhelpers.NewTestFerry()
	iterativeVerifier := &ghostferry.IterativeVerifier{}
	ran := false

	testcase := &testhelpers.IntegrationTestCase{
		T: t,
		SetupAction: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			setupSingleTableDatabase(ferry, sourceDB, targetDB)
			iterativeVerifier.IgnoredColumns = map[string]map[string]struct{}{"table1": {"data": struct{}{}}}
		},
		AfterRowCopyIsComplete: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			setupIterativeVerifierFromFerry(iterativeVerifier, ferry.Ferry)

			err := iterativeVerifier.Initialize()
			testhelpers.PanicIfError(err)

			err = iterativeVerifier.VerifyBeforeCutover()
			testhelpers.PanicIfError(err)
		},
		BeforeStoppingBinlogStreaming: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			ensureTestRowsAreReverified(ferry)
		},
		AfterStoppedBinlogStreaming: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			modifyDataColumnInSourceDB(ferry)

			result, err := iterativeVerifier.VerifyDuringCutover()
			assert.Nil(t, err)
			assert.True(t, result.DataCorrect)
			assert.Equal(t, "", result.Message)
			ran = true
		},
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0 / 3.0,
			ProbabilityOfUpdate: 1.0 / 3.0,
			ProbabilityOfDelete: 1.0 / 3.0,
			NumberOfWriters:     4,
			Tables:              []string{"gftest.table1"},
		},
		Ferry:                   ferry,
		DisableChecksumVerifier: true,
	}

	testcase.Run()
	assert.True(t, ran)
}

func TestIgnoresTables(t *testing.T) {
	ferry := testhelpers.NewTestFerry()
	iterativeVerifier := &ghostferry.IterativeVerifier{}
	ran := false

	testcase := &testhelpers.IntegrationTestCase{
		T: t,
		SetupAction: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			setupSingleTableDatabase(ferry, sourceDB, targetDB)
			iterativeVerifier.IgnoredTables = []string{"table1"}
		},
		AfterRowCopyIsComplete: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			setupIterativeVerifierFromFerry(iterativeVerifier, ferry.Ferry)

			err := iterativeVerifier.Initialize()
			testhelpers.PanicIfError(err)

			err = iterativeVerifier.VerifyBeforeCutover()
			testhelpers.PanicIfError(err)
		},
		BeforeStoppingBinlogStreaming: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			ensureTestRowsAreReverified(ferry)
		},
		AfterStoppedBinlogStreaming: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			modifyAllRows(ferry)
			result, err := iterativeVerifier.VerifyDuringCutover()
			assert.Nil(t, err)
			assert.True(t, result.DataCorrect)
			ran = true
		},
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0 / 3.0,
			ProbabilityOfUpdate: 1.0 / 3.0,
			ProbabilityOfDelete: 1.0 / 3.0,
			NumberOfWriters:     4,
			Tables:              []string{"gftest.table1"},
		},
		Ferry:                   ferry,
		DisableChecksumVerifier: true,
	}

	testcase.Run()
	assert.True(t, ran)
}

func TestVerificationPasses(t *testing.T) {
	ferry := testhelpers.NewTestFerry()
	iterativeVerifier := &ghostferry.IterativeVerifier{}
	ran := false

	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleTableDatabase,
		AfterRowCopyIsComplete: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			setupIterativeVerifierFromFerry(iterativeVerifier, ferry.Ferry)

			err := iterativeVerifier.Initialize()
			testhelpers.PanicIfError(err)

			err = iterativeVerifier.VerifyBeforeCutover()
			testhelpers.PanicIfError(err)
		},
		AfterStoppedBinlogStreaming: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			result, err := iterativeVerifier.VerifyDuringCutover()
			assert.Nil(t, err)
			assert.True(t, result.DataCorrect)
			ran = true
		},
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0 / 3.0,
			ProbabilityOfUpdate: 1.0 / 3.0,
			ProbabilityOfDelete: 1.0 / 3.0,
			NumberOfWriters:     4,
			Tables:              []string{"gftest.table1"},
		},
		Ferry: ferry,
	}

	testcase.Run()
	assert.True(t, ran)
}

func setupIterativeVerifierFromFerry(v *ghostferry.IterativeVerifier, f *ghostferry.Ferry) {
	v.CursorConfig = &ghostferry.CursorConfig{
		DB:          f.SourceDB,
		BatchSize:   &f.Config.UpdatableConfig.DataIterationBatchSize,
		ReadRetries: f.Config.DBReadRetries,
	}

	v.BinlogStreamer = f.BinlogStreamer
	v.SourceDB = f.SourceDB
	v.TargetDB = f.TargetDB
	v.Tables = f.Tables.AsSlice()
	v.TableSchemaCache = f.Tables
	v.Concurrency = 2
}

func ensureTestRowsAreReverified(ferry *testhelpers.TestFerry) {
	_, err := ferry.Ferry.SourceDB.Exec("INSERT IGNORE INTO gftest.table1 VALUES (42, \"OK\")")
	testhelpers.PanicIfError(err)
	_, err = ferry.Ferry.SourceDB.Exec("UPDATE gftest.table1 SET data=\"OK\" WHERE id = \"42\"")
	testhelpers.PanicIfError(err)

	_, err = ferry.Ferry.SourceDB.Exec("INSERT IGNORE INTO gftest.table1 VALUES (43, \"OK\")")
	testhelpers.PanicIfError(err)
	_, err = ferry.Ferry.SourceDB.Exec("UPDATE gftest.table1 SET data=\"OK\" WHERE id = \"43\"")
	testhelpers.PanicIfError(err)

	_, err = ferry.Ferry.SourceDB.Exec("INSERT IGNORE INTO gftest.table1 VALUES (44, \"OK\")")
	testhelpers.PanicIfError(err)
	_, err = ferry.Ferry.SourceDB.Exec("UPDATE gftest.table1 SET data=\"OK\" WHERE id = \"44\"")
	testhelpers.PanicIfError(err)
}

func modifyDataColumnInSourceDB(ferry *testhelpers.TestFerry) {
	_, err := ferry.Ferry.SourceDB.Exec("UPDATE gftest.table1 SET data=\"FAIL\" WHERE id = \"42\"")
	testhelpers.PanicIfError(err)

	_, err = ferry.Ferry.SourceDB.Exec("UPDATE gftest.table1 SET data=\"FAIL\" WHERE id = \"43\"")
	testhelpers.PanicIfError(err)
}

func modifyAllRows(ferry *testhelpers.TestFerry) {
	_, err := ferry.Ferry.TargetDB.Exec("UPDATE gftest.table1 SET data=\"FAIL\"")
	testhelpers.PanicIfError(err)
}

func deleteTestRowsToTriggerFailure(ferry *testhelpers.TestFerry) {
	_, err := ferry.Ferry.TargetDB.Exec("DELETE FROM gftest.table1 WHERE id = \"42\"")
	testhelpers.PanicIfError(err)

	_, err = ferry.Ferry.TargetDB.Exec("DELETE FROM gftest.table1 WHERE id = \"43\"")
	testhelpers.PanicIfError(err)
}

func TestHashesSqlWithCompositeKey(t *testing.T) {
	columns := []schema.TableColumn{
		{Name: "tenant_id", Type: schema.TYPE_NUMBER},
		{Name: "id", Type: schema.TYPE_NUMBER},
		{Name: "data"},
	}
	// Composite keys as comma-separated strings: "1,10", "2,20"
	paginationKeysInterface := []interface{}{"1,10", "2,20"}

	sql, args, err := ghostferry.GetMd5HashesSql(
		"gftest", "test_table",
		[]*schema.TableColumn{&columns[0], &columns[1]},
		columns,
		paginationKeysInterface,
	)

	assert.Nil(t, err)
	assert.Equal(t,
		"SELECT `tenant_id`, `id`, MD5(CONCAT(MD5(COALESCE(`tenant_id`, 'NULL')),MD5(COALESCE(`id`, 'NULL')),MD5(COALESCE(`data`, 'NULL')))) "+
			"AS row_fingerprint FROM `gftest`.`test_table` WHERE (`tenant_id`, `id`) IN ((?, ?), (?, ?)) ORDER BY `tenant_id`, `id`",
		sql)
	assert.Equal(t, []interface{}{uint64(1), uint64(10), uint64(2), uint64(20)}, args)
}

func TestHashesSqlWithThreeColumnCompositeKey(t *testing.T) {
	columns := []schema.TableColumn{
		{Name: "region_id", Type: schema.TYPE_NUMBER},
		{Name: "tenant_id", Type: schema.TYPE_NUMBER},
		{Name: "id", Type: schema.TYPE_NUMBER},
		{Name: "data"},
	}
	// Composite keys as comma-separated strings: "1,2,10", "1,3,20"
	paginationKeysInterface := []interface{}{"1,2,10", "1,3,20"}

	sql, args, err := ghostferry.GetMd5HashesSql(
		"gftest", "test_table",
		[]*schema.TableColumn{&columns[0], &columns[1], &columns[2]},
		columns,
		paginationKeysInterface,
	)

	assert.Nil(t, err)
	assert.Equal(t,
		"SELECT `region_id`, `tenant_id`, `id`, MD5(CONCAT(MD5(COALESCE(`region_id`, 'NULL')),MD5(COALESCE(`tenant_id`, 'NULL')),MD5(COALESCE(`id`, 'NULL')),MD5(COALESCE(`data`, 'NULL')))) "+
			"AS row_fingerprint FROM `gftest`.`test_table` WHERE (`region_id`, `tenant_id`, `id`) IN ((?, ?, ?), (?, ?, ?)) ORDER BY `region_id`, `tenant_id`, `id`",
		sql)
	assert.Equal(t, []interface{}{uint64(1), uint64(2), uint64(10), uint64(1), uint64(3), uint64(20)}, args)
}

func TestVerificationWithCompositeKeyDetectsMismatch(t *testing.T) {
	ferry := testhelpers.NewTestFerry()
	// Filter to only composite_table_2 to avoid mixing key types with table1
	ferry.Config.TableFilter = &testhelpers.TestTableFilter{
		DbsFunc: testhelpers.DbApplicabilityFilter([]string{"gftest"}),
		TablesFunc: func(tables []*ghostferry.TableSchema) []*ghostferry.TableSchema {
			for _, t := range tables {
				if t.Name == "composite_table_2" {
					return []*ghostferry.TableSchema{t}
				}
			}
			return nil
		},
	}

	iterativeVerifier := &ghostferry.IterativeVerifier{}
	ran := false

	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupCompositeKeyTableDatabase(2),
		AfterRowCopyIsComplete: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			setupIterativeVerifierFromFerry(iterativeVerifier, ferry.Ferry)

			err := iterativeVerifier.Initialize()
			testhelpers.PanicIfError(err)

			err = iterativeVerifier.VerifyBeforeCutover()
			testhelpers.PanicIfError(err)
		},
		BeforeStoppingBinlogStreaming: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			_, err := sourceDB.Exec("INSERT INTO gftest.composite_table_2 VALUES (1, 1, 'test') ON DUPLICATE KEY UPDATE data = 'reverify'")
			testhelpers.PanicIfError(err)
		},
		AfterStoppedBinlogStreaming: func(ferry *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			// Modify target data to create mismatch
			_, err := targetDB.Exec("UPDATE gftest.composite_table_2 SET data = 'MISMATCH' WHERE k1 = 1 AND k2 = 1")
			testhelpers.PanicIfError(err)

			result, err := iterativeVerifier.VerifyDuringCutover()
			assert.Nil(t, err)
			assert.False(t, result.DataCorrect)
			assert.Contains(t, result.Message, "composite_table_2")
			ran = true
		},
		Ferry:                   ferry,
		DisableChecksumVerifier: true,
	}

	testcase.Run()
	assert.True(t, ran)
}
