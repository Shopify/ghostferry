package test

import (
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/assert"
)

func TestHashesSql(t *testing.T) {
	columns := []schema.TableColumn{schema.TableColumn{Name: "id"}, schema.TableColumn{Name: "data"}, schema.TableColumn{Name: "float_col", Type: schema.TYPE_FLOAT}}
	pks := []uint64{1, 5, 42}

	sql, args, err := ghostferry.GetMd5HashesSql("gftest", "test_table", "id", columns, pks)

	assert.Nil(t, err)
	assert.Equal(t, "SELECT `id`, MD5(CONCAT(MD5(COALESCE(`id`, 'NULL')),MD5(COALESCE(`data`, 'NULL')),MD5(COALESCE((if (`float_col` = '-0', 0, `float_col`)), 'NULL')))) "+
		"AS row_fingerprint FROM `gftest`.`test_table` WHERE `id` IN (?,?,?) ORDER BY `id`", sql)
	for idx, arg := range args {
		assert.Equal(t, pks[idx], arg.(uint64))
	}
}

func TestVerificationFailsDeletedRow(t *testing.T) {
	ferry := testhelpers.NewTestFerry()
	iterativeVerifier := &ghostferry.IterativeVerifier{}
	ran := false

	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleTableDatabase,
		AfterRowCopyIsComplete: func(ferry *testhelpers.TestFerry) {
			setupIterativeVerifierFromFerry(iterativeVerifier, ferry.Ferry)

			err := iterativeVerifier.Initialize()
			testhelpers.PanicIfError(err)

			err = iterativeVerifier.VerifyBeforeCutover()
			testhelpers.PanicIfError(err)
		},
		BeforeStoppingBinlogStreaming: func(ferry *testhelpers.TestFerry) {
			ensureTestRowsAreReverified(ferry)
		},
		AfterStoppedBinlogStreaming: func(ferry *testhelpers.TestFerry) {
			deleteTestRowsToTriggerFailure(ferry)
			result, err := iterativeVerifier.VerifyDuringCutover()
			assert.Nil(t, err)
			assert.False(t, result.DataCorrect)
			assert.Regexp(t, "verification failed.*gftest.table1.*pks: (43,42)|(42,43)", result.Message)
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
		AfterRowCopyIsComplete: func(ferry *testhelpers.TestFerry) {
			setupIterativeVerifierFromFerry(iterativeVerifier, ferry.Ferry)

			err := iterativeVerifier.Initialize()
			testhelpers.PanicIfError(err)

			err = iterativeVerifier.VerifyBeforeCutover()
			testhelpers.PanicIfError(err)
		},
		BeforeStoppingBinlogStreaming: func(ferry *testhelpers.TestFerry) {
			ensureTestRowsAreReverified(ferry)
		},
		AfterStoppedBinlogStreaming: func(ferry *testhelpers.TestFerry) {
			modifyTestRowsInSource(ferry)
			result, err := iterativeVerifier.VerifyDuringCutover()
			assert.Nil(t, err)
			assert.False(t, result.DataCorrect)
			assert.Regexp(t, "verification failed.*gftest.table1.*pks: (43,42)|(42,43)", result.Message)
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
		SetupAction: func(ferry *testhelpers.TestFerry) {
			setupSingleTableDatabase(ferry)
			iterativeVerifier.IgnoredTables = []string{"table1"}
		},
		AfterRowCopyIsComplete: func(ferry *testhelpers.TestFerry) {
			setupIterativeVerifierFromFerry(iterativeVerifier, ferry.Ferry)

			err := iterativeVerifier.Initialize()
			testhelpers.PanicIfError(err)

			err = iterativeVerifier.VerifyBeforeCutover()
			testhelpers.PanicIfError(err)
		},
		BeforeStoppingBinlogStreaming: func(ferry *testhelpers.TestFerry) {
			ensureTestRowsAreReverified(ferry)
		},
		AfterStoppedBinlogStreaming: func(ferry *testhelpers.TestFerry) {
			modifyTestRowsInSource(ferry)
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
		AfterRowCopyIsComplete: func(ferry *testhelpers.TestFerry) {
			setupIterativeVerifierFromFerry(iterativeVerifier, ferry.Ferry)

			err := iterativeVerifier.Initialize()
			testhelpers.PanicIfError(err)

			err = iterativeVerifier.VerifyBeforeCutover()
			testhelpers.PanicIfError(err)
		},
		AfterStoppedBinlogStreaming: func(ferry *testhelpers.TestFerry) {
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
		BatchSize:   f.Config.DataIterationBatchSize,
		ReadRetries: f.Config.DBReadRetries,
	}

	v.BinlogStreamer = f.BinlogStreamer
	v.SourceDB = f.SourceDB
	v.TargetDB = f.TargetDB
	v.Tables = f.Tables.AsSlice()
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

func modifyTestRowsInSource(ferry *testhelpers.TestFerry) {
	_, err := ferry.Ferry.SourceDB.Exec("UPDATE gftest.table1 SET data=\"FAIL\" WHERE id = \"42\"")
	testhelpers.PanicIfError(err)

	_, err = ferry.Ferry.SourceDB.Exec("UPDATE gftest.table1 SET data=\"FAIL\" WHERE id = \"43\"")
	testhelpers.PanicIfError(err)
}

func deleteTestRowsToTriggerFailure(ferry *testhelpers.TestFerry) {
	_, err := ferry.Ferry.TargetDB.Exec("DELETE FROM gftest.table1 WHERE id = \"42\"")
	testhelpers.PanicIfError(err)

	_, err = ferry.Ferry.TargetDB.Exec("DELETE FROM gftest.table1 WHERE id = \"43\"")
	testhelpers.PanicIfError(err)
}
