package test

import (
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/sharding"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/assert"
)

func TestDryRunVerificationDoesNotModifyTarget(t *testing.T) {
	config := &sharding.Config{
		Config: testhelpers.NewTestConfig(),

		ShardingKey:   "tenant_id",
		ShardingValue: 1,
		SourceDB:      "gftest",
		TargetDB:      "gftest",

		MaxExpectedVerifierDowntime: time.Minute,
	}

	ferry, err := sharding.NewFerry(config)
	assert.Nil(t, err)

	ferry.Ferry.ErrorHandler = &ghostferry.PanicErrorHandler{Ferry: ferry.Ferry}

	err = ferry.Initialize()
	assert.Nil(t, err)

	sourceDB := ferry.Ferry.SourceDB
	targetDB := ferry.Ferry.TargetDB

	defer func() {
		testhelpers.DeleteTestDBs(sourceDB)
		testhelpers.DeleteTestDBs(targetDB)
	}()

	testhelpers.SeedInitialData(sourceDB, "gftest", "table1", 1000)
	testhelpers.SeedInitialData(targetDB, "gftest", "table1", 0)

	testhelpers.AddTenantID(sourceDB, "gftest", "table1", 3)
	testhelpers.AddTenantID(targetDB, "gftest", "table1", 3)

	dataWriter := &testhelpers.MixedActionDataWriter{
		ProbabilityOfInsert: 1.0,
		NumberOfWriters:     2,

		Tables: []string{"gftest.table1"},
		Db:     sourceDB,

		ExtraInsertData: func(tableName string, vals map[string]interface{}) {
			vals["tenant_id"] = 1
		},
	}
	go dataWriter.Run()

	ferry.DryRunVerification()

	dataWriter.Stop()
	dataWriter.Wait()

	var count int
	row := sourceDB.QueryRow("SELECT count(*) FROM gftest.table1")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.True(t, count > 1000)

	row = targetDB.QueryRow("SELECT count(*) FROM gftest.table1")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.Equal(t, 0, count)
}
