package test

import (
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
)

func setupSingleEntryTable(f *testhelpers.TestFerry) error {
	err := testhelpers.SeedInitialData(f.SourceDB, "gftest", "table1", 1)
	if err != nil {
		return err
	}

	return testhelpers.SeedInitialData(f.TargetDB, "gftest", "table1", 0)
}

func TestSelectUpdateBinlogCopy(t *testing.T) {
	testcase := testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleEntryTable,
		Ferry:       testhelpers.NewTestFerry(),
	}

	testcase.Ferry.BeforeRowCopyListener = func(events []ghostferry.DMLEvent) error {
		queries := make([]string, len(events))
		for i, ev := range events {
			id := ev.NewValues()[0].(int64)
			queries[i] = "UPDATE gftest.table1 SET data = 'changed' WHERE id = ?"

			go func(query string) {
				_, err := testcase.Ferry.SourceDB.Exec(query, id)
				if err != nil {
					panic(err)
				}
			}(queries[i])
		}

		// Waiting for sure until we can see the queries as they will be
		// locked due to the SELECT FOR UPDATE that is being performed.
		for !testhelpers.ProcessListContainsQueries(testcase.Ferry.SourceDB, queries) {
			time.Sleep(200 * time.Millisecond)
		}

		return nil
	}

	testcase.CustomVerifyAction = func(f *testhelpers.TestFerry) error {
		testcase.AssertOnlyDataOnSourceAndTargetIs("changed")
		return nil
	}

	testcase.Run()
}

func TestUpdateBinlogSelectCopy(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleEntryTable,
		Ferry:       testhelpers.NewTestFerry(),
	}

	testcase.AfterStartBinlogStreaming = func(f *testhelpers.TestFerry) error {
		_, err := f.SourceDB.Exec("UPDATE gftest.table1 SET data = 'changed' LIMIT 1")
		return err
	}

	testcase.CustomVerifyAction = func(f *testhelpers.TestFerry) error {
		testcase.AssertOnlyDataOnSourceAndTargetIs("changed")
		return nil
	}

	testcase.Run()
}

func TestSelectCopyUpdateBinlog(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleEntryTable,
		Ferry:       testhelpers.NewTestFerry(),
	}

	testcase.BeforeStoppingBinlogStreaming = func(f *testhelpers.TestFerry) error {
		_, err := f.SourceDB.Exec("UPDATE gftest.table1 SET data = 'changed' LIMIT 1")
		return err
	}

	testcase.CustomVerifyAction = func(f *testhelpers.TestFerry) error {
		testcase.AssertOnlyDataOnSourceAndTargetIs("changed")
		return nil
	}

	testcase.Run()
}
