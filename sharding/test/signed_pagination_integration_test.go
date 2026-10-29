package test

import (
	"fmt"
	"math"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/sharding"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/require"
)

func TestSignedPaginationJoinedTableSkipsNullReferences(t *testing.T) {
	ferry := testhelpers.NewTestFerry()
	ferry.Config.VerifierType = ghostferry.VerifierTypeInline
	ferry.Config.UpdatableConfig.DataIterationBatchSize = 1
	ferry.Config.TableFilter = &testhelpers.TestTableFilter{
		DbsFunc: testhelpers.DbApplicabilityFilter([]string{"gftest"}),
	}
	ferry.Config.CopyFilter = &sharding.ShardedCopyFilter{
		ShardingKey: "tenant_id", ShardingValue: int64(2),
		JoinedTables: map[string][]sharding.JoinTable{
			"shared_rows": {{TableName: "links", JoinColumn: "shared_id"}},
		},
	}
	testcase := &testhelpers.IntegrationTestCase{
		T: t, Ferry: ferry,
		SetupAction: func(_ *testhelpers.TestFerry, source, target *sql.DB) {
			for _, db := range []*sql.DB{source, target} {
				for _, statement := range []string{
					"CREATE DATABASE gftest",
					"CREATE TABLE gftest.shared_rows (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(100))",
					"CREATE TABLE gftest.links (id BIGINT NOT NULL PRIMARY KEY, tenant_id BIGINT NOT NULL, shared_id BIGINT NULL, KEY tenant_page (tenant_id, id))",
				} {
					_, err := db.Exec(statement)
					testhelpers.PanicIfError(err)
				}
			}
			for _, statement := range []string{
				"INSERT INTO gftest.shared_rows VALUES (-1, 'negative'), (0, 'zero'), (10, 'positive')",
				"INSERT INTO gftest.links VALUES (1, 2, NULL), (2, 2, -1), (3, 2, 0), (4, 2, 10)",
			} {
				_, err := source.Exec(statement)
				testhelpers.PanicIfError(err)
			}
		},
	}
	defer testcase.Teardown()
	testcase.CopyData()
	rows := testcase.AssertQueriesHaveEqualResult("SELECT * FROM gftest.shared_rows ORDER BY id")
	require.Len(t, rows, 3)
	result, err := ferry.Verifier.VerifyDuringCutover()
	require.NoError(t, err)
	require.True(t, result.DataCorrect, result.Message)
}

// Run with the standard source and target MySQL instances on N1_PORT/N2_PORT.
// Each table has a unique pagination key; the non-PK case deliberately orders
// the physical primary keys opposite to the pagination keys.
func TestSignedPaginationShardedInlineMigration(t *testing.T) {
	for _, tc := range []struct {
		name  string
		keys  []int64
		nonPK bool
	}{
		{"all_negative", []int64{math.MinInt64, -100, -7, -1}, false},
		{"cross_zero", []int64{math.MinInt64, -7, 0, 3, math.MaxInt64}, false},
		{"non_primary_key", []int64{math.MinInt64, -7, 0, 3, math.MaxInt64}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ferry := selectiveFerry(int64(2))
			ferry.Config.VerifierType = ghostferry.VerifierTypeInline
			ferry.Config.UpdatableConfig.DataIterationBatchSize = 2
			paginationColumn := "id"
			if tc.nonPK {
				paginationColumn = "sequence_id"
				ferry.Config.CascadingPaginationColumnConfig = &ghostferry.CascadingPaginationColumnConfig{
					PerTable: map[string]map[string]string{"gftest": {"signed_rows": paginationColumn}},
				}
			}

			var copiedKeys []int64
			var batchCount int
			ferry.BeforeBatchCopyListener = func(batch *ghostferry.RowBatch) error {
				batchCount++
				if batch.Size() > 2 || len(batch.Fingerprints()) != batch.Size() {
					return fmt.Errorf("expected a fingerprinted batch of at most two rows, got %d rows and %d fingerprints", batch.Size(), len(batch.Fingerprints()))
				}
				for _, row := range batch.Values() {
					key, err := ghostferry.NewPaginationKeyFromRow(row, batch.PaginationKeyIndex(), batch.TableSchema().GetPaginationColumn())
					if err != nil {
						return err
					}
					value := key.SQLValue().(int64)
					if len(copiedKeys) > 0 && value <= copiedKeys[len(copiedKeys)-1] {
						return fmt.Errorf("outer sharded JOIN returned out-of-order pagination keys: %d after %d", value, copiedKeys[len(copiedKeys)-1])
					}
					copiedKeys = append(copiedKeys, value)
				}
				return nil
			}

			testcase := &testhelpers.IntegrationTestCase{
				T: t, Ferry: ferry,
				SetupAction: func(_ *testhelpers.TestFerry, source, target *sql.DB) {
					ddl := "CREATE TABLE gftest.signed_rows (id BIGINT NOT NULL PRIMARY KEY, tenant_id BIGINT NOT NULL, payload VARCHAR(100), KEY tenant_page (tenant_id, id))"
					if tc.nonPK {
						ddl = "CREATE TABLE gftest.signed_rows (id BIGINT NOT NULL PRIMARY KEY, sequence_id BIGINT NOT NULL UNIQUE, tenant_id BIGINT NOT NULL, payload VARCHAR(100), KEY tenant_page (tenant_id, sequence_id))"
					}
					for _, db := range []*sql.DB{source, target} {
						_, err := db.Exec("CREATE DATABASE gftest")
						testhelpers.PanicIfError(err)
						_, err = db.Exec(ddl)
						testhelpers.PanicIfError(err)
					}
					for i, key := range tc.keys {
						var err error
						if tc.nonPK {
							_, err = source.Exec("INSERT INTO gftest.signed_rows (id, sequence_id, tenant_id, payload) VALUES (?, ?, 2, ?)", len(tc.keys)-i, key, fmt.Sprintf("row-%d", i))
						} else {
							_, err = source.Exec("INSERT INTO gftest.signed_rows (id, tenant_id, payload) VALUES (?, 2, ?)", key, fmt.Sprintf("row-%d", i))
						}
						testhelpers.PanicIfError(err)
					}
					// A row owned by another tenant must never reach the target.
					query := "INSERT INTO gftest.signed_rows (id, tenant_id, payload) VALUES (-42, 3, 'other tenant')"
					if tc.nonPK {
						query = "INSERT INTO gftest.signed_rows (id, sequence_id, tenant_id, payload) VALUES (42, -42, 3, 'other tenant')"
					}
					_, err := source.Exec(query)
					testhelpers.PanicIfError(err)
				},
				AfterRowCopyIsComplete: func(_ *testhelpers.TestFerry, source, _ *sql.DB) {
					query := "INSERT INTO gftest.signed_rows (id, tenant_id, payload) VALUES (-2, 2, 'inserted')"
					if tc.nonPK {
						query = "INSERT INTO gftest.signed_rows (id, sequence_id, tenant_id, payload) VALUES (999, -2, 2, 'inserted')"
					}
					_, err := source.Exec(query)
					testhelpers.PanicIfError(err)
					_, err = source.Exec("UPDATE gftest.signed_rows SET payload = 'updated' WHERE "+paginationColumn+" = ?", tc.keys[len(tc.keys)-1])
					testhelpers.PanicIfError(err)
					_, err = source.Exec("DELETE FROM gftest.signed_rows WHERE "+paginationColumn+" = ?", tc.keys[0])
					testhelpers.PanicIfError(err)
				},
			}
			defer testcase.Teardown()
			testcase.CopyData()
			require.Equal(t, tc.keys, copiedKeys)
			require.GreaterOrEqual(t, batchCount, 2)
			require.IsType(t, &ghostferry.InlineVerifier{}, ferry.Verifier)
			result, err := ferry.Verifier.VerifyDuringCutover()
			require.NoError(t, err)
			require.True(t, result.DataCorrect, result.Message)
			rows := testcase.AssertQueriesHaveEqualResult("SELECT * FROM gftest.signed_rows WHERE tenant_id = 2 ORDER BY " + paginationColumn)
			require.Len(t, rows, len(tc.keys))
			var otherTenantRows int
			require.NoError(t, ferry.TargetDB.QueryRow("SELECT COUNT(*) FROM gftest.signed_rows WHERE tenant_id <> 2").Scan(&otherTenantRows))
			require.Zero(t, otherTenantRows)
		})
	}
}
