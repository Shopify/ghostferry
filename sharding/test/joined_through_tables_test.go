package test

import (
	"math/rand"
	"net/http"
	"testing"

	sth "github.com/Shopify/ghostferry/sharding/testhelpers"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type JoinedThroughTablesTestSuite struct {
	*sth.ShardingUnitTestSuite

	DataWriter testhelpers.DataWriter
}

// TODO:
// 1. Multiple test_with_secondary_table rows with same join_id

/*
=== test_with_secondary_table (grants) ===
id   bigint
data text
join_id bigint

1 | null | 1
2 | null | 2
3 | null | 3
4 | null | 4

=== join_table (api_permission) ===
id   bigint
data text
join_id bigint
tenant_id bigint

1 | null | 1 | 1
2 | null | 2 | 2
3 | null | 3 | 3
4 | null | 4 | 1
*/

func (t *JoinedThroughTablesTestSuite) SetupTest() {
	t.ShardingUnitTestSuite.SetupTest()

	t.DataWriter = &testhelpers.MixedActionDataWriter{
		ProbabilityOfInsert: 1.0,
		NumberOfWriters:     2,
		Tables:              []string{"gftest1.join_table", "gftest1.test_with_secondary_table"},
		ExtraInsertData: func(tableName string, vals map[string]interface{}) {
			if tableName == "gftest1.join_table" {
				var maxIdInJoinedTable int
				row := t.Ferry.Ferry.SourceDB.QueryRow("SELECT MAX(id) FROM gftest1.test_with_secondary_table")
				err := row.Scan(&maxIdInJoinedTable)
				testhelpers.PanicIfError(err)

				vals["join_id"] = maxIdInJoinedTable
				vals["tenant_id"] = rand.Intn(3)
			}
		},
	}

	t.DataWriter.SetDB(t.Ferry.Ferry.SourceDB)
	err := t.Ferry.Start()
	t.Require().Nil(err)
}

func (t *JoinedThroughTablesTestSuite) TearDownTest() {
	t.ShardingUnitTestSuite.TearDownTest()
}

func (t *JoinedThroughTablesTestSuite) TestJoinedTablesWithDataWriter() {
	t.CutoverLock = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := t.Ferry.Ferry.SourceDB.Exec("SET GLOBAL read_only = ON")
		t.Require().Nil(err)

		t.DataWriter.Stop()
		t.DataWriter.Wait()
	})

	go t.DataWriter.Run()
	t.Ferry.Run()

	// Assert both tables were copied, with rows for only tenant 2.
	testhelpers.AssertTwoQueriesHaveEqualResult(
		t.T(),
		t.Ferry.Ferry,
		"SELECT * FROM gftest1.test_with_secondary_table jdt JOIN gftest1.join_table jt on jdt.join_id = jt.join_id WHERE jt.tenant_id = 2",
		"SELECT * FROM gftest2.test_with_secondary_table jdt JOIN gftest2.join_table jt on jdt.join_id = jt.join_id WHERE jt.tenant_id = 2",
	)

	var count int
	// Assert no other rows were copied.
	row := t.Ferry.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest2.join_table jt WHERE jt.tenant_id != 2")
	testhelpers.PanicIfError(row.Scan(&count))
	t.Require().Equal(0, count)

	/*var expectedCount int
	row = t.Ferry.Ferry.TargetDB.QueryRow("SELECT count(distinct(join_id)) FROM gftest2.join_table jt WHERE jt.tenant_id = 2")
	testhelpers.PanicIfError(row.Scan(&expectedCount))

	var actualCount int
	row = t.Ferry.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest2.joined_table")
	testhelpers.PanicIfError(row.Scan(&actualCount))
	t.Require().Equal(expectedCount, actualCount)*/
}

func TestJoinedThroughTablesTestSuite(t *testing.T) {
	suite.Run(t, &JoinedThroughTablesTestSuite{ShardingUnitTestSuite: &sth.ShardingUnitTestSuite{}})
}
