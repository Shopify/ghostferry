package test

import (
	"math/rand"
	"net/http"
	"testing"
	"time"

	rtesthelpers "github.com/Shopify/ghostferry/reloc/testhelpers"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type JoinedTablesTestSuite struct {
	*rtesthelpers.RelocUnitTestSuite

	DataWriter testhelpers.DataWriter
}

func (t *JoinedTablesTestSuite) SetupTest() {
	t.RelocUnitTestSuite.SetupTest()

	t.DataWriter = &testhelpers.MixedActionDataWriter{
		ProbabilityOfInsert: 1.0,
		NumberOfWriters:     2,
		Tables:              []string{"gftest1.join_table", "gftest1.joined_table"},

		ExtraInsertData: func(tableName string, vals map[string]interface{}) {
			if tableName == "gftest1.join_table" {
				var maxIdInJoinedTable int
				row := t.Ferry.Ferry.SourceDB.QueryRow("SELECT MAX(id) FROM gftest1.joined_table")
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

func (t *JoinedTablesTestSuite) TearDownTest() {
	t.RelocUnitTestSuite.TearDownTest()
}

func (t *JoinedTablesTestSuite) TestJoinedTablesWithDataWriter() {
	t.CutoverLock = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := t.Ferry.Ferry.SourceDB.Exec("SET GLOBAL read_only = ON")
		t.Require().Nil(err)
		time.Sleep(2 * time.Second)

		t.DataWriter.Stop()
		t.DataWriter.Wait()
		time.Sleep(5 * time.Second)
	})

	go t.DataWriter.Run()
	t.Ferry.Run()

	// Assert both tables were copied, with rows for only tenant 2.
	testhelpers.AssertTwoQueriesHaveEqualResult(
		t.T(),
		t.Ferry.Ferry,
		"SELECT * FROM gftest1.joined_table jdt JOIN gftest1.join_table jt on jdt.id = jt.join_id WHERE jt.tenant_id = 2",
		"SELECT * FROM gftest2.joined_table jdt JOIN gftest2.join_table jt on jdt.id = jt.join_id WHERE jt.tenant_id = 2",
	)

	var count int
	// Assert no other rows were copied.
	row := t.Ferry.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest2.join_table jt WHERE jt.tenant_id != 2")
	testhelpers.PanicIfError(row.Scan(&count))
	t.Require().Equal(0, count)

	var expectedCount int
	row = t.Ferry.Ferry.TargetDB.QueryRow("SELECT count(distinct(join_id)) FROM gftest2.join_table jt WHERE jt.tenant_id = 2")
	testhelpers.PanicIfError(row.Scan(&expectedCount))

	var actualCount int
	row = t.Ferry.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest2.joined_table")
	testhelpers.PanicIfError(row.Scan(&actualCount))
	t.Require().Equal(expectedCount, actualCount)
}

func TestJoinedTablesTestSuite(t *testing.T) {
	suite.Run(t, &JoinedTablesTestSuite{RelocUnitTestSuite: &rtesthelpers.RelocUnitTestSuite{}})
}
