package test

import (
	"net/http"
	"testing"
	"time"

	sth "github.com/Shopify/ghostferry/sharding/testhelpers"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type PrimaryKeyTableTestSuite struct {
	*sth.ShardingUnitTestSuite
}

func (t *PrimaryKeyTableTestSuite) SetupTest() {
	t.ShardingUnitTestSuite.SetupTest()

	err := t.Ferry.Start()
	t.Require().Nil(err)
}

func (t *PrimaryKeyTableTestSuite) TearDownTest() {
	t.ShardingUnitTestSuite.TearDownTest()
}

func (t *PrimaryKeyTableTestSuite) TestPrimaryKeyTableWithDataWriter() {
	dataWriter := &testhelpers.MixedActionDataWriter{
		ProbabilityOfInsert: 0.5,
		ProbabilityOfUpdate: 0.5,
		NumberOfWriters:     2,
		Tables:              []string{"gftest1.tenants_table"},
	}

	dataWriter.SetDB(t.Ferry.Ferry.SourceDB)

	t.CutoverLock = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dataWriter.Stop()

		_, err := t.Ferry.Ferry.SourceDB.Exec("SET GLOBAL read_only = ON")
		t.Require().Nil(err)

		dataWriter.Wait()
		time.Sleep(1 * time.Second)
	})

	go dataWriter.Run()

	t.Ferry.Run()

	var count int
	row := t.Ferry.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest2.tenants_table")
	testhelpers.PanicIfError(row.Scan(&count))
	t.Require().Equal(1, count)

	var id int
	row = t.Ferry.Ferry.TargetDB.QueryRow("SELECT id FROM gftest2.tenants_table")
	testhelpers.PanicIfError(row.Scan(&id))
	t.Require().Equal(2, id)

	var expected, actual string
	row = t.Ferry.Ferry.SourceDB.QueryRow("SELECT data FROM gftest1.tenants_table WHERE id = 2")
	testhelpers.PanicIfError(row.Scan(&expected))
	row = t.Ferry.Ferry.TargetDB.QueryRow("SELECT data FROM gftest2.tenants_table")
	testhelpers.PanicIfError(row.Scan(&actual))
	t.Require().Equal(expected, actual)
}

func TestPrimaryKeyTableTestSuite(t *testing.T) {
	suite.Run(t, &PrimaryKeyTableTestSuite{ShardingUnitTestSuite: &sth.ShardingUnitTestSuite{}})
}
