package test

import (
	"testing"

	rth "github.com/Shopify/ghostferry/reloc/testhelpers"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type PrimaryKeyTableTestSuite struct {
	*rth.RelocUnitTestSuite
}

func (t *PrimaryKeyTableTestSuite) SetupTest() {
	t.RelocUnitTestSuite.SetupTest()

	err := t.Ferry.Start()
	t.Require().Nil(err)
}

func (t *PrimaryKeyTableTestSuite) TearDownTest() {
	t.RelocUnitTestSuite.TearDownTest()
}

func (t *PrimaryKeyTableTestSuite) TestPrimaryKeyTableWithDataWriter() {
	t.Ferry.Run()

	var count, id int
	row := t.Ferry.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest2.single_row_table")
	testhelpers.PanicIfError(row.Scan(&count))
	t.Require().Equal(1, count)

	row = t.Ferry.Ferry.TargetDB.QueryRow("SELECT id FROM gftest2.single_row_table")
	testhelpers.PanicIfError(row.Scan(&id))
	t.Require().Equal(2, id)
}

func TestPrimaryKeyTableTestSuite(t *testing.T) {
	suite.Run(t, &PrimaryKeyTableTestSuite{RelocUnitTestSuite: &rth.RelocUnitTestSuite{}})
}
