package test

import (
	"math"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type PKTestSuite struct {
	suite.Suite
}

func (t *PKTestSuite) TestCompareUint64() {
	pk1 := ghostferry.PK{Value: uint64(10)}
	pk2 := ghostferry.PK{Value: uint64(math.MaxUint64)}

	t.Require().Equal(-1, pk1.Compare(pk2))
	t.Require().Equal(0, pk1.Compare(pk1))
	t.Require().Equal(1, pk2.Compare(pk1))
}

func TestPK(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, new(PKTestSuite))
}
