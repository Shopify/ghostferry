package testhelpers

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

func SetupTest() {
	var err error

	logrus.SetLevel(logrus.DebugLevel)

	seed := time.Now().UnixNano()
	envseed := os.Getenv("SEED")
	if envseed != "" {
		seed, err = strconv.ParseInt(envseed, 10, 64)
		PanicIfError(err)
	}

	logrus.Warnf("random seed: %d", seed)
	rand.Seed(seed)
}

const (
	TestSchemaName = "gftest"
	TestTable1Name = "test_table_1"
)

type GhostferryUnitTestSuite struct {
	suite.Suite

	TestFerry *TestFerry
	Ferry     *ghostferry.Ferry
}

func (this *GhostferryUnitTestSuite) SetupTest() {
	this.TestFerry = NewTestFerry()
	err := this.TestFerry.Initialize()
	this.Require().Nil(err)

	this.Ferry = this.TestFerry.Ferry
}

func (this *GhostferryUnitTestSuite) SeedInitialData(numberOfRows int) {
	err := SeedInitialData(this.Ferry.SourceDB, TestSchemaName, TestTable1Name, numberOfRows)
	this.Require().Nil(err)
}

func (this *GhostferryUnitTestSuite) TearDownTest() {
	_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", TestSchemaName))
	this.Require().Nil(err)
	_, err = this.Ferry.TargetDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", TestSchemaName))
	this.Require().Nil(err)
}
