package testhelpers

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	"github.com/Shopify/ghostferry"
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
	TestSchemaName            = "gftest"
	TestTable1Name            = "test_table_1"
	TestCompressedTable1Name  = "test_compressed_table_1"
	TestCompressedColumn1Name = "data"
	TestCompressedData1       = "\x08" + "\x0cabcd" + "\x01\x02" // abcdcdcd
	TestCompressedData2       = "\x08" + "\x0cabcd" + "\x01\x01" // abcddddd
)

var (
	TestCompressedData3 = loadFixtureFromFile("urls1.snappy")
	TestCompressedData4 = loadFixtureFromFile("urls2.snappy")
)

func loadFixtureFromFile(path string) string {
	decompressed, err := ioutil.ReadFile(FixturePath(path))
	if err != nil {
		panic(err)
	}

	return string(decompressed)
}

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
	this.resetDbs()
}

func (this *GhostferryUnitTestSuite) SeedTargetDB(numberOfRows int) {
	SeedInitialData(this.Ferry.TargetDB, TestSchemaName, TestTable1Name, numberOfRows)
	SeedInitialData(this.Ferry.TargetDB, TestSchemaName, TestCompressedTable1Name, numberOfRows)
}

func (this *GhostferryUnitTestSuite) SeedSourceDB(numberOfRows int) {
	SeedInitialData(this.Ferry.SourceDB, TestSchemaName, TestTable1Name, numberOfRows)
	SeedInitialData(this.Ferry.SourceDB, TestSchemaName, TestCompressedTable1Name, numberOfRows)
}

func (this *GhostferryUnitTestSuite) TearDownTest() {
	this.resetDbs()
}

func (this *GhostferryUnitTestSuite) resetDbs() {
	_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", TestSchemaName))
	this.Require().Nil(err)
	_, err = this.Ferry.TargetDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", TestSchemaName))
	this.Require().Nil(err)
}
