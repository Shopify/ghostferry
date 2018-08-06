package testhelpers

import (
	"fmt"
	"io/ioutil"
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
	TestSchemaName            = "gftest"
	TestTable1Name            = "test_table_1"
	TestCompressedTable1Name  = "test_compressed_table_1"
	TestCompressedColumn1Name = "data"
	TestCompressedData1       = "\x08" + "\x0cabcd" + "\x01\x02" // abcdcdcd
	TestCompressedData2       = "\x08" + "\x0cabcd" + "\x01\x01" // abcddddd
)

var (
	TestCompressedData3 string
	TestCompressedData4 string
)

func init() {
	filePaths := []string{
		FixturePath("urls1.snappy"),
		FixturePath("urls2.snappy"),
	}

	decompressed := [][]byte{}
	for _, path := range filePaths {
		fileStr, err := ioutil.ReadFile(path)
		if err != nil {
			panic(err)
		}

		decompressed = append(decompressed, fileStr)
	}

	TestCompressedData3 = string(decompressed[0])
	TestCompressedData4 = string(decompressed[1])
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
