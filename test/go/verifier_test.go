package test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/suite"

	"github.com/Shopify/ghostferry/v2"
	"github.com/Shopify/ghostferry/v2/testhelpers"
)

type ChecksumTableVerifierTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	verifier *ghostferry.ChecksumTableVerifier
}

func (this *ChecksumTableVerifierTestSuite) SetupTest() {
	this.GhostferryUnitTestSuite.SetupTest()

	this.verifier = &ghostferry.ChecksumTableVerifier{
		Tables: []*ghostferry.TableSchema{
			&ghostferry.TableSchema{
				Table: &schema.Table{
					Name:   testhelpers.TestTable1Name,
					Schema: testhelpers.TestSchemaName,
				},
			},
		},

		SourceDB: this.Ferry.SourceDB,
		TargetDB: this.Ferry.TargetDB,
	}

	this.SeedSourceDB(5)
}

func (this *ChecksumTableVerifierTestSuite) TestVerifyNoMatchWithEmptyTarget() {
	err := this.verifier.StartInBackground()
	this.Require().Nil(err)
	this.verifier.Wait()

	this.AssertVerifierErrored("cannot find table gftest.test_table_1 during verification")
}

func (this *ChecksumTableVerifierTestSuite) TestVerifyNoMatchWithDifferentTargetData() {
	testhelpers.SeedInitialData(this.Ferry.TargetDB, testhelpers.TestSchemaName, testhelpers.TestTable1Name, 1)

	err := this.verifier.StartInBackground()
	this.Require().Nil(err)
	this.verifier.Wait()
	this.AssertVerifierNotMatched()
}

func (this *ChecksumTableVerifierTestSuite) TestVerifyMatchAndRestartable() {
	this.copyDataFromSourceToTarget()

	err := this.verifier.StartInBackground()
	this.Require().Nil(err)
	this.verifier.Wait()
	this.AssertVerifierMatched()

	_, err = this.Ferry.TargetDB.Exec(fmt.Sprintf("DROP DATABASE %s", testhelpers.TestSchemaName))
	this.Require().Nil(err)

	err = this.verifier.StartInBackground()
	this.Require().Nil(err)
	this.verifier.Wait()
	this.AssertVerifierErrored("cannot find table gftest.test_table_1 during verification")

	this.copyDataFromSourceToTarget()
	query := fmt.Sprintf(
		"INSERT INTO %s.%s (id, data) VALUES (?, ?)",
		testhelpers.TestSchemaName,
		testhelpers.TestTable1Name,
	)

	_, err = this.Ferry.TargetDB.Exec(query, nil, "New Data")
	this.Require().Nil(err)

	err = this.verifier.StartInBackground()
	this.Require().Nil(err)
	this.verifier.Wait()
	this.AssertVerifierNotMatched()
}

func (this *ChecksumTableVerifierTestSuite) TestVerifyWithRewrites() {
	this.copyDataFromSourceToTargetTable("table2")

	this.verifier.TableRewrites = map[string]string{
		testhelpers.TestTable1Name: "table2",
	}

	err := this.verifier.StartInBackground()
	this.Require().Nil(err)
	this.verifier.Wait()
	this.AssertVerifierMatched()
}

func (this *ChecksumTableVerifierTestSuite) AssertVerifierMatched() {
	result, err := this.verifier.Result()
	this.Require().True(result.IsStarted())
	this.Require().True(result.IsDone())

	this.Require().Nil(err)
	this.Require().True(result.DataCorrect)
	this.Require().Equal("", result.Message)
}

func (this *ChecksumTableVerifierTestSuite) AssertVerifierNotMatched() {
	result, err := this.verifier.Result()
	this.Require().True(result.IsStarted())
	this.Require().True(result.IsDone())

	this.Require().Nil(err)
	this.Require().False(result.DataCorrect)
	this.Require().NotEmpty(result.Message)
}

func (this *ChecksumTableVerifierTestSuite) AssertVerifierErrored(msg ...string) {
	result, err := this.verifier.Result()
	this.Require().True(result.IsStarted())
	this.Require().True(result.IsDone())

	this.Require().NotNil(err)
	if len(msg) == 1 {
		this.Require().Equal(msg[0], err.Error())
	}

	this.Require().False(result.DataCorrect)
	this.Require().Empty(result.Message)
}

func (this *ChecksumTableVerifierTestSuite) copyDataFromSourceToTarget() {
	this.copyDataFromSourceToTargetTable(testhelpers.TestTable1Name)
}

func (this *ChecksumTableVerifierTestSuite) copyDataFromSourceToTargetTable(tablename string) {
	testhelpers.SeedInitialData(this.Ferry.TargetDB, testhelpers.TestSchemaName, tablename, 0)

	rows, err := this.Ferry.SourceDB.Query(fmt.Sprintf("SELECT * FROM `%s`.`%s`", testhelpers.TestSchemaName, testhelpers.TestTable1Name))
	this.Require().Nil(err)
	defer rows.Close()

	columns, err := rows.Columns()
	this.Require().Nil(err)

	columnPlaceholders := "(" + strings.Repeat("?,", len(columns)-1) + "?)"

	for rows.Next() {
		row, err := ghostferry.ScanGenericRow(rows, len(columns))
		this.Require().Nil(err)

		sql := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES "+columnPlaceholders, testhelpers.TestSchemaName, tablename)
		_, err = this.Ferry.TargetDB.Exec(sql, row...)
		this.Require().Nil(err)
	}
}

func TestChecksumTableVerifier(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &ChecksumTableVerifierTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
