package test

import (
	"fmt"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/suite"
)

type ChecksumTableVerifierTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	verifier ghostferry.Verifier
}

func (this *ChecksumTableVerifierTestSuite) SetupTest() {
	this.GhostferryUnitTestSuite.SetupTest()

	this.verifier = &ghostferry.ChecksumTableVerifier{
		TablesToCheck: []*schema.Table{
			&schema.Table{
				Name:   testhelpers.TestTable1Name,
				Schema: testhelpers.TestSchemaName,
			},
		},
	}

	this.Ferry.Verifier = this.verifier
	this.SeedInitialData(5)
}

func (this *ChecksumTableVerifierTestSuite) TestVerifyNoMatchWithEmptyTarget() {
	this.verifier.StartVerification(this.Ferry)
	this.verifier.Wait()

	this.AssertVerifierNotMatched()
}

func (this *ChecksumTableVerifierTestSuite) TestVerifyNoMatchWithDifferentTargetData() {
	testhelpers.SeedInitialData(this.Ferry.TargetDB, testhelpers.TestSchemaName, testhelpers.TestTable1Name, 1)

	this.verifier.StartVerification(this.Ferry)
	this.verifier.Wait()
	this.AssertVerifierNotMatched()
}

func (this *ChecksumTableVerifierTestSuite) TestVerifyMatchAndRestartable() {
	this.copyDataFromSourceToTarget()

	this.verifier.StartVerification(this.Ferry)
	this.verifier.Wait()
	this.AssertVerifierMatched()

	_, err := this.Ferry.TargetDB.Exec(fmt.Sprintf("DROP DATABASE %s", testhelpers.TestSchemaName))
	this.Require().Nil(err)

	this.verifier.StartVerification(this.Ferry)
	this.verifier.Wait()
	this.AssertVerifierNotMatched()
}

func (this *ChecksumTableVerifierTestSuite) AssertVerifierMatched() {
	this.Require().True(this.verifier.VerificationStarted())
	this.Require().True(this.verifier.VerificationDone())

	correct, err := this.verifier.VerifiedCorrect()
	this.Require().True(correct)
	this.Require().Nil(err)

	mismatchedTables, _ := this.verifier.MismatchedTables()
	this.Require().Equal([]string{}, mismatchedTables)
}

func (this *ChecksumTableVerifierTestSuite) AssertVerifierNotMatched() {
	this.Require().True(this.verifier.VerificationStarted())
	this.Require().True(this.verifier.VerificationDone())

	correct, err := this.verifier.VerifiedCorrect()
	this.Require().False(correct)
	this.Require().Nil(err)

	mismatchedTables, _ := this.verifier.MismatchedTables()
	this.Require().Equal([]string{fmt.Sprintf("%s.%s", testhelpers.TestSchemaName, testhelpers.TestTable1Name)}, mismatchedTables)
}

func (this *ChecksumTableVerifierTestSuite) copyDataFromSourceToTarget() {
	testhelpers.SeedInitialData(this.Ferry.TargetDB, testhelpers.TestSchemaName, testhelpers.TestTable1Name, 0)

	rows, err := this.Ferry.SourceDB.Query(fmt.Sprintf("SELECT * FROM `%s`.`%s`", testhelpers.TestSchemaName, testhelpers.TestTable1Name))
	this.Require().Nil(err)
	defer rows.Close()

	for rows.Next() {
		var id int64
		var data string
		err = rows.Scan(&id, &data)
		this.Require().Nil(err)

		sql := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (?, ?)", testhelpers.TestSchemaName, testhelpers.TestTable1Name)
		_, err = this.Ferry.TargetDB.Exec(sql, id, data)
		this.Require().Nil(err)
	}
}

func TestChecksumTableVerifier(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &ChecksumTableVerifierTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
