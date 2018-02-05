package test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type IterativeVerifierCollationTestSuite struct {
	*IterativeVerifierTestSuite

	unsafeDb    *sql.DB
	asciiData   string
	utf8mb3Data string
	utf8mb4Data string
}

func (t *IterativeVerifierCollationTestSuite) SetupTest() {
	t.IterativeVerifierTestSuite.SetupTest()

	unsafeDbConfig := t.Ferry.Source
	t.Require().Equal("'STRICT_ALL_TABLES,NO_BACKSLASH_ESCAPES'", unsafeDbConfig.Params["sql_mode"])
	unsafeDbConfig.Params["sql_mode"] = "'NO_BACKSLASH_ESCAPES'"

	unsafeConfig, err := t.Ferry.Source.MySQLConfig()
	t.Require().Nil(err)

	unsafeDSN := unsafeConfig.FormatDSN()

	t.unsafeDb, err = sql.Open("mysql", unsafeDSN)
	t.Require().Nil(err)

	t.asciiData = "foobar"
	t.utf8mb3Data = "これは普通なストリングです"
	t.utf8mb4Data = "𠜎𠜱𠝹𠱓𠱸𠲖𠳏𠳕𠴕𠵼𠵿𠸎𠸏𠹷"
}

func (t *IterativeVerifierCollationTestSuite) TeardownTest() {
	t.IterativeVerifierTestSuite.TearDownTest()
}

func (t *IterativeVerifierCollationTestSuite) TestFingerprintOfAsciiValueDoesNotChangeFromUtf8Mb3ToUtf8Mb4() {
	t.AssertIdentical(t.asciiData, "utf8mb3", "utf8mb4")
}

func (t *IterativeVerifierCollationTestSuite) TestFingerprintOfAsciiValueDoesNotChangeFromUtf8Mb4ToUtf8Mb3() {
	t.AssertIdentical(t.asciiData, "utf8mb4", "utf8mb3")
}

func (t *IterativeVerifierCollationTestSuite) TestFingerprintOfUtf8Mb3ValueDoesNotChangeFromUtf8Mb3ToUtf8Mb4() {
	t.AssertIdentical(t.utf8mb3Data, "utf8mb3", "utf8mb4")
}

func (t *IterativeVerifierCollationTestSuite) TestFingerprintOfUtf8Mb3ValueDoesNotChangeFromUtf8Mb4ToUtf8Mb3() {
	t.AssertIdentical(t.utf8mb3Data, "utf8mb4", "utf8mb3")
}

func (t *IterativeVerifierCollationTestSuite) TestFingerprintOfUtf8Mb4ValueDoesChangeFromUtf8Mb4ToUtf8Mb3() {
	t.AssertDifferent(t.utf8mb4Data, "utf8mb4", "utf8mb3")
}

func (t *IterativeVerifierCollationTestSuite) AssertIdentical(data, from, to string) {
	fingerprints := t.GetHashesFromDifferentCollations(data, from, to)
	t.Require().Equal(fingerprints[0], fingerprints[1])
}

func (t *IterativeVerifierCollationTestSuite) AssertDifferent(data, from, to string) {
	fingerprints := t.GetHashesFromDifferentCollations(data, from, to)
	t.Require().NotEqual(fingerprints[0], fingerprints[1])
}

func (t *IterativeVerifierCollationTestSuite) GetHashesFromDifferentCollations(data, from, to string) []string {
	var fingerprints []string

	t.SetDataColumnCollation(from)
	t.InsertRow(42, data)
	fingerprints = append(fingerprints, t.GetHashes([]uint64{42})[0])

	t.SetDataColumnCollation(to)
	fingerprints = append(fingerprints, t.GetHashes([]uint64{42})[0])

	for _, fingerprint := range fingerprints {
		t.Require().True(fingerprint != "")
	}

	return fingerprints
}

func (t *IterativeVerifierCollationTestSuite) SetDataColumnCollation(charset string) {
	var collation string
	if charset == "utf8mb4" {
		collation = "utf8mb4_unicode_ci"
	} else if charset == "utf8mb3" {
		collation = "utf8_unicode_ci"
	}
	t.Require().True(collation != "")

	_, err := t.unsafeDb.Exec(fmt.Sprintf(
		"ALTER TABLE %s.%s MODIFY data VARCHAR(255) CHARACTER SET %s COLLATE %s",
		testhelpers.TestSchemaName,
		testhelpers.TestTable1Name,
		charset,
		collation,
	))
	t.Require().Nil(err)
}

func TestIterativeVerifierCollationTestSuite(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &IterativeVerifierCollationTestSuite{
		IterativeVerifierTestSuite: &IterativeVerifierTestSuite{
			GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{},
		},
	})
}
