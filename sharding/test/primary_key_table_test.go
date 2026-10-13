package test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	sth "github.com/Shopify/ghostferry/sharding/testhelpers"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
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

		rows, err := t.Ferry.Ferry.TargetDB.Query("SELECT * FROM gftest2.tenants_table")
		t.Require().Nil(err)
		if rows.Next() {
			t.Require().Fail("did not expect primary key table rows to be copied")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
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
	// there should only be one tenant in the target and its data should match the source
	row = t.Ferry.Ferry.TargetDB.QueryRow("SELECT data FROM gftest2.tenants_table")
	testhelpers.PanicIfError(row.Scan(&actual))
	t.Require().Equal(expected, actual)
}

func (t *PrimaryKeyTableTestSuite) TestPrimaryKeyTableVerificationFailure() {
	query := "INSERT INTO %s.%s (id, data) VALUES (?, ?)"
	query = fmt.Sprintf(query, "gftest2", "tenants_table")
	t.Ferry.Ferry.TargetDB.Exec(query, 2, "foo")

	errHandler := &testhelpers.ErrorHandler{}
	t.Ferry.Ferry.ErrorHandler = errHandler

	t.Ferry.Run()

	err := errHandler.Err()
	t.Require().NotNil(err)

	t.Require().True(strings.HasPrefix(err.Error(), "row fingerprints for paginationKeys [{2"))
	t.Require().True(strings.HasSuffix(err.Error(), "on gftest1.tenants_table do not match"))
}

// VerifyDuringCutover runs after Ferry.Run has joined, just before
// copyPrimaryKeyTables: the only safe point to fake a source failover.
type sourceRuntimeSwappingVerifier struct {
	ghostferry.Verifier
	afterCutover func() error
}

func (v *sourceRuntimeSwappingVerifier) VerifyDuringCutover() (ghostferry.VerificationResult, error) {
	result, err := v.Verifier.VerifyDuringCutover()
	if err != nil || !result.DataCorrect {
		return result, err
	}
	return result, v.afterCutover()
}

// Regression: copyPrimaryKeyTables read schemas from the Initialize-time handle, stale after a failover.
func (t *PrimaryKeyTableTestSuite) TestPrimaryKeyTablesUseSourceRuntime() {
	f := t.Ferry.Ferry
	errorHandler := &testhelpers.ErrorHandler{}
	f.ErrorHandler = errorHandler

	staticSourceDB := f.SourceDB
	var replacement *sql.DB

	original := f.Verifier
	t.Require().NotNil(original)
	f.Verifier = &sourceRuntimeSwappingVerifier{
		Verifier: original,
		afterCutover: func() error {
			// Swap the source the way failover recovery does and retire the initial handle.
			newDB, err := f.SourceRuntime().Replace(f.Config.Source, nil)
			if err != nil {
				return err
			}
			replacement = newDB
			return staticSourceDB.Close()
		},
	}

	t.Ferry.Run()

	// Ferry goroutines have joined, so the recorded error is stable.
	t.Require().Nil(errorHandler.Err())
	t.Require().NotNil(replacement)

	// The suite teardown writes through this handle.
	f.SourceDB = replacement
	t.T().Cleanup(func() {
		f.SourceRuntime().CloseRetired()
		_ = replacement.Close()
	})

	var count int
	row := f.TargetDB.QueryRow("SELECT count(*) FROM gftest2.tenants_table")
	testhelpers.PanicIfError(row.Scan(&count))
	t.Require().Equal(1, count)

	var id int
	row = f.TargetDB.QueryRow("SELECT id FROM gftest2.tenants_table")
	testhelpers.PanicIfError(row.Scan(&id))
	t.Require().Equal(2, id)

	// The suite's own handle, which the swap did not touch.
	var expected, actual string
	row = t.SourceDB.QueryRow("SELECT data FROM gftest1.tenants_table WHERE id = 2")
	testhelpers.PanicIfError(row.Scan(&expected))
	row = f.TargetDB.QueryRow("SELECT data FROM gftest2.tenants_table")
	testhelpers.PanicIfError(row.Scan(&actual))
	t.Require().Equal(expected, actual)
}

func TestPrimaryKeyTableTestSuite(t *testing.T) {
	suite.Run(t, &PrimaryKeyTableTestSuite{ShardingUnitTestSuite: &sth.ShardingUnitTestSuite{}})
}
