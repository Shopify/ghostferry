package test

import (
	"fmt"
	"math"
	"os"
	"testing"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type ReplicationConfigTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	SourceDB                        *sql.DB
	ReplicatedMasterPositionFetcher *ghostferry.ReplicatedMasterPositionViaCustomQuery
	ReplicatedMasterGTIDFetcher     *ghostferry.ReplicatedMasterGTIDViaCustomQuery
	gtidMode                        bool
}

func (t *ReplicationConfigTestSuite) SetupTest() {
	var err error
	t.TestFerry = testhelpers.NewTestFerry()
	t.Ferry = t.TestFerry.Ferry
	t.SourceDB, err = t.Ferry.Source.SqlDB(nil)
	t.Require().Nil(err)

	t.gtidMode = os.Getenv("GHOSTFERRY_BINLOG_COORDINATE_MODE") == string(ghostferry.BinlogCoordinateGTID)

	wait := &ghostferry.WaitUntilReplicaIsCaughtUpToMaster{
		MasterDB:             t.SourceDB,
		BinlogCoordinateMode: t.Ferry.Config.BinlogCoordinateMode,
	}

	if t.gtidMode {
		// A high, unreachable GTID set so the replica is never "caught up".
		t.ReplicatedMasterGTIDFetcher = &ghostferry.ReplicatedMasterGTIDViaCustomQuery{
			Query: "SELECT '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-1'",
		}
		wait.ReplicatedMasterCoordinateFetcher = t.ReplicatedMasterGTIDFetcher
	} else {
		t.ReplicatedMasterPositionFetcher = &ghostferry.ReplicatedMasterPositionViaCustomQuery{
			Query: "SELECT 'mysql-bin.000003', 483685",
		}
		wait.ReplicatedMasterPositionFetcher = t.ReplicatedMasterPositionFetcher
	}

	t.Ferry.WaitUntilReplicaIsCaughtUpToMaster = wait
}

func (t *ReplicationConfigTestSuite) TearDownTest() {
	_, err := t.SourceDB.Exec("SET GLOBAL read_only = OFF")
	t.Require().Nil(err)
}

func (t *ReplicationConfigTestSuite) TestErrorsIfMasterNotProvidedOrUnreachable() {
	t.Ferry.WaitUntilReplicaIsCaughtUpToMaster.MasterDB = nil

	err := t.Ferry.Initialize()
	t.Require().NotNil(err)
}

func (t *ReplicationConfigTestSuite) TestErrorsIfItsRunFromAReplicaWithoutSettingFlag() {
	t.setReadOnly(t.SourceDB)

	t.Ferry.WaitUntilReplicaIsCaughtUpToMaster = nil
	err := t.Ferry.Initialize()
	t.Require().NotNil(err)
	t.Require().Equal("source is a read replica. running Ghostferry with a source replica is unsafe unless WaitUntilReplicaIsCaughtUpToMaster is used", err.Error())
}

func (t *ReplicationConfigTestSuite) TestErrorsIfPositionFetcherQueryIsNotProvided() {
	if t.gtidMode {
		// The GTID fetcher scans a single column, so a two-column query is the
		// error condition here.
		t.ReplicatedMasterGTIDFetcher.Query = "SELECT 1, 2"

		err := t.Ferry.Initialize()
		t.Require().NotNil(err)
		t.Require().Equal("sql: expected 2 destination arguments in Scan, not 1", err.Error())
		return
	}

	t.ReplicatedMasterPositionFetcher.Query = "SELECT 1"

	err := t.Ferry.Initialize()
	t.Require().NotNil(err)
	t.Require().Equal("sql: expected 1 destination arguments in Scan, not 2", err.Error())
}

func (t *ReplicationConfigTestSuite) TestErrorsIfProvidedMasterIsReadOnly() {
	t.setReadOnly(t.Ferry.WaitUntilReplicaIsCaughtUpToMaster.MasterDB)

	err := t.Ferry.Initialize()
	t.Require().NotNil(err)
	t.Require().Equal("source master is a read replica, not a master writer", err.Error())
}

func (t *ReplicationConfigTestSuite) TestCanInitializeFerryWithValidConfig() {
	if t.gtidMode {
		// A high, valid GTID set that parses cleanly; Initialize only probes
		// that the fetcher query is executable, so it must succeed.
		t.ReplicatedMasterGTIDFetcher.Query = "SELECT '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-999999999'"

		err := t.Ferry.Initialize()
		t.Require().Nil(err)
		return
	}

	t.ReplicatedMasterPositionFetcher.Query = fmt.Sprintf("SELECT 'mysql-bin.999999',%d", math.MaxUint32)

	err := t.Ferry.Initialize()
	t.Require().Nil(err)
}

func (t *ReplicationConfigTestSuite) setReadOnly(db *sql.DB) {
	_, err := db.Exec("SET GLOBAL read_only = ON")
	t.Require().Nil(err)
}

func TestReplicationConfigurationTestSuite(t *testing.T) {
	suite.Run(t, &ReplicationConfigTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
