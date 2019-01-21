package test

import (
	"database/sql"
	"fmt"
	"math"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type ReplicationConfigTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	SourceDB                        *sql.DB
	ReplicatedMasterPositionFetcher *ghostferry.ReplicatedMasterPositionViaCustomQuery
}

func (t *ReplicationConfigTestSuite) SetupTest() {
	var err error
	t.TestFerry = testhelpers.NewTestFerry()
	t.Ferry = t.TestFerry.Ferry
	t.SourceDB, err = t.Ferry.Source.SqlDB(nil)
	t.Require().Nil(err)

	t.ReplicatedMasterPositionFetcher = &ghostferry.ReplicatedMasterPositionViaCustomQuery{
		Query: "SELECT 'mysql-bin.000003', 483685",
	}

	t.Ferry.WaitUntilReplicaIsCaughtUpToMaster = &ghostferry.WaitUntilReplicaIsCaughtUpToMaster{
		MasterDB:                        t.SourceDB,
		ReplicatedMasterPositionFetcher: t.ReplicatedMasterPositionFetcher,
	}
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
