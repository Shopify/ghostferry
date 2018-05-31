package test

import (
	"database/sql"
	"fmt"
	"math"
	"testing"

	"github.com/Shopify/ghostferry"
	sth "github.com/Shopify/ghostferry/sharding/testhelpers"
	"github.com/stretchr/testify/suite"
)

type ReplicationConfigTestSuite struct {
	*sth.ShardingUnitTestSuite
}

func (t *ReplicationConfigTestSuite) SetupTest() {
	t.ShardingUnitTestSuite.SetupTest()
}

func (t *ReplicationConfigTestSuite) TearDownTest() {
	t.ShardingUnitTestSuite.TearDownTest()
}

func (t *ReplicationConfigTestSuite) TestErrorsIfMasterNotProvidedOrUnreachable() {
	t.Config.RunFerryFromReplica = true
	t.Config.SourceReplicationMaster = ghostferry.DatabaseConfig{}
	t.Config.ReplicatedMasterPositionQuery = "SELECT 1"

	err := t.Ferry.Start()
	t.Require().NotNil(err)
}

func (t *ReplicationConfigTestSuite) TestErrorsIfItsRunFromAReplicaWithoutSettingFlag() {
	t.setReadOnly(t.Ferry.Ferry.SourceDB)

	err := t.Ferry.Start()
	t.Require().NotNil(err)
	t.Require().Equal("running ferry from a read replica without providing RunFerryFromReplica and SourceReplicationMaster configs is dangerous", err.Error())
}

func (t *ReplicationConfigTestSuite) TestErrorsIfPositionFetcherQueryIsNotProvided() {
	t.Config.RunFerryFromReplica = true
	t.Config.SourceReplicationMaster = t.Config.Source
	t.setReadOnly(t.Ferry.Ferry.SourceDB)

	err := t.Ferry.Start()
	t.Require().NotNil(err)
	t.Require().Equal("must provide a query to get latest replicated master position in ReplicatedMasterPositionQuery", err.Error())
}

func (t *ReplicationConfigTestSuite) TestErrorsIfProvidedMasterIsReadOnly() {
	t.Config.RunFerryFromReplica = true
	t.Config.SourceReplicationMaster = t.Config.Source
	t.Config.ReplicatedMasterPositionQuery = "SELECT 1"
	t.setReadOnly(t.Ferry.Ferry.SourceDB)

	err := t.Ferry.Start()
	t.Require().NotNil(err)
	t.Require().Equal("expected SourceReplicationMaster config to be the master's config but master is readonly", err.Error())
}

func (t *ReplicationConfigTestSuite) TestCanRunFerrySuccessfullyWhenReadingFromReplica() {
	t.Config.RunFerryFromReplica = true
	t.Config.SourceReplicationMaster = t.Config.Source
	t.Config.ReplicatedMasterPositionQuery = fmt.Sprintf("SELECT 'mysql-bin.999999',%d", math.MaxUint32)

	err := t.Ferry.Start()
	t.Require().Nil(err)

	t.Ferry.Run()

	t.AssertTenantCopied()
}

func (t *ReplicationConfigTestSuite) setReadOnly(db *sql.DB) {
	_, err := db.Exec("SET GLOBAL read_only = ON")
	t.Require().Nil(err)
}

func TestReplicationConfigurationTestSuite(t *testing.T) {
	suite.Run(t, &ReplicationConfigTestSuite{ShardingUnitTestSuite: &sth.ShardingUnitTestSuite{}})
}
