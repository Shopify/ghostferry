package test

import (
	"database/sql"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/stretchr/testify/suite"
)

type WaitUntilReplicaIsCaughtUpToMasterSuite struct {
	suite.Suite

	outdatedMasterPosition mysql.Position
	w                      *ghostferry.WaitUntilReplicaIsCaughtUpToMaster
}

func (s *WaitUntilReplicaIsCaughtUpToMasterSuite) SetupTest() {
	config := testhelpers.NewTestConfig()

	// We'll setup a fake pt-heartbeat by updating the entries manually.
	masterDB, err := config.Source.SqlDB(nil)
	s.Require().Nil(err)
	replicaDB, err := config.Target.SqlDB(nil)
	s.Require().Nil(err)

	s.w = &ghostferry.WaitUntilReplicaIsCaughtUpToMaster{
		MasterDB:  masterDB,
		ReplicaDB: replicaDB,
		ReplicatedMasterPositionFetcher: ghostferry.ReplicatedMasterPositionViaCustomQuery{
			Query: "SELECT file, position FROM meta.heartbeat WHERE server_id = 1",
		},
	}

	s.outdatedMasterPosition, err = ghostferry.ShowMasterStatusBinlogPosition(s.w.MasterDB)
	s.Require().Nil(err)

	s.recreateTable(s.w.MasterDB)
	s.recreateTable(s.w.ReplicaDB)
}

func (s *WaitUntilReplicaIsCaughtUpToMasterSuite) recreateTable(db *sql.DB) {
	_, err := db.Exec("DROP DATABASE IF EXISTS meta")
	s.Require().Nil(err)

	_, err = db.Exec("CREATE DATABASE meta")
	s.Require().Nil(err)

	_, err = db.Exec("CREATE TABLE meta.heartbeat (server_id int unsigned NOT NULL, file varchar(255) DEFAULT NULL, position bigint unsigned DEFAULT NULL, PRIMARY KEY (server_id))")
	s.Require().Nil(err)

	s.updateHeartbeatMasterPos(db, s.outdatedMasterPosition)
}

func (s *WaitUntilReplicaIsCaughtUpToMasterSuite) updateHeartbeatMasterPos(db *sql.DB, pos mysql.Position) {
	_, err := db.Exec("REPLACE INTO meta.heartbeat (server_id, file, position) VALUES (1, ?, ?)", pos.Name, pos.Pos)
	s.Require().Nil(err)
}

func (s *WaitUntilReplicaIsCaughtUpToMasterSuite) TestIsCaughtUpIsCorrect() {
	currentPosition, err := ghostferry.ShowMasterStatusBinlogPosition(s.w.MasterDB)
	s.Require().Nil(err)
	s.Require().Equal(1, currentPosition.Compare(s.outdatedMasterPosition), "test setup error, master position did not advance")

	isCaughtUp, err := s.w.IsCaughtUp(currentPosition, 1)
	s.Require().Nil(err)
	s.Require().False(isCaughtUp)

	s.updateHeartbeatMasterPos(s.w.ReplicaDB, currentPosition)

	isCaughtUp, err = s.w.IsCaughtUp(currentPosition, 1)
	s.Require().Nil(err)
	s.Require().True(isCaughtUp)
}

func TestWaitUntilReplicaIsCaughtUpToMaster(t *testing.T) {
	suite.Run(t, new(WaitUntilReplicaIsCaughtUpToMasterSuite))
}
