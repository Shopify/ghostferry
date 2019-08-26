package test

import (
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/stretchr/testify/suite"
)

type StateTrackerTestSuite struct {
	suite.Suite
}

func (s *StateTrackerTestSuite) TestMinBinlogPosition() {
	serializedState := &ghostferry.SerializableState{
		LastWrittenBinlogPosition: mysql.Position{
			Name: "mysql-bin.00003",
			Pos:  4,
		},

		LastStoredBinlogPositionForInlineVerifier: mysql.Position{
			Name: "mysql-bin.00003",
			Pos:  10,
		},
	}
	s.Require().Equal(serializedState.MinBinlogPosition(), mysql.Position{"mysql-bin.00003", 4})

	serializedState = &ghostferry.SerializableState{
		LastWrittenBinlogPosition: mysql.Position{
			Name: "mysql-bin.00003",
			Pos:  4,
		},

		LastStoredBinlogPositionForInlineVerifier: mysql.Position{
			Name: "mysql-bin.00002",
			Pos:  10,
		},
	}
	s.Require().Equal(serializedState.MinBinlogPosition(), mysql.Position{"mysql-bin.00002", 10})
}

func TestStateTrackerTestSuite(t *testing.T) {
	suite.Run(t, new(StateTrackerTestSuite))
}
