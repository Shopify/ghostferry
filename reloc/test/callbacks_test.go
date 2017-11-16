package test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Shopify/ghostferry/reloc"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type CallbacksTestSuite struct {
	suite.Suite

	server   *httptest.Server
	lastReq  *http.Request
	respFunc func(http.ResponseWriter, *http.Request)

	ferry      *reloc.RelocFerry
	errHandler testhelpers.ErrorHandler
}

func (s *CallbacksTestSuite) SetupSuite() {
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.lastReq = r
		if s.respFunc != nil {
			s.respFunc(w, r)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
}

func (s *CallbacksTestSuite) TearDownSuite() {
	s.server.Close()
}

func (s *CallbacksTestSuite) SetupTest() {
	s.respFunc = nil
	s.lastReq = nil

	s.setupRelocFerry()
	s.dropTestDbs()

	testhelpers.SeedInitialData(s.ferry.Ferry.SourceDB, "gftest1", "table1", 1000)
	testhelpers.SeedInitialData(s.ferry.Ferry.TargetDB, "gftest2", "table1", 0)

	testhelpers.AddTenantID(s.ferry.Ferry.SourceDB, "gftest1", "table1", 3)
	testhelpers.AddTenantID(s.ferry.Ferry.TargetDB, "gftest2", "table1", 3)

	err := s.ferry.Start()
	s.Require().Nil(err)
}

func (s *CallbacksTestSuite) TearDownTest() {
	s.dropTestDbs()
}

func (s *CallbacksTestSuite) TestFailsRunOnUnlockError() {
	callbackReceived := false
	s.respFunc = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/unlock" {
			callbackReceived = true
			w.WriteHeader(http.StatusInternalServerError)
		}
	})

	s.ferry.Run()
	s.Require().True(callbackReceived)
	s.Require().NotNil(s.errHandler.LastError)
	s.Require().Equal("callback returned 500 Internal Server Error", s.errHandler.LastError.Error())
}

func (s *CallbacksTestSuite) TestFailsRunOnLockError() {
	callbackReceived := false
	s.respFunc = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/lock" {
			callbackReceived = true
			w.WriteHeader(http.StatusInternalServerError)
		}
	})

	s.ferry.Run()
	s.Require().True(callbackReceived)
	s.Require().NotNil(s.errHandler.LastError)
	s.Require().Equal("callback returned 500 Internal Server Error", s.errHandler.LastError.Error())
}

func (s *CallbacksTestSuite) TestPostsCallbacks() {
	lockReceived := false
	unlockReceived := false
	s.respFunc = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/lock" {
			lockReceived = true
			resp := s.requestMap(r)
			s.Require().Equal("test_lock", resp["Payload"])
		} else if r.URL.Path == "/unlock" {
			unlockReceived = true
			resp := s.requestMap(r)
			s.Require().Equal("test_unlock", resp["Payload"])
		} else {
			s.Fail("Unexpected callback made")
		}
	})

	s.ferry.Run()
	s.Require().True(lockReceived)
	s.Require().True(unlockReceived)

	testhelpers.AssertTwoQueriesHaveEqualResult(
		s.T(),
		s.ferry.Ferry,
		"SELECT * FROM gftest1.table1 WHERE tenant_id = 2",
		"SELECT * FROM gftest2.table1",
	)
}

func (s *CallbacksTestSuite) setupRelocFerry() {
	ghostferryConfig := testhelpers.NewTestConfig()

	config := &reloc.Config{
		Config:        *ghostferryConfig,
		ShardingKey:   "tenant_id",
		ShardingValue: 2,
		SourceDB:      "gftest1",
		TargetDB:      "gftest2",
		CutoverLock: reloc.HTTPCallback{
			URI:     fmt.Sprintf("%s/lock", s.server.URL),
			Payload: "test_lock",
		},
		CutoverUnlock: reloc.HTTPCallback{
			URI:     fmt.Sprintf("%s/unlock", s.server.URL),
			Payload: "test_unlock",
		},
	}

	var err error
	s.ferry, err = reloc.NewFerry(config)
	s.Require().Nil(err)

	s.ferry.Ferry.ErrorHandler = &s.errHandler

	err = s.ferry.Initialize()
	s.Require().Nil(err)
}

func (s *CallbacksTestSuite) dropTestDbs() {
	_, err := s.ferry.Ferry.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", "gftest1"))
	s.Require().Nil(err)

	_, err = s.ferry.Ferry.TargetDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", "gftest2"))
	s.Require().Nil(err)
}

func (s *CallbacksTestSuite) requestMap(r *http.Request) map[string]string {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)

	var t map[string]string
	err := decoder.Decode(&t)
	s.Require().Nil(err)

	return t
}

func TestCallbacksTestSuite(t *testing.T) {
	suite.Run(t, new(CallbacksTestSuite))
}
