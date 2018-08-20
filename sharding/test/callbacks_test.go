package test

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/Shopify/ghostferry"
	rtesthelpers "github.com/Shopify/ghostferry/sharding/testhelpers"
	"github.com/Shopify/ghostferry/testhelpers"

	"github.com/stretchr/testify/suite"
)

type CallbacksTestSuite struct {
	*rtesthelpers.ShardingUnitTestSuite

	errHandler testhelpers.ErrorHandler
}

func (t *CallbacksTestSuite) SetupTest() {
	t.ShardingUnitTestSuite.SetupTest()

	t.Ferry.Ferry.ErrorHandler = &t.errHandler

	err := t.Ferry.Start()
	t.Require().Nil(err)
}

func (t *CallbacksTestSuite) TearDownTest() {
	t.ShardingUnitTestSuite.TearDownTest()
}

func (t *CallbacksTestSuite) TestFailsRunOnUnlockError() {
	callbackReceived := false
	t.CutoverUnlock = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackReceived = true
		w.WriteHeader(http.StatusInternalServerError)
	})

	t.Ferry.Run()

	t.Require().True(callbackReceived)

	t.Require().NotNil(t.errHandler.LastError)
	t.Require().Equal("callback returned 500 Internal Server Error", t.errHandler.LastError.Error())
}

func (t *CallbacksTestSuite) TestFailsRunOnLockError() {
	callbackReceived := false
	t.CutoverLock = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackReceived = true
		w.WriteHeader(http.StatusInternalServerError)
	})

	t.Ferry.Run()

	t.Require().True(callbackReceived)

	t.Require().NotNil(t.errHandler.LastError)
	t.Require().Equal("callback returned 500 Internal Server Error", t.errHandler.LastError.Error())
}

func (t *CallbacksTestSuite) TestFailsRunOnPanicError() {
	callbackReceived := false
	t.ErrorCallback = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackReceived = true

		resp := t.requestMap(r)
		errorData := make(map[string]string)
		errorData["ErrFrom"] = "test_error"
		errorData["ErrMessage"] = "test error"

		errorDataBytes, jsonErr := json.MarshalIndent(errorData, "", "  ")
		t.Require().Nil(jsonErr)
		t.Require().Equal(string(errorDataBytes), resp["Payload"])

		w.WriteHeader(http.StatusInternalServerError)
	})

	t.Ferry.Ferry.ErrorHandler = &ghostferry.PanicErrorHandler{
		Ferry:         t.Ferry.Ferry,
		ErrorCallback: t.Config.ErrorCallback,
	}
	defer func() {
		if r := recover(); r != nil {
			t.Require().Equal(r, "fatal error detected, see logs for details")
			t.Require().True(callbackReceived)
		}
	}()
	t.Ferry.Ferry.ErrorHandler.Fatal("test_error", errors.New("test error"))

	t.Require().FailNow("ErrorHandler.Fatal never panicked")
}

func (t *CallbacksTestSuite) TestPostsCallbacks() {
	lockReceived := false
	t.CutoverLock = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lockReceived = true
		resp := t.requestMap(r)
		t.Require().Equal("test_lock", resp["Payload"])
	})

	unlockReceived := false
	t.CutoverUnlock = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		unlockReceived = true
		resp := t.requestMap(r)
		t.Require().Equal("test_unlock", resp["Payload"])
	})

	t.Ferry.Run()

	t.Require().True(lockReceived)
	t.Require().True(unlockReceived)

	t.AssertTenantCopied()
}

func (t *CallbacksTestSuite) requestMap(r *http.Request) map[string]string {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)

	var res map[string]string
	err := decoder.Decode(&res)
	t.Require().Nil(err)

	return res
}

func TestCallbacksTestSuite(t *testing.T) {
	suite.Run(t, &CallbacksTestSuite{ShardingUnitTestSuite: &rtesthelpers.ShardingUnitTestSuite{}})
}
