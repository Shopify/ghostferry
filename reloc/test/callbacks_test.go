package test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Shopify/ghostferry/reloc"
	rtesthelpers "github.com/Shopify/ghostferry/reloc/testhelpers"
	"github.com/Shopify/ghostferry/testhelpers"

	"github.com/stretchr/testify/suite"
)

type CallbacksTestSuite struct {
	*rtesthelpers.RelocUnitTestSuite

	server   *httptest.Server
	respFunc func(http.ResponseWriter, *http.Request)

	errHandler testhelpers.ErrorHandler
}

func (t *CallbacksTestSuite) SetupSuite() {
	t.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if t.respFunc != nil {
			t.respFunc(w, r)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
}

func (t *CallbacksTestSuite) TearDownSuite() {
	t.server.Close()
}

func (t *CallbacksTestSuite) SetupTest() {
	t.RelocUnitTestSuite.SetupTest()

	t.respFunc = nil

	t.Config.CutoverLock = reloc.HTTPCallback{
		URI:     fmt.Sprintf("%s/lock", t.server.URL),
		Payload: "test_lock",
	}

	t.Config.CutoverUnlock = reloc.HTTPCallback{
		URI:     fmt.Sprintf("%s/unlock", t.server.URL),
		Payload: "test_unlock",
	}

	t.Ferry.Ferry.ErrorHandler = &t.errHandler

	err := t.Ferry.Start()
	t.Require().Nil(err)
}

func (t *CallbacksTestSuite) TearDownTest() {
	t.RelocUnitTestSuite.TearDownTest()
}

func (t *CallbacksTestSuite) TestFailsRunOnUnlockError() {
	callbackReceived := false
	t.respFunc = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/unlock" {
			callbackReceived = true
			w.WriteHeader(http.StatusInternalServerError)
		}
	})

	t.Ferry.Run()

	t.Require().True(callbackReceived)

	t.Require().NotNil(t.errHandler.LastError)
	t.Require().Equal("callback returned 500 Internal Server Error", t.errHandler.LastError.Error())
}

func (t *CallbacksTestSuite) TestFailsRunOnLockError() {
	callbackReceived := false
	t.respFunc = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/lock" {
			callbackReceived = true
			w.WriteHeader(http.StatusInternalServerError)
		}
	})

	t.Ferry.Run()

	t.Require().True(callbackReceived)

	t.Require().NotNil(t.errHandler.LastError)
	t.Require().Equal("callback returned 500 Internal Server Error", t.errHandler.LastError.Error())
}

func (t *CallbacksTestSuite) TestPostsCallbacks() {
	lockReceived := false
	unlockReceived := false
	t.respFunc = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/lock" {
			lockReceived = true
			resp := t.requestMap(r)
			t.Require().Equal("test_lock", resp["Payload"])
		} else if r.URL.Path == "/unlock" {
			unlockReceived = true
			resp := t.requestMap(r)
			t.Require().Equal("test_unlock", resp["Payload"])
		} else {
			t.Fail("Unexpected callback made")
		}
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
	suite.Run(t, &CallbacksTestSuite{RelocUnitTestSuite: &rtesthelpers.RelocUnitTestSuite{}})
}
