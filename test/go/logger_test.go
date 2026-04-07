package test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

type LoggerTestSuite struct {
	suite.Suite

	// Save and restore logrus state between tests so we don't pollute other suites.
	origLevel     logrus.Level
	origFormatter logrus.Formatter
	origOutput    *os.File
}

func (s *LoggerTestSuite) SetupTest() {
	s.origLevel = logrus.GetLevel()
	s.origFormatter = logrus.StandardLogger().Formatter
}

func (s *LoggerTestSuite) TearDownTest() {
	logrus.SetLevel(s.origLevel)
	logrus.StandardLogger().Formatter = s.origFormatter
	logrus.SetOutput(os.Stderr)
}

// --- Interface satisfaction (compile-time) ---

func (s *LoggerTestSuite) TestLogWithFieldReturnsLogger() {
	var l ghostferry.Logger = ghostferry.LogWithField("tag", "test")
	s.Require().NotNil(l)
}

func (s *LoggerTestSuite) TestLogWithFieldsReturnsLogger() {
	var l ghostferry.Logger = ghostferry.LogWithFields(ghostferry.Fields{
		"a": 1,
		"b": "two",
	})
	s.Require().NotNil(l)
}

func (s *LoggerTestSuite) TestLogWithErrorReturnsLogger() {
	var l ghostferry.Logger = ghostferry.LogWithError(fmt.Errorf("boom"))
	s.Require().NotNil(l)
}

func (s *LoggerTestSuite) TestNewDefaultLoggerReturnsLogger() {
	var l ghostferry.Logger = ghostferry.NewDefaultLogger()
	s.Require().NotNil(l)
}

// --- Chaining returns Logger at every step ---

func (s *LoggerTestSuite) TestWithFieldChaining() {
	l := ghostferry.LogWithField("tag", "test")
	l2 := l.WithField("extra", "value")
	s.Require().NotNil(l2)
}

func (s *LoggerTestSuite) TestWithFieldsChaining() {
	l := ghostferry.LogWithField("tag", "test")
	l2 := l.WithFields(ghostferry.Fields{"a": 1, "b": 2})
	s.Require().NotNil(l2)
}

func (s *LoggerTestSuite) TestWithErrorChaining() {
	l := ghostferry.LogWithField("tag", "test")
	l2 := l.WithError(fmt.Errorf("oops"))
	s.Require().NotNil(l2)
}

func (s *LoggerTestSuite) TestDeepChaining() {
	l := ghostferry.LogWithField("tag", "test").
		WithField("a", 1).
		WithError(fmt.Errorf("err")).
		WithFields(ghostferry.Fields{"b": 2, "c": 3}).
		WithField("d", 4)
	s.Require().NotNil(l)
}

// --- Chaining does not mutate the original logger ---

func (s *LoggerTestSuite) TestChainingDoesNotMutateOriginal() {
	ghostferry.SetLogLevel(ghostferry.LogLevelDebug)
	var buf bytes.Buffer
	logrus.SetOutput(&buf)
	logrus.SetFormatter(&logrus.JSONFormatter{DisableTimestamp: true})

	original := ghostferry.LogWithField("tag", "original")
	_ = original.WithField("extra", "derived")

	// Log from the original -- it should NOT contain the "extra" field.
	original.Info("from original")
	output := buf.String()

	s.Require().Contains(output, `"tag":"original"`)
	s.Require().NotContains(output, `"extra"`)
}

// --- Empty Fields is safe ---

func (s *LoggerTestSuite) TestWithEmptyFields() {
	l := ghostferry.LogWithField("tag", "test").WithFields(ghostferry.Fields{})
	s.Require().NotNil(l)
	// Should not panic.
	l.Debug("empty fields ok")
}

// --- All log methods execute without panic ---

func (s *LoggerTestSuite) TestAllLogMethodsNoPanic() {
	// Send output to a buffer so we don't spam test output.
	logrus.SetOutput(&bytes.Buffer{})
	ghostferry.SetLogLevel(ghostferry.LogLevelDebug)

	l := ghostferry.LogWithField("tag", "test_methods")

	s.Require().NotPanics(func() { l.Debug("debug msg") })
	s.Require().NotPanics(func() { l.Debugf("debug %s", "formatted") })
	s.Require().NotPanics(func() { l.Info("info msg") })
	s.Require().NotPanics(func() { l.Infof("info %s", "formatted") })
	s.Require().NotPanics(func() { l.Warn("warn msg") })
	s.Require().NotPanics(func() { l.Warnf("warn %s", "formatted") })
	s.Require().NotPanics(func() { l.Error("error msg") })
	s.Require().NotPanics(func() { l.Errorf("error %s", "formatted") })
}

// --- SetLogLevel controls output ---

func (s *LoggerTestSuite) TestSetLogLevelDebug() {
	var buf bytes.Buffer
	logrus.SetOutput(&buf)

	ghostferry.SetLogLevel(ghostferry.LogLevelDebug)
	ghostferry.LogWithField("tag", "test").Debug("visible")

	s.Require().Contains(buf.String(), "visible")
}

func (s *LoggerTestSuite) TestSetLogLevelFiltersLowerLevels() {
	var buf bytes.Buffer
	logrus.SetOutput(&buf)

	ghostferry.SetLogLevel(ghostferry.LogLevelError)
	ghostferry.LogWithField("tag", "test").Debug("should not appear")
	ghostferry.LogWithField("tag", "test").Info("should not appear")
	ghostferry.LogWithField("tag", "test").Warn("should not appear")

	s.Require().Empty(buf.String())
}

func (s *LoggerTestSuite) TestSetLogLevelAllValues() {
	s.Require().NotPanics(func() { ghostferry.SetLogLevel(ghostferry.LogLevelDebug) })
	s.Require().NotPanics(func() { ghostferry.SetLogLevel(ghostferry.LogLevelInfo) })
	s.Require().NotPanics(func() { ghostferry.SetLogLevel(ghostferry.LogLevelWarn) })
	s.Require().NotPanics(func() { ghostferry.SetLogLevel(ghostferry.LogLevelError) })
}

// --- SetLogJSONFormatter switches to JSON ---

func (s *LoggerTestSuite) TestSetLogJSONFormatterProducesJSON() {
	var buf bytes.Buffer
	logrus.SetOutput(&buf)
	ghostferry.SetLogLevel(ghostferry.LogLevelDebug)

	ghostferry.SetLogJSONFormatter()
	ghostferry.LogWithField("tag", "json_test").Info("hello json")

	output := buf.String()
	s.Require().Contains(output, `"tag":"json_test"`)
	s.Require().Contains(output, `"msg":"hello json"`)
	s.Require().Contains(output, `"level":"info"`)
}

// --- WithError attaches error field ---

func (s *LoggerTestSuite) TestWithErrorAttachesField() {
	var buf bytes.Buffer
	logrus.SetOutput(&buf)
	logrus.SetFormatter(&logrus.JSONFormatter{DisableTimestamp: true})
	ghostferry.SetLogLevel(ghostferry.LogLevelDebug)

	ghostferry.LogWithField("tag", "test").
		WithError(fmt.Errorf("something failed")).
		Error("operation failed")

	output := buf.String()
	s.Require().Contains(output, `"error":"something failed"`)
	s.Require().Contains(output, `"msg":"operation failed"`)
}

// --- Fields appear in output ---

func (s *LoggerTestSuite) TestFieldsAppearInOutput() {
	var buf bytes.Buffer
	logrus.SetOutput(&buf)
	logrus.SetFormatter(&logrus.JSONFormatter{DisableTimestamp: true})
	ghostferry.SetLogLevel(ghostferry.LogLevelDebug)

	ghostferry.LogWithFields(ghostferry.Fields{
		"table":  "users",
		"action": "copy",
	}).Info("processing")

	output := buf.String()
	s.Require().Contains(output, `"table":"users"`)
	s.Require().Contains(output, `"action":"copy"`)
	s.Require().Contains(output, `"msg":"processing"`)
}

func TestLoggerTestSuite(t *testing.T) {
	suite.Run(t, new(LoggerTestSuite))
}
