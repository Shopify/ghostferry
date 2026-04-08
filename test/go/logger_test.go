package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// savedBackend captures and restores global logging state between tests.
type savedBackend struct {
	backend ghostferry.LogBackendType
}

func saveBackend() savedBackend {
	return savedBackend{backend: ghostferry.GetLogBackend()}
}

func (s savedBackend) restore() {
	ghostferry.SetLogBackend(s.backend)
}

// useBackend switches backend, sets debug level, and directs output to buf.
func useBackend(backend ghostferry.LogBackendType, buf *bytes.Buffer) {
	ghostferry.SetLogBackend(backend)
	ghostferry.SetLogLevel(ghostferry.LogLevelDebug)
	ghostferry.SetLogOutput(buf)
}

// backends is the list of backends to run conformance tests against.
var backends = []ghostferry.LogBackendType{
	ghostferry.LogBackendLogrus,
	ghostferry.LogBackendZerolog,
}

// ---------------------------------------------------------------------------
// LoggerTestSuite -- backend-agnostic conformance tests
// ---------------------------------------------------------------------------

type LoggerTestSuite struct {
	suite.Suite
	saved savedBackend
}

func (s *LoggerTestSuite) SetupTest() {
	s.saved = saveBackend()
}

func (s *LoggerTestSuite) TearDownTest() {
	s.saved.restore()
}

// --- SetLogBackend ---

func (s *LoggerTestSuite) TestDefaultBackendIsLogrus() {
	s.saved.restore() // ensure clean default
	s.Require().Equal(ghostferry.LogBackendLogrus, ghostferry.GetLogBackend())
}

func (s *LoggerTestSuite) TestSetLogBackendZerolog() {
	ghostferry.SetLogBackend(ghostferry.LogBackendZerolog)
	s.Require().Equal(ghostferry.LogBackendZerolog, ghostferry.GetLogBackend())
}

func (s *LoggerTestSuite) TestSetLogBackendInvalidFallsBackToLogrus() {
	ghostferry.SetLogBackend(ghostferry.LogBackendZerolog) // start from non-default
	s.Require().NotPanics(func() {
		ghostferry.SetLogBackend("invalid_backend")
	})
	s.Require().Equal(ghostferry.LogBackendLogrus, ghostferry.GetLogBackend())
}

// --- Factory functions return non-nil Logger (both backends) ---

func (s *LoggerTestSuite) TestLogWithFieldReturnsLogger() {
	for _, b := range backends {
		s.Run(string(b), func() {
			ghostferry.SetLogBackend(b)
			var l ghostferry.Logger = ghostferry.LogWithField("tag", "test")
			s.Require().NotNil(l)
		})
	}
}

func (s *LoggerTestSuite) TestLogWithFieldsReturnsLogger() {
	for _, b := range backends {
		s.Run(string(b), func() {
			ghostferry.SetLogBackend(b)
			var l ghostferry.Logger = ghostferry.LogWithFields(ghostferry.Fields{"a": 1, "b": "two"})
			s.Require().NotNil(l)
		})
	}
}

func (s *LoggerTestSuite) TestLogWithErrorReturnsLogger() {
	for _, b := range backends {
		s.Run(string(b), func() {
			ghostferry.SetLogBackend(b)
			var l ghostferry.Logger = ghostferry.LogWithError(fmt.Errorf("boom"))
			s.Require().NotNil(l)
		})
	}
}

func (s *LoggerTestSuite) TestNewDefaultLoggerReturnsLogger() {
	for _, b := range backends {
		s.Run(string(b), func() {
			ghostferry.SetLogBackend(b)
			var l ghostferry.Logger = ghostferry.NewDefaultLogger()
			s.Require().NotNil(l)
		})
	}
}

// --- Chaining (both backends) ---

func (s *LoggerTestSuite) TestWithFieldChaining() {
	for _, b := range backends {
		s.Run(string(b), func() {
			ghostferry.SetLogBackend(b)
			l := ghostferry.LogWithField("tag", "test").WithField("extra", "value")
			s.Require().NotNil(l)
		})
	}
}

func (s *LoggerTestSuite) TestWithFieldsChaining() {
	for _, b := range backends {
		s.Run(string(b), func() {
			ghostferry.SetLogBackend(b)
			l := ghostferry.LogWithField("tag", "test").WithFields(ghostferry.Fields{"a": 1})
			s.Require().NotNil(l)
		})
	}
}

func (s *LoggerTestSuite) TestWithErrorChaining() {
	for _, b := range backends {
		s.Run(string(b), func() {
			ghostferry.SetLogBackend(b)
			l := ghostferry.LogWithField("tag", "test").WithError(fmt.Errorf("oops"))
			s.Require().NotNil(l)
		})
	}
}

func (s *LoggerTestSuite) TestDeepChaining() {
	for _, b := range backends {
		s.Run(string(b), func() {
			ghostferry.SetLogBackend(b)
			l := ghostferry.LogWithField("tag", "test").
				WithField("a", 1).
				WithError(fmt.Errorf("err")).
				WithFields(ghostferry.Fields{"b": 2, "c": 3}).
				WithField("d", 4)
			s.Require().NotNil(l)
		})
	}
}

func (s *LoggerTestSuite) TestWithEmptyFields() {
	for _, b := range backends {
		s.Run(string(b), func() {
			ghostferry.SetLogBackend(b)
			l := ghostferry.LogWithField("tag", "test").WithFields(ghostferry.Fields{})
			s.Require().NotNil(l)
			s.Require().NotPanics(func() { l.Debug("empty fields ok") })
		})
	}
}

// --- All log methods execute without panic (both backends) ---

func (s *LoggerTestSuite) TestAllLogMethodsNoPanic() {
	for _, b := range backends {
		s.Run(string(b), func() {
			var buf bytes.Buffer
			useBackend(b, &buf)
			l := ghostferry.LogWithField("tag", "test_methods")

			s.Require().NotPanics(func() { l.Debug("debug msg") })
			s.Require().NotPanics(func() { l.Debugf("debug %s", "formatted") })
			s.Require().NotPanics(func() { l.Info("info msg") })
			s.Require().NotPanics(func() { l.Infof("info %s", "formatted") })
			s.Require().NotPanics(func() { l.Warn("warn msg") })
			s.Require().NotPanics(func() { l.Warnf("warn %s", "formatted") })
			s.Require().NotPanics(func() { l.Error("error msg") })
			s.Require().NotPanics(func() { l.Errorf("error %s", "formatted") })
		})
	}
}

// --- Chaining does not mutate original (both backends) ---

func (s *LoggerTestSuite) TestChainingDoesNotMutateOriginal() {
	for _, b := range backends {
		s.Run(string(b), func() {
			var buf bytes.Buffer
			useBackend(b, &buf)
			ghostferry.SetLogJSONFormatter()

			original := ghostferry.LogWithField("tag", "original")
			_ = original.WithField("extra", "derived")

			original.Info("from original")
			output := buf.String()

			s.Require().Contains(output, `"tag":"original"`)
			s.Require().NotContains(output, `"extra"`)
		})
	}
}

// --- SetLogLevel controls output (both backends) ---

func (s *LoggerTestSuite) TestSetLogLevelDebugShowsDebug() {
	for _, b := range backends {
		s.Run(string(b), func() {
			var buf bytes.Buffer
			useBackend(b, &buf)

			ghostferry.LogWithField("tag", "test").Debug("visible")
			s.Require().Contains(buf.String(), "visible")
		})
	}
}

func (s *LoggerTestSuite) TestSetLogLevelFiltersLowerLevels() {
	for _, b := range backends {
		s.Run(string(b), func() {
			var buf bytes.Buffer
			ghostferry.SetLogBackend(b)
			ghostferry.SetLogOutput(&buf)
			ghostferry.SetLogLevel(ghostferry.LogLevelError)

			ghostferry.LogWithField("tag", "test").Debug("no")
			ghostferry.LogWithField("tag", "test").Info("no")
			ghostferry.LogWithField("tag", "test").Warn("no")

			s.Require().Empty(buf.String())
		})
	}
}

func (s *LoggerTestSuite) TestSetLogLevelAllValues() {
	for _, b := range backends {
		s.Run(string(b), func() {
			ghostferry.SetLogBackend(b)
			s.Require().NotPanics(func() { ghostferry.SetLogLevel(ghostferry.LogLevelDebug) })
			s.Require().NotPanics(func() { ghostferry.SetLogLevel(ghostferry.LogLevelInfo) })
			s.Require().NotPanics(func() { ghostferry.SetLogLevel(ghostferry.LogLevelWarn) })
			s.Require().NotPanics(func() { ghostferry.SetLogLevel(ghostferry.LogLevelError) })
		})
	}
}

// --- JSON output verification (both backends) ---

func (s *LoggerTestSuite) TestJSONOutputContainsFields() {
	for _, b := range backends {
		s.Run(string(b), func() {
			var buf bytes.Buffer
			useBackend(b, &buf)
			ghostferry.SetLogJSONFormatter()

			ghostferry.LogWithFields(ghostferry.Fields{
				"table":  "users",
				"action": "copy",
			}).Info("processing")

			output := buf.String()
			s.Require().Contains(output, `"table":"users"`)
			s.Require().Contains(output, `"action":"copy"`)
			s.Require().Contains(output, `"msg":"processing"`)
		})
	}
}

func (s *LoggerTestSuite) TestJSONOutputContainsLevel() {
	for _, b := range backends {
		s.Run(string(b), func() {
			var buf bytes.Buffer
			useBackend(b, &buf)
			ghostferry.SetLogJSONFormatter()

			ghostferry.LogWithField("tag", "test").Info("hello")

			output := buf.String()
			s.Require().Contains(output, `"level"`)
			s.Require().Contains(output, `"msg":"hello"`)
		})
	}
}

// --- WithError attaches error field (both backends) ---

func (s *LoggerTestSuite) TestWithErrorAttachesField() {
	for _, b := range backends {
		s.Run(string(b), func() {
			var buf bytes.Buffer
			useBackend(b, &buf)
			ghostferry.SetLogJSONFormatter()

			ghostferry.LogWithField("tag", "test").
				WithError(fmt.Errorf("something failed")).
				Error("operation failed")

			output := buf.String()
			s.Require().Contains(output, `"error":"something failed"`)
			s.Require().Contains(output, `"msg":"operation failed"`)
		})
	}
}

// --- JSON output is valid JSON (both backends) ---

func (s *LoggerTestSuite) TestOutputIsValidJSON() {
	for _, b := range backends {
		s.Run(string(b), func() {
			var buf bytes.Buffer
			useBackend(b, &buf)
			ghostferry.SetLogJSONFormatter()

			ghostferry.LogWithField("tag", "json_check").Info("valid json test")

			var parsed map[string]any
			err := json.Unmarshal(buf.Bytes(), &parsed)
			s.Require().NoError(err, "output should be valid JSON: %s", buf.String())
			s.Require().Equal("json_check", parsed["tag"])
			s.Require().Equal("valid json test", parsed["msg"])
		})
	}
}

// --- SetLogJSONFormatter doesn't panic (both backends) ---

func (s *LoggerTestSuite) TestSetLogJSONFormatterNoPanic() {
	for _, b := range backends {
		s.Run(string(b), func() {
			ghostferry.SetLogBackend(b)
			s.Require().NotPanics(func() { ghostferry.SetLogJSONFormatter() })
		})
	}
}

// --- GetLogBackend reflects current state ---

func (s *LoggerTestSuite) TestGetLogBackendReflectsState() {
	ghostferry.SetLogBackend(ghostferry.LogBackendLogrus)
	s.Require().Equal(ghostferry.LogBackendLogrus, ghostferry.GetLogBackend())

	ghostferry.SetLogBackend(ghostferry.LogBackendZerolog)
	s.Require().Equal(ghostferry.LogBackendZerolog, ghostferry.GetLogBackend())
}

// --- ParseLogLevel ---

func (s *LoggerTestSuite) TestParseLogLevelValidValues() {
	cases := []struct {
		input    string
		expected ghostferry.LogLevel
	}{
		{"debug", ghostferry.LogLevelDebug},
		{"DEBUG", ghostferry.LogLevelDebug},
		{"Debug", ghostferry.LogLevelDebug},
		{"info", ghostferry.LogLevelInfo},
		{"INFO", ghostferry.LogLevelInfo},
		{"warn", ghostferry.LogLevelWarn},
		{"WARN", ghostferry.LogLevelWarn},
		{"warning", ghostferry.LogLevelWarn},
		{"WARNING", ghostferry.LogLevelWarn},
		{"error", ghostferry.LogLevelError},
		{"ERROR", ghostferry.LogLevelError},
	}
	for _, tc := range cases {
		s.Run(tc.input, func() {
			level, ok := ghostferry.ParseLogLevel(tc.input)
			s.Require().True(ok)
			s.Require().Equal(tc.expected, level)
		})
	}
}

func (s *LoggerTestSuite) TestParseLogLevelInvalidReturnsFalse() {
	invalids := []string{"", "trace", "fatal", "off", "42", "verbose"}
	for _, input := range invalids {
		s.Run(input, func() {
			_, ok := ghostferry.ParseLogLevel(input)
			s.Require().False(ok)
		})
	}
}

// --- Config.LogLevel validation ---

func (s *LoggerTestSuite) TestConfigInvalidLogLevelReturnsError() {
	config := minimalConfig()
	config.LogLevel = "bogus"

	err := config.ValidateConfig()
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "invalid LogLevel")
}

func (s *LoggerTestSuite) TestConfigValidLogLevelApplied() {
	for _, b := range backends {
		s.Run(string(b), func() {
			ghostferry.SetLogBackend(b)
			config := minimalConfig()
			config.LogLevel = "warn"
			config.LogBackend = string(b)

			err := config.ValidateConfig()
			s.Require().NoError(err)

			// After validation, warn level should be active: debug and info
			// messages should be suppressed.
			var buf bytes.Buffer
			ghostferry.SetLogOutput(&buf)
			ghostferry.LogWithField("tag", "test").Debug("should not appear")
			ghostferry.LogWithField("tag", "test").Info("should not appear")
			s.Require().Empty(buf.String())
		})
	}
}

// --- Config.LogBackend invalid falls back gracefully ---

func (s *LoggerTestSuite) TestConfigInvalidLogBackendFallsBack() {
	config := minimalConfig()
	config.LogBackend = "nonexistent"

	// Should not panic, should fall back to logrus.
	err := config.ValidateConfig()
	s.Require().NoError(err)
	s.Require().Equal(ghostferry.LogBackendLogrus, ghostferry.GetLogBackend())
}

// minimalConfig creates the smallest valid Config for testing ValidateConfig().
func minimalConfig() *ghostferry.Config {
	return &ghostferry.Config{
		Source: &ghostferry.DatabaseConfig{
			Host: "127.0.0.1",
			Port: 3306,
			User: "root",
		},
		Target: &ghostferry.DatabaseConfig{
			Host: "127.0.0.1",
			Port: 3306,
			User: "root",
		},
		TableFilter: &testhelpers.TestTableFilter{},
	}
}

func TestLoggerTestSuite(t *testing.T) {
	suite.Run(t, new(LoggerTestSuite))
}
