package ghostferry

import (
	"fmt"
	"io"
)

// Logger is the interface for structured logging throughout ghostferry.
// It is designed to be backend-agnostic, allowing swapping logrus for
// zerolog, zap, or any other structured logger without changing consuming code.
type Logger interface {
	Debug(args ...any)
	Debugf(format string, args ...any)
	Info(args ...any)
	Infof(format string, args ...any)
	Warn(args ...any)
	Warnf(format string, args ...any)
	Error(args ...any)
	Errorf(format string, args ...any)
	Panicf(format string, args ...any)

	WithField(key string, value any) Logger
	WithFields(fields Fields) Logger
	WithError(err error) Logger
}

// Fields is a map of key-value pairs for structured logging context.
type Fields map[string]any

// LogLevel represents the severity level for logging.
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// LogBackendType identifies a logging backend implementation.
type LogBackendType string

const (
	// LogBackendLogrus selects the logrus logging backend (default).
	LogBackendLogrus LogBackendType = "logrus"
	// LogBackendZerolog selects the zerolog logging backend.
	LogBackendZerolog LogBackendType = "zerolog"
)

// activeBackend holds the currently selected logging backend.
// It defaults to logrus for backward compatibility.
var activeBackend LogBackendType = LogBackendLogrus

// SetLogBackend switches the active logging backend.
// This should be called once at program startup, before any loggers are created.
// Valid values: LogBackendLogrus, LogBackendZerolog.
func SetLogBackend(backend LogBackendType) {
	switch backend {
	case LogBackendLogrus, LogBackendZerolog:
		activeBackend = backend
	default:
		panic(fmt.Sprintf("ghostferry: unknown log backend %q (valid: %q, %q)", backend, LogBackendLogrus, LogBackendZerolog))
	}
}

// GetLogBackend returns the currently active logging backend.
func GetLogBackend() LogBackendType {
	return activeBackend
}

// --- Public factory functions (dispatch to active backend) ---

// LogWithField creates a new Logger with a single key-value field.
// This is the primary way components create their tagged loggers.
func LogWithField(key string, value any) Logger {
	if activeBackend == LogBackendZerolog {
		return zerologWithField(key, value)
	}
	return logrusWithField(key, value)
}

// LogWithFields creates a new Logger with multiple key-value fields.
func LogWithFields(fields Fields) Logger {
	if activeBackend == LogBackendZerolog {
		return zerologWithFields(fields)
	}
	return logrusWithFields(fields)
}

// LogWithError creates a new Logger with an error field.
func LogWithError(err error) Logger {
	if activeBackend == LogBackendZerolog {
		return zerologWithError(err)
	}
	return logrusWithError(err)
}

// NewDefaultLogger creates a Logger from the active backend's default configuration.
// Used as a fallback when no logger is provided.
func NewDefaultLogger() Logger {
	if activeBackend == LogBackendZerolog {
		return newZerologDefaultLogger()
	}
	return newLogrusDefaultLogger()
}

// SetLogLevel sets the global log level for the active backend.
func SetLogLevel(level LogLevel) {
	if activeBackend == LogBackendZerolog {
		setZerologLevel(level)
		return
	}
	setLogrusLevel(level)
}

// SetLogJSONFormatter configures the active backend to output JSON.
// For zerolog this is a no-op since JSON is the default format.
func SetLogJSONFormatter() {
	if activeBackend == LogBackendZerolog {
		setZerologJSONFormatter()
		return
	}
	setLogrusJSONFormatter()
}

// SetLogOutput sets the output writer for the active backend.
// This is primarily useful for testing (capturing log output).
func SetLogOutput(w io.Writer) {
	if activeBackend == LogBackendZerolog {
		setZerologOutput(w)
		return
	}
	setLogrusOutput(w)
}
