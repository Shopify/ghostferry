package ghostferry

import (
	"fmt"
	"io"
	"os"
	"strings"
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

// ParseLogLevel converts a string to a LogLevel.
// It returns the parsed level and true on success, or LogLevelInfo and false
// if the string is not recognized.
func ParseLogLevel(s string) (LogLevel, bool) {
	switch strings.ToLower(s) {
	case "debug":
		return LogLevelDebug, true
	case "info":
		return LogLevelInfo, true
	case "warn", "warning":
		return LogLevelWarn, true
	case "error":
		return LogLevelError, true
	default:
		return LogLevelInfo, false
	}
}

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

func init() {
	if backend := os.Getenv("GHOSTFERRY_LOG_BACKEND"); backend != "" {
		SetLogBackend(LogBackendType(backend))
	}
	if level := os.Getenv("GHOSTFERRY_LOG_LEVEL"); level != "" {
		if parsed, ok := ParseLogLevel(level); ok {
			SetLogLevel(parsed)
		} else {
			fmt.Fprintf(os.Stderr, "ghostferry: unknown log level %q from GHOSTFERRY_LOG_LEVEL, ignoring\n", level)
		}
	}
}

// SetLogBackend switches the active logging backend.
// This should be called once at program startup, before any loggers are created.
// If an unknown backend is specified, a warning is printed to stderr and the
// backend falls back to logrus.
func SetLogBackend(backend LogBackendType) {
	switch backend {
	case LogBackendLogrus, LogBackendZerolog:
		activeBackend = backend
	default:
		fmt.Fprintf(os.Stderr, "ghostferry: unknown log backend %q, falling back to %q\n", backend, LogBackendLogrus)
		activeBackend = LogBackendLogrus
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
