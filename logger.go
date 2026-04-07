package ghostferry

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
