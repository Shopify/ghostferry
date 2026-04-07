package ghostferry

import (
	"github.com/sirupsen/logrus"
)

// logrusLogger wraps *logrus.Entry to implement the Logger interface.
type logrusLogger struct {
	entry *logrus.Entry
}

func (l *logrusLogger) Debug(args ...any)                 { l.entry.Debug(args...) }
func (l *logrusLogger) Debugf(format string, args ...any) { l.entry.Debugf(format, args...) }
func (l *logrusLogger) Info(args ...any)                  { l.entry.Info(args...) }
func (l *logrusLogger) Infof(format string, args ...any)  { l.entry.Infof(format, args...) }
func (l *logrusLogger) Warn(args ...any)                  { l.entry.Warn(args...) }
func (l *logrusLogger) Warnf(format string, args ...any)  { l.entry.Warnf(format, args...) }
func (l *logrusLogger) Error(args ...any)                 { l.entry.Error(args...) }
func (l *logrusLogger) Errorf(format string, args ...any) { l.entry.Errorf(format, args...) }
func (l *logrusLogger) Panicf(format string, args ...any) { l.entry.Panicf(format, args...) }

func (l *logrusLogger) WithField(key string, value any) Logger {
	return &logrusLogger{entry: l.entry.WithField(key, value)}
}

func (l *logrusLogger) WithFields(fields Fields) Logger {
	return &logrusLogger{entry: l.entry.WithFields(logrus.Fields(fields))}
}

func (l *logrusLogger) WithError(err error) Logger {
	return &logrusLogger{entry: l.entry.WithError(err)}
}

// LogWithField creates a new Logger with a single key-value field.
// This is the primary way components create their tagged loggers.
func LogWithField(key string, value any) Logger {
	return &logrusLogger{entry: logrus.WithField(key, value)}
}

// LogWithFields creates a new Logger with multiple key-value fields.
func LogWithFields(fields Fields) Logger {
	return &logrusLogger{entry: logrus.WithFields(logrus.Fields(fields))}
}

// LogWithError creates a new Logger with an error field.
func LogWithError(err error) Logger {
	return &logrusLogger{entry: logrus.WithError(err)}
}

// NewDefaultLogger creates a Logger from the standard/global logrus logger.
// Used as a fallback when no logger is provided.
func NewDefaultLogger() Logger {
	return &logrusLogger{entry: logrus.NewEntry(logrus.StandardLogger())}
}

// SetLogLevel sets the global log level for the logrus backend.
func SetLogLevel(level LogLevel) {
	switch level {
	case LogLevelDebug:
		logrus.SetLevel(logrus.DebugLevel)
	case LogLevelInfo:
		logrus.SetLevel(logrus.InfoLevel)
	case LogLevelWarn:
		logrus.SetLevel(logrus.WarnLevel)
	case LogLevelError:
		logrus.SetLevel(logrus.ErrorLevel)
	}
}

// SetLogJSONFormatter sets the logrus formatter to JSON output.
func SetLogJSONFormatter() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
}
