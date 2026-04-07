package ghostferry

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/rs/zerolog"
)

// globalZerologMu protects globalZerologLogger during reconfiguration.
var globalZerologMu sync.RWMutex

// globalZerologLogger is the package-level zerolog instance.
// It is initialized to write JSON to os.Stderr with timestamps.
var globalZerologLogger = newGlobalZerolog(os.Stderr)

func newGlobalZerolog(w io.Writer) zerolog.Logger {
	return zerolog.New(w).With().Timestamp().Logger()
}

// zerologLogger wraps zerolog.Logger to implement the Logger interface.
type zerologLogger struct {
	logger zerolog.Logger
}

func (l *zerologLogger) Debug(args ...any)                 { l.logger.Debug().Msg(fmt.Sprint(args...)) }
func (l *zerologLogger) Debugf(format string, args ...any) { l.logger.Debug().Msgf(format, args...) }
func (l *zerologLogger) Info(args ...any)                  { l.logger.Info().Msg(fmt.Sprint(args...)) }
func (l *zerologLogger) Infof(format string, args ...any)  { l.logger.Info().Msgf(format, args...) }
func (l *zerologLogger) Warn(args ...any)                  { l.logger.Warn().Msg(fmt.Sprint(args...)) }
func (l *zerologLogger) Warnf(format string, args ...any)  { l.logger.Warn().Msgf(format, args...) }
func (l *zerologLogger) Error(args ...any)                 { l.logger.Error().Msg(fmt.Sprint(args...)) }
func (l *zerologLogger) Errorf(format string, args ...any) { l.logger.Error().Msgf(format, args...) }
func (l *zerologLogger) Panicf(format string, args ...any) { l.logger.Panic().Msgf(format, args...) }

func (l *zerologLogger) WithField(key string, value any) Logger {
	return &zerologLogger{logger: l.logger.With().Interface(key, value).Logger()}
}

func (l *zerologLogger) WithFields(fields Fields) Logger {
	ctx := l.logger.With()
	for k, v := range fields {
		ctx = ctx.Interface(k, v)
	}
	return &zerologLogger{logger: ctx.Logger()}
}

func (l *zerologLogger) WithError(err error) Logger {
	return &zerologLogger{logger: l.logger.With().Err(err).Logger()}
}

// --- internal factory functions (called by logger.go dispatch) ---

func zerologWithField(key string, value any) Logger {
	globalZerologMu.RLock()
	base := globalZerologLogger
	globalZerologMu.RUnlock()
	return &zerologLogger{logger: base.With().Interface(key, value).Logger()}
}

func zerologWithFields(fields Fields) Logger {
	globalZerologMu.RLock()
	base := globalZerologLogger
	globalZerologMu.RUnlock()
	ctx := base.With()
	for k, v := range fields {
		ctx = ctx.Interface(k, v)
	}
	return &zerologLogger{logger: ctx.Logger()}
}

func zerologWithError(err error) Logger {
	globalZerologMu.RLock()
	base := globalZerologLogger
	globalZerologMu.RUnlock()
	return &zerologLogger{logger: base.With().Err(err).Logger()}
}

func newZerologDefaultLogger() Logger {
	globalZerologMu.RLock()
	base := globalZerologLogger
	globalZerologMu.RUnlock()
	return &zerologLogger{logger: base}
}

func setZerologLevel(level LogLevel) {
	switch level {
	case LogLevelDebug:
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case LogLevelInfo:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case LogLevelWarn:
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case LogLevelError:
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	}
}

func setZerologJSONFormatter() {
	// zerolog outputs JSON by default -- this is a no-op.
}

func setZerologOutput(w io.Writer) {
	globalZerologMu.Lock()
	globalZerologLogger = newGlobalZerolog(w)
	globalZerologMu.Unlock()
}

func init() {
	// Match logrus field names so the Ruby integration tests (which parse
	// JSON output by "msg", "level", "error") work with either backend.
	zerolog.MessageFieldName = "msg"
	zerolog.ErrorFieldName = "error"
	zerolog.TimestampFieldName = "time"
	zerolog.LevelFieldName = "level"
}
