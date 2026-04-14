package ghostferry

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// globalZerologMu protects globalZerologLogger during reconfiguration.
var globalZerologMu sync.RWMutex

// globalZerologLogger is the package-level zerolog instance.
// It is initialized to write JSON to os.Stderr with timestamps.
var globalZerologLogger = newGlobalZerolog(os.Stderr)

// zerologDefaultLevel sets zerolog's global level to InfoLevel so that the
// zerolog backend matches logrus's default behaviour (Info and above only).
//
// This must be a package-level var initializer rather than an init() function.
// init() functions within a package run in alphabetical source-file order, so
// zerolog_logger.go's init() would execute AFTER logger.go's init() — which
// means it would overwrite any level already applied from GHOSTFERRY_LOG_LEVEL.
// A var initializer runs before any init() in the same package, giving us the
// right ordering:
//
//  1. zerolog pkg init (ctx.go)          → SetGlobalLevel(TraceLevel)   [-1]
//  2. zerolog_logger.go var initializer  → SetGlobalLevel(InfoLevel)    [ 1]
//  3. logger.go init()                   → env-var override (if set)    [?]
//  4. Config.ValidateConfig() / caller   → explicit override (if set)   [?]
var _ = func() struct{} { zerolog.SetGlobalLevel(zerolog.InfoLevel); return struct{}{} }()

func newGlobalZerolog(w io.Writer) zerolog.Logger {
	return zerolog.New(w).With().Timestamp().Logger()
}

// zerologContextField adds a key-value pair to a zerolog.Context using the
// native typed method when possible, avoiding Interface() reflection for the
// most common types in the codebase (string, int, uint64, uint32, error, etc.).
func zerologContextField(ctx zerolog.Context, key string, value any) zerolog.Context {
	switch v := value.(type) {
	case string:
		return ctx.Str(key, v)
	case int:
		return ctx.Int(key, v)
	case int64:
		return ctx.Int64(key, v)
	case uint16:
		return ctx.Uint16(key, v)
	case uint32:
		return ctx.Uint32(key, v)
	case uint64:
		return ctx.Uint64(key, v)
	case bool:
		return ctx.Bool(key, v)
	case error:
		return ctx.AnErr(key, v)
	case time.Duration:
		return ctx.Dur(key, v)
	case fmt.Stringer:
		return ctx.Stringer(key, v)
	default:
		return ctx.Interface(key, v)
	}
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
	return &zerologLogger{logger: zerologContextField(l.logger.With(), key, value).Logger()}
}

func (l *zerologLogger) WithFields(fields Fields) Logger {
	ctx := l.logger.With()
	for k, v := range fields {
		ctx = zerologContextField(ctx, k, v)
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
	return &zerologLogger{logger: zerologContextField(base.With(), key, value).Logger()}
}

func zerologWithFields(fields Fields) Logger {
	globalZerologMu.RLock()
	base := globalZerologLogger
	globalZerologMu.RUnlock()
	ctx := base.With()
	for k, v := range fields {
		ctx = zerologContextField(ctx, k, v)
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
