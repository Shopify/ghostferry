package ghostferry

import (
	"context"
	"log/slog"
)

// loggerSlogHandler implements slog.Handler on top of a ghostferry Logger.
// It is backend-agnostic: the same handler works whether the active backend
// is zerolog, logrus, or any future implementation, because it only calls
// methods defined on the Logger interface.
//
// Level filtering is delegated to the backend — Enabled() always returns true
// so that the backend's own level gate (e.g. zerolog's global atomic level)
// applies consistently. The cost of building an undelivered slog.Record is
// negligible for the non-hot logging paths this handler is designed for
// (e.g. go-mysql's BinlogSyncer).
type loggerSlogHandler struct {
	logger Logger
	attrs  []slog.Attr // pre-attached via WithAttrs
	group  string      // dot-prefix accumulated via WithGroup
}

// Enabled always returns true; the ghostferry Logger backend filters by level.
func (h *loggerSlogHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return true
}

// Handle converts a slog.Record into a ghostferry Logger call.
// Attributes from both WithAttrs and the record itself are applied via
// WithField so they appear as structured fields in the backend's output.
func (h *loggerSlogHandler) Handle(_ context.Context, r slog.Record) error {
	l := h.logger

	for _, a := range h.attrs {
		l = applySlogAttrToLogger(l, a, h.group)
	}
	r.Attrs(func(a slog.Attr) bool {
		l = applySlogAttrToLogger(l, a, h.group)
		return true
	})

	msg := r.Message
	switch {
	case r.Level < slog.LevelInfo:
		l.Debug(msg)
	case r.Level < slog.LevelWarn:
		l.Info(msg)
	case r.Level < slog.LevelError:
		l.Warn(msg)
	default:
		l.Error(msg)
	}
	return nil
}

// WithAttrs returns a new handler with the given attributes pre-attached.
// They will be included in every subsequent log record.
func (h *loggerSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	h2 := *h
	h2.attrs = make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(h2.attrs, h.attrs)
	copy(h2.attrs[len(h.attrs):], attrs)
	return &h2
}

// WithGroup returns a new handler that nests all subsequent attribute keys
// under the given group name, using a dot separator (e.g. "db.host").
func (h *loggerSlogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	h2 := *h
	if h2.group != "" {
		h2.group = h2.group + "." + name
	} else {
		h2.group = name
	}
	return &h2
}

// applySlogAttrToLogger attaches a single slog.Attr to a Logger as a field.
// Groups are flattened using dot-separated keys (e.g. slog.Group("db", "host",
// "localhost") becomes the field key "db.host"). Empty keys are skipped.
func applySlogAttrToLogger(l Logger, a slog.Attr, prefix string) Logger {
	a.Value = a.Value.Resolve()

	if a.Value.Kind() == slog.KindGroup {
		groupPrefix := joinSlogPrefix(prefix, a.Key)
		for _, ga := range a.Value.Group() {
			l = applySlogAttrToLogger(l, ga, groupPrefix)
		}
		return l
	}

	if a.Key == "" {
		return l
	}
	return l.WithField(joinSlogPrefix(prefix, a.Key), a.Value.Any())
}

// joinSlogPrefix concatenates prefix and key with a dot, eliding the dot when
// either part is empty. This mirrors the behaviour of zerolog's SlogHandler.
func joinSlogPrefix(prefix, key string) string {
	if prefix == "" {
		return key
	}
	if key == "" {
		return prefix
	}
	return prefix + "." + key
}

// NewSlogLogger returns a *slog.Logger that routes all log output through the
// given ghostferry Logger. Use this to bridge third-party libraries that log
// via log/slog into ghostferry's active backend (zerolog or logrus).
//
// Example — route go-mysql BinlogSyncer logs through a tagged ghostferry logger:
//
//	syncerConfig := replication.BinlogSyncerConfig{
//	    ...
//	    Logger: ghostferry.NewSlogLogger(ghostferry.LogWithField("tag", "binlog_syncer")),
//	}
func NewSlogLogger(logger Logger) *slog.Logger {
	return slog.New(&loggerSlogHandler{logger: logger})
}
