package ghostferry

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
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
	return l.WithField(joinSlogPrefix(prefix, a.Key), safeFieldValue(a.Value.Any()))
}

// safeFieldValue returns a value safe to hand to a structured-logging backend.
//
// Some third-party libraries log values that cannot be JSON-encoded. For
// example, go-mysql's BinlogSyncer logs its entire config via
// slog.Any("config", cfg), and that config contains func fields
// (Option func(*client.Conn) error, Dialer). logrus's JSON formatter calls
// json.Marshal on each field and fails on such values, printing
// "Failed to obtain reader, failed to marshal fields to JSON, json:
// unsupported type: func(*client.Conn) error" to stderr for every binlog
// syncer created — which floods the test logs.
//
// To stay backend-agnostic, any value that contains an unmarshalable kind
// (func, chan, or unsafe.Pointer) is rendered to a string via fmt instead of
// being passed through as a live Go value. Ordinary values are returned
// unchanged so normal structured fields are unaffected.
func safeFieldValue(v any) any {
	if v == nil {
		return nil
	}
	if containsUnmarshalableKind(reflect.ValueOf(v), 0) {
		return fmt.Sprintf("%+v", v)
	}
	return v
}

// containsUnmarshalableKind reports whether v contains a func, chan, or
// unsafe.Pointer anywhere in its (possibly nested) structure. It guards
// against unbounded recursion with a depth limit and treats anything beyond it
// as unmarshalable so the value is stringified rather than risk a marshal
// failure.
func containsUnmarshalableKind(v reflect.Value, depth int) bool {
	if !v.IsValid() {
		return false
	}
	if depth > 8 {
		return true
	}

	switch v.Kind() {
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return true
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			return false
		}
		return containsUnmarshalableKind(v.Elem(), depth+1)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if containsUnmarshalableKind(v.Field(i), depth+1) {
				return true
			}
		}
		return false
	case reflect.Slice, reflect.Array:
		elem := v.Type().Elem()
		if isUnmarshalableType(elem) {
			return true
		}
		for i := 0; i < v.Len(); i++ {
			if containsUnmarshalableKind(v.Index(i), depth+1) {
				return true
			}
		}
		return false
	case reflect.Map:
		if isUnmarshalableType(v.Type().Elem()) || isUnmarshalableType(v.Type().Key()) {
			return true
		}
		for _, k := range v.MapKeys() {
			if containsUnmarshalableKind(v.MapIndex(k), depth+1) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// isUnmarshalableType reports whether a type is (or trivially contains) a kind
// that cannot be JSON-encoded. Used to short-circuit empty containers whose
// element type alone makes them unsafe.
func isUnmarshalableType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return true
	default:
		return false
	}
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
