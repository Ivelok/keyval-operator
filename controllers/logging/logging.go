package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/go-logr/logr"
)

type contextKey struct{}

var (
	once          sync.Once
	defaultLogger *slog.Logger
)

// Initialize sets the package-wide default logger. If called multiple times, only the first call wins.
func Initialize(handler slog.Handler) {
	once.Do(func() {
		if handler == nil {
			handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
		}
		defaultLogger = slog.New(handler)
	})
}

// Logger is a thin wrapper around slog.Logger that preserves familiar Info/Error semantics.
type Logger struct {
	inner     *slog.Logger
	verbosity int
}

// IsZero reports whether the logger has been initialized.
func (l Logger) IsZero() bool { return l.inner == nil }

// New constructs a Logger from the provided slog.Logger.
func New(l *slog.Logger) Logger {
	if l == nil {
		l = ensureDefault()
	}
	return Logger{inner: l}
}

// FromContext retrieves a Logger from context or falls back to the default.
func FromContext(ctx context.Context) Logger {
	if ctx != nil {
		if existing, ok := ctx.Value(contextKey{}).(*slog.Logger); ok && existing != nil {
			return New(existing)
		}
		if lg := logr.FromContextOrDiscard(ctx); lg.GetSink() != nil {
			return FromLogr(lg)
		}
	}
	return New(nil)
}

// IntoContext stores the logger inside the context for downstream use.
func IntoContext(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, contextKey{}, logger.inner)
}

// WithValues returns a new Logger enriched with key/value pairs.
func (l Logger) WithValues(kv ...any) Logger {
	return Logger{inner: l.inner.With(kv...), verbosity: l.verbosity}
}

// WithGroup creates a new Logger with the supplied group name.
func (l Logger) WithGroup(name string) Logger {
	return Logger{inner: l.inner.WithGroup(name), verbosity: l.verbosity}
}

// V sets the verbosity level. V(1) maps to Debug.
func (l Logger) V(level int) Logger {
	return Logger{inner: l.inner, verbosity: level}
}

// Info logs an informational message.
func (l Logger) Info(msg string, kv ...any) {
	if l.verbosity > 0 {
		l.inner.Debug(msg, kv...)
		return
	}
	l.inner.Info(msg, kv...)
}

// Debug logs at debug level.
func (l Logger) Debug(msg string, kv ...any) {
	l.inner.Debug(msg, kv...)
}

// Error logs an error with the provided message.
func (l Logger) Error(err error, msg string, kv ...any) {
	if err != nil {
		kv = append(kv, "error", err)
	}
	l.inner.Error(msg, kv...)
}

// With returns a new Logger with pre-bound attributes (alias for WithValues).
func (l Logger) With(kv ...any) Logger { return l.WithValues(kv...) }

func ensureDefault() *slog.Logger {
	Initialize(nil)
	return defaultLogger
}

// FromLogr adapts a logr.Logger to the unified logging interface.
func FromLogr(l logr.Logger) Logger {
	return New(newFromLogr(l))
}

// Logr converts the Logger into a logr.Logger for integration points that still use logr.
func (l Logger) Logr() logr.Logger {
	if l.inner == nil {
		return logr.New(&slogSink{logger: ensureDefault()})
	}
	return logr.New(&slogSink{logger: l.inner})
}

// newFromLogr builds a slog.Logger that delegates to the supplied logr implementation.
func newFromLogr(l logr.Logger) *slog.Logger {
	return slog.New(&logrHandler{base: l})
}

type logrHandler struct {
	base   logr.Logger
	attrs  []slog.Attr
	groups []string
}

func (h *logrHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *logrHandler) Handle(_ context.Context, record slog.Record) error {
	logger := h.base
	if len(h.groups) > 0 {
		for _, g := range h.groups {
			logger = logger.WithName(g)
		}
	}
	if len(h.attrs) > 0 {
		logger = logger.WithValues(flattenAttrs(h.attrs)...)
	}
	args := make([]any, 0, record.NumAttrs()*2)
	record.Attrs(func(a slog.Attr) bool {
		args = append(args, flattenAttr(h.groups, a)...)
		return true
	})
	msg := record.Message
	switch {
	case record.Level >= slog.LevelError:
		logger.Error(nil, msg, args...)
	case record.Level >= slog.LevelWarn:
		logger.Info(msg, append(args, "severity", "warn")...)
	case record.Level >= slog.LevelInfo:
		logger.Info(msg, args...)
	default:
		logger.V(1).Info(msg, args...)
	}
	return nil
}

func (h *logrHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	copy(newAttrs[len(h.attrs):], attrs)
	return &logrHandler{base: h.base, attrs: newAttrs, groups: append([]string{}, h.groups...)}
}

func (h *logrHandler) WithGroup(name string) slog.Handler {
	return &logrHandler{base: h.base, attrs: append([]slog.Attr{}, h.attrs...), groups: append(append([]string{}, h.groups...), name)}
}

func flattenAttrs(attrs []slog.Attr) []any {
	var out []any
	for _, attr := range attrs {
		out = append(out, flattenAttr(nil, attr)...)
	}
	return out
}

func flattenAttr(prefix []string, attr slog.Attr) []any {
	if attr.Value.Kind() == slog.KindGroup {
		var out []any
		for _, a := range attr.Value.Group() {
			out = append(out, flattenAttr(append(prefix, attr.Key), a)...)
		}
		return out
	}
	key := attr.Key
	if len(prefix) > 0 {
		key = strings.Join(append(prefix, key), ".")
	}
	return []any{key, attr.Value.Any()}
}

type slogSink struct {
	logger *slog.Logger
}

func (s *slogSink) Init(logr.RuntimeInfo) {}

func (s *slogSink) Enabled(level int) bool { return true }

func (s *slogSink) Info(level int, msg string, kv ...any) {
	if level > 0 {
		s.logger.Debug(msg, kv...)
		return
	}
	s.logger.Info(msg, kv...)
}

func (s *slogSink) Error(err error, msg string, kv ...any) {
	if err != nil {
		kv = append(kv, "error", err)
	}
	s.logger.Error(msg, kv...)
}

func (s *slogSink) WithValues(kv ...any) logr.LogSink {
	return &slogSink{logger: s.logger.With(kv...)}
}

func (s *slogSink) WithName(name string) logr.LogSink {
	return &slogSink{logger: s.logger.With("logger", name)}
}
