package logging

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestLoggerVerbosityRoutesToDebug(t *testing.T) {
	handler := newRecordingHandler()
	logger := New(slog.New(handler))

	logger.V(1).Info("verbose-path")

	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].level != slog.LevelDebug {
		t.Fatalf("expected debug level, got %v", records[0].level)
	}
}

func TestLoggerInfoUsesInfoLevel(t *testing.T) {
	handler := newRecordingHandler()
	logger := New(slog.New(handler))

	logger.Info("info-path")

	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].level != slog.LevelInfo {
		t.Fatalf("expected info level, got %v", records[0].level)
	}
}

func TestLoggerErrorAppendsErrorAttribute(t *testing.T) {
	handler := newRecordingHandler()
	logger := New(slog.New(handler))

	boom := errors.New("boom")
	logger.Error(boom, "something failed")

	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].level != slog.LevelError {
		t.Fatalf("expected error level, got %v", records[0].level)
	}
	if !hasAttr(records[0].attrs, "error", boom) {
		t.Fatalf("expected error attribute with value %v", boom)
	}
}

func TestLoggerContextRoundTrip(t *testing.T) {
	handler := newRecordingHandler()
	base := New(slog.New(handler)).WithValues("component", "ctx-test")

	ctx := IntoContext(context.Background(), base)
	logger := FromContext(ctx)
	logger.Info("context message")

	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if !hasAttr(records[0].attrs, "component", "ctx-test") {
		t.Fatalf("expected component attribute in context record")
	}
}

type recorded struct {
	level slog.Level
	attrs []slog.Attr
}

type recordingHandler struct {
	mu        *sync.Mutex
	records   *[]recorded
	baseAttrs []slog.Attr
	groups    []string
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{
		mu:      &sync.Mutex{},
		records: &[]recorded{},
	}
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	rec := recorded{level: r.Level}

	attrs := make([]slog.Attr, len(h.baseAttrs))
	for i, a := range h.baseAttrs {
		attrs[i] = h.decorateAttr(a)
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, h.decorateAttr(a))
		return true
	})
	rec.attrs = append([]slog.Attr{}, attrs...)

	h.mu.Lock()
	defer h.mu.Unlock()
	*h.records = append(*h.records, rec)
	return nil
}

func (h *recordingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	combined := append([]slog.Attr{}, h.baseAttrs...)
	combined = append(combined, attrs...)
	return &recordingHandler{
		mu:        h.mu,
		records:   h.records,
		baseAttrs: combined,
		groups:    append([]string{}, h.groups...),
	}
}

func (h *recordingHandler) WithGroup(name string) slog.Handler {
	return &recordingHandler{
		mu:        h.mu,
		records:   h.records,
		baseAttrs: append([]slog.Attr{}, h.baseAttrs...),
		groups:    append(append([]string{}, h.groups...), name),
	}
}

func (h *recordingHandler) Records() []recorded {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]recorded, len(*h.records))
	copy(out, *h.records)
	return out
}

func (h *recordingHandler) decorateAttr(a slog.Attr) slog.Attr {
	if len(h.groups) == 0 {
		return a
	}
	key := strings.Join(append(h.groups, a.Key), ".")
	return slog.Attr{Key: key, Value: a.Value}
}

func hasAttr(attrs []slog.Attr, key string, want any) bool {
	for _, a := range attrs {
		if a.Key != key {
			continue
		}
		switch a.Value.Kind() {
		case slog.KindString:
			if s, ok := want.(string); ok && a.Value.String() == s {
				return true
			}
		default:
			if reflect.DeepEqual(a.Value.Any(), want) {
				return true
			}
		}
	}
	return false
}
