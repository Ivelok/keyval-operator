package sla_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/ivelok/keyval-operator/internal/sla"
)

func TestGenerateReportFromFixtures(t *testing.T) {
	fixturesRoot := filepath.Join("..", "..", "test", "sla", "fixtures")
	cfg := sla.Config{
		ChaosDirs: []string{filepath.Join(fixturesRoot, "chaos")},
		E2EDirs:   []string{filepath.Join(fixturesRoot, "e2e")},
		Now:       func() time.Time { return time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC) },
	}

	report, err := sla.Generate(cfg)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}

	got, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}

	wantPath := filepath.Join("..", "..", "test", "sla", "example-report.json")
	want, err := os.ReadFile(wantPath)
	if errors.Is(err, os.ErrNotExist) {
		t.Skipf("example report fixture missing: %s", wantPath)
	}
	if err != nil {
		t.Fatalf("read example report: %v", err)
	}

	if diff := cmp.Diff(string(want), string(got)); diff != "" {
		t.Fatalf("report mismatch (-want +got):\n%s", diff)
	}
}
