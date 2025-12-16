package controllers

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDefaultClientFactoryConfig_ExpectedValues(t *testing.T) {
	cfg := DefaultClientFactoryConfig()

	durationCases := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{name: "DialTimeout", got: cfg.DialTimeout, want: 3 * time.Second},
		{name: "ReadTimeout", got: cfg.ReadTimeout, want: 2 * time.Second},
		{name: "WriteTimeout", got: cfg.WriteTimeout, want: 2 * time.Second},
		{name: "PoolTimeout", got: cfg.PoolTimeout, want: 2 * time.Second},
		{name: "OperationTimeout", got: cfg.OperationTimeout, want: 2 * time.Second},
		{name: "SentinelOperationTimeout", got: cfg.SentinelOperationTimeout, want: 5 * time.Second},
		{name: "RetryInitialBackoff", got: cfg.RetryInitialBackoff, want: 200 * time.Millisecond},
		{name: "RetryMaxBackoff", got: cfg.RetryMaxBackoff, want: time.Second},
	}
	for _, tc := range durationCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("%s: got %s, want %s", tc.name, tc.got, tc.want)
			}
		})
	}

	intCases := []struct {
		name string
		got  int
		want int
	}{
		{name: "MaxRetries", got: cfg.MaxRetries, want: 2},
		{name: "MinIdleConns", got: cfg.MinIdleConns, want: 1},
	}
	for _, tc := range intCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("%s: got %d, want %d", tc.name, tc.got, tc.want)
			}
		})
	}

	floatCases := []struct {
		name string
		got  float64
		want float64
	}{
		{name: "RetryBackoffFactor", got: cfg.RetryBackoffFactor, want: 2.0},
		{name: "RetryJitter", got: cfg.RetryJitter, want: 0.1},
	}
	for _, tc := range floatCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("%s: got %v, want %v", tc.name, tc.got, tc.want)
			}
		})
	}
}

func TestDefaultClientFactoryConfig_ReadmeInSync(t *testing.T) {
	readmePath := filepath.Join(repoRoot(t), "README.md")
	contents, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	defaults, err := parseReadmeControllerConfigDefaults(string(contents))
	if err != nil {
		t.Fatalf("parse README.md controller config defaults: %v", err)
	}

	cfg := DefaultClientFactoryConfig()

	durationFlags := map[string]time.Duration{
		"--redis-dial-timeout":          cfg.DialTimeout,
		"--redis-read-timeout":          cfg.ReadTimeout,
		"--redis-write-timeout":         cfg.WriteTimeout,
		"--redis-pool-timeout":          cfg.PoolTimeout,
		"--redis-operation-timeout":     cfg.OperationTimeout,
		"--sentinel-operation-timeout":  cfg.SentinelOperationTimeout,
		"--redis-retry-backoff-initial": cfg.RetryInitialBackoff,
		"--redis-retry-backoff-max":     cfg.RetryMaxBackoff,
	}
	for flag, want := range durationFlags {
		gotRaw, ok := defaults[flag]
		if !ok {
			t.Fatalf("README.md missing %s default", flag)
		}
		got, err := time.ParseDuration(gotRaw)
		if err != nil {
			t.Fatalf("README.md %s default %q is not a duration: %v", flag, gotRaw, err)
		}
		if got != want {
			t.Fatalf("README.md %s: got %s, want %s", flag, got, want)
		}
	}

	intFlags := map[string]int{
		"--redis-max-retries":    cfg.MaxRetries,
		"--redis-min-idle-conns": cfg.MinIdleConns,
	}
	for flag, want := range intFlags {
		gotRaw, ok := defaults[flag]
		if !ok {
			t.Fatalf("README.md missing %s default", flag)
		}
		got, err := strconv.Atoi(gotRaw)
		if err != nil {
			t.Fatalf("README.md %s default %q is not an int: %v", flag, gotRaw, err)
		}
		if got != want {
			t.Fatalf("README.md %s: got %d, want %d", flag, got, want)
		}
	}

	floatFlags := map[string]float64{
		"--redis-retry-backoff-factor": cfg.RetryBackoffFactor,
		"--redis-retry-jitter":         cfg.RetryJitter,
	}
	for flag, want := range floatFlags {
		gotRaw, ok := defaults[flag]
		if !ok {
			t.Fatalf("README.md missing %s default", flag)
		}
		got, err := strconv.ParseFloat(gotRaw, 64)
		if err != nil {
			t.Fatalf("README.md %s default %q is not a float: %v", flag, gotRaw, err)
		}
		if got != want {
			t.Fatalf("README.md %s: got %v, want %v", flag, got, want)
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	controllersDir := filepath.Dir(file)
	root := filepath.Dir(controllersDir)
	if root == controllersDir {
		t.Fatalf("unexpected repo root resolution")
	}
	return root
}

func parseReadmeControllerConfigDefaults(readme string) (map[string]string, error) {
	defaults := make(map[string]string)

	lines := strings.Split(readme, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") {
			continue
		}
		if !strings.Contains(line, "`--") {
			continue
		}

		cols := strings.Split(line, "|")
		if len(cols) < 4 {
			continue
		}
		flag := extractBacktickedFlag(cols[1])
		if flag == "" {
			continue
		}
		defaultValue := strings.TrimSpace(cols[3])
		defaultValue = strings.Trim(defaultValue, "`")
		defaultValue = strings.TrimSpace(defaultValue)
		defaults[flag] = defaultValue
	}

	return defaults, nil
}

func extractBacktickedFlag(input string) string {
	start := strings.Index(input, "`--")
	if start == -1 {
		return ""
	}
	rest := input[start+1:]
	end := strings.Index(rest, "`")
	if end == -1 {
		return ""
	}
	return rest[:end]
}
