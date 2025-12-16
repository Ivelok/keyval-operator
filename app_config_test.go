package main

import (
	"flag"
	"io"
	"testing"
	"time"
)

func parseAppConfig(t *testing.T, args []string) AppConfig {
	t.Helper()

	cfg := DefaultAppConfig()
	cfg.ApplyEnv()

	fs := flag.NewFlagSet("app-config-test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cfg.BindFlags(fs)

	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	return cfg
}

func TestDefaultAppConfig_Defaults(t *testing.T) {
	cfg := DefaultAppConfig()

	t.Run("manager", func(t *testing.T) {
		stringCases := []struct {
			name string
			got  string
			want string
		}{
			{name: "metrics-bind-address", got: cfg.MetricsAddr, want: ":8080"},
			{name: "health-probe-bind-address", got: cfg.ProbeAddr, want: ":8081"},
		}
		for _, tc := range stringCases {
			t.Run(tc.name, func(t *testing.T) {
				if tc.got != tc.want {
					t.Fatalf("%s: got %q, want %q", tc.name, tc.got, tc.want)
				}
			})
		}

		boolCases := []struct {
			name string
			got  bool
			want bool
		}{
			{name: "leader-elect", got: cfg.LeaderElection, want: true},
			{name: "zap-devel", got: cfg.ZapDev, want: true},
		}
		for _, tc := range boolCases {
			t.Run(tc.name, func(t *testing.T) {
				if tc.got != tc.want {
					t.Fatalf("%s: got %t, want %t", tc.name, tc.got, tc.want)
				}
			})
		}
	})

	t.Run("client", func(t *testing.T) {
		durationCases := []struct {
			name string
			got  time.Duration
			want time.Duration
		}{
			{name: "DialTimeout", got: cfg.Client.DialTimeout, want: 3 * time.Second},
			{name: "ReadTimeout", got: cfg.Client.ReadTimeout, want: 2 * time.Second},
			{name: "WriteTimeout", got: cfg.Client.WriteTimeout, want: 2 * time.Second},
			{name: "PoolTimeout", got: cfg.Client.PoolTimeout, want: 2 * time.Second},
			{name: "OperationTimeout", got: cfg.Client.OperationTimeout, want: 2 * time.Second},
			{name: "SentinelOperationTimeout", got: cfg.Client.SentinelOperationTimeout, want: 5 * time.Second},
			{name: "RetryInitialBackoff", got: cfg.Client.RetryInitialBackoff, want: 200 * time.Millisecond},
			{name: "RetryMaxBackoff", got: cfg.Client.RetryMaxBackoff, want: time.Second},
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
			{name: "MaxRetries", got: cfg.Client.MaxRetries, want: 2},
			{name: "MinIdleConns", got: cfg.Client.MinIdleConns, want: 1},
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
			{name: "RetryBackoffFactor", got: cfg.Client.RetryBackoffFactor, want: 2.0},
			{name: "RetryJitter", got: cfg.Client.RetryJitter, want: 0.1},
		}
		for _, tc := range floatCases {
			t.Run(tc.name, func(t *testing.T) {
				if tc.got != tc.want {
					t.Fatalf("%s: got %v, want %v", tc.name, tc.got, tc.want)
				}
			})
		}
	})
}

func TestAppConfig_Precedence_DefaultsEnvFlags(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		args []string
		want time.Duration
	}{
		{
			name: "env overrides flag default",
			env: map[string]string{
				"KEYVAL_REDIS_DIAL_TIMEOUT": "10s",
			},
			args: nil,
			want: 10 * time.Second,
		},
		{
			name: "flags override env",
			env: map[string]string{
				"KEYVAL_REDIS_DIAL_TIMEOUT": "10s",
			},
			args: []string{"--redis-dial-timeout=7s"},
			want: 7 * time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			cfg := parseAppConfig(t, tc.args)
			if cfg.Client.DialTimeout != tc.want {
				t.Fatalf("DialTimeout: got %s, want %s", cfg.Client.DialTimeout, tc.want)
			}
		})
	}
}

func TestAppConfig_InvalidEnvIgnored(t *testing.T) {
	defaults := DefaultAppConfig()

	t.Run("duration", func(t *testing.T) {
		cases := []struct {
			name string
			key  string
			val  string
			want time.Duration
		}{
			{
				name: "invalid duration",
				key:  "KEYVAL_REDIS_DIAL_TIMEOUT",
				val:  "notaduration",
				want: defaults.Client.DialTimeout,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Setenv(tc.key, tc.val)
				cfg := parseAppConfig(t, nil)
				if cfg.Client.DialTimeout != tc.want {
					t.Fatalf("DialTimeout: got %s, want %s", cfg.Client.DialTimeout, tc.want)
				}
			})
		}
	})

	t.Run("int", func(t *testing.T) {
		cases := []struct {
			name string
			key  string
			val  string
			want int
		}{
			{
				name: "invalid int",
				key:  "KEYVAL_REDIS_MAX_RETRIES",
				val:  "oops",
				want: defaults.Client.MaxRetries,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Setenv(tc.key, tc.val)
				cfg := parseAppConfig(t, nil)
				if cfg.Client.MaxRetries != tc.want {
					t.Fatalf("MaxRetries: got %d, want %d", cfg.Client.MaxRetries, tc.want)
				}
			})
		}
	})

	t.Run("float", func(t *testing.T) {
		cases := []struct {
			name string
			key  string
			val  string
			want float64
		}{
			{
				name: "NaN",
				key:  "KEYVAL_REDIS_RETRY_JITTER",
				val:  "NaN",
				want: defaults.Client.RetryJitter,
			},
			{
				name: "Inf",
				key:  "KEYVAL_REDIS_RETRY_JITTER",
				val:  "Inf",
				want: defaults.Client.RetryJitter,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Setenv(tc.key, tc.val)
				cfg := parseAppConfig(t, nil)
				if cfg.Client.RetryJitter != tc.want {
					t.Fatalf("RetryJitter: got %v, want %v", cfg.Client.RetryJitter, tc.want)
				}
			})
		}
	})
}
