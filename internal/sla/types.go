package sla

import "time"

// Config describes input directories and output options for generating an SLA report.
type Config struct {
	ChaosDirs []string
	E2EDirs   []string
	Now       func() time.Time
}

// Report represents the aggregated SLA data.
type Report struct {
	GeneratedAt time.Time    `json:"generatedAt"`
	Inputs      InputSummary `json:"inputs"`
	Summary     Summary      `json:"summary"`
	Scenarios   []Scenario   `json:"scenarios,omitempty"`
	E2ERuntime  []E2ETest    `json:"e2eRuntime,omitempty"`
}

// InputSummary captures directories that were scanned.
type InputSummary struct {
	ChaosDirs []string `json:"chaosDirs,omitempty"`
	E2EDirs   []string `json:"e2eDirs,omitempty"`
}

// Summary contains aggregated SLA metrics.
type Summary struct {
	TotalScenarios         int       `json:"totalScenarios"`
	ScenarioRuntimeSeconds float64   `json:"scenarioRuntimeSeconds"`
	MTTRSeconds            Aggregate `json:"mttrSeconds"`
	FailoverSeconds        Aggregate `json:"failoverSeconds"`
	DisruptionsBlockedPct  float64   `json:"disruptionsBlockedPercent"`
	DisruptionsSamples     int       `json:"disruptionsSamples"`
	Notes                  []string  `json:"notes,omitempty"`
}

// Aggregate summarises numeric samples.
type Aggregate struct {
	Count   int     `json:"count"`
	Average float64 `json:"average"`
	Max     float64 `json:"max"`
	Min     float64 `json:"min"`
}

// Scenario captures metrics derived from chaos scenarios.
type Scenario struct {
	Name         string             `json:"name"`
	Cluster      string             `json:"cluster"`
	Namespace    string             `json:"namespace"`
	TotalSeconds float64            `json:"totalSeconds"`
	Durations    map[string]float64 `json:"durations,omitempty"`
	Notes        map[string]string  `json:"notes,omitempty"`
	Source       string             `json:"source"`
}

// E2ETest summarises runtime data from e2e tests.
type E2ETest struct {
	Name         string    `json:"name"`
	TotalSeconds float64   `json:"totalSeconds"`
	Steps        []E2EStep `json:"steps,omitempty"`
}

// E2EStep contains per-step runtime from the e2e suite.
type E2EStep struct {
	Name    string  `json:"name"`
	Seconds float64 `json:"seconds"`
}
