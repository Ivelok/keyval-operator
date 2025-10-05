package sla

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var moduleRoot = detectModuleRoot()

type scenarioFile struct {
	Scenario    string             `json:"scenario"`
	Cluster     string             `json:"cluster"`
	Namespace   string             `json:"namespace"`
	StartedAt   time.Time          `json:"startedAt"`
	CompletedAt time.Time          `json:"completedAt"`
	Durations   map[string]float64 `json:"durations"`
	Marks       map[string]string  `json:"marks"`
	Notes       map[string]string  `json:"notes"`
}

type runtimeEntry struct {
	Test         string        `json:"test"`
	Total        string        `json:"total"`
	TotalSeconds float64       `json:"totalSeconds"`
	Steps        []runtimeStep `json:"steps"`
}

type runtimeStep struct {
	Name            string  `json:"name"`
	Duration        string  `json:"duration"`
	DurationSeconds float64 `json:"seconds"`
}

// Generate builds an SLA report based on the supplied configuration.
func Generate(cfg Config) (*Report, error) {
	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	chaosReports, err := loadChaosScenarios(cfg.ChaosDirs)
	if err != nil {
		return nil, err
	}
	e2eReports, err := loadE2ERuntimes(cfg.E2EDirs)
	if err != nil {
		return nil, err
	}

	summary := summarize(chaosReports)

	report := &Report{
		GeneratedAt: nowFn().UTC().Truncate(time.Second),
		Inputs: InputSummary{
			ChaosDirs: append([]string{}, cfg.ChaosDirs...),
			E2EDirs:   append([]string{}, cfg.E2EDirs...),
		},
		Summary:    summary,
		Scenarios:  chaosReports,
		E2ERuntime: e2eReports,
	}
	for i, dir := range report.Inputs.ChaosDirs {
		abs := toAbs(dir)
		report.Inputs.ChaosDirs[i] = makeRelative(abs)
	}
	for i, dir := range report.Inputs.E2EDirs {
		abs := toAbs(dir)
		report.Inputs.E2EDirs[i] = makeRelative(abs)
	}
	sort.Slice(report.Inputs.ChaosDirs, func(i, j int) bool { return report.Inputs.ChaosDirs[i] < report.Inputs.ChaosDirs[j] })
	sort.Slice(report.Inputs.E2EDirs, func(i, j int) bool { return report.Inputs.E2EDirs[i] < report.Inputs.E2EDirs[j] })

	sort.Slice(report.Scenarios, func(i, j int) bool {
		if report.Scenarios[i].Name == report.Scenarios[j].Name {
			return report.Scenarios[i].Cluster < report.Scenarios[j].Cluster
		}
		return report.Scenarios[i].Name < report.Scenarios[j].Name
	})

	sort.Slice(report.E2ERuntime, func(i, j int) bool { return report.E2ERuntime[i].Name < report.E2ERuntime[j].Name })
	for i := range report.E2ERuntime {
		sort.Slice(report.E2ERuntime[i].Steps, func(a, b int) bool { return report.E2ERuntime[i].Steps[a].Name < report.E2ERuntime[i].Steps[b].Name })
	}

	return report, nil
}

func loadChaosScenarios(dirs []string) ([]Scenario, error) {
	var scenarios []Scenario
	seen := make(map[string]struct{})
	for _, dir := range dirs {
		if strings.TrimSpace(dir) == "" {
			continue
		}
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if !strings.HasSuffix(d.Name(), "-metrics.json") {
				return nil
			}
			absPath := toAbs(path)
			key := filepath.Clean(absPath)
			if _, ok := seen[key]; ok {
				return nil
			}
			seen[key] = struct{}{}
			data, err := os.ReadFile(absPath)
			if err != nil {
				return fmt.Errorf("read chaos metrics %s: %w", absPath, err)
			}
			var payload scenarioFile
			if err := json.Unmarshal(data, &payload); err != nil {
				return fmt.Errorf("parse chaos metrics %s: %w", absPath, err)
			}
			total := payload.CompletedAt.Sub(payload.StartedAt).Seconds()
			if total < 0 {
				total = 0
			}
			durations := make(map[string]float64, len(payload.Durations))
			for k, v := range payload.Durations {
				durations[k] = roundFloat(v)
			}
			notes := payload.Notes
			if len(notes) == 0 {
				notes = nil
			}
			scenarios = append(scenarios, Scenario{
				Name:         payload.Scenario,
				Cluster:      payload.Cluster,
				Namespace:    payload.Namespace,
				TotalSeconds: roundFloat(total),
				Durations:    durations,
				Notes:        notes,
				Source:       makeRelative(absPath),
			})
			return nil
		})
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
	}
	return scenarios, nil
}

func loadE2ERuntimes(dirs []string) ([]E2ETest, error) {
	var tests []E2ETest
	seen := make(map[string]struct{})
	for _, dir := range dirs {
		if strings.TrimSpace(dir) == "" {
			continue
		}
		path := filepath.Join(dir, "e2e-runtime.json")
		absPath := toAbs(path)
		key := filepath.Clean(absPath)
		if _, ok := seen[key]; ok {
			continue
		}
		data, err := os.ReadFile(absPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("read e2e runtime %s: %w", absPath, err)
		}
		var entries []runtimeEntry
		if err := json.Unmarshal(data, &entries); err != nil {
			return nil, fmt.Errorf("parse e2e runtime %s: %w", absPath, err)
		}
		for _, entry := range entries {
			test := E2ETest{
				Name:         entry.Test,
				TotalSeconds: roundFloat(entry.TotalSeconds),
			}
			for _, step := range entry.Steps {
				test.Steps = append(test.Steps, E2EStep{
					Name:    step.Name,
					Seconds: roundFloat(step.DurationSeconds),
				})
			}
			tests = append(tests, test)
		}
		seen[key] = struct{}{}
	}
	return tests, nil
}

func summarize(scenarios []Scenario) Summary {
	summary := Summary{}
	if len(scenarios) == 0 {
		return summary
	}
	var mttrSamples, failoverSamples []float64
	var blockSeconds float64
	var totalScenarioSeconds float64
	for _, s := range scenarios {
		totalScenarioSeconds += s.TotalSeconds
		if pause, ok := s.Durations["disruptions.pauseSeconds"]; ok {
			blockSeconds += pause
			summary.DisruptionsSamples++
		}
		for key, value := range s.Durations {
			switch {
			case strings.Contains(key, "mttr"):
				mttrSamples = append(mttrSamples, value)
			case strings.Contains(key, "quorum") || strings.Contains(key, "failover"):
				failoverSamples = append(failoverSamples, value)
			}
		}
	}
	summary.TotalScenarios = len(scenarios)
	summary.ScenarioRuntimeSeconds = roundFloat(totalScenarioSeconds)
	summary.MTTRSeconds = aggregate(mttrSamples)
	summary.FailoverSeconds = aggregate(failoverSamples)
	if totalScenarioSeconds > 0 {
		summary.DisruptionsBlockedPct = roundFloat((blockSeconds / totalScenarioSeconds) * 100)
	}
	if summary.MTTRSeconds.Count == 0 {
		summary.Notes = append(summary.Notes, "no mttr samples found in chaos metrics")
	}
	if summary.FailoverSeconds.Count == 0 {
		summary.Notes = append(summary.Notes, "no failover samples found in chaos metrics")
	}
	if summary.DisruptionsSamples == 0 {
		summary.Notes = append(summary.Notes, "no disruptions.pauseSeconds durations captured")
	}
	return summary
}

func aggregate(samples []float64) Aggregate {
	if len(samples) == 0 {
		return Aggregate{}
	}
	var sum, min, max float64
	for i, s := range samples {
		sum += s
		if i == 0 || s < min {
			min = s
		}
		if i == 0 || s > max {
			max = s
		}
	}
	avg := sum / float64(len(samples))
	return Aggregate{
		Count:   len(samples),
		Average: roundFloat(avg),
		Max:     roundFloat(max),
		Min:     roundFloat(min),
	}
}

func roundFloat(v float64) float64 {
	return math.Round(v*1000) / 1000
}

func makeRelative(path string) string {
	if !filepath.IsAbs(path) {
		return filepath.ToSlash(path)
	}
	base := moduleRoot
	if base == "" {
		if wd, err := os.Getwd(); err == nil {
			base = wd
		}
	}
	if base == "" {
		return filepath.ToSlash(path)
	}
	rel, err := filepath.Rel(base, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(rel)
}

func detectModuleRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	current := wd
	for {
		goMod := filepath.Join(current, "go.mod")
		if _, err := os.Stat(goMod); err == nil {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}
	return ""
}

func toAbs(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}
