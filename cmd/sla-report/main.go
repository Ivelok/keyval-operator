package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ivelok/keyval-operator/internal/sla"
)

type multiFlag []string

func (m *multiFlag) String() string {
	return strings.Join(*m, ",")
}

func (m *multiFlag) Set(value string) error {
	if value == "" {
		return nil
	}
	*m = append(*m, value)
	return nil
}

func main() {
	var chaosDirs multiFlag
	var e2eDirs multiFlag
	var output string
	flag.Var(&chaosDirs, "chaos", "path to chaos artifacts (repeatable)")
	flag.Var(&e2eDirs, "e2e", "path to e2e artifacts (repeatable)")
	flag.StringVar(&output, "output", "test/sla/report.json", "output JSON file path")
	flag.Parse()

	if len(chaosDirs) == 0 {
		chaosDirs = append(chaosDirs, "test/chaos/artifacts")
	}
	if len(e2eDirs) == 0 {
		e2eDirs = append(e2eDirs, "test/e2e/artifacts")
	}

	cfg := sla.Config{ChaosDirs: chaosDirs, E2EDirs: e2eDirs}
	if override := os.Getenv("SLA_REPORT_NOW"); override != "" {
		if ts, err := time.Parse(time.RFC3339, override); err == nil {
			cfg.Now = func() time.Time { return ts }
		}
	}

	report, err := sla.Generate(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sla-report: %v\n", err)
		os.Exit(1)
	}

	if output != "" {
		if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "sla-report: create output dir: %v\n", err)
			os.Exit(1)
		}
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "sla-report: marshal report: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(output, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "sla-report: write report: %v\n", err)
			os.Exit(1)
		}
	}

	printSummary(report)
}

func printSummary(report *sla.Report) {
	fmt.Printf("SLA summary @ %s\n", report.GeneratedAt.Format(timeLayout()))
	fmt.Printf("- chaos scenarios: %d\n", len(report.Scenarios))
	fmt.Printf("- total scenario runtime: %.3fs\n", report.Summary.ScenarioRuntimeSeconds)
	if report.Summary.MTTRSeconds.Count > 0 {
		fmt.Printf("- MTTR average %.3fs (max %.3fs, n=%d)\n",
			report.Summary.MTTRSeconds.Average,
			report.Summary.MTTRSeconds.Max,
			report.Summary.MTTRSeconds.Count)
	} else {
		fmt.Println("- MTTR: no samples")
	}
	if report.Summary.FailoverSeconds.Count > 0 {
		fmt.Printf("- Failover average %.3fs (max %.3fs, n=%d)\n",
			report.Summary.FailoverSeconds.Average,
			report.Summary.FailoverSeconds.Max,
			report.Summary.FailoverSeconds.Count)
	} else {
		fmt.Println("- Failover: no samples")
	}
	if report.Summary.DisruptionsSamples > 0 {
		fmt.Printf("- Disruptions blocked %.2f%% (samples=%d)\n",
			report.Summary.DisruptionsBlockedPct,
			report.Summary.DisruptionsSamples)
	} else {
		fmt.Println("- Disruptions blocked: no samples")
	}
	if len(report.Summary.Notes) > 0 {
		for _, note := range report.Summary.Notes {
			fmt.Printf("  note: %s\n", note)
		}
	}
}

func timeLayout() string {
	return time.RFC3339
}
