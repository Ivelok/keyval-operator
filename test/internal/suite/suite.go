//go:build e2e || chaos

package suite

import (
	"context"
	"testing"
	"time"

	"github.com/ivelok/keyval-operator/test/internal/harness"
)

// Suite wraps the harness with step tracking utilities.
type Suite struct {
	T       *testing.T
	Harness *harness.Harness
	steps   []Step
}

// Step records metadata about a scenario stage.
type Step struct {
	Name     string
	Duration time.Duration
}

// New creates a suite bound to the provided testing state.
func New(t *testing.T) *Suite {
	return &Suite{T: t, Harness: harness.New(t)}
}

// Context returns the suite-wide context.
func (s *Suite) Context() context.Context {
	return s.Harness.Context()
}

// Step executes a named function and stores its runtime for reporting in t.Log.
func (s *Suite) Step(name string, fn func(context.Context)) {
	s.T.Helper()
	ctx := s.Context()
	start := time.Now()
	fn(ctx)
	duration := time.Since(start)
	s.steps = append(s.steps, Step{Name: name, Duration: duration})
	s.T.Logf("step %s completed in %s", name, duration)
}

// Steps returns a copy of recorded steps.
func (s *Suite) Steps() []Step {
	steps := make([]Step, len(s.steps))
	copy(steps, s.steps)
	return steps
}
