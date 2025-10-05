//go:build !(e2e || chaos)

package suite

import (
	"context"
	"testing"
	"time"
)

type Suite struct{}

type Step struct {
	Name     string
	Duration time.Duration
}

func New(t *testing.T) *Suite {
	t.Helper()
	t.Skip("test/internal/suite requires -tags=e2e")
	return &Suite{}
}

func (s *Suite) Context() context.Context {
	return context.Background()
}

func (s *Suite) Step(string, func(context.Context)) {}
