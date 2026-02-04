//go:build e2e || chaos

package chaosmetrics

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/test/internal/harness"
)

const (
	pingTimeout         = 5 * time.Second
	probeReadyTimeout   = 2 * time.Minute
	probeCleanupTimeout = 30 * time.Second
)

// Downtime captures downtime statistics for a scenario step.
type Downtime struct {
	Max   time.Duration
	Total time.Duration
	Count int
}

// Scenario collects downtime metrics across chaos steps.
type Scenario struct {
	t         *testing.T
	h         *harness.Harness
	cfg       Config
	name      string
	cluster   *keyvalv1alpha1.KeyValCluster
	started   time.Time
	durations map[string]float64
	notes     map[string]string
}

// NewScenario initializes a scenario metrics recorder.
func NewScenario(t *testing.T, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster, cfg Config, name string) *Scenario {
	t.Helper()
	notes := map[string]string{
		"pingInterval": cfg.PingInterval.String(),
	}
	if cfg.Iterations > 0 {
		notes["iterations"] = fmt.Sprintf("%d", cfg.Iterations)
	}
	return &Scenario{
		t:         t,
		h:         h,
		cfg:       cfg,
		name:      strings.TrimSpace(name),
		cluster:   cluster,
		started:   time.Now().UTC(),
		durations: make(map[string]float64),
		notes:     notes,
	}
}

// RecordDowntime stores downtime metrics for a step.
func (s *Scenario) RecordDowntime(step string, downtime Downtime) {
	if s == nil {
		return
	}
	key := normalizeKey(step)
	s.durations[fmt.Sprintf("mttr.%s", key)] = roundSeconds(downtime.Max)
	s.durations[fmt.Sprintf("downtime.%s.total", key)] = roundSeconds(downtime.Total)
	s.durations[fmt.Sprintf("outages.%s.count", key)] = float64(downtime.Count)
}

// RecordDuration stores a duration under the provided key.
func (s *Scenario) RecordDuration(key string, duration time.Duration) {
	if s == nil {
		return
	}
	s.durations[normalizeKey(key)] = roundSeconds(duration)
}

// Finish writes the scenario metrics file when artifacts are enabled.
func (s *Scenario) Finish() {
	if s == nil || !s.cfg.KeepArtifacts {
		return
	}
	if s.cluster == nil {
		s.t.Fatalf("scenario %s missing cluster", s.name)
	}
	dir := filepath.Join(moduleRoot(), "test", "chaos", "artifacts")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		s.t.Fatalf("create chaos artifacts dir: %v", err)
	}
	filename := fmt.Sprintf("%s-%s-metrics.json", sanitizeFile(s.name), sanitizeFile(s.cluster.Name))
	path := filepath.Join(dir, filename)

	payload := scenarioFile{
		Scenario:    s.name,
		Cluster:     s.cluster.Name,
		Namespace:   s.cluster.Namespace,
		StartedAt:   s.started,
		CompletedAt: time.Now().UTC(),
		Durations:   s.durations,
		Notes:       s.notes,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		s.t.Fatalf("marshal chaos metrics: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		s.t.Fatalf("write chaos metrics: %v", err)
	}
	s.t.Logf("chaos metrics saved to %s", path)
}

type scenarioFile struct {
	Scenario    string             `json:"scenario"`
	Cluster     string             `json:"cluster"`
	Namespace   string             `json:"namespace"`
	StartedAt   time.Time          `json:"startedAt"`
	CompletedAt time.Time          `json:"completedAt"`
	Durations   map[string]float64 `json:"durations,omitempty"`
	Marks       map[string]string  `json:"marks,omitempty"`
	Notes       map[string]string  `json:"notes,omitempty"`
}

// AvailabilityRecorder pings the master service and records outages.
type AvailabilityRecorder struct {
	t       *testing.T
	h       *harness.Harness
	cluster *keyvalv1alpha1.KeyValCluster
	cfg     Config

	probeName string
	stopCh    chan struct{}
	doneCh    chan struct{}
	stopOnce  sync.Once

	mu           sync.Mutex
	outageActive bool
	outageStart  time.Time
	total        time.Duration
	max          time.Duration
	count        int
}

// NewAvailabilityRecorder builds a recorder bound to the provided cluster.
func NewAvailabilityRecorder(t *testing.T, h *harness.Harness, cluster *keyvalv1alpha1.KeyValCluster, cfg Config) *AvailabilityRecorder {
	t.Helper()
	return &AvailabilityRecorder{
		t:       t,
		h:       h,
		cluster: cluster,
		cfg:     cfg,
	}
}

// Start launches the probe and begins sampling.
func (r *AvailabilityRecorder) Start(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("availability recorder is nil")
	}
	if r.cluster == nil {
		return fmt.Errorf("availability recorder missing cluster")
	}
	if r.stopCh != nil {
		return fmt.Errorf("availability recorder already started")
	}
	suffix := shortSuffix()
	base := fmt.Sprintf("chaos-probe-%s", sanitizeFile(r.cluster.Name))
	maxBase := 63 - 1 - len(suffix)
	if maxBase < 1 {
		maxBase = 1
	}
	if len(base) > maxBase {
		base = strings.Trim(base[:maxBase], "-")
		if base == "" {
			base = "probe"
		}
	}
	r.probeName = fmt.Sprintf("%s-%s", base, suffix)
	if err := r.ensureProbePod(ctx); err != nil {
		return err
	}
	if err := r.waitForProbeReady(ctx); err != nil {
		return err
	}
	r.stopCh = make(chan struct{})
	r.doneCh = make(chan struct{})
	r.t.Cleanup(func() {
		_ = r.Stop(context.Background())
	})
	go r.loop()
	return nil
}

// Stop ends sampling and returns downtime metrics.
func (r *AvailabilityRecorder) Stop(ctx context.Context) Downtime {
	if r == nil || r.stopCh == nil {
		return Downtime{}
	}
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
	select {
	case <-r.doneCh:
	case <-ctx.Done():
		r.t.Logf("availability recorder stop timed out: %v", ctx.Err())
	}
	r.finalize(time.Now())
	if !r.cfg.KeepResources {
		r.deleteProbe(ctx)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return Downtime{
		Max:   r.max,
		Total: r.total,
		Count: r.count,
	}
}

func (r *AvailabilityRecorder) loop() {
	ticker := time.NewTicker(r.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			close(r.doneCh)
			return
		case <-ticker.C:
			r.sample()
		}
	}
}

func (r *AvailabilityRecorder) sample() {
	ok := r.ping()
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if ok {
		if r.outageActive {
			r.recordDowntime(now)
		}
		return
	}
	if !r.outageActive {
		r.outageActive = true
		r.outageStart = now
	}
}

func (r *AvailabilityRecorder) recordDowntime(now time.Time) {
	r.outageActive = false
	duration := now.Sub(r.outageStart)
	r.total += duration
	if duration > r.max {
		r.max = duration
	}
	r.count++
}

func (r *AvailabilityRecorder) finalize(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.outageActive {
		r.recordDowntime(now)
	}
}

func (r *AvailabilityRecorder) ping() bool {
	if r.cluster == nil {
		return false
	}
	masterSvc := fmt.Sprintf("%s-master", r.cluster.Name)
	ctx, cancel := context.WithTimeout(r.h.Context(), pingTimeout)
	defer cancel()
	cmd := []string{"redis-cli", "--no-auth-warning", "-h", masterSvc, "-p", "6379", "PING"}
	stdout, _, err := r.h.Exec(ctx, r.probeName, "redis", cmd...)
	if err != nil {
		return false
	}
	return strings.TrimSpace(stdout) == "PONG"
}

func (r *AvailabilityRecorder) ensureProbePod(ctx context.Context) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.probeName,
			Namespace: r.h.Namespace(),
			Labels: map[string]string{
				"app":           "chaos-probe",
				"keyvalcluster": r.cluster.Name,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{
				{
					Name:    "redis",
					Image:   r.h.RedisImage(),
					Command: []string{"sh", "-c", "sleep infinity"},
				},
			},
		},
	}
	if err := r.h.Client().Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create probe pod: %w", err)
	}
	return nil
}

func (r *AvailabilityRecorder) waitForProbeReady(ctx context.Context) error {
	key := types.NamespacedName{Namespace: r.h.Namespace(), Name: r.probeName}
	return wait.PollUntilContextTimeout(ctx, time.Second, probeReadyTimeout, true, func(ctx context.Context) (bool, error) {
		var pod corev1.Pod
		if err := r.h.Client().Get(ctx, key, &pod); err != nil {
			return false, err
		}
		return isPodReady(&pod), nil
	})
}

func (r *AvailabilityRecorder) deleteProbe(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, probeCleanupTimeout)
	defer cancel()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: r.probeName, Namespace: r.h.Namespace()}}
	if err := r.h.Client().Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		r.t.Logf("delete probe pod %s: %v", r.probeName, err)
	}
}

func isPodReady(pod *corev1.Pod) bool {
	for i := range pod.Status.Conditions {
		cond := pod.Status.Conditions[i]
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func normalizeKey(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.ReplaceAll(value, " ", "_")
	return value
}

func sanitizeFile(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, " ", "-")
	value = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return '-'
	}, value)
	value = strings.Trim(value, "-")
	if value == "" {
		return "unknown"
	}
	return value
}

func shortSuffix() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

func roundSeconds(d time.Duration) float64 {
	seconds := d.Seconds()
	return math.Round(seconds*1000) / 1000
}

func moduleRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return wd
		}
		dir = parent
	}
}
