//go:build e2e || chaos

package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultNamespace    = "keyval-e2e"
	defaultRedisImage   = "valkey/valkey:7.2"
	defaultSuiteTimeout = 6 * time.Minute
	pollInterval        = time.Second
)

var (
	parallelOnce  sync.Once
	parallelSlots chan struct{}
)

// Harness encapsulates Kubernetes client wiring and lifecycle helpers for new E2E suites.
type Harness struct {
	t             *testing.T
	ctx           context.Context
	cancel        context.CancelFunc
	client        client.Client
	kube          kubernetes.Interface
	restConfig    *rest.Config
	namespace     string
	keepResources bool
	tracked       map[types.NamespacedName]struct{}
	mu            sync.Mutex
}

// New constructs a Harness bound to the provided testing.T instance.
func New(t *testing.T) *Harness {
	t.Helper()

	cfg, err := ctrl.GetConfig()
	if err != nil {
		t.Fatalf("fetch kube config: %v", err)
	}

	scheme := apiruntime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("register core scheme: %v", err)
	}
	if err := keyvalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("register keyval scheme: %v", err)
	}

	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("construct controller client: %v", err)
	}

	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("construct typed client: %v", err)
	}

	ns := namespace()
	timeout := suiteTimeout()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	h := &Harness{
		t:             t,
		ctx:           ctx,
		cancel:        cancel,
		client:        cl,
		kube:          kube,
		restConfig:    cfg,
		namespace:     ns,
		keepResources: keepResources(),
		tracked:       make(map[types.NamespacedName]struct{}),
	}

	h.ensureNamespace()
	releaseSlot := acquireSuiteSlot()
	t.Cleanup(releaseSlot)
	t.Cleanup(h.cleanup)
	return h
}

// Context exposes the shared test context.
func (h *Harness) Context() context.Context {
	return h.ctx
}

// Client returns the controller-runtime client.
func (h *Harness) Client() client.Client {
	return h.client
}

// Kube returns the typed kubernetes.Interface.
func (h *Harness) Kube() kubernetes.Interface {
	return h.kube
}

// RestConfig returns the REST config used by the harness.
func (h *Harness) RestConfig() *rest.Config {
	return h.restConfig
}

// Namespace reports the target namespace used for resources.
func (h *Harness) Namespace() string {
	return h.namespace
}

// RedisImage returns the default Redis/Valkey image for tests.
func (h *Harness) RedisImage() string {
	if img := os.Getenv("E2E_REDIS_IMAGE"); img != "" {
		return img
	}
	return defaultRedisImage
}

// TrackCluster registers a KeyValCluster for automatic cleanup.
func (h *Harness) TrackCluster(obj client.Object) {
	if obj == nil {
		return
	}
	key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tracked[key] = struct{}{}
}

// WaitForCondition waits until the specified condition reaches the desired status.
func (h *Harness) WaitForCondition(ctx context.Context, name string, cond keyvalv1alpha1.ConditionType, status metav1.ConditionStatus, timeout time.Duration) *keyvalv1alpha1.KeyValCluster {
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	key := types.NamespacedName{Namespace: h.namespace, Name: name}
	var latest keyvalv1alpha1.KeyValCluster
	err := wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		if err := h.client.Get(ctx, key, &latest); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		for _, c := range latest.Status.Conditions {
			if c.Type == string(cond) && c.Status == status {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		h.t.Fatalf("wait for condition %s=%s: %v", cond, status, err)
	}
	return latest.DeepCopy()
}

// WaitForReadyReplicas waits until status.readyReplicas equals the desired count.
func (h *Harness) WaitForReadyReplicas(ctx context.Context, name string, want int32, timeout time.Duration) *keyvalv1alpha1.KeyValCluster {
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	key := types.NamespacedName{Namespace: h.namespace, Name: name}
	var latest keyvalv1alpha1.KeyValCluster
	err := wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		if err := h.client.Get(ctx, key, &latest); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return latest.Status.ReadyReplicas == want, nil
	})
	if err != nil {
		h.t.Fatalf("wait for ready replicas=%d: %v", want, err)
	}
	return latest.DeepCopy()
}

// WaitForDeploymentRollout waits for the operator deployment to become available.
func (h *Harness) WaitForDeploymentRollout(ctx context.Context, name string, timeout time.Duration) {
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	key := types.NamespacedName{Namespace: h.namespace, Name: name}
	err := wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		var deploy appsv1.Deployment
		if err := h.client.Get(ctx, key, &deploy); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return deploy.Status.ReadyReplicas == *deploy.Spec.Replicas, nil
	})
	if err != nil {
		h.t.Fatalf("wait for deployment %s rollout: %v", name, err)
	}
}

// WaitForService ensures the named Service exists.
func (h *Harness) WaitForService(ctx context.Context, name string, timeout time.Duration) *corev1.Service {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	var svc corev1.Service
	key := types.NamespacedName{Namespace: h.namespace, Name: name}
	err := wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		if err := h.client.Get(ctx, key, &svc); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
	if err != nil {
		h.t.Fatalf("wait for service %s: %v", name, err)
	}
	return svc.DeepCopy()
}

func (h *Harness) ensureNamespace() {
	ctx, cancel := context.WithTimeout(h.ctx, 30*time.Second)
	defer cancel()
	_, err := h.kube.CoreV1().Namespaces().Get(ctx, h.namespace, metav1.GetOptions{})
	if err == nil || apierrors.IsAlreadyExists(err) {
		return
	}
	if !apierrors.IsNotFound(err) {
		h.t.Fatalf("get namespace %s: %v", h.namespace, err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: h.namespace}}
	if _, err := h.kube.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		h.t.Fatalf("create namespace %s: %v", h.namespace, err)
	}
}

func (h *Harness) cleanup() {
	h.cancel()
	if h.keepResources {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	h.mu.Lock()
	keys := make([]types.NamespacedName, 0, len(h.tracked))
	for key := range h.tracked {
		keys = append(keys, key)
	}
	h.mu.Unlock()
	for _, key := range keys {
		var cluster keyvalv1alpha1.KeyValCluster
		if err := h.client.Get(ctx, key, &cluster); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			h.t.Logf("cleanup: get cluster %s failed: %v", key.String(), err)
			continue
		}
		if err := h.client.Delete(ctx, &cluster); err != nil && !apierrors.IsNotFound(err) {
			h.t.Logf("cleanup: delete cluster %s failed: %v", key.String(), err)
			continue
		}
		_ = wait.PollUntilContextTimeout(ctx, pollInterval, 90*time.Second, true, func(ctx context.Context) (bool, error) {
			err := h.client.Get(ctx, key, &cluster)
			switch {
			case err == nil:
				return false, nil
			case apierrors.IsNotFound(err):
				return true, nil
			default:
				return false, err
			}
		})
	}
}

func namespace() string {
	if ns := os.Getenv("E2E_NAMESPACE"); ns != "" {
		return ns
	}
	return defaultNamespace
}

func suiteTimeout() time.Duration {
	if raw := os.Getenv("E2E_TIMEOUT"); raw != "" {
		if dur, err := time.ParseDuration(raw); err == nil {
			return dur
		}
	}
	if raw := os.Getenv("TESTS_NEW_TIMEOUT"); raw != "" {
		if dur, err := time.ParseDuration(raw); err == nil {
			return dur
		}
	}
	return defaultSuiteTimeout
}

func keepResources() bool {
	if raw := os.Getenv("KEEP_RESOURCES"); raw != "" {
		keep, err := parseBool(raw)
		if err == nil {
			return keep
		}
	}
	if raw := os.Getenv("TESTS_NEW_KEEP_RESOURCES"); raw != "" {
		keep, err := parseBool(raw)
		if err == nil {
			return keep
		}
	}
	return false
}

func acquireSuiteSlot() func() {
	sem := parallelSemaphore()
	sem <- struct{}{}
	return func() { <-sem }
}

func parallelSemaphore() chan struct{} {
	parallelOnce.Do(func() {
		size := determineParallelism()
		if size < 1 {
			size = 1
		}
		parallelSlots = make(chan struct{}, size)
	})
	return parallelSlots
}

func determineParallelism() int {
	if v := lookupParallelism("E2E_PARALLELISM"); v > 0 {
		return v
	}
	if v := lookupParallelism("TESTS_NEW_PARALLELISM"); v > 0 {
		return v
	}
	n := runtime.NumCPU() / 2
	if n < 1 {
		n = 1
	}
	if n > 6 {
		n = 6
	}
	return n
}

func lookupParallelism(env string) int {
	if raw := os.Getenv(env); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			return v
		}
	}
	return 0
}

func parseBool(value string) (bool, error) {
	switch value {
	case "1", "t", "T", "true", "TRUE", "True", "yes", "Y", "y":
		return true, nil
	case "0", "f", "F", "false", "FALSE", "False", "no", "N", "n":
		return false, nil
	default:
		return false, errors.New("invalid bool")
	}
}

// Exec runs the provided command inside the specified pod/container and returns stdout, stderr.
func (h *Harness) Exec(ctx context.Context, podName, container string, command ...string) (string, string, error) {
	if podName == "" {
		return "", "", fmt.Errorf("pod name must be provided for exec")
	}
	if len(command) == 0 {
		return "", "", fmt.Errorf("exec requires at least one command argument")
	}
	req := h.kube.CoreV1().RESTClient().Post().Namespace(h.namespace).Resource("pods").Name(podName).SubResource("exec")
	opts := &corev1.PodExecOptions{
		Container: container,
		Command:   command,
		Stdout:    true,
		Stderr:    true,
	}
	req.VersionedParams(opts, clientgoscheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(h.restConfig, http.MethodPost, req.URL())
	if err != nil {
		return "", "", err
	}
	var stdout, stderr bytes.Buffer
	streamErr := executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	return stdout.String(), stderr.String(), streamErr
}
