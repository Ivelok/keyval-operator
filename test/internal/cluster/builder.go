//go:build e2e || chaos

package cluster

import (
	"crypto/sha1"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

// Builder constructs KeyValCluster manifests with expressive modifiers.
type Builder struct {
	cluster keyvalv1alpha1.KeyValCluster
}

// NewBuilder initialises a builder with sane defaults for a standalone cluster.
func NewBuilder(namespace, image string) *Builder {
	return &Builder{
		cluster: keyvalv1alpha1.KeyValCluster{
			Spec: keyvalv1alpha1.KeyValClusterSpec{
				Mode:          keyvalv1alpha1.ModeStandalone,
				Image:         image,
				RedisReplicas: 1,
				Service: &keyvalv1alpha1.ServiceSpec{
					Create: ptr.To(true),
				},
			},
			ObjectMeta: metav1Metadata(namespace),
		},
	}
}

// WithName sets the cluster name, falling back to a timestamped identifier when empty.
func (b *Builder) WithName(name string) *Builder {
	if name == "" {
		name = fmt.Sprintf("tests-%d", time.Now().UnixNano())
	}
	b.cluster.Name = name
	return b
}

// WithNameForTest derives a DNS-1123 compatible name using the provided base and test identifier.
// This helps avoid cross-test collisions when suites run in parallel.
func (b *Builder) WithNameForTest(tb testing.TB, base string) *Builder {
	b.cluster.Name = uniqueTestName(tb, base)
	return b
}

// WithLabels merges the provided labels into metadata.
func (b *Builder) WithLabels(labels map[string]string) *Builder {
	if len(labels) == 0 {
		return b
	}
	if b.cluster.Labels == nil {
		b.cluster.Labels = map[string]string{}
	}
	for k, v := range labels {
		b.cluster.Labels[k] = v
	}
	return b
}

// WithReplicas sets spec.redisReplicas.
func (b *Builder) WithReplicas(count int32) *Builder {
	if count > 0 {
		b.cluster.Spec.RedisReplicas = count
	}
	return b
}

// WithMode switches the operating mode.
func (b *Builder) WithMode(mode keyvalv1alpha1.Mode) *Builder {
	if mode != "" {
		b.cluster.Spec.Mode = mode
	}
	return b
}

// WithImage sets spec.image.
func (b *Builder) WithImage(image string) *Builder {
	if image != "" {
		b.cluster.Spec.Image = image
	}
	return b
}

// WithEngine sets spec.engine.
func (b *Builder) WithEngine(engine keyvalv1alpha1.Engine) *Builder {
	if engine != "" {
		b.cluster.Spec.Engine = engine
	}
	return b
}

// WithSentinelCount sets spec.sentinelCount.
func (b *Builder) WithSentinelCount(count int32) *Builder {
	if count > 0 {
		b.cluster.Spec.SentinelCount = ptr.To(count)
	}
	return b
}

// WithEphemeralStorage forces redis pods to use emptyDir volumes.
func (b *Builder) WithEphemeralStorage() *Builder {
	b.cluster.Spec.Storage = &keyvalv1alpha1.StorageSpec{Type: "Ephemeral"}
	return b
}

// Build returns a deep copy of the configured cluster manifest.
func (b *Builder) Build() *keyvalv1alpha1.KeyValCluster {
	if b.cluster.Name == "" {
		b.cluster.Name = fmt.Sprintf("tests-%d", time.Now().UnixNano())
	}
	copy := b.cluster.DeepCopy()
	return copy
}

var dns1123Regexp = regexp.MustCompile("[^a-z0-9-]")

func uniqueTestName(tb testing.TB, base string) string {
	name := sanitizeName(base)
	if tb == nil {
		if name == "" {
			return fmt.Sprintf("tests-%d", time.Now().UnixNano())
		}
		if len(name) > 63 {
			name = strings.TrimRight(name[:63], "-")
		}
		if name == "" {
			return "tests"
		}
		return name
	}
	suffix := shortHash(tb.Name())
	if suffix == "" {
		suffix = "t"
	}
	maxBaseLen := 63 - len(suffix) - 1
	if maxBaseLen < 1 {
		maxBaseLen = 1
	}
	if len(name) > maxBaseLen {
		name = name[:maxBaseLen]
	}
	name = strings.Trim(name, "-")
	if name == "" {
		name = "tests"
	}
	return fmt.Sprintf("%s-%s", name, suffix)
}

func sanitizeName(base string) string {
	trimmed := strings.Trim(strings.ToLower(base), " \t\n-_")
	trimmed = dns1123Regexp.ReplaceAllString(trimmed, "-")
	trimmed = strings.Trim(trimmed, "-")
	return trimmed
}

func shortHash(input string) string {
	sum := sha1.Sum([]byte(input))
	return fmt.Sprintf("%x", sum[:3])
}

func metav1Metadata(namespace string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: namespace}
}
