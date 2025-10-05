//go:build e2e || chaos

package cluster

import (
	"fmt"
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

func metav1Metadata(namespace string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: namespace}
}
