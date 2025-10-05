package finalizer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
)

func TestPipeline_SuccessWithoutPVCs(t *testing.T) {
	scheme := newScheme(t)
	cr := buildCluster("success", true)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cr).WithObjects(cr).Build()
	deps := Dependencies{
		Client:           c,
		Recorder:         record.NewFakeRecorder(10),
		CleanupTimeout:   time.Minute,
		OperationTimeout: 10 * time.Second,
		Now: func() time.Time {
			return time.Unix(0, 0)
		},
	}
	pipe := NewPipeline(deps, HandlerFunc(Execute))

	if err := pipe.Run(context.Background(), cr); err != nil {
		t.Fatalf("pipeline run: %v", err)
	}

	var got keyvalv1alpha1.KeyValCluster
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(cr), &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if _, ok := got.Annotations[core.AnnotationFinalizerStarted]; ok {
		t.Fatalf("expected finalizer annotation cleared")
	}
	cond := findCondition(&got, keyvalv1alpha1.ConditionStorageCleanup)
	if cond == nil {
		t.Fatalf("storage cleanup condition not set")
	}
	if cond.Status != metav1.ConditionTrue || cond.Reason != "PVCsDeleted" {
		t.Fatalf("unexpected condition: %#v", cond)
	}
	if cond.ObservedGeneration != got.Generation {
		t.Fatalf("expected observedGeneration %d, got %d", got.Generation, cond.ObservedGeneration)
	}
}

func TestPipeline_WaitsWhilePending(t *testing.T) {
	scheme := newScheme(t)
	cr := buildCluster("pending", true)
	pvc := buildPVC("pending", 0)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cr).WithObjects(cr, pvc).Build()
	deps := Dependencies{
		Client:           c,
		Recorder:         record.NewFakeRecorder(10),
		CleanupTimeout:   time.Minute,
		OperationTimeout: 10 * time.Second,
		Now: func() time.Time {
			return time.Unix(0, 0)
		},
	}
	pipe := NewPipeline(deps, HandlerFunc(Execute))

	err := pipe.Run(context.Background(), cr)
	if err == nil {
		t.Fatalf("expected wait error, got nil")
	}
	if have, want := err.Error(), "waiting for pvc cleanup"; !contains(have, want) {
		t.Fatalf("expected error containing %q, got %q", want, have)
	}

	var got keyvalv1alpha1.KeyValCluster
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(cr), &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Annotations[core.AnnotationFinalizerStarted] == "" {
		t.Fatalf("expected finalizer start annotation recorded")
	}
	cond := findCondition(&got, keyvalv1alpha1.ConditionStorageCleanup)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "DeletingPVCs" {
		t.Fatalf("unexpected condition: %#v", cond)
	}
}

func TestPipeline_TimesOut(t *testing.T) {
	scheme := newScheme(t)
	cr := buildCluster("timeout", true)
	cr.Annotations = map[string]string{
		core.AnnotationFinalizerStarted: time.Now().Add(-3 * time.Minute).UTC().Format(time.RFC3339Nano),
	}
	pvc := buildPVC("timeout", 0)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cr).WithObjects(cr, pvc).Build()
	deps := Dependencies{
		Client:           c,
		Recorder:         record.NewFakeRecorder(10),
		CleanupTimeout:   time.Minute,
		OperationTimeout: 10 * time.Second,
		Now: func() time.Time {
			return time.Now()
		},
	}
	pipe := NewPipeline(deps, HandlerFunc(Execute))

	if err := pipe.Run(context.Background(), cr); err != nil {
		t.Fatalf("pipeline run: %v", err)
	}

	var got keyvalv1alpha1.KeyValCluster
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(cr), &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if _, ok := got.Annotations[core.AnnotationFinalizerStarted]; ok {
		t.Fatalf("expected finalizer annotation cleared after timeout")
	}
	cond := findCondition(&got, keyvalv1alpha1.ConditionStorageCleanup)
	if cond == nil || cond.Reason != "DeletingPVCs" || cond.Status != metav1.ConditionFalse {
		t.Fatalf("unexpected condition after timeout: %#v", cond)
	}
}

func TestPipeline_DeleteError(t *testing.T) {
	scheme := newScheme(t)
	cr := buildCluster("delete-error", true)
	pvc := buildPVC("delete-error", 0)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cr).WithObjects(cr, pvc).Build()
	c := &failingDeleteClient{Client: fakeClient, target: pvc.Name, err: errors.New("boom")}
	deps := Dependencies{
		Client:           c,
		Recorder:         record.NewFakeRecorder(10),
		CleanupTimeout:   time.Minute,
		OperationTimeout: 10 * time.Second,
		Now: func() time.Time {
			return time.Unix(0, 0)
		},
	}
	pipe := NewPipeline(deps, HandlerFunc(Execute))

	err := pipe.Run(context.Background(), cr)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if have, want := err.Error(), "cleanup pvcs"; !contains(have, want) {
		t.Fatalf("expected error containing %q, got %q", want, have)
	}

	var got keyvalv1alpha1.KeyValCluster
	if err := fakeClient.Get(context.Background(), client.ObjectKeyFromObject(cr), &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	cond := findCondition(&got, keyvalv1alpha1.ConditionStorageCleanup)
	if cond == nil || cond.Reason != "DeletionFailed" {
		t.Fatalf("expected deletion failed condition, got %#v", cond)
	}
}

type failingDeleteClient struct {
	client.Client
	target string
	err    error
}

func (f *failingDeleteClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok && pvc.Name == f.target {
		return f.err
	}
	return f.Client.Delete(ctx, obj, opts...)
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := keyvalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add keyval scheme: %v", err)
	}
	return scheme
}

func buildCluster(name string, cleanup bool) *keyvalv1alpha1.KeyValCluster {
	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: keyvalv1alpha1.KeyValClusterSpec{
			Mode:          keyvalv1alpha1.ModeStandalone,
			Image:         "valkey/valkey:7.2",
			RedisReplicas: 1,
		},
	}
	if cleanup {
		cr.Spec.Storage = &keyvalv1alpha1.StorageSpec{
			Type:            "Persistent",
			CleanupOnDelete: true,
			AccessModes:     []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		}
	}
	return cr
}

func buildPVC(name string, ordinal int) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("data-%s-%d", name, ordinal),
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       name,
			}},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
}

func findCondition(cr *keyvalv1alpha1.KeyValCluster, t keyvalv1alpha1.ConditionType) *metav1.Condition {
	for i := range cr.Status.Conditions {
		cond := cr.Status.Conditions[i]
		if cond.Type == string(t) {
			return &cond
		}
	}
	return nil
}

func contains(have, want string) bool {
	return strings.Contains(have, want)
}
