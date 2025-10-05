package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

func withNow(t *testing.T, ts time.Time) {
	t.Helper()
	prev := nowFunc
	nowFunc = func() time.Time { return ts }
	t.Cleanup(func() { nowFunc = prev })
}

func TestEnsureResize_PatchesPVCAndDefersRestart(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, time.January, 1, 10, 0, 0, 0, time.UTC)
	withNow(t, baseTime)

	cr := keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
	}
	if err := json.Unmarshal([]byte(`{"storage":{"size":"2Gi"}}`), &cr.Spec); err != nil {
		t.Fatalf("unmarshal storage spec: %v", err)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-demo-0",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       "demo",
				UID:        types.UID("ssdemo"),
			}},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
		},
	}

	c := fake.NewClientBuilder().WithObjects(pvc).Build()
	pods := []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "demo-0"}}}

	res, err := EnsureResize(ctx, c, &cr, pods, nil, logging.New(nil))
	if err != nil {
		t.Fatalf("EnsureResize unexpected error: %v", err)
	}
	if !res.Waiting {
		t.Fatalf("expected resize to require waiting")
	}
	if reasons := res.PodReasons["demo-0"]; len(reasons) != 0 {
		t.Fatalf("expected no restart reasons yet, got %v", reasons)
	}
	hasPending := false
	for _, name := range res.PendingPVCs {
		if name == "data-demo-0" {
			hasPending = true
			break
		}
	}
	if !hasPending {
		t.Fatalf("expected pending pvc, got %v", res.PendingPVCs)
	}

	updated := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Name: "data-demo-0", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get pvc after resize: %v", err)
	}
	want := resource.MustParse("2Gi")
	if got := updated.Spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(want) != 0 {
		t.Fatalf("expected request %s, got %s", want.String(), got.String())
	}
	if updated.Annotations == nil || updated.Annotations[core.AnnotationPVCResizeTarget] != want.String() {
		t.Fatalf("missing resize target annotation: %v", updated.Annotations)
	}
}

func TestEnsureResize_WaitsForFilesystemResizeBeforeRestart(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, time.January, 1, 11, 0, 0, 0, time.UTC)
	withNow(t, baseTime)

	cr := keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}}
	if err := json.Unmarshal([]byte(`{"storage":{"size":"2Gi"}}`), &cr.Spec); err != nil {
		t.Fatalf("unmarshal storage spec: %v", err)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-demo-0",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       "demo",
				UID:        types.UID("ssdemo"),
			}},
			Annotations: map[string]string{
				core.AnnotationPVCResizeTarget:      "2Gi",
				core.AnnotationPVCResizeRequestedAt: baseTime.Add(-30 * time.Second).Format(time.RFC3339Nano),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("2Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("2Gi"),
			},
			Conditions: []corev1.PersistentVolumeClaimCondition{{
				Type:               corev1.PersistentVolumeClaimFileSystemResizePending,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(baseTime.Add(-30 * time.Second)),
			}},
		},
	}

	c := fake.NewClientBuilder().WithObjects(pvc).Build()
	pods := []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "demo-0"}}}

	res, err := EnsureResize(ctx, c, &cr, pods, nil, logging.New(nil))
	if err != nil {
		t.Fatalf("EnsureResize unexpected error: %v", err)
	}
	if !res.Waiting {
		t.Fatalf("expected waiting when filesystem resize pending")
	}
	if reasons := res.PodReasons["demo-0"]; len(reasons) != 0 {
		t.Fatalf("expected to defer restart while pending condition young, got %v", reasons)
	}

	capacity := pvc.Status.Capacity[corev1.ResourceStorage]
	if capacity.Cmp(resource.MustParse("2Gi")) != 0 {
		t.Fatalf("expected capacity to remain 2Gi for online resize scenario")
	}
}

func TestEnsureResize_QueuesRestartWhenFilesystemResizeStalls(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, time.January, 1, 12, 0, 0, 0, time.UTC)
	withNow(t, baseTime)

	cr := keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}}
	if err := json.Unmarshal([]byte(`{"storage":{"size":"2Gi"}}`), &cr.Spec); err != nil {
		t.Fatalf("unmarshal storage spec: %v", err)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-demo-0",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       "demo",
				UID:        types.UID("ssdemo"),
			}},
			Annotations: map[string]string{
				core.AnnotationPVCResizeTarget:      "2Gi",
				core.AnnotationPVCResizeRequestedAt: baseTime.Add(-resizeFilesystemGrace - time.Minute).Format(time.RFC3339Nano),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("2Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("2Gi"),
			},
			Conditions: []corev1.PersistentVolumeClaimCondition{{
				Type:               corev1.PersistentVolumeClaimFileSystemResizePending,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(baseTime.Add(-resizeFilesystemGrace - time.Minute)),
			}},
		},
	}

	c := fake.NewClientBuilder().WithObjects(pvc).Build()
	pods := []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "demo-0"}}}

	res, err := EnsureResize(ctx, c, &cr, pods, nil, logging.New(nil))
	if err != nil {
		t.Fatalf("EnsureResize unexpected error: %v", err)
	}
	if !res.Waiting {
		t.Fatalf("expected waiting when filesystem resize stalled")
	}
	reasons := res.PodReasons["demo-0"]
	if len(reasons) == 0 {
		t.Fatalf("expected restart reason when filesystem resize stalls")
	}
	found := false
	for _, r := range reasons {
		if r == ReasonResize {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected storage:resize reason, got %v", reasons)
	}
}

func TestEnsureResize_QueuesRestartWhenCapacityStalls(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, time.January, 1, 13, 0, 0, 0, time.UTC)
	withNow(t, baseTime)

	cr := keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}}
	if err := json.Unmarshal([]byte(`{"storage":{"size":"2Gi"}}`), &cr.Spec); err != nil {
		t.Fatalf("unmarshal storage spec: %v", err)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-demo-0",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       "demo",
				UID:        types.UID("ssdemo"),
			}},
			Annotations: map[string]string{
				core.AnnotationPVCResizeTarget:      "2Gi",
				core.AnnotationPVCResizeRequestedAt: baseTime.Add(-resizeCapacityGrace - time.Minute).Format(time.RFC3339Nano),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("2Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
		},
	}

	c := fake.NewClientBuilder().WithObjects(pvc).Build()
	pods := []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "demo-0"}}}

	res, err := EnsureResize(ctx, c, &cr, pods, nil, logging.New(nil))
	if err != nil {
		t.Fatalf("EnsureResize unexpected error: %v", err)
	}
	if !res.Waiting {
		t.Fatalf("expected waiting when capacity still below desired")
	}
	reasons := res.PodReasons["demo-0"]
	if len(reasons) == 0 {
		t.Fatalf("expected restart reason when capacity stalls")
	}
	found := false
	for _, r := range reasons {
		if r == ReasonResize {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected storage:resize reason, got %v", reasons)
	}
}

func TestResizeResultPreservesThrottleBackoff(t *testing.T) {
	res := ResizeResult{}
	applyThrottle(&res, "pvc-throttle", 5*time.Minute)
	markWaiting(&res, "pvc-normal", defaultRequeue)

	if !res.Waiting {
		t.Fatalf("expected waiting to be true")
	}
	if res.RequeueAfter != 5*time.Minute {
		t.Fatalf("expected requeue to stay at 5m, got %s", res.RequeueAfter)
	}
	if res.MinRequeueAfter != 5*time.Minute {
		t.Fatalf("expected minDelay to track throttle backoff")
	}
	if len(res.PendingPVCs) != 2 {
		t.Fatalf("expected both PVCs to be tracked, got %v", res.PendingPVCs)
	}
}

func TestResizeResultThrottleRaisesExistingDelay(t *testing.T) {
	res := ResizeResult{}
	markWaiting(&res, "pvc-normal", defaultRequeue)
	applyThrottle(&res, "pvc-throttle", 5*time.Minute)

	if res.RequeueAfter != 5*time.Minute {
		t.Fatalf("expected throttle to raise requeue to 5m, got %s", res.RequeueAfter)
	}
	if res.MinRequeueAfter != 5*time.Minute {
		t.Fatalf("expected minDelay to be 5m, got %s", res.MinRequeueAfter)
	}
}

func TestCleanupPVCs_DeletesOwnedClaims(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cr := keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}}

	pvcs := []*corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data-demo-0",
				Namespace: "default",
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "demo",
					UID:        types.UID("ssdemo"),
				}},
				Labels: map[string]string{
					core.LabelClusterKey: "demo",
					core.LabelAppKey:     core.AppLabelForName("demo"),
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data-demo-1",
				Namespace: "default",
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "demo",
					UID:        types.UID("ssdemo"),
				}},
				Labels: map[string]string{
					core.LabelClusterKey: "demo",
					core.LabelAppKey:     core.AppLabelForName("demo"),
				},
			},
		},
	}

	objs := make([]client.Object, 0, len(pvcs))
	for i := range pvcs {
		objs = append(objs, pvcs[i])
	}
	c := fake.NewClientBuilder().WithObjects(objs...).Build()

	res, err := CleanupPVCs(ctx, c, &cr, 0, nil, logging.New(nil))
	if err != nil {
		t.Fatalf("cleanup unexpected error: %v", err)
	}
	if len(res.Pending) != 2 {
		t.Fatalf("expected two pending pvc entries, got %v", res.Pending)
	}
	if len(res.Deleted) != 2 {
		t.Fatalf("expected two deleted entries, got %v", res.Deleted)
	}
	for _, name := range res.Deleted {
		if name != "data-demo-0" && name != "data-demo-1" {
			t.Fatalf("unexpected deleted pvc name %s", name)
		}
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "data-demo-0", Namespace: "default"}, &corev1.PersistentVolumeClaim{}); err == nil {
		t.Fatalf("pvc data-demo-0 still present after cleanup")
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected error fetching pvc: %v", err)
	}
}

func TestCleanupPVCs_RespectsLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cr := keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}}

	pvcs := []client.Object{}
	for i := 0; i < 4; i++ {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("data-demo-%d", i),
				Namespace: "default",
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "demo",
				}},
			},
		}
		pvcs = append(pvcs, pvc)
	}
	c := fake.NewClientBuilder().WithObjects(pvcs...).Build()

	res, err := CleanupPVCs(ctx, c, &cr, 2, nil, logging.New(nil))
	if err != nil {
		t.Fatalf("cleanup unexpected error: %v", err)
	}
	if len(res.Deleted) != 2 {
		t.Fatalf("expected two deletions, got %v", res.Deleted)
	}
	if len(res.Pending) != 4 {
		t.Fatalf("expected four pending entries, got %v", res.Pending)
	}
	for _, name := range []string{"data-demo-0", "data-demo-1"} {
		if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &corev1.PersistentVolumeClaim{}); err == nil {
			t.Fatalf("expected pvc %s to be deleted", name)
		} else if !apierrors.IsNotFound(err) {
			t.Fatalf("unexpected get error for pvc %s: %v", name, err)
		}
	}
	for _, name := range []string{"data-demo-2", "data-demo-3"} {
		if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &corev1.PersistentVolumeClaim{}); err != nil {
			if apierrors.IsNotFound(err) {
				t.Fatalf("pvc %s should still exist", name)
			}
			if err != nil {
				t.Fatalf("unexpected get error for pvc %s: %v", name, err)
			}
		}
	}
}
