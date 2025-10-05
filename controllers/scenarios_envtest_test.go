//go:build envtest
// +build envtest

package controllers

import (
	"context"
	"os"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	labels2 "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
)

func newEnvMgr(t *testing.T) (*envtest.Environment, ctrl.Manager, context.Context, context.CancelFunc) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest")
	}
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../config/crd/bases"}}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("env start: %v", err)
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	r := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
	})
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatalf("setup: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = mgr.Start(ctx) }()
	return env, mgr, ctx, cancel
}

func ensureNS(t *testing.T, mgr ctrl.Manager, ns string) {
	_ = mgr.GetClient().Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
}

func createCR(t *testing.T, mgr ctrl.Manager, name string, mode keyvalv1alpha1.Mode, replicas int32) *keyvalv1alpha1.KeyValCluster {
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}, Spec: keyvalv1alpha1.KeyValClusterSpec{Mode: mode, Image: "valkey/valkey:7.2", RedisReplicas: replicas}}
	if err := mgr.GetClient().Create(context.Background(), cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}
	return cr
}

func createPodObj(name string, labels map[string]string, anns map[string]string, ready bool) *corev1.Pod {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels, Annotations: anns}}
	if ready {
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	}
	return p
}

func mustGetSS(t *testing.T, mgr ctrl.Manager, name string) *appsv1.StatefulSet {
	var ss appsv1.StatefulSet
	mustEventually(t, 15*time.Second, func() bool {
		return mgr.GetClient().Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, &ss) == nil
	})
	return &ss
}

func TestEnv_RoleLabelInvariant_OnStart(t *testing.T) {
	env, mgr, ctx, cancel := newEnvMgr(t)
	defer func() { cancel(); _ = env.Stop() }()
	ensureNS(t, mgr, "default")
	cr := createCR(t, mgr, "demo", keyvalv1alpha1.ModeStandalone, 2)

	// ensure statefulset exists so selector is known
	_ = mustGetSS(t, mgr, "demo")

	// create two pods both labeled master -> controller should demote one
	app := core.AppLabel(cr)
	base := map[string]string{core.LabelAppKey: app, core.LabelClusterKey: cr.Name}
	p0 := createPodObj("demo-0", map[string]string{core.LabelAppKey: app, core.LabelClusterKey: cr.Name, core.RoleLabelKey: string(keyvalv1alpha1.PodRoleMaster)}, nil, true)
	p1 := createPodObj("demo-1", map[string]string{core.LabelAppKey: app, core.LabelClusterKey: cr.Name, core.RoleLabelKey: string(keyvalv1alpha1.PodRoleMaster)}, nil, true)
	if err := mgr.GetClient().Create(ctx, p0); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	if err := mgr.GetClient().Create(ctx, p1); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	// wait until exactly one master remains
	mustEventually(t, 15*time.Second, func() bool {
		var list corev1.PodList
		sel := labels.SelectorFromSet(map[string]string{core.LabelAppKey: base[core.LabelAppKey], core.RoleLabelKey: string(keyvalv1alpha1.PodRoleMaster)})
		if err := mgr.GetClient().List(ctx, &list, &client.ListOptions{Namespace: "default", LabelSelector: sel}); err != nil {
			return false
		}
		return len(list.Items) == 1
	})
}

func TestEnv_RollingUpdate_OnePodAtATime(t *testing.T) {
	env, mgr, ctx, cancel := newEnvMgr(t)
	defer func() { cancel(); _ = env.Stop() }()
	ensureNS(t, mgr, "default")
	_ = createCR(t, mgr, "demo", keyvalv1alpha1.ModeStandalone, 2)
	ss := mustGetSS(t, mgr, "demo")
	desiredHash := ss.Spec.Template.Annotations[resources.ConfigHashAnnotationKey]
	// create two pods with wrong hash -> controller should delete one, then the other after replacement
	labels := map[string]string{core.LabelAppKey: core.AppLabel(&keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}), core.LabelClusterKey: "demo", core.RoleLabelKey: string(keyvalv1alpha1.PodRoleReplica)}
	bad := map[string]string{resources.ConfigHashAnnotationKey: "bad"}
	p0 := createPodObj("demo-0", labels, bad, true)
	p1 := createPodObj("demo-1", labels, bad, true)
	_ = mgr.GetClient().Create(ctx, p0)
	_ = mgr.GetClient().Create(ctx, p1)
	// wait until one is deleted
	mustEventually(t, 20*time.Second, func() bool {
		var list corev1.PodList
		sel := labels2.SelectorFromSet(map[string]string{core.LabelAppKey: labels[core.LabelAppKey], core.LabelClusterKey: "demo"})
		_ = mgr.GetClient().List(ctx, &list, &client.ListOptions{Namespace: "default", LabelSelector: sel})
		return len(list.Items) == 1
	})
	// recreate missing pod with good hash
	var remaining corev1.PodList
	_ = mgr.GetClient().List(ctx, &remaining, &client.ListOptions{Namespace: "default", LabelSelector: labels2.SelectorFromSet(map[string]string{core.LabelAppKey: labels[core.LabelAppKey], core.LabelClusterKey: "demo"})})
	missing := "demo-0"
	if len(remaining.Items) == 1 && remaining.Items[0].Name == "demo-0" {
		missing = "demo-1"
	}
	good := map[string]string{resources.ConfigHashAnnotationKey: desiredHash}
	_ = mgr.GetClient().Create(ctx, createPodObj(missing, labels, good, true))
	// expect the other to be deleted next
	mustEventually(t, 20*time.Second, func() bool {
		var list corev1.PodList
		_ = mgr.GetClient().List(ctx, &list, &client.ListOptions{Namespace: "default", LabelSelector: labels2.SelectorFromSet(map[string]string{core.LabelAppKey: labels[core.LabelAppKey], core.LabelClusterKey: "demo"})})
		return len(list.Items) == 1 && list.Items[0].Name == missing
	})
}

func TestEnv_ScaleDown_DeletesHighestOrdinal(t *testing.T) {
	env, mgr, ctx, cancel := newEnvMgr(t)
	defer func() { cancel(); _ = env.Stop() }()
	ensureNS(t, mgr, "default")
	cr := createCR(t, mgr, "demo", keyvalv1alpha1.ModeStandalone, 3)
	_ = mustGetSS(t, mgr, "demo")
	labels := map[string]string{core.LabelAppKey: core.AppLabel(cr), core.LabelClusterKey: cr.Name, core.RoleLabelKey: string(keyvalv1alpha1.PodRoleReplica)}
	// create three pods
	_ = mgr.GetClient().Create(ctx, createPodObj("demo-0", labels, nil, true))
	_ = mgr.GetClient().Create(ctx, createPodObj("demo-1", labels, nil, true))
	_ = mgr.GetClient().Create(ctx, createPodObj("demo-2", labels, nil, true))
	// scale down to 1
	var cur keyvalv1alpha1.KeyValCluster
	_ = mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}, &cur)
	base := cur.DeepCopy()
	cur.Spec.RedisReplicas = 1
	if err := mgr.GetClient().Patch(ctx, &cur, client.MergeFrom(base)); err != nil {
		t.Fatalf("patch replicas: %v", err)
	}
	// expect demo-2 is deleted first
	mustEventually(t, 20*time.Second, func() bool {
		var list corev1.PodList
		_ = mgr.GetClient().List(ctx, &list, &client.ListOptions{Namespace: "default", LabelSelector: labels2.SelectorFromSet(map[string]string{core.LabelAppKey: labels[core.LabelAppKey], core.LabelClusterKey: cr.Name})})
		for _, p := range list.Items {
			if p.Name == "demo-2" {
				return false
			}
		}
		return len(list.Items) <= 2
	})
}
