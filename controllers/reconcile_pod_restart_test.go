package controllers

import (
	"context"
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	"github.com/ivelok/keyval-operator/controllers/internal/security"
)

func TestReconcile_PodRestart_ReattachesReplica(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = keyvalv1alpha1.AddToScheme(scheme)

	cr := &keyvalv1alpha1.KeyValCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       keyvalv1alpha1.KeyValClusterSpec{Mode: keyvalv1alpha1.ModeStandalone, Image: "valkey/valkey:7.2", RedisReplicas: 2},
	}
	sec := &security.Settings{}
	hash := resources.ConfigHash(cr, sec)
	podSpec := resources.StatefulSet(cr, "", "", sec).Spec.Template.Spec
	desiredConfig := resources.EffectiveRedisConfig(cr, sec)

	masterPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-0",
			Namespace: "default",
			Labels: map[string]string{
				core.LabelAppKey:     core.AppLabel(cr),
				core.LabelClusterKey: cr.Name,
				core.RoleLabelKey:    string(keyvalv1alpha1.PodRoleMaster),
			},
			Annotations: map[string]string{resources.ConfigHashAnnotationKey: hash},
		},
		Spec: podSpec,
		Status: corev1.PodStatus{
			PodIP:      "10.0.0.1",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}

	restarted := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-1",
			Namespace: "default",
			Labels: map[string]string{
				core.LabelAppKey:     core.AppLabel(cr),
				core.LabelClusterKey: cr.Name,
				core.RoleLabelKey:    string(keyvalv1alpha1.PodRoleReplica),
			},
			Annotations: map[string]string{resources.ConfigHashAnnotationKey: hash},
		},
		Spec:   podSpec,
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}

	// Fake statefulset to satisfy ListStatefulSetPods selector logic.
	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{core.LabelAppKey: core.AppLabel(cr), core.LabelClusterKey: cr.Name}},
		},
	}

	baseClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&keyvalv1alpha1.KeyValCluster{}, &appsv1.StatefulSet{}, &corev1.Pod{}).
		WithObjects(cr, masterPod.DeepCopy(), restarted.DeepCopy(), ss).
		Build()

	client := &applyAwareClient{Client: baseClient}

	factory := &trackingFactory{clients: map[string]*trackingClient{
		"demo-0": {name: "demo-0", role: "master", masterHost: "", masterPort: 0, config: cloneStringMap(desiredConfig)},
		"demo-1": {name: "demo-1", role: "master", masterHost: "", masterPort: 0, connectErr: fmt.Errorf("not ready"), config: cloneStringMap(desiredConfig)},
	}}

	r := NewKeyValClusterReconciler(ReconcilerDependencies{
		Client:        client,
		APIReader:     client,
		Scheme:        scheme,
		ClientFactory: factory,
	})
	ctx := context.Background()
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}})
	if err != nil {
		t.Fatalf("first reconcile err: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("expected requeue when replica is not ready, got %s", res.RequeueAfter)
	}
	if len(factory.clients["demo-1"].replicaOfCalls) != 0 {
		t.Fatalf("replicaOf should not be called before pod becomes ready")
	}

	// Mark restarted pod as ready and allow connections.
	var podList corev1.PodList
	if err := client.List(ctx, &podList); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(podList.Items) == 0 {
		t.Fatalf("no pods remaining after first reconcile")
	}
	if err := client.Create(ctx, restarted.DeepCopy()); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("recreate restarted pod: %v", err)
	}
	var readyPod corev1.Pod
	if err := client.Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo-1"}, &readyPod); err != nil {
		t.Fatalf("get restarted pod: %v", err)
	}
	readyPod.Status.PodIP = "10.0.0.2"
	readyPod.Status.Phase = corev1.PodRunning
	readyPod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := client.Status().Update(ctx, &readyPod); err != nil {
		t.Fatalf("status update: %v", err)
	}
	factory.setState("demo-1", "master", nil)

	// Second reconcile should attach replica and clear requeue.
	res, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"}})
	if err != nil {
		t.Fatalf("second reconcile err: %v", err)
	}
	if res.RequeueAfter != stableRequeueInterval {
		t.Fatalf("expected periodic requeue of %s once pod ready, got %s", stableRequeueInterval, res.RequeueAfter)
	}
	calls := factory.clients["demo-1"].replicaOfCalls
	if len(calls) == 0 {
		t.Fatalf("expected ReplicaOf invoked after readiness")
	}
	if calls[0] != "10.0.0.1:6379" {
		t.Fatalf("unexpected ReplicaOf target: %v", calls)
	}
}

type applyAwareClient struct{ client.Client }

func (a *applyAwareClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if patch.Type() == types.ApplyPatchType {
		copyObj := obj.DeepCopyObject().(client.Object)
		key := client.ObjectKeyFromObject(obj)
		err := a.Client.Get(ctx, key, copyObj)
		if apierrors.IsNotFound(err) {
			return a.Client.Create(ctx, obj)
		}
		if err != nil {
			return err
		}
		obj.SetResourceVersion(copyObj.GetResourceVersion())
		return a.Client.Update(ctx, obj)
	}
	return a.Client.Patch(ctx, obj, patch, opts...)
}

type trackingFactory struct {
	clients map[string]*trackingClient
}

func (f *trackingFactory) ForPod(ctx context.Context, pod corev1.Pod, _ ClientOptions) (Client, error) {
	cl, ok := f.clients[pod.Name]
	if !ok {
		return nil, fmt.Errorf("no client for pod %s", pod.Name)
	}
	if cl.connectErr != nil {
		return nil, cl.connectErr
	}
	return cl, nil
}

func (f *trackingFactory) setState(name, role string, err error) {
	if c, ok := f.clients[name]; ok {
		c.role = role
		c.connectErr = err
	}
}

type trackingClient struct {
	name           string
	role           string
	masterHost     string
	masterPort     int
	connectErr     error
	replicaOfCalls []string
	config         map[string]string
}

func (c *trackingClient) Role(ctx context.Context) (string, error) {
	if c.connectErr != nil {
		return "", c.connectErr
	}
	return c.role, nil
}

func (c *trackingClient) ReplicationInfo(ctx context.Context) (ReplicationInfo, error) {
	if c.connectErr != nil {
		return ReplicationInfo{}, c.connectErr
	}
	return ReplicationInfo{Role: c.role, MasterHost: c.masterHost, MasterPort: c.masterPort, MasterLinkStatus: "up"}, nil
}

func (c *trackingClient) ReplicaOf(ctx context.Context, host string, port int) error {
	if c.connectErr != nil {
		return c.connectErr
	}
	c.replicaOfCalls = append(c.replicaOfCalls, fmt.Sprintf("%s:%d", host, port))
	c.role = "replica"
	c.masterHost = host
	c.masterPort = port
	return nil
}

func (c *trackingClient) NoOne(ctx context.Context) error {
	if c.connectErr != nil {
		return c.connectErr
	}
	c.role = "master"
	c.masterHost = ""
	c.masterPort = 0
	return nil
}

func (c *trackingClient) ResetSentinel(ctx context.Context, cr *keyvalv1alpha1.KeyValCluster) error {
	return nil
}

func (c *trackingClient) ConfigGet(ctx context.Context, parameter string) (string, bool, error) {
	if c.config == nil {
		return "", false, nil
	}
	v, ok := c.config[parameter]
	return v, ok, nil
}

func (c *trackingClient) ConfigSet(ctx context.Context, parameter, value string) error {
	if c.config == nil {
		c.config = map[string]string{}
	}
	c.config[parameter] = value
	return nil
}

func (c *trackingClient) ConfigRewrite(ctx context.Context) error { return nil }

func (c *trackingClient) Auth(context.Context, string, string) error { return nil }
func (c *trackingClient) DBSize(context.Context) (int64, error)      { return 0, nil }

// ensure trackingFactory satisfies ClientFactory.
var _ ClientFactory = (*trackingFactory)(nil)

// ensure trackingClient satisfies Client.
var _ Client = (*trackingClient)(nil)
