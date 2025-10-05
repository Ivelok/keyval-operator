package runtime

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

// TLSSecretToClusterRequests enqueues clusters that reference the given TLS secret.
func TLSSecretToClusterRequests(ctx context.Context, obj client.Object, c client.Client) []reconcile.Request {
	secret, ok := obj.(*corev1.Secret)
	if !ok || c == nil {
		return nil
	}
	var list keyvalv1alpha1.KeyValClusterList
	if err := c.List(ctx, &list, client.InNamespace(secret.Namespace)); err != nil {
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		cluster := &list.Items[i]
		if cluster.Spec.Security == nil || cluster.Spec.Security.TLS == nil || !cluster.Spec.Security.TLS.Enabled {
			continue
		}
		if cluster.Spec.Security.TLS.SecretName != secret.Name {
			continue
		}
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}})
	}
	return reqs
}
