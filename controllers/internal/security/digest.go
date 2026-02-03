package security

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	"github.com/ivelok/keyval-operator/controllers/logging"
)

// TLSDigest computes a stable hash for the TLS secret used by the cluster.
// It returns the digest string and a boolean indicating whether TLS is enabled and the secret was fetched.
func TLSDigest(ctx context.Context, reader client.Reader, cr *keyvalv1alpha1.KeyValCluster) (string, bool) {
	if cr == nil || cr.Spec.Security == nil || cr.Spec.Security.TLS == nil || !cr.Spec.Security.TLS.Enabled {
		return "", false
	}
	secretName := cr.Spec.Security.TLS.SecretName
	if secretName == "" {
		return "", false
	}
	var secret corev1.Secret
	if err := reader.Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: secretName}, &secret); err != nil {
		logger := logging.FromContext(ctx)
		if !logger.IsZero() {
			logger.V(1).Info("tls digest secret read failed", "error", controllererrors.WrapKubeAPI(err), "secret", secretName)
		}
		return "", false
	}
	h := sha256.New()
	keys := make([]string, 0, len(secret.Data))
	for k := range secret.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write(secret.Data[k])
	}
	return hex.EncodeToString(h.Sum(nil)), true
}
