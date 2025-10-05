package security

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

// Settings aggregates authentication and TLS settings resolved from the cluster spec.
type Settings struct {
	Auth AuthSettings
	TLS  TLSSettings
}

// AuthSettings represents resolved authentication configuration.
type AuthSettings struct {
	Enabled         bool
	Username        string
	Password        string
	SecretName      string
	SecretKey       string
	ResourceVersion string
}

// TLSSettings represents resolved TLS configuration.
type TLSSettings struct {
	Enabled           bool
	SecretName        string
	CACertKey         string
	CertKey           string
	KeyKey            string
	RequireClientAuth bool
	DisablePlaintext  bool
	ResourceVersion   string
	caBundle          []byte
	clientCertificate *tls.Certificate
}

// HasAuth reports whether authentication is enabled.
func (s Settings) HasAuth() bool { return s.Auth.Enabled }

// HasTLS reports whether TLS is enabled.
func (s Settings) HasTLS() bool { return s.TLS.Enabled }

// ClientTLSConfig builds a tls.Config suitable for Redis/Sentinel clients.
func (s Settings) ClientTLSConfig() (*tls.Config, error) {
	if !s.TLS.Enabled {
		return nil, nil
	}
	if len(s.TLS.caBundle) == 0 {
		return nil, errors.New("tls enabled but ca bundle empty")
	}
	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(s.TLS.caBundle); !ok {
		return nil, errors.New("failed to parse TLS CA bundle")
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
	}
	if s.TLS.RequireClientAuth && s.TLS.clientCertificate != nil {
		cfg.Certificates = []tls.Certificate{*s.TLS.clientCertificate}
	}
	return cfg, nil
}

// FromSpec resolves secrets and builds Settings for the given KeyValCluster.
func FromSpec(ctx context.Context, reader client.Reader, cr *keyvalv1alpha1.KeyValCluster) (Settings, error) {
	var out Settings
	if cr == nil {
		return out, errors.New("cluster is nil")
	}
	if cr.Spec.Security == nil {
		return out, nil
	}
	if authSpec := cr.Spec.Security.Auth; authSpec != nil && authSpec.Enabled {
		if authSpec.PasswordSecretRef == nil {
			return out, fmt.Errorf("auth enabled but passwordSecretRef not set")
		}
		secretName := authSpec.PasswordSecretRef.Name
		if secretName == "" {
			return out, fmt.Errorf("auth passwordSecretRef missing name")
		}
		var secret corev1.Secret
		if err := reader.Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: secretName}, &secret); err != nil {
			return out, fmt.Errorf("get auth secret %q: %w", secretName, err)
		}
		key := authSpec.PasswordSecretRef.Key
		if key == "" {
			return out, fmt.Errorf("auth passwordSecretRef missing key")
		}
		val, ok := secret.Data[key]
		if !ok {
			return out, fmt.Errorf("auth secret %q missing key %q", secretName, key)
		}
		out.Auth = AuthSettings{
			Enabled:         true,
			Username:        authSpec.Username,
			Password:        string(val),
			SecretName:      secretName,
			SecretKey:       key,
			ResourceVersion: secret.ResourceVersion,
		}
	}
	if tlsSpec := cr.Spec.Security.TLS; tlsSpec != nil && tlsSpec.Enabled {
		if tlsSpec.SecretName == "" {
			return out, fmt.Errorf("tls enabled but secretName not set")
		}
		var secret corev1.Secret
		if err := reader.Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: tlsSpec.SecretName}, &secret); err != nil {
			return out, fmt.Errorf("get tls secret %q: %w", tlsSpec.SecretName, err)
		}
		caKey := tlsSpec.CACertKey
		if caKey == "" {
			caKey = "ca.crt"
		}
		certKey := tlsSpec.CertKey
		if certKey == "" {
			certKey = "tls.crt"
		}
		keyKey := tlsSpec.KeyKey
		if keyKey == "" {
			keyKey = "tls.key"
		}
		caData, ok := secret.Data[caKey]
		if !ok || len(caData) == 0 {
			return out, fmt.Errorf("tls secret %q missing key %q", tlsSpec.SecretName, caKey)
		}
		certData, ok := secret.Data[certKey]
		if !ok || len(certData) == 0 {
			return out, fmt.Errorf("tls secret %q missing key %q", tlsSpec.SecretName, certKey)
		}
		keyData, ok := secret.Data[keyKey]
		if !ok || len(keyData) == 0 {
			return out, fmt.Errorf("tls secret %q missing key %q", tlsSpec.SecretName, keyKey)
		}
		tlsSettings := TLSSettings{
			Enabled:           true,
			SecretName:        tlsSpec.SecretName,
			CACertKey:         caKey,
			CertKey:           certKey,
			KeyKey:            keyKey,
			RequireClientAuth: tlsSpec.RequireClientAuth != nil && *tlsSpec.RequireClientAuth,
			DisablePlaintext:  tlsSpec.DisablePlaintext != nil && *tlsSpec.DisablePlaintext,
			ResourceVersion:   secret.ResourceVersion,
			caBundle:          cloneBytes(caData),
		}
		if tlsSettings.RequireClientAuth {
			cert, err := tls.X509KeyPair(certData, keyData)
			if err != nil {
				return out, fmt.Errorf("parse tls client certificate: %w", err)
			}
			tlsSettings.clientCertificate = &cert
		}
		out.TLS = tlsSettings
	}
	return out, nil
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

// ResolveAuth returns the username/password pair that should be used when connecting to Redis
// for the given cluster. When security settings disable auth it falls back to spec.redisConfig
// entries such as requirepass/masterauth to preserve backward compatibility.
func ResolveAuth(cr *keyvalv1alpha1.KeyValCluster, sec *Settings) (string, string) {
	if sec != nil && sec.Auth.Enabled {
		return sec.Auth.Username, sec.Auth.Password
	}
	if cr != nil && cr.Spec.RedisConfig != nil {
		if v := cr.Spec.RedisConfig["requirepass"]; v != "" {
			return "", v
		}
		if v := cr.Spec.RedisConfig["masterauth"]; v != "" {
			return "", v
		}
	}
	return "", ""
}
