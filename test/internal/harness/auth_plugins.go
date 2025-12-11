//go:build e2e || chaos

package harness

// Import auth plugins needed for kubeconfigs that use auth-provider blocks.
// client-go keeps them behind side-effect imports, so pull in OIDC explicitly.
import _ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
