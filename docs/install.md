# Installing KeyVal Operator

This guide describes how to deploy the KeyVal operator using the Helm chart packaged in this repository and how to bootstrap example `KeyValCluster` resources.

## Prerequisites
- Kubernetes v1.26 or newer
- Helm v3.8+
- Access to a container registry for custom images (optional)
- `cosign` (optional, for verifying signed release images)

## 1. Add the Helm Repository
The release workflow publishes the chart as an OCI artifact. Authenticate (if required) and add it locally:

```bash
helm registry login ghcr.io -u <gh-username>
helm pull oci://ghcr.io/ivelok/keyval-operator/keyval-operator --version 0.1.1 --untar
```

Alternatively, package the chart locally during development:

```bash
make helm-package CHART_VERSION=0.1.1
```

## 2. Install the Operator
Install the chart into the `keyval-operator-system` namespace (create it if absent):

```bash
helm install keyval-operator oci://ghcr.io/ivelok/keyval-operator/keyval-operator \
  --namespace keyval-operator-system \
  --create-namespace \
  --version 0.1.1
```

Verify deployment:

```bash
kubectl -n keyval-operator-system get deploy keyval-operator-controller-manager
kubectl -n keyval-operator-system get pods
```

## 3. Configure Values
The chart exposes a comprehensive `values.yaml`. Common overrides:

| Section | Purpose | Example |
|---------|---------|---------|
| `image.repository`, `image.tag` | Use a custom controller image | `image.repository=ghcr.io/acme/keyval-operator` |
| `manager.replicas` | Increase controller HA | `manager.replicas=2` |
| `manager.metricsService.create` | Disable metrics service | `manager.metricsService.create=false` |
| `manager.podDisruptionBudget.enabled` | Enforce operator PDB | `manager.podDisruptionBudget.enabled=true` |
| `manager.watchNamespace` | Scope reconciliation to a single namespace (sets `WATCH_NAMESPACE`) | `manager.watchNamespace=apps` |
| `examples.enabled` | Deploy sample clusters | `examples.enabled=true` |
| `examples.clusters[].spec` | Provide ready-to-use `KeyValCluster` specs | See below |

Scoping the operator to a single namespace (via `manager.watchNamespace`) reduces informer load and ensures the controller only caches objects it needs. The chart wires this value into the controller’s `WATCH_NAMESPACE` environment variable automatically; leave it empty to keep the default cluster-wide view.

### TLS and Auth Example
```yaml
examples:
  enabled: true
  clusters:
    - name: kv-prod
      spec:
        mode: Sentinel
        redisReplicas: 5
        sentinelCount: 5
        storage:
          type: Persistent
          size: 200Gi
          storageClassName: fast-ssd
        security:
          auth:
            enabled: true
            username: sentinel
            passwordSecretRef:
              name: kv-auth
              key: password
          tls:
            enabled: true
            secretName: kv-tls
            requireClientAuth: true
```
Apply the release and confirm sample clusters are created:

```bash
kubectl -n keyval-operator-system get keyvalclusters
```

## 4. Upgrading the Chart
To upgrade in place:

```bash
helm upgrade keyval-operator oci://ghcr.io/ivelok/keyval-operator/keyval-operator \
  --namespace keyval-operator-system \
  --version <new-version> \
  -f my-values.yaml
```

Review the runbooks (`docs/runbook/`) for safe scaling and upgrade procedures prior to applying disruptive changes.

## 5. Uninstall
Remove all `KeyValCluster` resources first, then uninstall the chart:

```bash
kubectl delete keyvalclusters.keyval.ivelok.io --all-namespaces --all
helm uninstall keyval-operator --namespace keyval-operator-system
```

## Verification Checklist
- [ ] Operator deployment Ready replicas match desired count
- [ ] CRDs present: `kubectl get crd keyvalclusters.keyval.ivelok.io`
- [ ] Metrics endpoint accessible (default `keyval-operator-controller` port 8080)
- [ ] Example clusters (if enabled) report `Available=True`

For troubleshooting refer to the operational runbooks and observability guide.
