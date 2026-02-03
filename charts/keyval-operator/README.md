# keyval-operator Helm Chart

## Usage
This chart packages the KeyVal operator controller. Use a values file or `--set` to override defaults.

```bash
helm install keyval-operator oci://ghcr.io/ivelok/keyval-operator/keyval-operator \
  --version <chart-version> \
  --namespace keyval-operator-system \
  --create-namespace
```

## Values
Key values (see `values.yaml` for the full list):

| Value | Description | Default |
|-------|-------------|---------|
| `image.repository` | Controller image repository. | `ghcr.io/ivelok/keyval-operator` |
| `image.tag` | Controller image tag. | `""` |
| `manager.replicas` | Operator deployment replicas. | `1` |
| `manager.resources.requests.cpu` | CPU request for the controller. | `100m` |
| `manager.resources.requests.memory` | Memory request for the controller. | `128Mi` |
| `manager.resources.limits.cpu` | CPU limit for the controller. | `500m` |
| `manager.resources.limits.memory` | Memory limit for the controller. | `512Mi` |
| `manager.metricsService.create` | Expose metrics Service. | `true` |
| `examples.enabled` | Deploy sample KeyValCluster resources. | `false` |
