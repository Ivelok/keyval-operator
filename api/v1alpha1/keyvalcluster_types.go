package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Mode represents the KeyValCluster operating mode.
// +kubebuilder:validation:Enum=Standalone;Sentinel
type Mode string

const (
	ModeStandalone Mode = "Standalone"
	ModeSentinel   Mode = "Sentinel"
)

// Engine represents the KeyValCluster database engine.
// +kubebuilder:validation:Enum=Redis;Valkey
type Engine string

const (
	EngineRedis  Engine = "Redis"
	EngineValkey Engine = "Valkey"
)

// +k8s:deepcopy-gen=true

// KeyValClusterSpec defines the desired state of KeyValCluster.
// Validation notes:
// - Standalone: redisReplicas must equal 1; sentinelCount and sentinelConfig must be omitted.
// - Sentinel: redisReplicas >= 3; sentinelCount present, odd, and >= 3.
// +kubebuilder:validation:XValidation:rule="self.mode == 'Standalone' ? self.redisReplicas == 1 : true",message="Standalone mode requires spec.redisReplicas == 1"
// +kubebuilder:validation:XValidation:rule="self.mode == 'Standalone' ? !has(self.sentinelCount) : true",message="sentinelCount must be omitted in Standalone mode"
// +kubebuilder:validation:XValidation:rule="self.mode == 'Standalone' ? !has(self.sentinelConfig) : true",message="sentinelConfig must be omitted in Standalone mode"
// +kubebuilder:validation:XValidation:rule="self.mode == 'Sentinel' ? self.redisReplicas >= 3 : true",message="Sentinel mode requires spec.redisReplicas >= 3"
// +kubebuilder:validation:XValidation:rule="self.mode == 'Sentinel' ? has(self.sentinelCount) : true",message="Sentinel mode requires spec.sentinelCount"
// +kubebuilder:validation:XValidation:rule="self.mode == 'Sentinel' ? self.sentinelCount >= 3 : true",message="sentinelCount must be >= 3 in Sentinel mode"
// +kubebuilder:validation:XValidation:rule="self.mode == 'Sentinel' ? (self.sentinelCount % 2 == 1) : true",message="sentinelCount must be odd in Sentinel mode"
type KeyValClusterSpec struct {
	// mode selects the operating mode.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="mode is immutable after creation"
	Mode Mode `json:"mode"`

	// engine selects the database engine when image is not explicitly set.
	// Defaults to Valkey for lighter images.
	// +kubebuilder:default=Valkey
	// +optional
	Engine Engine `json:"engine,omitempty"`

	// image is the container image (Redis/Valkey 7+), e.g. "valkey/valkey:7.2".
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// sentinelImage optionally overrides the image used for the Sentinel sidecar in Sentinel mode.
	// If omitted, the operator may reuse `image` for the sidecar.
	// +optional
	SentinelImage *string `json:"sentinelImage,omitempty"`

	// redisReplicas is the number of Redis Pods managed via StatefulSet.
	// Standalone: must be 1. Sentinel: must be >=3.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	RedisReplicas int32 `json:"redisReplicas"`

	// sentinelCount is the number of Sentinel sidecars/Pods participating in quorum.
	// Required in Sentinel mode; must be odd and >=3. Omit in Standalone mode.
	// +kubebuilder:validation:Minimum=3
	// Note: precise constraints are enforced by XValidation above.
	// +optional
	SentinelCount *int32 `json:"sentinelCount,omitempty"`

	// resources defines Pod resource requests/limits for the Redis container (and Sentinel sidecar when enabled).
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// sentinelResources defines Pod resource requests/limits for Sentinel pods.
	// If omitted, falls back to Resources.
	// +optional
	SentinelResources *corev1.ResourceRequirements `json:"sentinelResources,omitempty"`

	// storage configures data persistence for Redis.
	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`

	// redisConfig is a key/value map rendered into redis.conf.
	// Example: {"appendonly":"yes","protected-mode":"no"}
	// +optional
	RedisConfig map[string]string `json:"redisConfig,omitempty"`

	// sentinelConfig is a key/value map rendered into sentinel.conf (Sentinel mode only).
	// +optional
	SentinelConfig map[string]string `json:"sentinelConfig,omitempty"`

	// service configures the master Service (<cr>-master) and headless Service (<cr>-headless) options.
	// +optional
	Service *ServiceSpec `json:"service,omitempty"`

	// sentinelService configures the Sentinel Service (<cr>-sentinel) when mode=Sentinel.
	// +optional
	SentinelService *ServiceSpec `json:"sentinelService,omitempty"`

	// replicasService configures the read-replicas Service (<cr>-replicas).
	// +optional
	ReplicasService *ServiceSpec `json:"replicasService,omitempty"`

	// sentinelPod allows customizing the sentinel Pod template.
	// +optional
	SentinelPod *SentinelPodSpec `json:"sentinelPod,omitempty"`

	// sentinelPDB specifies a PodDisruptionBudget for sentinel pods.
	// +optional
	SentinelPDB *SentinelPDB `json:"sentinelPDB,omitempty"`

	// podLabels are merged into the StatefulSet Pod template labels.
	// +optional
	PodLabels map[string]string `json:"podLabels,omitempty"`

	// podAnnotations are merged into the StatefulSet Pod template annotations.
	// +optional
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// replicationHealth tunes Redis replication health thresholds used by the operator.
	// Deprecated: use spec.health instead. Retained for backward compatibility until v1beta1.
	// +optional
	ReplicationHealth *ReplicationHealthSpec `json:"replicationHealth,omitempty"`

	// health configures thresholds that influence the cluster availability gates.
	// +optional
	Health *HealthSpec `json:"health,omitempty"`

	// security configures authentication, TLS, and related hardening options.
	// +optional
	Security *SecuritySpec `json:"security,omitempty"`

	// bootstrap configures optional data seeding before the cluster becomes primary.
	// +optional
	Bootstrap *BootstrapSpec `json:"bootstrap,omitempty"`

	// topology configures how Redis and Sentinel pods are spread across the cluster.
	// When omitted, the operator applies safe defaults that prefer even distribution across nodes
	// while still allowing scheduling on single-node clusters.
	// +optional
	Topology *TopologySpec `json:"topology,omitempty"`
}

// ReplicationHealthSpec defines thresholds for interpreting Redis health metrics.
type ReplicationHealthSpec struct {
	// lagThresholdSeconds is the maximum acceptable master_last_io_seconds_ago before
	// a replica is considered lagging.
	// +kubebuilder:default=5
	// +kubebuilder:validation:Minimum=0
	// +optional
	LagThresholdSeconds int32 `json:"lagThresholdSeconds,omitempty"`
}

// HealthSpec defines health gating thresholds for the cluster.
type HealthSpec struct {
	// replicationLagSecondsMax is the maximum acceptable lag for replicas to be
	// considered healthy.
	// +kubebuilder:default=5
	// +kubebuilder:validation:Minimum=0
	// +optional
	ReplicationLagSecondsMax int32 `json:"replicationLagSecondsMax,omitempty"`

	// failoverTimeoutSeconds controls how long the operator tolerates a
	// failover sequence before marking it as stalled.
	// +kubebuilder:default=20
	// +kubebuilder:validation:Minimum=0
	// +optional
	FailoverTimeoutSeconds int32 `json:"failoverTimeoutSeconds,omitempty"`

	// minReplicasForSafety is the minimum number of healthy Redis members
	// (master + replicas) required before voluntary disruptions are allowed.
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	// +optional
	MinReplicasForSafety int32 `json:"minReplicasForSafety,omitempty"`
}

// SecuritySpec defines authentication and TLS settings applied to Redis and Sentinel components.
type SecuritySpec struct {
	// auth enables Redis AUTH/ACL support.
	// +optional
	Auth *AuthSpec `json:"auth,omitempty"`

	// tls configures Redis and Sentinel TLS settings.
	// +optional
	TLS *TLSSpec `json:"tls,omitempty"`
}

// BootstrapSpec defines bootstrap-time configuration.
type BootstrapSpec struct {
	// externalSource instructs the operator to import data from a remote Redis/Valkey deployment
	// before promoting local pods to master.
	// +optional
	ExternalSource *ExternalSourceSpec `json:"externalSource,omitempty"`
}

// ExternalSourceSpec configures bootstrap import from an external Redis/Valkey instance.
// +kubebuilder:validation:XValidation:rule="self.address != \"\"",message="address must be set"
// +kubebuilder:validation:XValidation:rule="!(has(self.tls) && self.tls.enabled == true) || has(self.tls.caBundleSecretRef)",message="tls.caBundleSecretRef must be set when TLS is enabled"
type ExternalSourceSpec struct {
	// address is the Redis/Valkey endpoint to import data from. Supports redis:// and rediss:// URIs.
	// +kubebuilder:validation:Pattern=`^(redis|rediss)://.+$`
	Address string `json:"address"`

	// auth references optional credentials used when connecting to the source instance.
	// +optional
	Auth *ExternalSourceAuthSpec `json:"auth,omitempty"`

	// tls configures TLS settings for the connection.
	// +optional
	TLS *ExternalSourceTLSSpec `json:"tls,omitempty"`

	// syncMode controls how the operator synchronises data from the external source.
	// +kubebuilder:default=Snapshot
	// +optional
	SyncMode ExternalSourceSyncMode `json:"syncMode,omitempty"`

	// abortIfExistingData stops the import if the local cluster already has keys.
	// Defaults to true to avoid overwriting data accidentally.
	// +kubebuilder:default=true
	// +optional
	AbortIfExistingData *bool `json:"abortIfExistingData,omitempty"`

	// maxInitialSyncDuration bounds how long the operator waits for the initial data copy to complete.
	// +kubebuilder:default="30m"
	// +optional
	MaxInitialSyncDuration *metav1.Duration `json:"maxInitialSyncDuration,omitempty"`
}

// ExternalSourceAuthSpec points to the secret data required to authenticate against the source cluster.
type ExternalSourceAuthSpec struct {
	// usernameSecretRef points to the secret containing the ACL username (optional).
	// +optional
	UsernameSecretRef *corev1.SecretKeySelector `json:"usernameSecretRef,omitempty"`

	// passwordSecretRef points to the secret containing the ACL password/token (optional).
	// +optional
	PasswordSecretRef *corev1.SecretKeySelector `json:"passwordSecretRef,omitempty"`
}

// ExternalSourceTLSSpec configures TLS materials for connecting to the external source.
type ExternalSourceTLSSpec struct {
	// enabled toggles TLS for the remote connection. When using a rediss:// address, this is implied.
	Enabled bool `json:"enabled"`

	// caBundleSecretRef references the secret key containing the trusted CA bundle.
	// +optional
	CABundleSecretRef *corev1.SecretKeySelector `json:"caBundleSecretRef,omitempty"`

	// clientCertSecretRef references the secret key containing the optional client certificate.
	// +optional
	ClientCertSecretRef *corev1.SecretKeySelector `json:"clientCertSecretRef,omitempty"`

	// clientKeySecretRef references the secret key containing the optional client private key.
	// +optional
	ClientKeySecretRef *corev1.SecretKeySelector `json:"clientKeySecretRef,omitempty"`

	// insecureSkipVerify disables server certificate verification (not recommended).
	// +optional
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`
}

// ExternalSourceSyncMode enumerates the external import options.
// +kubebuilder:validation:Enum=Snapshot;Live
type ExternalSourceSyncMode string

const (
	// ExternalSourceSyncModeSnapshot copies data once and then breaks the link.
	ExternalSourceSyncModeSnapshot ExternalSourceSyncMode = "Snapshot"
	// ExternalSourceSyncModeLive keeps a temporary replica link until the operator cuts over.
	ExternalSourceSyncModeLive ExternalSourceSyncMode = "Live"
)

// TopologySpec configures pod distribution mechanisms such as topology spread constraints and pod anti-affinity.
type TopologySpec struct {
	// spread controls topologySpreadConstraints applied to Redis and Sentinel pods.
	// +optional
	Spread *TopologySpreadSpec `json:"spread,omitempty"`

	// antiAffinity controls the generated podAntiAffinity rules.
	// +optional
	AntiAffinity *TopologyAntiAffinitySpec `json:"antiAffinity,omitempty"`
}

// TopologySpreadSpec configures generated topologySpreadConstraints.
type TopologySpreadSpec struct {
	// disabled disables operator-provided topologySpreadConstraints entirely.
	// +optional
	Disabled bool `json:"disabled,omitempty"`

	// maxSkew controls the allowable skew across topology domains. Defaults to 1 when unset.
	// +optional
	MaxSkew *int32 `json:"maxSkew,omitempty"`

	// topologyKeys is the ordered list of topology keys to balance against. Defaults to zone and hostname.
	// +kubebuilder:validation:MaxItems=4
	// +optional
	TopologyKeys []string `json:"topologyKeys,omitempty"`

	// whenUnsatisfiable controls the action taken when the constraint cannot be satisfied.
	// Defaults to ScheduleAnyway to avoid blocking single-node clusters.
	// +kubebuilder:validation:Enum=ScheduleAnyway;DoNotSchedule
	// +optional
	WhenUnsatisfiable corev1.UnsatisfiableConstraintAction `json:"whenUnsatisfiable,omitempty"`
}

// TopologyAntiAffinitySpec configures generated podAntiAffinity rules.
type TopologyAntiAffinitySpec struct {
	// disabled disables the operator-managed podAntiAffinity section.
	// +optional
	Disabled bool `json:"disabled,omitempty"`

	// required toggles requiredDuringSchedulingIgnoredDuringExecution anti-affinity. Defaults to false (preferred only).
	// +optional
	Required bool `json:"required,omitempty"`
}

// AuthSpec configures Redis AUTH/ACL credentials.
// +kubebuilder:validation:XValidation:rule="self.enabled == false || has(self.passwordSecretRef)",message="passwordSecretRef must be set when auth is enabled"
type AuthSpec struct {
	// enabled toggles Redis AUTH.
	Enabled bool `json:"enabled"`

	// username optionally specifies an ACL username. When empty, default user is used.
	// +optional
	Username string `json:"username,omitempty"`

	// passwordSecretRef references the Secret key containing the Redis password.
	// +optional
	PasswordSecretRef *corev1.SecretKeySelector `json:"passwordSecretRef,omitempty"`
}

// TLSSpec configures TLS certificates and policies for Redis and Sentinel endpoints.
// +kubebuilder:validation:XValidation:rule="self.enabled == false || self.secretName != \"\"",message="secretName must be set when TLS is enabled"
type TLSSpec struct {
	// enabled toggles TLS for Redis and Sentinel listeners.
	Enabled bool `json:"enabled"`

	// secretName references the Secret containing TLS material (ca.crt, tls.crt, tls.key by default).
	// +optional
	SecretName string `json:"secretName,omitempty"`

	// caCertKey overrides the key name used for the CA bundle.
	// +optional
	CACertKey string `json:"caCertKey,omitempty"`

	// certKey overrides the key name used for the server certificate.
	// +optional
	CertKey string `json:"certKey,omitempty"`

	// keyKey overrides the key name used for the private key.
	// +optional
	KeyKey string `json:"keyKey,omitempty"`

	// requireClientAuth enables mTLS by requiring client certificates.
	// +optional
	RequireClientAuth *bool `json:"requireClientAuth,omitempty"`

	// disablePlaintext disallows non-TLS ports when TLS is enabled.
	// +optional
	DisablePlaintext *bool `json:"disablePlaintext,omitempty"`
}

// StorageSpec defines data persistence settings for Redis.
type StorageSpec struct {
	// type selects storage type: Persistent (PVC) or Ephemeral (emptyDir).
	// +kubebuilder:validation:Enum=Persistent;Ephemeral
	// +kubebuilder:default=Persistent
	Type string `json:"type,omitempty"`

	// size of the PersistentVolumeClaim (required when Type=Persistent).
	// +optional
	Size *resourceQuantity `json:"size,omitempty"`

	// cleanupOnDelete controls whether PVCs are removed when the cluster CR is deleted.
	// When false (default), the operator preserves PVCs for potential disaster recovery.
	// +optional
	CleanupOnDelete bool `json:"cleanupOnDelete,omitempty"`

	// storageClassName for the PersistentVolumeClaim.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// accessModes for the PersistentVolumeClaim (default: ["ReadWriteOnce"]).
	// +optional
	AccessModes []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`
}

// resourceQuantity is a minimal alias to carry a quantity string in the CRD without importing k8s apimachinery Quantity parsing in validation.
// It serializes as a string and is validated for non-empty content; the controller is responsible for parsing to resource.Quantity.
// +kubebuilder:validation:Type=string
// +kubebuilder:validation:MinLength=1
type resourceQuantity string

// ServiceSpec configures Services managed by the operator.
type ServiceSpec struct {
	// create toggles Service creation.
	// +kubebuilder:default=true
	Create *bool `json:"create,omitempty"`

	// type is the Service type.
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	// +kubebuilder:default=ClusterIP
	Type corev1.ServiceType `json:"type,omitempty"`

	// annotations added to the Service.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// labels added to the Service.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// ports allows customizing exposed ports for Redis/Sentinel Services.
	// +optional
	Ports *ServicePorts `json:"ports,omitempty"`

	// publishNotReadyAddresses controls whether the headless Service publishes not ready pod addresses.
	// Applies to the headless service only; defaults to true when omitted.
	// +optional
	PublishNotReadyAddresses *bool `json:"publishNotReadyAddresses,omitempty"`
}

// ServicePorts defines ports for Redis and Sentinel.
type ServicePorts struct {
	// redis port (default 6379)
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=6379
	Redis int32 `json:"redis,omitempty"`

	// sentinel port (default 26379) — relevant when mode=Sentinel
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=26379
	Sentinel int32 `json:"sentinel,omitempty"`
}

// SentinelPodSpec configures the sentinel Pod template in Dedicated mode.
type SentinelPodSpec struct {
	// labels added to sentinel pods.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// annotations added to sentinel pods.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// nodeSelector for sentinel pods.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// tolerations for sentinel pods.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// affinity for sentinel pods.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
}

// SentinelPDB specifies a PodDisruptionBudget for sentinel pods.
// +kubebuilder:validation:XValidation:rule="!(has(self.minAvailable) && has(self.maxUnavailable))",message="Only one of minAvailable or maxUnavailable may be set"
type SentinelPDB struct {
	// minAvailable pods during voluntary disruptions.
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`
	// maxUnavailable pods during voluntary disruptions.
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// KeyValClusterStatus defines the observed state of KeyValCluster.
type KeyValClusterStatus struct {
	// masterPod is the current master Pod name.
	MasterPod string `json:"masterPod,omitempty"`

	// replicas is the desired number of Redis Pods (mirrors spec.redisReplicas).
	Replicas int32 `json:"replicas,omitempty"`

	// rolesSource identifies how the operator determined the current master/replica roles.
	RolesSource RolesSource `json:"rolesSource,omitempty"`

	// readyReplicas is the count of ready Redis Pods.
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// roles lists roles per Pod (master/replica/sentinel) with readiness.
	Roles []PodRoleStatus `json:"roles,omitempty"`

	// conditions represent the latest available observations of an object's state.
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// healthGate summarizes disruption gating decisions derived from conditions.
	HealthGate *HealthGateStatus `json:"healthGate,omitempty"`

	// externalImport reports progress when seeding from an external source.
	// +optional
	ExternalImport *ExternalImportStatus `json:"externalImport,omitempty"`
}

// RolesSource enumerates the source of truth used to derive master/replica roles.
// +kubebuilder:validation:Enum=sentinel;probe;forced
type RolesSource string

const (
	// RolesSourceSentinel indicates roles were determined via Sentinel metadata.
	RolesSourceSentinel RolesSource = "sentinel"
	// RolesSourceProbe indicates roles were detected by probing Redis instances directly.
	RolesSourceProbe RolesSource = "probe"
	// RolesSourceForced indicates roles were forced explicitly (e.g. bootstrap selection).
	RolesSourceForced RolesSource = "forced"
)

// HealthGateStatus reports the aggregate disruption gate decision.
type HealthGateStatus struct {
	// allowDisruptions is true when voluntary evictions/updates are considered safe.
	AllowDisruptions bool `json:"allowDisruptions"`
}

// ExternalImportStatus captures the observed external import state.
type ExternalImportStatus struct {
	// state is a human-readable summary (e.g. Pending, InProgress, Completed, Failed).
	State string `json:"state,omitempty"`

	// source records the address of the external Redis/Valkey instance.
	Source string `json:"source,omitempty"`

	// mode records the sync mode used for the import (Snapshot or Live).
	Mode string `json:"mode,omitempty"`

	// startedAt records when the current import attempt began.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// lastSynced records the most recent successful sync timestamp.
	// +optional
	LastSynced *metav1.Time `json:"lastSynced,omitempty"`

	// message contains additional diagnostic information.
	// +optional
	Message string `json:"message,omitempty"`
}

// ConditionType enumerates status condition identifiers maintained by the operator.
type ConditionType string

const (
	ConditionAvailable            ConditionType = "Available"
	ConditionSentinelQuorum       ConditionType = "SentinelQuorum"
	ConditionReplicationHealthy   ConditionType = "ReplicationHealthy"
	ConditionFailoverInProgress   ConditionType = "FailoverInProgress"
	ConditionDisruptionsPaused    ConditionType = "DisruptionsPaused"
	ConditionUpgradeInProgress    ConditionType = "UpgradeInProgress"
	ConditionBootstrapInProgress  ConditionType = "BootstrapInProgress"
	ConditionRuntimeConfigApplied ConditionType = "RuntimeConfigApplied"
	ConditionReconciled           ConditionType = "Reconciled"
	ConditionStorageCleanup       ConditionType = "StorageCleanup"
	ConditionExternalImport       ConditionType = "ExternalImport"
)

// PodRole enumerates the role of a Pod within the KeyValCluster.
// +kubebuilder:validation:Enum=master;replica;sentinel
type PodRole string

const (
	PodRoleMaster   PodRole = "master"
	PodRoleReplica  PodRole = "replica"
	PodRoleSentinel PodRole = "sentinel"
)

// PodHealth represents the health assessment of a Pod based on Redis metrics.
// +kubebuilder:validation:Enum=Healthy;Lagging;Desynced;Offline
type PodHealth string

const (
	PodHealthHealthy  PodHealth = "Healthy"
	PodHealthLagging  PodHealth = "Lagging"
	PodHealthDesynced PodHealth = "Desynced"
	PodHealthOffline  PodHealth = "Offline"
)

// PodRoleStatus captures role and readiness of a Pod.
type PodRoleStatus struct {
	Name               string       `json:"name"`
	Role               PodRole      `json:"role"`
	Ready              bool         `json:"ready"`
	Health             PodHealth    `json:"health,omitempty"`
	LagSeconds         *int32       `json:"lagSeconds,omitempty"`
	LastTransitionTime *metav1.Time `json:"lastTransitionTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=kvc
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.redisReplicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Master",type=string,JSONPath=`.status.masterPod`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=`.metadata.creationTimestamp`

// KeyValCluster is the Schema for the keyvalclusters API.
type KeyValCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KeyValClusterSpec   `json:"spec,omitempty"`
	Status KeyValClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// KeyValClusterList contains a list of KeyValCluster.
type KeyValClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KeyValCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KeyValCluster{}, &KeyValClusterList{})
}
