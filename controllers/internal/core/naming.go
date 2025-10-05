package core

import (
	"fmt"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

const (
	LabelAppKey            = "app"
	LabelClusterKey        = "keyvalcluster"
	RoleLabelKey           = "role"
	appLabelFormat         = "%s-redis"
	sentinelAppLabelFormat = "%s-sentinel"
)

const (
	DataVolumeName                         = "data"
	RuntimeConfigVolumeName                = "runtime-conf"
	RedisContainerName                     = "redis"
	SentinelContainerName                  = "sentinel"
	BootstrapInitContainerName             = "kv-bootstrap-role"
	FinalizerName                          = "keyval.ivelok.io/finalizer"
	AnnotationRestartFormerMaster          = "keyval.ivelok.io/restart-former-master"
	AnnotationForceMaster                  = "keyval.ivelok.io/force-master"
	AnnotationUpdateBlocked                = "keyval.ivelok.io/update-blocked"
	AnnotationScalePhase                   = "keyval.ivelok.io/scale-phase"
	FieldOwner                             = "keyval-operator"
	FieldOwnerPDB                          = "keyval-operator-pdb"
	AnnotationPVCReplicationOffset         = "keyval.ivelok.io/pvc-repl-offset"
	AnnotationPVCReplicationRole           = "keyval.ivelok.io/pvc-last-role"
	AnnotationPVCOffsetTimestamp           = "keyval.ivelok.io/pvc-last-update"
	AnnotationSentinelResetMaster          = "keyval.ivelok.io/reset-master"
	AnnotationSentinelResetAttempts        = "keyval.ivelok.io/reset-attempts"
	AnnotationSentinelResetNext            = "keyval.ivelok.io/reset-next"
	AnnotationSentinelClusterResetAttempts = "keyval.ivelok.io/reset-cluster-attempts"
	AnnotationSentinelClusterResetCooldown = "keyval.ivelok.io/reset-cluster-cooldown"
	AnnotationSentinelQuorumLossSince      = "keyval.ivelok.io/quorum-loss-since"
	AnnotationFinalizerStarted             = "keyval.ivelok.io/finalizing-since"
	AnnotationPVCResizeTarget              = "keyval.ivelok.io/pvc-resize-target"
	AnnotationPVCResizeRequestedAt         = "keyval.ivelok.io/pvc-resize-requested-at"
	AnnotationPVCResizeCompletedAt         = "keyval.ivelok.io/pvc-resize-completed-at"
)

func AppLabelForName(name string) string {
	return fmt.Sprintf(appLabelFormat, name)
}

func AppLabel(cr *keyvalv1alpha1.KeyValCluster) string {
	return AppLabelForName(cr.Name)
}

func LabelsFor(cr *keyvalv1alpha1.KeyValCluster) map[string]string {
	return map[string]string{
		LabelAppKey:     AppLabel(cr),
		LabelClusterKey: cr.Name,
	}
}

func SentinelAppLabelForName(name string) string {
	return fmt.Sprintf(sentinelAppLabelFormat, name)
}

func SentinelAppLabel(cr *keyvalv1alpha1.KeyValCluster) string {
	return SentinelAppLabelForName(cr.Name)
}

func SentinelLabelsFor(cr *keyvalv1alpha1.KeyValCluster) map[string]string {
	return map[string]string{
		LabelAppKey:     SentinelAppLabel(cr),
		LabelClusterKey: cr.Name,
	}
}

func HeadlessName(cr *keyvalv1alpha1.KeyValCluster) string {
	return fmt.Sprintf("%s-headless", cr.Name)
}

func MasterServiceName(cr *keyvalv1alpha1.KeyValCluster) string {
	return fmt.Sprintf("%s-master", cr.Name)
}

func ConfigMapName(cr *keyvalv1alpha1.KeyValCluster) string {
	return fmt.Sprintf("%s-config", cr.Name)
}

func SentinelServiceName(cr *keyvalv1alpha1.KeyValCluster) string {
	return fmt.Sprintf("%s-sentinel", cr.Name)
}

func ReplicasServiceName(cr *keyvalv1alpha1.KeyValCluster) string {
	return fmt.Sprintf("%s-replicas", cr.Name)
}

func SentinelHeadlessName(cr *keyvalv1alpha1.KeyValCluster) string {
	return fmt.Sprintf("%s-sentinel-headless", cr.Name)
}
