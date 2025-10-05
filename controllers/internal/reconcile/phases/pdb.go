package phases

import (
	"context"
	"fmt"
	"strings"

	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	ophealthy "github.com/ivelok/keyval-operator/controllers/internal/ops/health"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
	"github.com/ivelok/keyval-operator/controllers/internal/resources"
	"github.com/ivelok/keyval-operator/controllers/internal/ssa"
)

// PDB ensures Redis and Sentinel PodDisruptionBudgets align with the current disruption policy.
func PDB(ctx context.Context, state *reconcile.State) error {
	if state == nil || state.Cluster == nil {
		return nil
	}

	deps := state.Dependencies
	if deps.Client == nil {
		return controllererrors.WrapTransient(fmt.Errorf("pdb phase missing client"))
	}
	if deps.Scheme == nil {
		return controllererrors.WrapTransient(fmt.Errorf("pdb phase missing scheme"))
	}

	cr := state.Cluster
	logger := state.Logger
	if logger.IsZero() {
		logger = deps.BaseLogger
	}
	logger = logger.WithValues("phase", "pdb")

	allowDisruptions := state.Disruption.AllowDisruptions
	allowSentinel := state.Disruption.AllowSentinel

	settings := ophealthy.SettingsFor(cr)

	// Redis PDB handling
	redisMin := int32(0)
	if cr.Spec.RedisReplicas > 0 {
		computed := reconcile.ComputeRedisPDBMinAvailable(cr, allowDisruptions, settings)
		redisMin = computed
		pdbObj := resources.RedisPDB(cr, computed)
		pdbObj.APIVersion = "policy/v1"
		pdbObj.Kind = "PodDisruptionBudget"

		var existing policyv1.PodDisruptionBudget
		prevMin, prevMinOK := int32(0), false
		if err := deps.Client.Get(ctx, client.ObjectKey{Namespace: pdbObj.Namespace, Name: pdbObj.Name}, &existing); err == nil {
			prevMin, prevMinOK = reconcile.MinAvailableValue(existing.Spec)
		} else if !apierrors.IsNotFound(err) {
			logger.Error(err, "get redis pdb failed")
		}

		desiredMin, preserve := reconcile.ResolvePDBMinAvailable(prevMin, prevMinOK, computed, existing.Annotations)
		reconcile.SetMinAvailable(pdbObj, desiredMin)
		redisMin = desiredMin
		if preserve {
			ensureAnnotation(&pdbObj.ObjectMeta, reconcile.PDBPreserveMinAnnotation, annotationOrDefault(existing.Annotations, reconcile.PDBPreserveMinAnnotation, "true"))
		}

		apply := ssa.PodDisruptionBudget(pdbObj)
		if err := controllerutil.SetOwnerReference(cr, apply, deps.Scheme); err != nil {
			logger.Error(err, "set owner on redis pdb failed")
		} else if err := deps.Client.Patch(ctx, apply, client.Apply, client.FieldOwner(core.FieldOwnerPDB)); err != nil {
			logger.Error(err, "apply redis pdb failed")
		} else if min, ok := reconcile.MinAvailableValue(pdbObj.Spec); ok {
			if !prevMinOK || prevMin != min {
				logger.Info("redis PDB minAvailable updated", "minAvailable", min, "previous", prevMin, "allowDisruptions", allowDisruptions)
				opobs.EventPDBModeSwitched(deps.Recorder, cr, "redis", pdbMode(allowDisruptions), min)
			}
		}
	}
	state.Disruption.RedisMin = redisMin

	// Sentinel PDB (dedicated mode)
	sentinelMin := int32(0)
	if cr.Spec.Mode == keyvalv1alpha1.ModeSentinel {
		desiredCount := reconcile.SentinelDesiredCount(cr)
		if desiredCount > 0 {
			computed := reconcile.ComputeSentinelPDBMinAvailable(cr, allowSentinel)
			sentinelMin = computed
			pdbObj := resources.SentinelPDB(cr, computed)
			pdbObj.APIVersion = "policy/v1"
			pdbObj.Kind = "PodDisruptionBudget"

			var existing policyv1.PodDisruptionBudget
			prevMin, prevMinOK := int32(0), false
			if err := deps.Client.Get(ctx, client.ObjectKey{Namespace: pdbObj.Namespace, Name: pdbObj.Name}, &existing); err == nil {
				prevMin, prevMinOK = reconcile.MinAvailableValue(existing.Spec)
			} else if !apierrors.IsNotFound(err) {
				logger.Error(err, "get sentinel pdb failed")
			}

			desiredMin, preserve := reconcile.ResolvePDBMinAvailable(prevMin, prevMinOK, computed, existing.Annotations)
			reconcile.SetMinAvailable(pdbObj, desiredMin)
			sentinelMin = desiredMin
			if preserve {
				ensureAnnotation(&pdbObj.ObjectMeta, reconcile.PDBPreserveMinAnnotation, annotationOrDefault(existing.Annotations, reconcile.PDBPreserveMinAnnotation, "true"))
			}

			apply := ssa.PodDisruptionBudget(pdbObj)
			if err := controllerutil.SetOwnerReference(cr, apply, deps.Scheme); err != nil {
				logger.Error(err, "set owner on sentinel pdb failed")
			} else if err := deps.Client.Patch(ctx, apply, client.Apply, client.FieldOwner(core.FieldOwnerPDB)); err != nil {
				logger.Error(err, "apply sentinel pdb failed")
			} else if min, ok := reconcile.MinAvailableValue(pdbObj.Spec); ok {
				if !prevMinOK || prevMin != min {
					logger.Info("sentinel PDB minAvailable updated", "minAvailable", min, "previous", prevMin, "allowDisruptions", allowSentinel)
					opobs.EventPDBModeSwitched(deps.Recorder, cr, "sentinel", pdbMode(allowDisruptions), min)
				}
			}
		}
	}
	state.Disruption.SentinelMin = sentinelMin

	return nil
}

func ensureAnnotation(meta *metav1.ObjectMeta, key, val string) {
	if meta.Annotations == nil {
		meta.Annotations = map[string]string{}
	}
	meta.Annotations[key] = val
}

func annotationOrDefault(annotations map[string]string, key, fallback string) string {
	if annotations == nil {
		return fallback
	}
	if val, ok := annotations[key]; ok && strings.TrimSpace(val) != "" {
		return val
	}
	return fallback
}

func pdbMode(allow bool) string {
	if allow {
		return "Soft"
	}
	return "Hard"
}
