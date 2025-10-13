package phases

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	controllererrors "github.com/ivelok/keyval-operator/controllers/errors"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	opupdate "github.com/ivelok/keyval-operator/controllers/internal/ops/update"
	"github.com/ivelok/keyval-operator/controllers/internal/reconcile"
)

// PodMetadata reconciles template-managed labels and annotations on existing pods.
func PodMetadata(ctx context.Context, state *reconcile.State) error {
	if state == nil || state.Cluster == nil {
		return nil
	}

	deps := state.Dependencies
	if deps.Client == nil {
		return controllererrors.WrapTransient(fmt.Errorf("pod metadata phase missing client"))
	}

	if err := syncPodMetadata(ctx, state, &state.RedisTemplateMetadata, &state.RedisPods, opupdate.ComponentRedis); err != nil {
		return err
	}

	if state.Cluster.Spec.Mode == keyvalv1alpha1.ModeSentinel {
		if err := syncPodMetadata(ctx, state, &state.SentinelTemplateMetadata, &state.SentinelPods, opupdate.ComponentSentinel); err != nil {
			return err
		}
	}

	return nil
}

func syncPodMetadata(ctx context.Context, state *reconcile.State, meta *reconcile.PodTemplateMetadata, pods *[]corev1.Pod, component string) error {
	if meta == nil || pods == nil {
		return nil
	}
	podList := *pods
	if len(podList) == 0 {
		return nil
	}

	labelKeys := managedKeys(meta.PreviousLabels, meta.DesiredLabels)
	annotationKeys := managedKeys(meta.PreviousAnnotations, meta.DesiredAnnotations)
	delete(labelKeys, core.RoleLabelKey)

	if len(labelKeys) == 0 && len(annotationKeys) == 0 {
		return nil
	}

	deps := state.Dependencies
	logger := state.Logger
	if logger.IsZero() {
		logger = deps.BaseLogger
	}
	metaLogger := logger.WithValues("component", component)

	for i := range podList {
		cur := &podList[i]
		updated := cur.DeepCopy()
		changed := false

		if len(labelKeys) > 0 {
			if updated.Labels == nil && len(meta.DesiredLabels) > 0 {
				updated.Labels = map[string]string{}
			}
			for key := range labelKeys {
				desiredVal, want := meta.DesiredLabels[key]
				currentVal, have := updated.Labels[key]
				if want {
					if updated.Labels == nil {
						updated.Labels = map[string]string{}
					}
					if !have || currentVal != desiredVal {
						updated.Labels[key] = desiredVal
						changed = true
					}
				} else if have {
					delete(updated.Labels, key)
					changed = true
				}
			}
		}

		if len(annotationKeys) > 0 {
			if updated.Annotations == nil && len(meta.DesiredAnnotations) > 0 {
				updated.Annotations = map[string]string{}
			}
			for key := range annotationKeys {
				desiredVal, want := meta.DesiredAnnotations[key]
				currentVal, have := updated.Annotations[key]
				if want {
					if updated.Annotations == nil {
						updated.Annotations = map[string]string{}
					}
					if !have || currentVal != desiredVal {
						updated.Annotations[key] = desiredVal
						changed = true
					}
				} else if have {
					delete(updated.Annotations, key)
					changed = true
				}
			}
		}

		if !changed {
			continue
		}

		base := cur.DeepCopy()
		patch := client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})
		if err := deps.Client.Patch(ctx, updated, patch); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			if apierrors.IsConflict(err) {
				metaLogger.V(1).Info("pod metadata patch conflict", "pod", cur.Name)
				state.RequeueMin(2 * time.Second)
				var refreshed corev1.Pod
				if getErr := deps.Client.Get(ctx, client.ObjectKey{Namespace: cur.Namespace, Name: cur.Name}, &refreshed); getErr == nil {
					podList[i] = refreshed
				}
				continue
			}
			metaLogger.Error(err, "patch pod metadata failed", "pod", cur.Name)
			return controllererrors.WrapTransient(fmt.Errorf("patch pod metadata %s/%s: %w", cur.Namespace, cur.Name, err))
		}
		podList[i] = *updated
	}

	*pods = podList
	return nil
}

func managedKeys(prev, desired map[string]string) map[string]struct{} {
	keys := make(map[string]struct{}, len(prev)+len(desired))
	for k := range prev {
		keys[k] = struct{}{}
	}
	for k := range desired {
		keys[k] = struct{}{}
	}
	return keys
}
