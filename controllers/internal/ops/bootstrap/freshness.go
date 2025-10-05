package bootstrap

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
	clientspkg "github.com/ivelok/keyval-operator/controllers/internal/clients"
	"github.com/ivelok/keyval-operator/controllers/internal/core"
	opobs "github.com/ivelok/keyval-operator/controllers/internal/ops/observability"
	"github.com/ivelok/keyval-operator/controllers/internal/runtime"
)

const (
	candidateSourceForced   = "force-master"
	candidateSourceFreshest = "freshness"
	candidateSourceSeed     = "seed"
)

// Candidate represents the selected Redis pod to promote as master during
// bootstrap recovery.
type Candidate struct {
	Name    string
	Source  string
	Offset  int64
	Updated time.Time
}

// UpdatePVCFreshness persists replication freshness signals gathered from live
// Redis pods onto their backing PVCs. These annotations are later used during
// disaster-recovery bootstrap to determine which replica has the most recent
// data when Sentinels cannot provide authoritative answers. Persistent storage
// is required; ephemeral clusters skip this step.
func UpdatePVCFreshness(ctx context.Context, c client.Client, cr *keyvalv1alpha1.KeyValCluster, pods []corev1.Pod, infos map[string]clientspkg.ReplicationInfo) {
	if !core.HasPersistentData(cr) || len(pods) == 0 || c == nil {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for i := range pods {
		pod := &pods[i]
		info, ok := infos[pod.Name]
		if !ok {
			continue
		}
		pvcName := core.DataPVCName(cr, pod.Name)
		var pvc corev1.PersistentVolumeClaim
		if err := c.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pvcName}, &pvc); err != nil {
			continue
		}
		desired := pvc.DeepCopy()
		if desired.Annotations == nil {
			desired.Annotations = make(map[string]string, 3)
		}
		offset := freshnessOffset(info)
		desired.Annotations[core.AnnotationPVCReplicationOffset] = strconv.FormatInt(offset, 10)
		desired.Annotations[core.AnnotationPVCReplicationRole] = freshnessRole(info)
		desired.Annotations[core.AnnotationPVCOffsetTimestamp] = now
		if pvcEqualAnnotations(&pvc, desired) {
			continue
		}
		_ = c.Patch(ctx, desired, client.MergeFrom(&pvc))
	}
}

// SelectCandidate chooses the best Redis pod to promote as master during
// bootstrap. Priority order:
//  1. Pods annotated with keyval.ivelok.io/force-master=true (freshest first).
//  2. Pod with the highest stored replication offset (ties resolved by
//     timestamp, previous role, then ordinal).
//  3. Ordinal-0 pod when no stronger signals exist.
func SelectCandidate(ctx context.Context, c client.Client, cr *keyvalv1alpha1.KeyValCluster, pods []corev1.Pod) Candidate {
	if len(pods) == 0 {
		return Candidate{}
	}
	records := loadFreshnessRecords(ctx, c, cr, pods)
	forced := make([]freshnessRecord, 0, len(records))
	for _, r := range records {
		if r.forceMaster {
			forced = append(forced, r)
		}
	}
	if len(forced) > 0 {
		sortRecords(forced)
		best := forced[0]
		observeFreshnessMetrics(cr, candidateSourceForced, best, records)
		return Candidate{Name: best.podName, Source: candidateSourceForced, Offset: best.offset, Updated: best.updatedAt}
	}
	if len(records) > 0 {
		sortRecords(records)
		best := records[0]
		if best.offset > 0 {
			observeFreshnessMetrics(cr, candidateSourceFreshest, best, records)
			return Candidate{Name: best.podName, Source: candidateSourceFreshest, Offset: best.offset, Updated: best.updatedAt}
		}
	}
	seed := runtime.SortedPodsByName(pods)[0]
	observeFreshnessMetrics(cr, candidateSourceSeed, freshnessRecord{podName: seed.Name}, records)
	return Candidate{Name: seed.Name, Source: candidateSourceSeed}
}

type freshnessRecord struct {
	podName     string
	offset      int64
	updatedAt   time.Time
	lastRole    string
	forceMaster bool
}

func loadFreshnessRecords(ctx context.Context, c client.Client, cr *keyvalv1alpha1.KeyValCluster, pods []corev1.Pod) []freshnessRecord {
	recs := make([]freshnessRecord, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
		rec := freshnessRecord{podName: pod.Name}
		if pod.Annotations != nil {
			if v := pod.Annotations[core.AnnotationForceMaster]; strings.EqualFold(v, "true") {
				rec.forceMaster = true
			}
		}
		if core.HasPersistentData(cr) && c != nil {
			pvcName := core.DataPVCName(cr, pod.Name)
			var pvc corev1.PersistentVolumeClaim
			if err := c.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pvcName}, &pvc); err == nil {
				if pvc.Annotations != nil {
					if offStr := pvc.Annotations[core.AnnotationPVCReplicationOffset]; offStr != "" {
						if off, err := strconv.ParseInt(offStr, 10, 64); err == nil {
							rec.offset = off
						}
					}
					if role := pvc.Annotations[core.AnnotationPVCReplicationRole]; role != "" {
						rec.lastRole = role
					}
					if ts := pvc.Annotations[core.AnnotationPVCOffsetTimestamp]; ts != "" {
						if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
							rec.updatedAt = parsed
						}
					}
				}
			}
		}
		if rec.updatedAt.IsZero() {
			rec.updatedAt = pod.CreationTimestamp.Time
		}
		recs = append(recs, rec)
	}
	return recs
}

func sortRecords(recs []freshnessRecord) {
	sort.SliceStable(recs, func(i, j int) bool {
		if recs[i].offset != recs[j].offset {
			return recs[i].offset > recs[j].offset
		}
		if !recs[i].updatedAt.Equal(recs[j].updatedAt) {
			return recs[i].updatedAt.After(recs[j].updatedAt)
		}
		li := strings.ToLower(recs[i].lastRole)
		lj := strings.ToLower(recs[j].lastRole)
		if li == "master" && lj != "master" {
			return true
		}
		if li != "master" && lj == "master" {
			return false
		}
		iOrd := core.Ordinal(recs[i].podName)
		jOrd := core.Ordinal(recs[j].podName)
		if iOrd >= 0 && jOrd >= 0 && iOrd != jOrd {
			return iOrd < jOrd
		}
		return recs[i].podName < recs[j].podName
	})
}

func freshnessOffset(info clientspkg.ReplicationInfo) int64 {
	if info.Role == "master" {
		if info.MasterReplOffset > 0 {
			return info.MasterReplOffset
		}
		if info.ReplicaReplOffset > 0 {
			return info.ReplicaReplOffset
		}
		return 0
	}
	if info.ReplicaReplOffset > 0 {
		return info.ReplicaReplOffset
	}
	if info.MasterReplOffset > 0 {
		return info.MasterReplOffset
	}
	return 0
}

func freshnessRole(info clientspkg.ReplicationInfo) string {
	if info.Role != "" {
		return info.Role
	}
	return "unknown"
}

func observeFreshnessMetrics(cr *keyvalv1alpha1.KeyValCluster, source string, winner freshnessRecord, records []freshnessRecord) {
	opobs.SetBootstrapFreshnessSource(cr, source)
	if source != candidateSourceFreshest && source != candidateSourceForced {
		opobs.SetBootstrapFreshnessGap(cr, 0)
		return
	}
	if len(records) < 2 {
		opobs.SetBootstrapFreshnessGap(cr, 0)
		return
	}
	sortRecords(records)
	var next freshnessRecord
	if records[0].podName == winner.podName {
		if len(records) < 2 {
			opobs.SetBootstrapFreshnessGap(cr, 0)
			return
		}
		next = records[1]
	} else {
		next = records[0]
	}
	delta := winner.offset - next.offset
	if delta < 0 {
		delta = 0
	}
	opobs.SetBootstrapFreshnessGap(cr, delta)
}

func pvcEqualAnnotations(cur, desired *corev1.PersistentVolumeClaim) bool {
	if cur == nil || desired == nil {
		return true
	}
	for _, key := range []string{core.AnnotationPVCReplicationOffset, core.AnnotationPVCReplicationRole, core.AnnotationPVCOffsetTimestamp} {
		if (cur.Annotations == nil || cur.Annotations[key] == "") && (desired.Annotations == nil || desired.Annotations[key] == "") {
			continue
		}
		if cur.Annotations == nil || desired.Annotations == nil {
			return false
		}
		if cur.Annotations[key] != desired.Annotations[key] {
			return false
		}
	}
	return true
}
