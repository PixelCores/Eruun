package job

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type requiredStatefulSetPodDeletionTarget struct {
	ref                     cleanupResourceRef
	statefulSetUID          types.UID
	statefulSetWasFound     bool
	podUIDs                 map[string]types.UID
	ownerJobs               map[string]requiredStatefulSetPodOwnerJobIdentity
	ownerJobsCaptured       bool
	checkpointPersisted     bool
	checkpointEverPersisted bool
}

type requiredStatefulSetPodOwnerJobIdentity struct {
	podNames map[string]struct{}
	name     string
	uid      types.UID
}

type requiredStatefulSetPodOwnerJobTarget struct {
	name            string
	uid             types.UID
	resourceVersion string
}

// deferRequiredStatefulSetJobDelete keeps every Job out of the generic
// generated/labeled cleanup paths for required StatefulSet rebuilds. A local
// worker's owner checkpoint can lag a concurrently persisted checkpoint, so
// deciding this from the local ownerJobs map is unsafe. The dedicated
// reconciler below handles both checkpointed and label-discovered Jobs.
func (c *CleanupResourcesJobCtl) deferRequiredStatefulSetJobDelete(ref cleanupResourceRef) bool {
	return c != nil && c.job != nil && ref.kind == domainspec.ResourceJob &&
		versionUpdateCleanupRequiresStatefulSetDeletion(c.job.InternalInfo)
}

func (c *CleanupResourcesJobCtl) ensureRequiredStatefulSetPodDeletionAllowed(ctx context.Context, component *model.ApplicationComponent) error {
	if c == nil || c.job == nil || !versionUpdateCleanupRequiresStatefulSetDeletion(c.job.InternalInfo) {
		return nil
	}
	if err := c.ensureRequiredStatefulSetWorkflowTaskActive(ctx, "checking required StatefulSet Pod deletion"); err != nil {
		return err
	}
	if err := c.refreshRequiredStatefulSetPodTargets(ctx, component); err != nil {
		return err
	}
	return nil
}

func (c *CleanupResourcesJobCtl) requiredStatefulSetPodsGone(ctx context.Context, component *model.ApplicationComponent) (bool, error) {
	if err := c.ensureRequiredStatefulSetWorkflowTaskActive(ctx, "reconciling required StatefulSet Pod deletion"); err != nil {
		return false, err
	}
	// Re-read the persisted JobInfo status before any retention or deletion
	// mutation so JobInfo terminalization also closes this destructive path.
	if err := c.refreshRequiredStatefulSetPodTargets(ctx, component); err != nil {
		return false, err
	}
	converged, err := c.rememberedStatefulSetPVCOwnerReferencesConverged(ctx)
	if err != nil {
		return false, err
	}
	if !converged {
		return false, nil
	}

	ownerJobsGone, err := c.requiredStatefulSetPodOwnerJobsGone(ctx, component)
	if err != nil {
		return false, err
	}
	if !ownerJobsGone {
		return false, nil
	}
	labeledJobsGone, err := c.requiredStatefulSetLabeledJobsGone(ctx, component)
	if err != nil {
		return false, err
	}
	if !labeledJobsGone {
		return false, nil
	}

	pods := make([]*corev1.Pod, 0, len(c.requiredStatefulSetPodNames()))
	for _, name := range c.requiredStatefulSetPodNames() {
		pod, exists, err := c.getRequiredStatefulSetPodTarget(ctx, name)
		if err != nil {
			return false, err
		}
		if !exists {
			continue
		}
		pods = append(pods, pod)
	}

	pendingDelete := false
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			pendingDelete = true
			continue
		}

		// Owner Job deletion may race Job GC, label changes, or Pod updates. Always
		// pin a new live Pod snapshot before deleting it, even when every owner Job
		// was already absent.
		livePod, exists, err := c.getRequiredStatefulSetPodTarget(ctx, pod.Name)
		if err != nil {
			return false, err
		}
		if !exists {
			continue
		}
		if err := requiredStatefulSetPodProtectionError(component, livePod); err != nil {
			return false, err
		}
		if livePod.DeletionTimestamp != nil {
			pendingDelete = true
			continue
		}
		ownerJobs, err := c.requiredStatefulSetPodOwnerJobs(ctx, component, livePod)
		if err != nil {
			return false, err
		}
		ownerJobsChanged, err := c.rememberRequiredStatefulSetPodOwnerJobs(livePod.Name, ownerJobs)
		if err != nil {
			return false, err
		}
		if ownerJobsChanged {
			if err := c.persistRequiredStatefulSetPodTarget(ctx); err != nil {
				return false, err
			}
			// The newly observed owner must be deleted and confirmed gone before
			// this Pod can be removed.
			pendingDelete = true
			continue
		}
		if livePod.UID == "" {
			return false, requiredStatefulSetPodConflict(c.requiredStatefulSetPodTarget.ref, livePod.Name, "live Pod has an empty UID before deletion")
		}
		if strings.TrimSpace(livePod.ResourceVersion) == "" {
			return false, requiredStatefulSetPodConflict(c.requiredStatefulSetPodTarget.ref, livePod.Name, "live Pod has an empty resourceVersion before deletion")
		}
		uid := livePod.UID
		resourceVersion := livePod.ResourceVersion
		deleteOptions := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{
			UID:             &uid,
			ResourceVersion: &resourceVersion,
		}}
		if err := c.ensureRequiredStatefulSetWorkflowTaskActive(ctx, fmt.Sprintf("deleting required StatefulSet Pod %s/%s", livePod.Namespace, livePod.Name)); err != nil {
			return false, err
		}
		if err := c.client.CoreV1().Pods(livePod.Namespace).Delete(ctx, livePod.Name, deleteOptions); err != nil {
			if k8serrors.IsConflict(err) || k8serrors.IsNotFound(err) {
				gone, verifyErr := c.verifyRequiredStatefulSetPodDeleteFailure(ctx, component, livePod)
				if verifyErr != nil {
					return false, verifyErr
				}
				if gone {
					continue
				}
				pendingDelete = true
				continue
			}
			return false, fmt.Errorf("delete required StatefulSet pod %s/%s: %w", livePod.Namespace, livePod.Name, err)
		}
		pendingDelete = true
	}
	return !pendingDelete, nil
}

// deleteRequiredStatefulSetPodOwnerJob returns needsRetry when the pinned Job
// still exists without live share protection after a precondition failure.
func (c *CleanupResourcesJobCtl) deleteRequiredStatefulSetPodOwnerJob(
	ctx context.Context,
	component *model.ApplicationComponent,
	pod *corev1.Pod,
	target requiredStatefulSetPodOwnerJobTarget,
) (bool, error) {
	if err := c.ensureRequiredStatefulSetWorkflowTaskActive(ctx, fmt.Sprintf("deleting required StatefulSet Pod owner Job %s/%s", pod.Namespace, target.name)); err != nil {
		return false, err
	}
	propagation := metav1.DeletePropagationOrphan
	uid := target.uid
	resourceVersion := target.resourceVersion
	err := c.client.BatchV1().Jobs(pod.Namespace).Delete(ctx, target.name, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
		Preconditions: &metav1.Preconditions{
			UID:             &uid,
			ResourceVersion: &resourceVersion,
		},
	})
	if err == nil {
		return false, nil
	}
	if !k8serrors.IsConflict(err) && !k8serrors.IsNotFound(err) {
		return false, fmt.Errorf("delete required StatefulSet pod owner Job %s/%s: %w", pod.Namespace, target.name, err)
	}

	job, getErr := c.client.BatchV1().Jobs(pod.Namespace).Get(ctx, target.name, metav1.GetOptions{})
	if getErr != nil {
		if k8serrors.IsNotFound(getErr) {
			return false, nil
		}
		return false, fmt.Errorf("verify required StatefulSet pod owner Job %s/%s after delete failure: %w", pod.Namespace, target.name, getErr)
	}
	if job == nil {
		return false, fmt.Errorf("verify required StatefulSet pod owner Job %s/%s after delete failure: returned nil object", pod.Namespace, target.name)
	}
	if _, protected := cleanupResourceShareProtected(job.Labels); protected {
		return false, requiredStatefulSetPodOwnerJobProtectionError(component, pod, target.name)
	}
	if job.UID != target.uid {
		return false, c.requiredStatefulSetPodOwnerJobConflict(pod, target.name, fmt.Sprintf("Job UID changed from %q to %q after delete returned %s", target.uid, job.UID, k8serrors.ReasonForError(err)))
	}
	if strings.TrimSpace(job.ResourceVersion) == "" {
		return false, c.requiredStatefulSetPodOwnerJobConflict(pod, target.name, "live Job has an empty resourceVersion after delete failure")
	}
	return true, nil
}

func (c *CleanupResourcesJobCtl) verifyRequiredStatefulSetPodDeleteFailure(
	ctx context.Context,
	component *model.ApplicationComponent,
	deletedSnapshot *corev1.Pod,
) (bool, error) {
	pod, exists, err := c.getRequiredStatefulSetPodTarget(ctx, deletedSnapshot.Name)
	if err != nil {
		return false, err
	}
	if !exists {
		return true, nil
	}
	if err := requiredStatefulSetPodProtectionError(component, pod); err != nil {
		return false, err
	}
	if strings.TrimSpace(pod.ResourceVersion) == "" {
		return false, requiredStatefulSetPodConflict(c.requiredStatefulSetPodTarget.ref, pod.Name, "live Pod has an empty resourceVersion after delete failure")
	}
	return false, nil
}

func (c *CleanupResourcesJobCtl) requiredStatefulSetPodOwnerJobsGone(
	ctx context.Context,
	component *model.ApplicationComponent,
) (bool, error) {
	target := c.requiredStatefulSetPodTarget
	if target == nil {
		return false, fmt.Errorf("required StatefulSet pod identity scan was not initialized")
	}
	if !target.ownerJobsCaptured {
		return false, requiredStatefulSetPodConflict(target.ref, target.ref.name, "owner Job identity checkpoint is incomplete")
	}
	checkpointChanged, err := c.captureRequiredStatefulSetOwnerJobPods(ctx, component)
	if err != nil {
		return false, err
	}
	if checkpointChanged {
		if err := c.persistRequiredStatefulSetPodTarget(ctx); err != nil {
			return false, err
		}
		// Persist every Pod that can be linked to a pinned Job UID before any
		// owner Job mutation. The next poll re-runs the full live preflight.
		return false, nil
	}

	type ownerJobDeleteTarget struct {
		pod    *corev1.Pod
		target requiredStatefulSetPodOwnerJobTarget
	}
	deleteTargets := make([]ownerJobDeleteTarget, 0, len(target.ownerJobs))
	pendingDelete := false
	for _, name := range sortedRequiredStatefulSetPodOwnerJobNames(target.ownerJobs) {
		identity := target.ownerJobs[name]
		podNames := sortedRequiredStatefulSetPodOwnerJobPodNames(identity.podNames)
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: identity.name, Namespace: target.ref.namespace,
		}}
		if len(podNames) > 0 {
			pod.Name = podNames[0]
		}
		for _, podName := range podNames {
			livePod, exists, err := c.getRequiredStatefulSetPodTarget(ctx, podName)
			if err != nil {
				return false, err
			}
			if exists {
				if err := requiredStatefulSetPodProtectionError(component, livePod); err != nil {
					return false, err
				}
				pod = livePod
			}
		}
		job, err := c.client.BatchV1().Jobs(target.ref.namespace).Get(ctx, identity.name, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("inspect required StatefulSet pod owner Job %s/%s: %w", target.ref.namespace, identity.name, err)
		}
		if job == nil {
			return false, fmt.Errorf("inspect required StatefulSet pod owner Job %s/%s: returned nil object", target.ref.namespace, identity.name)
		}
		if _, protected := cleanupResourceShareProtected(job.Labels); protected {
			return false, requiredStatefulSetPodOwnerJobProtectionError(component, pod, identity.name)
		}
		if job.UID == "" {
			return false, c.requiredStatefulSetPodOwnerJobConflict(pod, identity.name, "live Job has an empty UID")
		}
		if job.UID != identity.uid {
			return false, c.requiredStatefulSetPodOwnerJobConflict(pod, identity.name, fmt.Sprintf("Job UID changed from %q to %q", identity.uid, job.UID))
		}
		if strings.TrimSpace(job.ResourceVersion) == "" {
			return false, c.requiredStatefulSetPodOwnerJobConflict(pod, identity.name, "live Job has an empty resourceVersion")
		}
		if job.DeletionTimestamp != nil {
			pendingDelete = true
			continue
		}
		deleteTargets = append(deleteTargets, ownerJobDeleteTarget{
			pod: pod,
			target: requiredStatefulSetPodOwnerJobTarget{
				name:            identity.name,
				uid:             identity.uid,
				resourceVersion: job.ResourceVersion,
			},
		})
	}

	// Do not mutate any owner Job until every persisted Job and associated Pod
	// has passed the same fresh identity and share-protection preflight.
	for _, deleteTarget := range deleteTargets {
		_, err := c.deleteRequiredStatefulSetPodOwnerJob(ctx, component, deleteTarget.pod, deleteTarget.target)
		if err != nil {
			return false, err
		}
		// Even a successful DELETE must be observed as NotFound before Pod
		// cleanup can proceed; a Job finalizer may keep its controller live.
		pendingDelete = true
	}
	return !pendingDelete, nil
}

// captureRequiredStatefulSetOwnerJobPods expands the durable checkpoint with
// Pods that can still be tied to a pinned Job UID. The controller UID label is
// retained by ordinary Job Pods even after an Orphan delete removes their
// ownerReference, so a worker restart can still finish explicit Pod cleanup.
func (c *CleanupResourcesJobCtl) captureRequiredStatefulSetOwnerJobPods(
	ctx context.Context,
	component *model.ApplicationComponent,
) (bool, error) {
	target := c.requiredStatefulSetPodTarget
	if target == nil {
		return false, fmt.Errorf("required StatefulSet pod identity scan was not initialized")
	}
	if len(target.ownerJobs) == 0 {
		return false, nil
	}

	pods, err := c.client.CoreV1().Pods(target.ref.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("list namespace Pods for required StatefulSet owner Job checkpoint: %w", err)
	}
	ownerJobByUID := make(map[types.UID]string, len(target.ownerJobs))
	for name, identity := range target.ownerJobs {
		if identity.uid == "" {
			return false, c.requiredStatefulSetPodOwnerJobConflict(
				&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: target.ref.namespace}},
				name,
				"checkpointed Job has an empty UID",
			)
		}
		if previous, duplicate := ownerJobByUID[identity.uid]; duplicate && previous != name {
			return false, c.requiredStatefulSetPodOwnerJobConflict(
				&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: target.ref.namespace}},
				name,
				fmt.Sprintf("checkpointed Jobs %s and %s share UID %q", previous, name, identity.uid),
			)
		}
		ownerJobByUID[identity.uid] = name
	}

	changed := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		matchedJobs := make(map[string]struct{})
		for _, owner := range pod.OwnerReferences {
			if !strings.EqualFold(owner.Kind, string(domainspec.KubeKindJob)) {
				continue
			}
			name := strings.TrimSpace(owner.Name)
			identity, checkpointed := target.ownerJobs[name]
			if !checkpointed {
				continue
			}
			if owner.UID == "" || owner.UID != identity.uid {
				return false, c.requiredStatefulSetPodOwnerJobConflict(
					pod,
					name,
					fmt.Sprintf("Pod owner UID %q does not match checkpointed Job UID %q", owner.UID, identity.uid),
				)
			}
			matchedJobs[name] = struct{}{}
		}
		controllerUID := types.UID(strings.TrimSpace(pod.Labels[batchv1.ControllerUidLabel]))
		if controllerUID == "" {
			controllerUID = types.UID(strings.TrimSpace(pod.Labels["controller-uid"]))
		}
		if name, found := ownerJobByUID[controllerUID]; found {
			matchedJobs[name] = struct{}{}
		}
		if len(matchedJobs) == 0 {
			continue
		}
		if len(matchedJobs) > 1 {
			return false, requiredStatefulSetPodConflict(target.ref, pod.Name, "Pod matches multiple checkpointed owner Jobs")
		}
		if err := requiredStatefulSetPodProtectionError(component, pod); err != nil {
			return false, err
		}
		if pod.Name == "" || pod.UID == "" {
			return false, requiredStatefulSetPodConflict(target.ref, pod.Name, "owner Job Pod has an incomplete identity")
		}
		if strings.TrimSpace(pod.ResourceVersion) == "" {
			return false, requiredStatefulSetPodConflict(target.ref, pod.Name, "owner Job Pod has an empty resourceVersion")
		}
		var ownerJobName string
		for name := range matchedJobs {
			ownerJobName = name
		}
		if expectedUID, remembered := target.podUIDs[pod.Name]; remembered {
			if expectedUID != pod.UID {
				return false, requiredStatefulSetPodConflict(target.ref, pod.Name, fmt.Sprintf("Pod UID changed from %q to %q", expectedUID, pod.UID))
			}
		} else {
			target.podUIDs[pod.Name] = pod.UID
			changed = true
		}
		identity := target.ownerJobs[ownerJobName]
		if identity.podNames == nil {
			identity.podNames = make(map[string]struct{})
		}
		if _, linked := identity.podNames[pod.Name]; !linked {
			identity.podNames[pod.Name] = struct{}{}
			target.ownerJobs[ownerJobName] = identity
			changed = true
		}
	}
	if changed {
		target.checkpointPersisted = false
	}
	return changed, nil
}

// requiredStatefulSetLabeledJobsGone owns every component-labeled Job that was
// deferred from generic cleanup. Every unprotected Job is pinned in the same
// durable owner checkpoint before mutation, including Jobs whose Pods have not
// appeared yet. This keeps duplicate workers and Job/Pod creation races on the
// same UID-guarded Orphan reconciliation path.
func (c *CleanupResourcesJobCtl) requiredStatefulSetLabeledJobsGone(
	ctx context.Context,
	component *model.ApplicationComponent,
) (bool, error) {
	target := c.requiredStatefulSetPodTarget
	if target == nil {
		return false, fmt.Errorf("required StatefulSet pod identity scan was not initialized")
	}
	selector := cleanupLabelSelector(component)
	if selector == "" {
		return false, fmt.Errorf("required StatefulSet deletion Job selector is empty for component %s", component.Name)
	}

	jobs, err := c.client.BatchV1().Jobs(target.ref.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, fmt.Errorf("list labeled Jobs for required StatefulSet deletion: %w", err)
	}
	pods, err := c.client.CoreV1().Pods(target.ref.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("list namespace Pods for required StatefulSet labeled Job deletion: %w", err)
	}

	checkpointChanged := false
	for i := range jobs.Items {
		job := &jobs.Items[i]
		job.Name = strings.TrimSpace(job.Name)
		if job.Name == "" {
			return false, fmt.Errorf("required StatefulSet deletion found a labeled Job with an empty name")
		}
		if identity, checkpointed := target.ownerJobs[job.Name]; checkpointed {
			if identity.uid != job.UID {
				return false, c.requiredStatefulSetLabeledJobConflict(job.Name, fmt.Sprintf("Job UID changed from %q to %q", identity.uid, job.UID))
			}
			// The preceding checkpoint reconciler must observe this Job as gone
			// before label-only reconciliation can proceed.
			return false, nil
		}
		if _, protected := cleanupResourceShareProtected(job.Labels); protected {
			// Preserve the ordinary generic-cleanup contract for a standalone
			// shared Job. Checkpointed owner Jobs still block in the handler above.
			continue
		}
		if job.UID == "" {
			return false, c.requiredStatefulSetLabeledJobConflict(job.Name, "live Job has an empty UID")
		}
		if strings.TrimSpace(job.ResourceVersion) == "" {
			return false, c.requiredStatefulSetLabeledJobConflict(job.Name, "live Job has an empty resourceVersion")
		}

		ownedPods := make([]*corev1.Pod, 0)
		for podIndex := range pods.Items {
			pod := &pods.Items[podIndex]
			owned, ownershipErr := requiredStatefulSetPodOwnedByLabeledJob(pod, job)
			if ownershipErr != nil {
				return false, ownershipErr
			}
			if owned {
				ownedPods = append(ownedPods, pod.DeepCopy())
			}
		}
		if len(ownedPods) > 0 {
			for podIndex := range ownedPods {
				pod := ownedPods[podIndex]
				if err := requiredStatefulSetPodProtectionError(component, pod); err != nil {
					return false, err
				}
				if pod.UID == "" {
					return false, requiredStatefulSetPodConflict(target.ref, pod.Name, "labeled Job Pod has an empty UID")
				}
				if strings.TrimSpace(pod.ResourceVersion) == "" {
					return false, requiredStatefulSetPodConflict(target.ref, pod.Name, "labeled Job Pod has an empty resourceVersion")
				}
				if rememberedUID, remembered := target.podUIDs[pod.Name]; remembered {
					if rememberedUID != pod.UID {
						return false, requiredStatefulSetPodConflict(target.ref, pod.Name, fmt.Sprintf("Pod UID changed from %q to %q", rememberedUID, pod.UID))
					}
				} else {
					target.podUIDs[pod.Name] = pod.UID
					target.ownerJobsCaptured = false
					target.checkpointPersisted = false
				}
				if _, err := c.rememberRequiredStatefulSetPodOwnerJobs(pod.Name, []requiredStatefulSetPodOwnerJobTarget{{
					name:            job.Name,
					uid:             job.UID,
					resourceVersion: job.ResourceVersion,
				}}); err != nil {
					return false, err
				}
			}
			checkpointChanged = true
		} else {
			target.ownerJobs[job.Name] = requiredStatefulSetPodOwnerJobIdentity{
				podNames: make(map[string]struct{}),
				name:     job.Name,
				uid:      job.UID,
			}
			target.checkpointPersisted = false
			checkpointChanged = true
		}
	}

	if checkpointChanged {
		if !target.ownerJobsCaptured {
			target.ownerJobsCaptured = true
			target.checkpointPersisted = false
		}
		if err := c.persistRequiredStatefulSetPodTarget(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

func requiredStatefulSetPodOwnedByLabeledJob(pod *corev1.Pod, job *batchv1.Job) (bool, error) {
	if pod == nil || job == nil {
		return false, nil
	}
	for _, owner := range pod.OwnerReferences {
		if !strings.EqualFold(owner.Kind, string(domainspec.KubeKindJob)) || strings.TrimSpace(owner.Name) != job.Name {
			continue
		}
		if owner.UID == "" || owner.UID != job.UID {
			return false, k8serrors.NewConflict(
				schema.GroupResource{Group: "batch", Resource: "jobs"},
				job.Name,
				fmt.Errorf("labeled Job %s/%s UID %q does not match Pod %s owner UID %q", job.Namespace, job.Name, job.UID, pod.Name, owner.UID),
			)
		}
		return true, nil
	}
	return false, nil
}

func (c *CleanupResourcesJobCtl) requiredStatefulSetLabeledJobConflict(name, detail string) error {
	return k8serrors.NewConflict(
		schema.GroupResource{Group: "batch", Resource: "jobs"},
		name,
		fmt.Errorf("required StatefulSet labeled Job %s identity conflict: %s", name, detail),
	)
}

func (c *CleanupResourcesJobCtl) rememberRequiredStatefulSetPodOwnerJobs(
	podName string,
	ownerJobs []requiredStatefulSetPodOwnerJobTarget,
) (bool, error) {
	target := c.requiredStatefulSetPodTarget
	if target == nil {
		return false, fmt.Errorf("required StatefulSet pod identity scan was not initialized")
	}
	if target.ownerJobs == nil {
		target.ownerJobs = make(map[string]requiredStatefulSetPodOwnerJobIdentity)
	}
	changed := false
	for _, ownerJob := range ownerJobs {
		podName = strings.TrimSpace(podName)
		pinnedPodUID, remembered := target.podUIDs[podName]
		identity := requiredStatefulSetPodOwnerJobIdentity{
			podNames: map[string]struct{}{podName: {}},
			name:     strings.TrimSpace(ownerJob.name),
			uid:      ownerJob.uid,
		}
		if podName == "" || !remembered || pinnedPodUID == "" || identity.name == "" || identity.uid == "" {
			return false, c.requiredStatefulSetPodOwnerJobConflict(
				&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: target.ref.namespace}},
				identity.name,
				"owner Job identity is incomplete before checkpoint persist",
			)
		}
		existing, found := target.ownerJobs[identity.name]
		if found {
			if existing.uid != identity.uid {
				return false, c.requiredStatefulSetPodOwnerJobConflict(
					&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: target.ref.namespace}},
					identity.name,
					fmt.Sprintf("owner Job UID changed from %q to %q before checkpoint persist", existing.uid, identity.uid),
				)
			}
			if existing.podNames == nil {
				existing.podNames = make(map[string]struct{})
			}
			if _, linked := existing.podNames[podName]; !linked {
				existing.podNames[podName] = struct{}{}
				target.ownerJobs[identity.name] = existing
				changed = true
			}
			continue
		}
		target.ownerJobs[identity.name] = identity
		changed = true
	}
	if changed {
		target.checkpointPersisted = false
	}
	return changed, nil
}

func (c *CleanupResourcesJobCtl) refreshRequiredStatefulSetPodTargets(ctx context.Context, component *model.ApplicationComponent) error {
	ref, err := requiredStatefulSetCleanupRef(component)
	if err != nil {
		return err
	}
	selectorText := cleanupLabelSelector(component)
	if selectorText == "" {
		return fmt.Errorf("required StatefulSet deletion pod selector is empty for component %s", component.Name)
	}
	selector, err := labels.Parse(selectorText)
	if err != nil {
		return fmt.Errorf("parse required StatefulSet deletion pod selector for component %s: %w", component.Name, err)
	}

	statefulSet, err := c.client.AppsV1().StatefulSets(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	if err == nil && statefulSet == nil {
		return fmt.Errorf("get required StatefulSet %s before pod identity scan: returned nil object", cleanupResourceDisplayName(ref))
	}
	statefulSetFound := err == nil
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("get required StatefulSet %s before pod identity scan: %w", cleanupResourceDisplayName(ref), err)
	}
	if c.requiredStatefulSetPodTarget == nil {
		if err := c.initializeRequiredStatefulSetPodTarget(ctx, ref, statefulSet, statefulSetFound); err != nil {
			return err
		}
	} else if err := c.verifyRequiredStatefulSetPodOwnerIdentity(ref, statefulSet, statefulSetFound); err != nil {
		return err
	}
	target := c.requiredStatefulSetPodTarget
	incompleteOwnerCapturePods := make(map[string]struct{})
	if !target.ownerJobsCaptured {
		for name := range target.podUIDs {
			incompleteOwnerCapturePods[name] = struct{}{}
		}
	}

	list, err := c.client.CoreV1().Pods(ref.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list namespace pods before required StatefulSet deletion: %w", err)
	}
	liveTargets := make(map[string]*corev1.Pod, len(list.Items))
	for i := range list.Items {
		pod := &list.Items[i]
		name := strings.TrimSpace(pod.Name)
		if name == "" {
			continue
		}
		expectedUID, remembered := target.podUIDs[name]
		if remembered {
			if expectedUID != pod.UID {
				return requiredStatefulSetPodConflict(ref, name, fmt.Sprintf("Pod UID changed from %q to %q", expectedUID, pod.UID))
			}
			liveTargets[name] = pod
			continue
		}

		ownedByTarget, unprovenTargetOwner := requiredStatefulSetPodOwnership(pod, ref.name, target.statefulSetUID)
		ordinal := isStatefulSetOrdinalPodName(ref.name, name)
		labelMatched := selector.Matches(labels.Set(pod.Labels))
		if unprovenTargetOwner && (ordinal || labelMatched) {
			return requiredStatefulSetPodConflict(ref, name, "Pod owner does not match the pinned StatefulSet UID")
		}
		if ordinal && !ownedByTarget {
			return requiredStatefulSetPodConflict(ref, name, "ordinal Pod identity cannot be proven from the pinned StatefulSet UID")
		}
		if !ownedByTarget && !labelMatched {
			continue
		}
		if ownedByTarget && pod.UID == "" {
			return requiredStatefulSetPodConflict(ref, name, "owned Pod has an empty UID")
		}
		target.podUIDs[name] = pod.UID
		target.ownerJobsCaptured = false
		target.checkpointPersisted = false
		liveTargets[name] = pod
	}
	for _, name := range c.requiredStatefulSetPodNames() {
		pod, exists := liveTargets[name]
		if !exists {
			if _, incomplete := incompleteOwnerCapturePods[name]; incomplete {
				return requiredStatefulSetPodConflict(ref, name, "Pod disappeared before its owner Job identity was checkpointed")
			}
			continue
		}
		ownerJobs, err := c.requiredStatefulSetPodOwnerJobs(ctx, component, pod)
		if err != nil {
			return err
		}
		if _, err := c.rememberRequiredStatefulSetPodOwnerJobs(name, ownerJobs); err != nil {
			return err
		}
	}
	if !target.ownerJobsCaptured {
		target.ownerJobsCaptured = true
		target.checkpointPersisted = false
	}
	return c.persistRequiredStatefulSetPodTarget(ctx)
}

const requiredStatefulSetPodCheckpointKey = "statefulSetPodDeletionTarget"

type requiredStatefulSetPodDeletionCheckpoint struct {
	Namespace           string                                     `json:"namespace"`
	StatefulSetName     string                                     `json:"statefulSetName"`
	StatefulSetUID      types.UID                                  `json:"statefulSetUID"`
	StatefulSetWasFound bool                                       `json:"statefulSetWasFound"`
	Pods                []requiredStatefulSetPodIdentityCheckpoint `json:"pods,omitempty"`
	OwnerJobsCaptured   bool                                       `json:"ownerJobsCaptured"`
	OwnerJobs           []requiredStatefulSetPodOwnerJobCheckpoint `json:"ownerJobs,omitempty"`
}

type requiredStatefulSetPodIdentityCheckpoint struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}

type requiredStatefulSetPodOwnerJobCheckpoint struct {
	// PodName preserves read compatibility with checkpoints written before
	// owner Jobs tracked every associated Pod.
	PodName  string    `json:"podName,omitempty"`
	PodNames []string  `json:"podNames,omitempty"`
	Name     string    `json:"name"`
	UID      types.UID `json:"uid"`
}

func (c *CleanupResourcesJobCtl) initializeRequiredStatefulSetPodTarget(
	ctx context.Context,
	ref cleanupResourceRef,
	statefulSet metav1.Object,
	statefulSetFound bool,
) error {
	if c.job.Status == config.StatusRunning {
		existing, err := findExistingVersionUpdateCleanupJobInfo(ctx, c.store, c.job)
		if err != nil {
			return fmt.Errorf("load required StatefulSet pod identity checkpoint: %w", err)
		}
		if existing == nil {
			return fmt.Errorf("precreated version update cleanup job info not found while loading required StatefulSet pod identity")
		}
		if err := c.ensureVersionUpdateCleanupJobInfoOwned(existing, "loading required StatefulSet Pod identity checkpoint"); err != nil {
			return err
		}
		checkpoint, found, err := parseRequiredStatefulSetPodDeletionCheckpoint(existing.InternalInfo)
		if err != nil {
			return fmt.Errorf("parse required StatefulSet pod identity checkpoint: %w", err)
		}
		if found {
			target, err := requiredStatefulSetPodTargetFromCheckpoint(ref, checkpoint)
			if err != nil {
				return err
			}
			c.requiredStatefulSetPodTarget = target
			c.job.InternalInfo = existing.InternalInfo
			return c.verifyRequiredStatefulSetPodOwnerIdentity(ref, statefulSet, statefulSetFound)
		}
	}

	c.requiredStatefulSetPodTarget = &requiredStatefulSetPodDeletionTarget{
		ref:                 ref,
		statefulSetWasFound: statefulSetFound,
		podUIDs:             make(map[string]types.UID),
		ownerJobs:           make(map[string]requiredStatefulSetPodOwnerJobIdentity),
	}
	if statefulSetFound {
		c.requiredStatefulSetPodTarget.statefulSetUID = statefulSet.GetUID()
	}
	return nil
}

const requiredStatefulSetPodCheckpointPersistMaxAttempts = 8

func (c *CleanupResourcesJobCtl) persistRequiredStatefulSetPodTarget(ctx context.Context) error {
	target := c.requiredStatefulSetPodTarget
	if target == nil {
		return nil
	}
	if c.job == nil || c.job.Status != config.StatusRunning {
		// The production path reaches this helper only after markRunning. Direct
		// package-level helper calls do not have a persistent execution to resume.
		target.checkpointPersisted = true
		return nil
	}
	conditionalStore, ok := c.store.(datastore.ConditionalCompareAndSwap)
	if !ok {
		return fmt.Errorf("persist required StatefulSet pod identity checkpoint: datastore does not support conditional compare-and-swap")
	}

	for attempt := 1; attempt <= requiredStatefulSetPodCheckpointPersistMaxAttempts; attempt++ {
		existing, err := findExistingVersionUpdateCleanupJobInfo(ctx, c.store, c.job)
		if err != nil {
			return fmt.Errorf("load required StatefulSet pod identity checkpoint before persist: %w", err)
		}
		if existing == nil {
			return fmt.Errorf("precreated version update cleanup job info not found while persisting required StatefulSet pod identity")
		}
		if err := c.ensureVersionUpdateCleanupJobInfoOwned(existing, "persisting required StatefulSet Pod identity checkpoint"); err != nil {
			return err
		}
		status := config.Status(strings.TrimSpace(existing.Status))
		if status != config.StatusRunning {
			return c.requiredStatefulSetPodCheckpointStatusError(status, existing.Status)
		}
		c.job.InternalInfo = existing.InternalInfo
		checkpoint := requiredStatefulSetPodCheckpointFromTarget(target)
		current, found, err := parseRequiredStatefulSetPodDeletionCheckpoint(existing.InternalInfo)
		if err != nil {
			return fmt.Errorf("parse required StatefulSet pod identity checkpoint before persist: %w", err)
		}
		if found && requiredStatefulSetPodCheckpointsEqual(current, checkpoint) {
			target.checkpointPersisted = true
			target.checkpointEverPersisted = true
			return nil
		}
		target.checkpointPersisted = false
		if found {
			target.checkpointEverPersisted = true
			localIsSubset := requiredStatefulSetPodCheckpointIsSubset(checkpoint, current)
			persistedIsSubset := requiredStatefulSetPodCheckpointIsSubset(current, checkpoint)
			switch {
			case localIsSubset:
				hydrated, hydrateErr := requiredStatefulSetPodTargetFromCheckpoint(target.ref, current)
				if hydrateErr != nil {
					return fmt.Errorf("adopt concurrent required StatefulSet pod identity checkpoint: %w", hydrateErr)
				}
				c.requiredStatefulSetPodTarget = hydrated
				c.job.InternalInfo = existing.InternalInfo
				return nil
			case !persistedIsSubset:
				return k8serrors.NewConflict(
					schema.GroupResource{Resource: "jobinfos"},
					c.job.TaskID,
					fmt.Errorf("required StatefulSet %s pod identity checkpoint forked concurrently", cleanupResourceDisplayName(target.ref)),
				)
			}
		} else if target.checkpointEverPersisted {
			return k8serrors.NewConflict(
				schema.GroupResource{Resource: "jobinfos"},
				c.job.TaskID,
				fmt.Errorf("required StatefulSet %s persisted pod identity checkpoint disappeared", cleanupResourceDisplayName(target.ref)),
			)
		}

		nextInternalInfo, err := marshalRequiredStatefulSetPodDeletionCheckpoint(existing.InternalInfo, checkpoint)
		if err != nil {
			return fmt.Errorf("marshal required StatefulSet pod identity checkpoint: %w", err)
		}
		if err := c.ensureRequiredStatefulSetWorkflowTaskActive(ctx, "persisting required StatefulSet Pod identity checkpoint"); err != nil {
			return err
		}
		updated, err := conditionalStore.CompareAndSwapWithConditions(ctx, existing, versionUpdateCleanupJobInfoConditions(existing), map[string]interface{}{
			"internal_info": nextInternalInfo,
		})
		if err != nil {
			return fmt.Errorf("persist required StatefulSet pod identity checkpoint: %w", err)
		}
		if !updated {
			continue
		}
		target.checkpointPersisted = true
		target.checkpointEverPersisted = true
		c.job.InternalInfo = nextInternalInfo
		return nil
	}
	return k8serrors.NewConflict(
		schema.GroupResource{Resource: "jobinfos"},
		c.job.TaskID,
		fmt.Errorf("required StatefulSet %s pod identity checkpoint did not converge after %d attempts", cleanupResourceDisplayName(target.ref), requiredStatefulSetPodCheckpointPersistMaxAttempts),
	)
}

func (c *CleanupResourcesJobCtl) requiredStatefulSetPodCheckpointStatusError(status config.Status, rawStatus string) error {
	if isTerminalVersionUpdateCleanupStatus(status) {
		return c.abortTerminalizedVersionUpdateCleanup(status, rawStatus)
	}
	return fmt.Errorf("version update cleanup job info for task %s component %s is %s while persisting required StatefulSet pod identity, expected running", c.job.TaskID, resolveJobServiceName(c.job), rawStatus)
}

func requiredStatefulSetPodCheckpointFromTarget(target *requiredStatefulSetPodDeletionTarget) requiredStatefulSetPodDeletionCheckpoint {
	checkpoint := requiredStatefulSetPodDeletionCheckpoint{
		Namespace:           target.ref.namespace,
		StatefulSetName:     target.ref.name,
		StatefulSetUID:      target.statefulSetUID,
		StatefulSetWasFound: target.statefulSetWasFound,
		Pods:                make([]requiredStatefulSetPodIdentityCheckpoint, 0, len(target.podUIDs)),
		OwnerJobsCaptured:   target.ownerJobsCaptured,
		OwnerJobs:           make([]requiredStatefulSetPodOwnerJobCheckpoint, 0, len(target.ownerJobs)),
	}
	for _, name := range sortedRequiredStatefulSetPodUIDNames(target.podUIDs) {
		checkpoint.Pods = append(checkpoint.Pods, requiredStatefulSetPodIdentityCheckpoint{Name: name, UID: target.podUIDs[name]})
	}
	for _, name := range sortedRequiredStatefulSetPodOwnerJobNames(target.ownerJobs) {
		ownerJob := target.ownerJobs[name]
		checkpoint.OwnerJobs = append(checkpoint.OwnerJobs, requiredStatefulSetPodOwnerJobCheckpoint{
			PodNames: sortedRequiredStatefulSetPodOwnerJobPodNames(ownerJob.podNames),
			Name:     ownerJob.name,
			UID:      ownerJob.uid,
		})
	}
	return checkpoint
}

func requiredStatefulSetPodTargetFromCheckpoint(
	ref cleanupResourceRef,
	checkpoint requiredStatefulSetPodDeletionCheckpoint,
) (*requiredStatefulSetPodDeletionTarget, error) {
	checkpoint.Namespace = namespaceOrDefault(checkpoint.Namespace)
	checkpoint.StatefulSetName = strings.TrimSpace(checkpoint.StatefulSetName)
	if checkpoint.Namespace != ref.namespace || checkpoint.StatefulSetName != ref.name {
		return nil, k8serrors.NewConflict(
			schema.GroupResource{Group: "apps", Resource: "statefulsets"},
			ref.name,
			fmt.Errorf("required StatefulSet pod identity checkpoint targets %s/%s instead of %s", checkpoint.Namespace, checkpoint.StatefulSetName, cleanupResourceDisplayName(ref)),
		)
	}
	if checkpoint.StatefulSetWasFound && checkpoint.StatefulSetUID == "" {
		return nil, fmt.Errorf("required StatefulSet %s pod identity checkpoint has an empty StatefulSet UID", cleanupResourceDisplayName(ref))
	}
	target := &requiredStatefulSetPodDeletionTarget{
		ref:                     ref,
		statefulSetUID:          checkpoint.StatefulSetUID,
		statefulSetWasFound:     checkpoint.StatefulSetWasFound,
		podUIDs:                 make(map[string]types.UID, len(checkpoint.Pods)),
		ownerJobs:               make(map[string]requiredStatefulSetPodOwnerJobIdentity, len(checkpoint.OwnerJobs)),
		ownerJobsCaptured:       checkpoint.OwnerJobsCaptured,
		checkpointPersisted:     true,
		checkpointEverPersisted: true,
	}
	for _, pod := range checkpoint.Pods {
		name := strings.TrimSpace(pod.Name)
		if name == "" {
			return nil, fmt.Errorf("required StatefulSet %s pod identity checkpoint has an empty Pod name", cleanupResourceDisplayName(ref))
		}
		if _, duplicate := target.podUIDs[name]; duplicate {
			return nil, fmt.Errorf("required StatefulSet %s pod identity checkpoint repeats Pod %s", cleanupResourceDisplayName(ref), name)
		}
		if isStatefulSetOrdinalPodName(ref.name, name) && pod.UID == "" {
			return nil, fmt.Errorf("required StatefulSet %s pod identity checkpoint has an empty UID for ordinal Pod %s", cleanupResourceDisplayName(ref), name)
		}
		target.podUIDs[name] = pod.UID
	}
	if !checkpoint.OwnerJobsCaptured && len(checkpoint.OwnerJobs) > 0 {
		return nil, fmt.Errorf("required StatefulSet %s pod identity checkpoint has owner Jobs without a completed owner capture", cleanupResourceDisplayName(ref))
	}
	for _, ownerJob := range checkpoint.OwnerJobs {
		name := strings.TrimSpace(ownerJob.Name)
		podNames, err := requiredStatefulSetPodOwnerJobCheckpointPodNames(ownerJob)
		if err != nil || name == "" || ownerJob.UID == "" {
			return nil, fmt.Errorf("required StatefulSet %s pod identity checkpoint has an incomplete owner Job identity", cleanupResourceDisplayName(ref))
		}
		for _, podName := range podNames {
			pinnedPodUID, remembered := target.podUIDs[podName]
			if !remembered {
				return nil, fmt.Errorf("required StatefulSet %s pod identity checkpoint owner Job %s references unknown Pod %s", cleanupResourceDisplayName(ref), name, podName)
			}
			if pinnedPodUID == "" {
				return nil, fmt.Errorf("required StatefulSet %s pod identity checkpoint owner Job %s references Pod %s without a pinned UID", cleanupResourceDisplayName(ref), name, podName)
			}
		}
		if previous, duplicate := target.ownerJobs[name]; duplicate {
			return nil, fmt.Errorf("required StatefulSet %s pod identity checkpoint repeats owner Job %s with UIDs %q and %q", cleanupResourceDisplayName(ref), name, previous.uid, ownerJob.UID)
		}
		associatedPods := make(map[string]struct{}, len(podNames))
		for _, podName := range podNames {
			associatedPods[podName] = struct{}{}
		}
		target.ownerJobs[name] = requiredStatefulSetPodOwnerJobIdentity{
			podNames: associatedPods,
			name:     name,
			uid:      ownerJob.UID,
		}
	}
	return target, nil
}

func parseRequiredStatefulSetPodDeletionCheckpoint(raw string) (requiredStatefulSetPodDeletionCheckpoint, bool, error) {
	var document map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &document); err != nil {
		return requiredStatefulSetPodDeletionCheckpoint{}, false, err
	}
	encoded, found := document[requiredStatefulSetPodCheckpointKey]
	if !found {
		return requiredStatefulSetPodDeletionCheckpoint{}, false, nil
	}
	var checkpoint requiredStatefulSetPodDeletionCheckpoint
	if err := json.Unmarshal(encoded, &checkpoint); err != nil {
		return requiredStatefulSetPodDeletionCheckpoint{}, false, err
	}
	return checkpoint, true, nil
}

func marshalRequiredStatefulSetPodDeletionCheckpoint(raw string, checkpoint requiredStatefulSetPodDeletionCheckpoint) (string, error) {
	var document map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &document); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(checkpoint)
	if err != nil {
		return "", err
	}
	document[requiredStatefulSetPodCheckpointKey] = encoded
	result, err := json.Marshal(document)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

func requiredStatefulSetPodOwnerJobCheckpointPodNames(checkpoint requiredStatefulSetPodOwnerJobCheckpoint) ([]string, error) {
	podNames := make(map[string]struct{}, len(checkpoint.PodNames)+1)
	if legacyPodName := strings.TrimSpace(checkpoint.PodName); legacyPodName != "" {
		podNames[legacyPodName] = struct{}{}
	}
	for _, rawPodName := range checkpoint.PodNames {
		podName := strings.TrimSpace(rawPodName)
		if podName == "" {
			return nil, fmt.Errorf("owner Job checkpoint has an empty associated Pod name")
		}
		podNames[podName] = struct{}{}
	}
	return sortedRequiredStatefulSetPodOwnerJobPodNames(podNames), nil
}

func requiredStatefulSetPodCheckpointIsSubset(
	subset requiredStatefulSetPodDeletionCheckpoint,
	superset requiredStatefulSetPodDeletionCheckpoint,
) bool {
	if namespaceOrDefault(subset.Namespace) != namespaceOrDefault(superset.Namespace) ||
		strings.TrimSpace(subset.StatefulSetName) != strings.TrimSpace(superset.StatefulSetName) ||
		subset.StatefulSetUID != superset.StatefulSetUID ||
		subset.StatefulSetWasFound != superset.StatefulSetWasFound ||
		(subset.OwnerJobsCaptured && !superset.OwnerJobsCaptured) {
		return false
	}

	podUIDs := func(checkpoint requiredStatefulSetPodDeletionCheckpoint) (map[string]types.UID, bool) {
		result := make(map[string]types.UID, len(checkpoint.Pods))
		for _, pod := range checkpoint.Pods {
			name := strings.TrimSpace(pod.Name)
			if name == "" {
				return nil, false
			}
			if _, duplicate := result[name]; duplicate {
				return nil, false
			}
			result[name] = pod.UID
		}
		return result, true
	}
	subsetPods, valid := podUIDs(subset)
	if !valid {
		return false
	}
	supersetPods, valid := podUIDs(superset)
	if !valid {
		return false
	}
	for name, uid := range subsetPods {
		if supersetUID, exists := supersetPods[name]; !exists || supersetUID != uid {
			return false
		}
	}

	type ownerJobIdentity struct {
		uid      types.UID
		podNames map[string]struct{}
	}
	ownerJobs := func(
		checkpoint requiredStatefulSetPodDeletionCheckpoint,
		pods map[string]types.UID,
	) (map[string]ownerJobIdentity, bool) {
		if !checkpoint.OwnerJobsCaptured && len(checkpoint.OwnerJobs) > 0 {
			return nil, false
		}
		result := make(map[string]ownerJobIdentity, len(checkpoint.OwnerJobs))
		for _, ownerJob := range checkpoint.OwnerJobs {
			name := strings.TrimSpace(ownerJob.Name)
			podNames, err := requiredStatefulSetPodOwnerJobCheckpointPodNames(ownerJob)
			if err != nil || name == "" || ownerJob.UID == "" {
				return nil, false
			}
			if _, duplicate := result[name]; duplicate {
				return nil, false
			}
			associatedPods := make(map[string]struct{}, len(podNames))
			for _, podName := range podNames {
				if pinnedUID, exists := pods[podName]; !exists || pinnedUID == "" {
					return nil, false
				}
				associatedPods[podName] = struct{}{}
			}
			result[name] = ownerJobIdentity{uid: ownerJob.UID, podNames: associatedPods}
		}
		return result, true
	}
	subsetOwnerJobs, valid := ownerJobs(subset, subsetPods)
	if !valid {
		return false
	}
	supersetOwnerJobs, valid := ownerJobs(superset, supersetPods)
	if !valid {
		return false
	}
	for name, identity := range subsetOwnerJobs {
		supersetIdentity, exists := supersetOwnerJobs[name]
		if !exists || supersetIdentity.uid != identity.uid {
			return false
		}
		for podName := range identity.podNames {
			if _, exists := supersetIdentity.podNames[podName]; !exists {
				return false
			}
		}
	}
	return true
}

func requiredStatefulSetPodCheckpointsEqual(a, b requiredStatefulSetPodDeletionCheckpoint) bool {
	if namespaceOrDefault(a.Namespace) != namespaceOrDefault(b.Namespace) ||
		strings.TrimSpace(a.StatefulSetName) != strings.TrimSpace(b.StatefulSetName) ||
		a.StatefulSetUID != b.StatefulSetUID ||
		a.StatefulSetWasFound != b.StatefulSetWasFound ||
		a.OwnerJobsCaptured != b.OwnerJobsCaptured ||
		len(a.Pods) != len(b.Pods) ||
		len(a.OwnerJobs) != len(b.OwnerJobs) {
		return false
	}
	aPods := make(map[string]types.UID, len(a.Pods))
	for _, pod := range a.Pods {
		aPods[strings.TrimSpace(pod.Name)] = pod.UID
	}
	for _, pod := range b.Pods {
		uid, exists := aPods[strings.TrimSpace(pod.Name)]
		if !exists || uid != pod.UID {
			return false
		}
	}
	type ownerJobIdentity struct {
		uid      types.UID
		podNames string
	}
	aOwnerJobs := make(map[string]ownerJobIdentity, len(a.OwnerJobs))
	for _, ownerJob := range a.OwnerJobs {
		podNames, err := requiredStatefulSetPodOwnerJobCheckpointPodNames(ownerJob)
		if err != nil {
			return false
		}
		aOwnerJobs[strings.TrimSpace(ownerJob.Name)] = ownerJobIdentity{
			uid:      ownerJob.UID,
			podNames: strings.Join(podNames, "\x00"),
		}
	}
	for _, ownerJob := range b.OwnerJobs {
		name := strings.TrimSpace(ownerJob.Name)
		podNames, err := requiredStatefulSetPodOwnerJobCheckpointPodNames(ownerJob)
		if err != nil {
			return false
		}
		current, exists := aOwnerJobs[name]
		if !exists || current.podNames != strings.Join(podNames, "\x00") || current.uid != ownerJob.UID {
			return false
		}
	}
	return true
}

func sortedRequiredStatefulSetPodUIDNames(podUIDs map[string]types.UID) []string {
	names := make([]string, 0, len(podUIDs))
	for name := range podUIDs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedRequiredStatefulSetPodOwnerJobNames(ownerJobs map[string]requiredStatefulSetPodOwnerJobIdentity) []string {
	names := make([]string, 0, len(ownerJobs))
	for name := range ownerJobs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedRequiredStatefulSetPodOwnerJobPodNames(podNames map[string]struct{}) []string {
	names := make([]string, 0, len(podNames))
	for name := range podNames {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (c *CleanupResourcesJobCtl) verifyRequiredStatefulSetPodOwnerIdentity(
	ref cleanupResourceRef,
	statefulSet metav1.Object,
	statefulSetFound bool,
) error {
	target := c.requiredStatefulSetPodTarget
	if target == nil {
		return nil
	}
	if target.ref.namespace != ref.namespace || target.ref.name != ref.name {
		return fmt.Errorf("required StatefulSet pod target changed from %s to %s", cleanupResourceDisplayName(target.ref), cleanupResourceDisplayName(ref))
	}
	if !statefulSetFound {
		return nil
	}
	if !target.statefulSetWasFound {
		return k8serrors.NewConflict(
			schema.GroupResource{Group: "apps", Resource: "statefulsets"},
			ref.name,
			fmt.Errorf("StatefulSet %s appeared after the required pod identity scan", cleanupResourceDisplayName(ref)),
		)
	}
	if target.statefulSetUID != statefulSet.GetUID() {
		return k8serrors.NewConflict(
			schema.GroupResource{Group: "apps", Resource: "statefulsets"},
			ref.name,
			fmt.Errorf("StatefulSet %s UID changed from %q to %q after the required pod identity scan", cleanupResourceDisplayName(ref), target.statefulSetUID, statefulSet.GetUID()),
		)
	}
	return nil
}

func requiredStatefulSetPodOwnership(pod *corev1.Pod, statefulSetName string, statefulSetUID types.UID) (bool, bool) {
	if pod == nil {
		return false, false
	}
	ownedByTarget := false
	unprovenTargetOwner := false
	for _, owner := range pod.OwnerReferences {
		if !strings.EqualFold(owner.Kind, string(domainspec.KubeKindStatefulSet)) {
			continue
		}
		if statefulSetUID != "" && owner.UID == statefulSetUID {
			ownedByTarget = true
			continue
		}
		if strings.TrimSpace(owner.Name) == statefulSetName {
			unprovenTargetOwner = true
		}
	}
	return ownedByTarget, unprovenTargetOwner
}

func (c *CleanupResourcesJobCtl) getRequiredStatefulSetPodTarget(ctx context.Context, name string) (*corev1.Pod, bool, error) {
	target := c.requiredStatefulSetPodTarget
	if target == nil {
		return nil, false, fmt.Errorf("required StatefulSet pod identity scan was not initialized")
	}
	expectedUID, remembered := target.podUIDs[name]
	if !remembered {
		return nil, false, fmt.Errorf("required StatefulSet pod %s/%s was not remembered", target.ref.namespace, name)
	}
	pod, err := c.client.CoreV1().Pods(target.ref.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get required StatefulSet pod %s/%s: %w", target.ref.namespace, name, err)
	}
	if pod.UID != expectedUID {
		return nil, false, requiredStatefulSetPodConflict(target.ref, name, fmt.Sprintf("Pod UID changed from %q to %q", expectedUID, pod.UID))
	}
	return pod, true, nil
}

func (c *CleanupResourcesJobCtl) requiredStatefulSetPodOwnerJobs(
	ctx context.Context,
	component *model.ApplicationComponent,
	pod *corev1.Pod,
) ([]requiredStatefulSetPodOwnerJobTarget, error) {
	if pod == nil {
		return nil, nil
	}
	if err := requiredStatefulSetPodProtectionError(component, pod); err != nil {
		return nil, err
	}
	if pod.UID == "" {
		return nil, requiredStatefulSetPodConflict(c.requiredStatefulSetPodTarget.ref, pod.Name, "live Pod has an empty UID before owner Job deletion")
	}
	if strings.TrimSpace(pod.ResourceVersion) == "" {
		return nil, requiredStatefulSetPodConflict(c.requiredStatefulSetPodTarget.ref, pod.Name, "live Pod has an empty resourceVersion before owner Job deletion")
	}

	ownerJobs := make([]requiredStatefulSetPodOwnerJobTarget, 0, len(pod.OwnerReferences))
	ownerUIDsByName := make(map[string]types.UID)
	for _, owner := range pod.OwnerReferences {
		if !strings.EqualFold(owner.Kind, string(domainspec.KubeKindJob)) {
			continue
		}
		name := strings.TrimSpace(owner.Name)
		if name == "" {
			return nil, c.requiredStatefulSetPodOwnerJobConflict(pod, "<empty>", "Pod Job owner reference has an empty name")
		}
		if previousUID, duplicate := ownerUIDsByName[name]; duplicate {
			if previousUID != owner.UID {
				return nil, c.requiredStatefulSetPodOwnerJobConflict(pod, name, fmt.Sprintf("Pod has conflicting Job owner UIDs %q and %q", previousUID, owner.UID))
			}
			continue
		}
		ownerUIDsByName[name] = owner.UID

		job, err := c.client.BatchV1().Jobs(pod.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				if owner.UID == "" {
					return nil, c.requiredStatefulSetPodOwnerJobConflict(pod, name, "Pod Job owner reference has an empty UID")
				}
				continue
			}
			return nil, fmt.Errorf("inspect required StatefulSet pod owner Job %s/%s: %w", pod.Namespace, name, err)
		}
		if job == nil {
			return nil, fmt.Errorf("inspect required StatefulSet pod owner Job %s/%s: returned nil object", pod.Namespace, name)
		}
		if _, protected := cleanupResourceShareProtected(job.Labels); protected {
			return nil, requiredStatefulSetPodOwnerJobProtectionError(component, pod, name)
		}
		if owner.UID == "" {
			return nil, c.requiredStatefulSetPodOwnerJobConflict(pod, name, "Pod Job owner reference has an empty UID")
		}
		if job.UID == "" {
			return nil, c.requiredStatefulSetPodOwnerJobConflict(pod, name, "live Job has an empty UID")
		}
		if job.UID != owner.UID {
			return nil, c.requiredStatefulSetPodOwnerJobConflict(pod, name, fmt.Sprintf("live Job UID %q does not match Pod owner UID %q", job.UID, owner.UID))
		}
		if strings.TrimSpace(job.ResourceVersion) == "" {
			return nil, c.requiredStatefulSetPodOwnerJobConflict(pod, name, "live Job has an empty resourceVersion")
		}
		ownerJobs = append(ownerJobs, requiredStatefulSetPodOwnerJobTarget{
			name:            name,
			uid:             owner.UID,
			resourceVersion: job.ResourceVersion,
		})
	}
	return ownerJobs, nil
}

func requiredStatefulSetPodProtectionError(component *model.ApplicationComponent, pod *corev1.Pod) error {
	if pod == nil {
		return nil
	}
	if _, protected := cleanupResourceShareProtected(pod.Labels); !protected {
		return nil
	}
	return fmt.Errorf("required StatefulSet deletion blocked for component %s: pod %s/%s is protected by live share labels", component.Name, pod.Namespace, pod.Name)
}

func requiredStatefulSetPodOwnerJobProtectionError(component *model.ApplicationComponent, pod *corev1.Pod, ownerName string) error {
	return fmt.Errorf("required StatefulSet deletion blocked for component %s: pod %s/%s owner job %s is protected by live share labels", component.Name, pod.Namespace, pod.Name, ownerName)
}

func (c *CleanupResourcesJobCtl) requiredStatefulSetPodOwnerJobConflict(pod *corev1.Pod, name, detail string) error {
	statefulSet := "<unknown>"
	if c != nil && c.requiredStatefulSetPodTarget != nil {
		statefulSet = cleanupResourceDisplayName(c.requiredStatefulSetPodTarget.ref)
	}
	return k8serrors.NewConflict(
		schema.GroupResource{Group: "batch", Resource: "jobs"},
		name,
		fmt.Errorf("required StatefulSet %s pod %s/%s owner Job %s identity conflict: %s", statefulSet, pod.Namespace, pod.Name, name, detail),
	)
}

func (c *CleanupResourcesJobCtl) requiredStatefulSetPodNames() []string {
	if c == nil || c.requiredStatefulSetPodTarget == nil {
		return nil
	}
	names := make([]string, 0, len(c.requiredStatefulSetPodTarget.podUIDs))
	for name := range c.requiredStatefulSetPodTarget.podUIDs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func requiredStatefulSetPodConflict(ref cleanupResourceRef, name, detail string) error {
	return k8serrors.NewConflict(
		schema.GroupResource{Resource: "pods"},
		name,
		fmt.Errorf("required StatefulSet %s pod %s/%s identity conflict: %s", cleanupResourceDisplayName(ref), ref.namespace, name, detail),
	)
}
