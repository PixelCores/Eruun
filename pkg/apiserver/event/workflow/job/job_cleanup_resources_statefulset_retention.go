package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

type statefulSetRetentionTarget struct {
	ref       cleanupResourceRef
	uid       types.UID
	templates []string
}

const requiredStatefulSetSafetyRefreshMaxAttempts = 3

type requiredStatefulSetSafetyRefreshError struct {
	err       error
	retryable bool
}

func (e *requiredStatefulSetSafetyRefreshError) Error() string { return e.err.Error() }

func (e *requiredStatefulSetSafetyRefreshError) Unwrap() error { return e.err }

func newRequiredStatefulSetSafetyRefreshError(err error) *requiredStatefulSetSafetyRefreshError {
	if err == nil {
		return nil
	}
	return &requiredStatefulSetSafetyRefreshError{
		err: err,
		retryable: k8serrors.IsTimeout(err) || k8serrors.IsServerTimeout(err) ||
			k8serrors.IsTooManyRequests(err) || k8serrors.IsServiceUnavailable(err) || k8serrors.IsInternalError(err),
	}
}

// ensureRequiredStatefulSetSafetyCurrent refreshes the destructive cleanup
// fence without changing either Kubernetes objects or persisted checkpoints.
// Mutation gates call it after the workflow task status check so a safety
// decision made during the initial preflight cannot go stale during retention
// convergence or before the final StatefulSet delete.
func (c *CleanupResourcesJobCtl) ensureRequiredStatefulSetSafetyCurrent(ctx context.Context) error {
	if c == nil || c.job == nil || c.job.Status != config.StatusRunning ||
		!versionUpdateCleanupRequiresStatefulSetDeletion(c.job.InternalInfo) {
		return nil
	}
	target := c.requiredStatefulSetPodTarget
	if target == nil || !target.checkpointPersisted {
		// The initial preflight establishes and persists this identity before any
		// destructive mutation. Avoid recursively initializing that checkpoint.
		return nil
	}
	if c.client == nil {
		return fmt.Errorf("refresh required StatefulSet safety: client is nil")
	}
	if target.statefulSetWasFound && target.statefulSetUID == "" {
		return fmt.Errorf("required StatefulSet %s safety checkpoint has an empty StatefulSet UID", cleanupResourceDisplayName(target.ref))
	}
	if !target.statefulSetWasFound && target.statefulSetUID != "" {
		return fmt.Errorf("required StatefulSet %s safety checkpoint records UID %q for a missing StatefulSet", cleanupResourceDisplayName(target.ref), target.statefulSetUID)
	}

	component, err := cleanupComponentFromJobInfo(c.job)
	if err != nil {
		return fmt.Errorf("load required StatefulSet cleanup component while refreshing safety: %w", err)
	}
	requiredRef, err := requiredStatefulSetCleanupRef(component)
	if err != nil {
		return err
	}
	if target.ref.namespace != requiredRef.namespace || target.ref.name != requiredRef.name {
		return fmt.Errorf("required StatefulSet safety target changed from %s to %s", cleanupResourceDisplayName(target.ref), cleanupResourceDisplayName(requiredRef))
	}

	statefulSet, err := c.client.AppsV1().StatefulSets(requiredRef.namespace).Get(ctx, requiredRef.name, metav1.GetOptions{})
	statefulSetFound := err == nil
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("get required StatefulSet %s while refreshing safety: %w", cleanupResourceDisplayName(requiredRef), err)
	}
	if statefulSetFound && statefulSet == nil {
		return fmt.Errorf("get required StatefulSet %s while refreshing safety: returned nil object", cleanupResourceDisplayName(requiredRef))
	}
	if err := c.verifyRequiredStatefulSetPodOwnerIdentity(requiredRef, statefulSet, statefulSetFound); err != nil {
		return err
	}
	if statefulSetFound {
		if statefulSet.UID == "" {
			return fmt.Errorf("required StatefulSet %s has an empty UID while refreshing safety", cleanupResourceDisplayName(requiredRef))
		}
		if _, protected := cleanupResourceShareProtected(statefulSet.Labels); protected {
			return c.requiredStatefulSetDeletionProtectedError(requiredRef)
		}
	}
	return c.ensureRequiredStatefulSetDeletionTargetsAllowed(ctx, component, requiredRef)
}

func (c *CleanupResourcesJobCtl) prepareRequiredStatefulSetDeletion(ctx context.Context, component *model.ApplicationComponent) error {
	if c == nil || c.job == nil || !versionUpdateCleanupRequiresStatefulSetDeletion(c.job.InternalInfo) {
		return nil
	}
	ref, err := requiredStatefulSetCleanupRef(component)
	if err != nil {
		return err
	}
	templates := versionUpdateCleanupStatefulSetPVCTemplatesToDelete(c.job.InternalInfo)
	if generated := GenerateStoreService(component); generated != nil {
		if statefulSet, ok := generated.Service.(*appsv1.StatefulSet); ok && statefulSet != nil {
			templates = append(templates, statefulSetVolumeClaimTemplateNames(statefulSet)...)
		}
	}
	// The live StatefulSet may already be gone on a retry. Keep the marker's
	// template contract so orphaned/scaled-down PVCs are still made safe before
	// the cleanup continues.
	checkpointUID := types.UID("")
	if target := c.requiredStatefulSetPodTarget; target != nil {
		if target.ref.namespace != ref.namespace || target.ref.name != ref.name {
			return fmt.Errorf("required StatefulSet pod target changed from %s to %s before PVC retention", cleanupResourceDisplayName(target.ref), cleanupResourceDisplayName(ref))
		}
		switch {
		case target.statefulSetWasFound && target.statefulSetUID == "":
			return fmt.Errorf("required StatefulSet %s pod identity checkpoint has an empty StatefulSet UID", cleanupResourceDisplayName(ref))
		case !target.statefulSetWasFound && target.statefulSetUID != "":
			return fmt.Errorf("required StatefulSet %s pod identity checkpoint records UID %q for a missing StatefulSet", cleanupResourceDisplayName(ref), target.statefulSetUID)
		case target.statefulSetWasFound:
			checkpointUID = target.statefulSetUID
		}
	}
	if err := c.rememberStatefulSetRetentionTarget(ref, checkpointUID, templates); err != nil {
		return err
	}
	return c.ensureStatefulSetPVCRetention(ctx, ref)
}

func (c *CleanupResourcesJobCtl) ensureStatefulSetPVCRetention(ctx context.Context, ref cleanupResourceRef) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("client is nil")
	}
	if ref.kind != domainspec.ResourceStatefulSet || strings.TrimSpace(ref.name) == "" {
		return fmt.Errorf("invalid StatefulSet retention target %s", cleanupResourceDisplayName(ref))
	}
	ref.namespace = namespaceOrDefault(ref.namespace)
	timeout := time.Duration(c.timeout()) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(config.DeployTimeout) * time.Second
	}
	var lastRetryableErr error
	err := wait.PollUntilContextTimeout(ctx, cleanupPollInterval, timeout, true, func(checkCtx context.Context) (bool, error) {
		if err := c.ensureRequiredStatefulSetWorkflowTaskActive(checkCtx, fmt.Sprintf("reconciling StatefulSet %s PVC retention", cleanupResourceDisplayName(ref))); err != nil {
			if retryableStatefulSetRetentionError(err) {
				lastRetryableErr = err
				return false, nil
			}
			return false, err
		}
		statefulSet, err := c.client.AppsV1().StatefulSets(ref.namespace).Get(checkCtx, ref.name, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return true, nil
			}
			if retryableStatefulSetRetentionError(err) {
				lastRetryableErr = err
				return false, nil
			}
			return false, fmt.Errorf("get StatefulSet %s before retention update: %w", cleanupResourceDisplayName(ref), err)
		}
		if err := c.verifyRequiredStatefulSetPodOwnerIdentity(ref, statefulSet, true); err != nil {
			return false, err
		}
		if err := c.rememberStatefulSetRetentionTarget(ref, statefulSet.UID, statefulSetVolumeClaimTemplateNames(statefulSet)); err != nil {
			return false, err
		}
		lastRetryableErr = nil

		if !statefulSetPVCRetentionIsRetain(statefulSet) {
			updated := statefulSet.DeepCopy()
			updated.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			}
			if err := c.ensureRequiredStatefulSetWorkflowTaskActive(checkCtx, fmt.Sprintf("updating StatefulSet %s PVC retention", cleanupResourceDisplayName(ref))); err != nil {
				if retryableStatefulSetRetentionError(err) {
					lastRetryableErr = err
					return false, nil
				}
				return false, err
			}
			statefulSet, err = c.client.AppsV1().StatefulSets(ref.namespace).Update(checkCtx, updated, metav1.UpdateOptions{})
			if err != nil {
				if k8serrors.IsNotFound(err) {
					return true, nil
				}
				if retryableStatefulSetRetentionError(err) {
					lastRetryableErr = err
					return false, nil
				}
				return false, fmt.Errorf("set StatefulSet %s PVC retention to Retain: %w", cleanupResourceDisplayName(ref), err)
			}
			if statefulSet == nil {
				return false, fmt.Errorf("set StatefulSet %s PVC retention to Retain: update returned nil object", cleanupResourceDisplayName(ref))
			}
		}

		// A real API server increments generation for the spec update. Waiting for
		// the controller to observe it prevents a stale reconciliation from adding
		// deletion owner references after the worker removes them below. Fake
		// clients leave the generation at zero, so the same convergence path stays
		// testable.
		if statefulSet.Generation > 0 && statefulSet.Status.ObservedGeneration < statefulSet.Generation {
			return false, nil
		}
		converged, err := c.statefulSetPVCOwnerReferencesConverged(checkCtx, statefulSet)
		if err != nil {
			if retryableStatefulSetRetentionError(err) {
				lastRetryableErr = err
				return false, nil
			}
			return false, err
		}
		lastRetryableErr = nil
		return converged, nil
	})
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, wait.ErrWaitTimeout) {
		if lastRetryableErr != nil {
			return NewStatusError(config.StatusTimeout, fmt.Errorf("wait for StatefulSet %s PVC retention convergence: %w", cleanupResourceDisplayName(ref), lastRetryableErr))
		}
		return NewStatusError(config.StatusTimeout, fmt.Errorf("wait for StatefulSet %s PVC retention convergence timeout", cleanupResourceDisplayName(ref)))
	}
	return err
}

func (c *CleanupResourcesJobCtl) statefulSetPVCOwnerReferencesConverged(ctx context.Context, statefulSet *appsv1.StatefulSet) (bool, error) {
	if statefulSet == nil {
		return false, fmt.Errorf("StatefulSet is nil while checking PVC retention")
	}
	templates := statefulSetVolumeClaimTemplateNames(statefulSet)
	if len(templates) == 0 {
		return true, nil
	}
	ref, ok := newCleanupResourceRef(domainspec.ResourceStatefulSet, statefulSet.Namespace, statefulSet.Name, false)
	if !ok {
		return false, fmt.Errorf("StatefulSet identity is empty while checking PVC retention")
	}
	if err := c.rememberStatefulSetRetentionTarget(ref, statefulSet.UID, templates); err != nil {
		return false, err
	}
	return c.ensureStatefulSetPVCOwnerReferencesRemoved(ctx, ref, statefulSet.UID, templates)
}

func (c *CleanupResourcesJobCtl) ensureStatefulSetPVCOwnerReferencesRemoved(
	ctx context.Context,
	ref cleanupResourceRef,
	statefulSetUID types.UID,
	templates []string,
) (bool, error) {
	list, err := c.client.CoreV1().PersistentVolumeClaims(ref.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("list StatefulSet %s PVCs while waiting for retention convergence: %w", cleanupResourceDisplayName(ref), err)
	}
	var plannedDeletionTemplates []string
	if c.job != nil {
		plannedDeletionTemplates = versionUpdateCleanupStatefulSetPVCTemplatesToDelete(c.job.InternalInfo)
	}
	updatedAny := false
	for i := range list.Items {
		pvc := &list.Items[i]
		if !statefulSetVolumeClaimTemplatePVC(ref.name, templates, pvc.Name) {
			continue
		}
		if pvc.DeletionTimestamp != nil && !statefulSetVolumeClaimTemplatePVC(
			ref.name,
			plannedDeletionTemplates,
			pvc.Name,
		) {
			return false, fmt.Errorf(
				"retain StatefulSet %s PVC %s/%s: PVC is already terminating and is not planned for deletion",
				cleanupResourceDisplayName(ref), ref.namespace, pvc.Name,
			)
		}
		retainedOwnerReferences, changed := retainedStatefulSetPVCOwnerReferences(ref.name, statefulSetUID, pvc.OwnerReferences)
		if !changed {
			continue
		}
		patch, err := json.Marshal(map[string]any{
			"metadata": map[string]any{
				"resourceVersion": pvc.ResourceVersion,
				"ownerReferences": retainedOwnerReferences,
			},
		})
		if err != nil {
			return false, fmt.Errorf("build StatefulSet %s PVC %s/%s owner-reference patch: %w", cleanupResourceDisplayName(ref), ref.namespace, pvc.Name, err)
		}
		if err := c.ensureRequiredStatefulSetWorkflowTaskActive(ctx, fmt.Sprintf("patching StatefulSet %s PVC %s/%s owner references", cleanupResourceDisplayName(ref), ref.namespace, pvc.Name)); err != nil {
			return false, err
		}
		if _, err := c.client.CoreV1().PersistentVolumeClaims(ref.namespace).Patch(ctx, pvc.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
			if retryableStatefulSetRetentionError(err) {
				return false, err
			}
			return false, fmt.Errorf("remove StatefulSet %s deletion owner references from PVC %s/%s: %w", cleanupResourceDisplayName(ref), ref.namespace, pvc.Name, err)
		}
		updatedAny = true
	}
	return !updatedAny, nil
}

func (c *CleanupResourcesJobCtl) rememberedStatefulSetPVCOwnerReferencesConverged(ctx context.Context) (bool, error) {
	if c == nil || len(c.statefulSetRetentionTargets) == 0 {
		return true, nil
	}
	for _, target := range c.statefulSetRetentionTargets {
		converged, err := c.ensureStatefulSetPVCOwnerReferencesRemoved(ctx, target.ref, target.uid, target.templates)
		if err != nil {
			if retryableStatefulSetRetentionError(err) {
				return false, nil
			}
			return false, err
		}
		if !converged {
			return false, nil
		}
	}
	return true, nil
}

func (c *CleanupResourcesJobCtl) rememberStatefulSetRetentionTarget(ref cleanupResourceRef, uid types.UID, templates []string) error {
	if c == nil || ref.kind != domainspec.ResourceStatefulSet || strings.TrimSpace(ref.name) == "" {
		return nil
	}
	if c.statefulSetRetentionTargets == nil {
		c.statefulSetRetentionTargets = make(map[string]statefulSetRetentionTarget)
	}
	key := ref.namespace + "/" + ref.name
	existing := c.statefulSetRetentionTargets[key]
	if existing.ref.name == "" {
		existing.ref = ref
	}
	if uid != "" {
		if existing.uid != "" && existing.uid != uid {
			return k8serrors.NewConflict(
				schema.GroupResource{Group: "apps", Resource: "statefulsets"},
				ref.name,
				fmt.Errorf(
					"StatefulSet %s changed during PVC retention convergence: UID %q replaced by %q",
					cleanupResourceDisplayName(ref), existing.uid, uid,
				),
			)
		}
		existing.uid = uid
	}
	seen := make(map[string]struct{}, len(existing.templates)+len(templates))
	for _, template := range existing.templates {
		if template = strings.TrimSpace(template); template != "" {
			seen[template] = struct{}{}
		}
	}
	for _, template := range templates {
		if template = strings.TrimSpace(template); template != "" {
			seen[template] = struct{}{}
		}
	}
	existing.templates = existing.templates[:0]
	for template := range seen {
		existing.templates = append(existing.templates, template)
	}
	sort.Strings(existing.templates)
	c.statefulSetRetentionTargets[key] = existing
	return nil
}

func statefulSetVolumeClaimTemplateNames(statefulSet *appsv1.StatefulSet) []string {
	if statefulSet == nil {
		return nil
	}
	templates := make([]string, 0, len(statefulSet.Spec.VolumeClaimTemplates))
	for i := range statefulSet.Spec.VolumeClaimTemplates {
		if name := strings.TrimSpace(statefulSet.Spec.VolumeClaimTemplates[i].Name); name != "" {
			templates = append(templates, name)
		}
	}
	sort.Strings(templates)
	return templates
}

func retainedStatefulSetPVCOwnerReferences(
	statefulSetName string,
	statefulSetUID types.UID,
	ownerReferences []metav1.OwnerReference,
) ([]metav1.OwnerReference, bool) {
	retained := make([]metav1.OwnerReference, 0, len(ownerReferences))
	changed := false
	for _, owner := range ownerReferences {
		remove := strings.EqualFold(owner.Kind, string(domainspec.KubeKindStatefulSet)) &&
			statefulSetOwnerReferenceMatches(statefulSetName, statefulSetUID, owner.Name, owner.UID)
		remove = remove || strings.EqualFold(owner.Kind, "Pod") && isStatefulSetOrdinalPodName(statefulSetName, owner.Name)
		if remove {
			changed = true
			continue
		}
		retained = append(retained, owner)
	}
	return retained, changed
}

func statefulSetPVCRetentionIsRetain(statefulSet *appsv1.StatefulSet) bool {
	if statefulSet == nil || statefulSet.Spec.PersistentVolumeClaimRetentionPolicy == nil {
		return false
	}
	policy := statefulSet.Spec.PersistentVolumeClaimRetentionPolicy
	return policy.WhenDeleted == appsv1.RetainPersistentVolumeClaimRetentionPolicyType &&
		policy.WhenScaled == appsv1.RetainPersistentVolumeClaimRetentionPolicyType
}

func statefulSetVolumeClaimTemplatePVC(statefulSetName string, templates []string, pvcName string) bool {
	for _, template := range templates {
		if isStatefulSetTemplatePVCName(template, statefulSetName, pvcName) {
			return true
		}
	}
	return false
}

func statefulSetPVCDeletionOwnerReferencePresent(statefulSet *appsv1.StatefulSet, pvc *corev1.PersistentVolumeClaim) bool {
	if statefulSet == nil || pvc == nil {
		return false
	}
	for _, owner := range pvc.OwnerReferences {
		switch {
		case strings.EqualFold(owner.Kind, string(domainspec.KubeKindStatefulSet)) && statefulSetOwnerReferenceMatches(statefulSet.Name, statefulSet.UID, owner.Name, owner.UID):
			return true
		case strings.EqualFold(owner.Kind, "Pod") && isStatefulSetOrdinalPodName(statefulSet.Name, owner.Name):
			return true
		}
	}
	return false
}

func statefulSetOwnerReferenceMatches(statefulSetName string, statefulSetUID types.UID, ownerName string, ownerUID types.UID) bool {
	if statefulSetUID != "" && ownerUID != "" {
		return statefulSetUID == ownerUID
	}
	return strings.TrimSpace(statefulSetName) != "" && statefulSetName == strings.TrimSpace(ownerName)
}

func isStatefulSetOrdinalPodName(statefulSetName, podName string) bool {
	prefix := strings.TrimSpace(statefulSetName) + "-"
	if prefix == "-" || !strings.HasPrefix(podName, prefix) {
		return false
	}
	ordinal, err := strconv.Atoi(strings.TrimPrefix(podName, prefix))
	return err == nil && ordinal >= 0
}

func retryableStatefulSetRetentionError(err error) bool {
	var safetyRefreshErr *requiredStatefulSetSafetyRefreshError
	if errors.As(err, &safetyRefreshErr) {
		return safetyRefreshErr.retryable
	}
	return k8serrors.IsConflict(err) || k8serrors.IsTimeout(err) || k8serrors.IsServerTimeout(err) ||
		k8serrors.IsTooManyRequests(err) || k8serrors.IsServiceUnavailable(err) || k8serrors.IsInternalError(err)
}
