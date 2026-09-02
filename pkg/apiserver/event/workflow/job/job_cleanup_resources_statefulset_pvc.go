package job

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type requiredStatefulSetPVCDeletionTarget struct {
	ref                 cleanupResourceRef
	templates           []string
	pvcUIDs             map[string]types.UID
	checkpointPersisted bool
}

const requiredStatefulSetPVCCheckpointKey = "statefulSetPVCDeletionTarget"

type requiredStatefulSetPVCDeletionCheckpoint struct {
	Namespace       string                                     `json:"namespace"`
	StatefulSetName string                                     `json:"statefulSetName"`
	Templates       []string                                   `json:"templates"`
	PVCs            []requiredStatefulSetPVCIdentityCheckpoint `json:"pvcs,omitempty"`
}

type requiredStatefulSetPVCIdentityCheckpoint struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}

func requiredStatefulSetCleanupRef(component *model.ApplicationComponent) (cleanupResourceRef, error) {
	if component == nil {
		return cleanupResourceRef{}, fmt.Errorf("required StatefulSet deletion component is nil")
	}
	result := GenerateStoreService(component)
	namespace := component.Namespace
	name := buildStoreSeverName(component.Name, component.ResourceNameKey())
	if result != nil {
		if statefulSet, ok := result.Service.(*appsv1.StatefulSet); ok && statefulSet != nil {
			namespace = pickNonEmpty(statefulSet.GetNamespace(), namespace)
			name = pickNonEmpty(statefulSet.GetName(), name)
		}
	}
	ref, ok := newCleanupResourceRef(config.ResourceStatefulSet, namespace, name, false)
	if !ok {
		return cleanupResourceRef{}, fmt.Errorf("required StatefulSet deletion target is empty for component %s", component.Name)
	}
	return ref, nil
}

func (c *CleanupResourcesJobCtl) ensureRequiredStatefulSetPVCDeletionAllowed(ctx context.Context, component *model.ApplicationComponent) error {
	if c == nil || c.job == nil || len(versionUpdateCleanupStatefulSetPVCTemplatesToDelete(c.job.InternalInfo)) == 0 {
		return nil
	}
	if err := c.ensureRequiredStatefulSetWorkflowTaskActive(ctx, "checking required StatefulSet PVC deletion"); err != nil {
		return err
	}
	if err := c.refreshRequiredStatefulSetPVCTargets(ctx, component); err != nil {
		return err
	}
	for _, name := range c.requiredStatefulSetPVCNames() {
		pvc, exists, err := c.getRequiredStatefulSetPVCTarget(ctx, name)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		if _, protected := cleanupResourceShareProtected(pvc.Labels); protected {
			return c.requiredStatefulSetPVCDeletionProtectedError(c.requiredStatefulSetPVCTarget.refForPVC(name))
		}
	}
	return nil
}

func (c *CleanupResourcesJobCtl) requiredStatefulSetPVCsGone(ctx context.Context, component *model.ApplicationComponent) (bool, error) {
	if c == nil || c.job == nil || len(versionUpdateCleanupStatefulSetPVCTemplatesToDelete(c.job.InternalInfo)) == 0 {
		return true, nil
	}
	if err := c.ensureRequiredStatefulSetWorkflowTaskActive(ctx, "reconciling required StatefulSet PVC deletion"); err != nil {
		return false, err
	}
	if err := c.refreshRequiredStatefulSetPVCTargets(ctx, component); err != nil {
		return false, err
	}

	pendingDelete := false
	for _, name := range c.requiredStatefulSetPVCNames() {
		targetRef := c.requiredStatefulSetPVCTarget.refForPVC(name)
		pvc, exists, err := c.getRequiredStatefulSetPVCTarget(ctx, name)
		if err != nil {
			return false, err
		}
		if !exists {
			continue
		}
		if pvc.DeletionTimestamp != nil {
			pendingDelete = true
			continue
		}
		if _, protected := cleanupResourceShareProtected(pvc.Labels); protected {
			return false, c.requiredStatefulSetPVCDeletionProtectedError(targetRef)
		}
		if pvc.UID == "" {
			return false, requiredStatefulSetPVCConflict(c.requiredStatefulSetPVCTarget.ref, name, "live PVC has an empty UID before deletion")
		}
		if strings.TrimSpace(pvc.ResourceVersion) == "" {
			return false, requiredStatefulSetPVCConflict(c.requiredStatefulSetPVCTarget.ref, name, "live PVC has an empty resourceVersion before deletion")
		}
		if err := c.ensureRequiredStatefulSetWorkflowTaskActive(ctx, fmt.Sprintf("deleting required StatefulSet PVC %s", cleanupResourceDisplayName(targetRef))); err != nil {
			return false, err
		}
		uid := pvc.UID
		resourceVersion := pvc.ResourceVersion
		deleteOptions := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{
			UID:             &uid,
			ResourceVersion: &resourceVersion,
		}}
		if err := c.client.CoreV1().PersistentVolumeClaims(targetRef.namespace).Delete(ctx, targetRef.name, deleteOptions); err != nil {
			if k8serrors.IsNotFound(err) {
				continue
			}
			if k8serrors.IsConflict(err) {
				livePVC, exists, refreshErr := c.getRequiredStatefulSetPVCTarget(ctx, name)
				if refreshErr != nil {
					return false, refreshErr
				}
				if !exists {
					continue
				}
				if _, protected := cleanupResourceShareProtected(livePVC.Labels); protected {
					return false, fmt.Errorf(
						"%v after delete returned Conflict: %w",
						c.requiredStatefulSetPVCDeletionProtectedError(targetRef), err,
					)
				}
				if livePVC.UID == "" {
					return false, requiredStatefulSetPVCConflict(c.requiredStatefulSetPVCTarget.ref, name, "live PVC has an empty UID after delete returned Conflict")
				}
				if strings.TrimSpace(livePVC.ResourceVersion) == "" {
					return false, requiredStatefulSetPVCConflict(c.requiredStatefulSetPVCTarget.ref, name, "live PVC has an empty resourceVersion after delete returned Conflict")
				}
				// A same-UID, unprotected PVC is still the pinned target. Let the
				// enclosing cleanup poll retry it with the refreshed resourceVersion.
				return false, nil
			}
			return false, fmt.Errorf("delete required StatefulSet PVC %s: %w", cleanupResourceDisplayName(targetRef), err)
		}
		pendingDelete = true
	}
	if pendingDelete {
		return false, nil
	}
	// Close the NotFound/replacement window once more before declaring success.
	// A replacement or a newly matching PVC is rejected by the pinned checkpoint.
	if err := c.refreshRequiredStatefulSetPVCTargets(ctx, component); err != nil {
		return false, err
	}
	return true, nil
}

func (c *CleanupResourcesJobCtl) refreshRequiredStatefulSetPVCTargets(ctx context.Context, component *model.ApplicationComponent) error {
	ref, templates, err := c.requiredStatefulSetPVCTargetSpec(component)
	if err != nil {
		return err
	}
	current, err := c.listRequiredStatefulSetPVCs(ctx, ref, templates)
	if err != nil {
		return err
	}
	if c.requiredStatefulSetPVCTarget == nil {
		if err := c.initializeRequiredStatefulSetPVCTarget(ctx, ref, templates, current); err != nil {
			return err
		}
	}
	target := c.requiredStatefulSetPVCTarget
	if target.ref.namespace != ref.namespace || target.ref.name != ref.name || !equalStringSlices(target.templates, templates) {
		return requiredStatefulSetPVCConflict(ref, ref.name, "PVC deletion target changed after the identity checkpoint")
	}
	for name, pvc := range current {
		expectedUID, remembered := target.pvcUIDs[name]
		if !remembered {
			return requiredStatefulSetPVCConflict(ref, name, "matching PVC appeared after the identity checkpoint")
		}
		if pvc.UID != expectedUID {
			return requiredStatefulSetPVCConflict(ref, name, fmt.Sprintf("PVC UID changed from %q to %q", expectedUID, pvc.UID))
		}
	}
	return c.persistRequiredStatefulSetPVCTarget(ctx)
}

func (c *CleanupResourcesJobCtl) requiredStatefulSetPVCTargetSpec(component *model.ApplicationComponent) (cleanupResourceRef, []string, error) {
	if c == nil || c.job == nil {
		return cleanupResourceRef{}, nil, fmt.Errorf("required StatefulSet PVC deletion controller is nil")
	}
	templates := versionUpdateCleanupStatefulSetPVCTemplatesToDelete(c.job.InternalInfo)
	if len(templates) == 0 {
		return cleanupResourceRef{}, nil, fmt.Errorf("required StatefulSet PVC deletion templates are empty")
	}
	if component == nil || component.ComponentType != config.StoreJob {
		return cleanupResourceRef{}, nil, fmt.Errorf("required StatefulSet PVC deletion is only valid for store components")
	}
	ref, err := requiredStatefulSetCleanupRef(component)
	if err != nil {
		return cleanupResourceRef{}, nil, err
	}
	return ref, templates, nil
}

func (c *CleanupResourcesJobCtl) listRequiredStatefulSetPVCs(
	ctx context.Context,
	ref cleanupResourceRef,
	templates []string,
) (map[string]*corev1.PersistentVolumeClaim, error) {
	list, err := c.client.CoreV1().PersistentVolumeClaims(ref.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list required StatefulSet PVC deletion targets in namespace %s: %w", ref.namespace, err)
	}
	result := make(map[string]*corev1.PersistentVolumeClaim)
	for i := range list.Items {
		pvc := &list.Items[i]
		for _, template := range templates {
			if isStatefulSetTemplatePVCName(template, ref.name, pvc.Name) {
				result[pvc.Name] = pvc.DeepCopy()
				break
			}
		}
	}
	return result, nil
}

func (c *CleanupResourcesJobCtl) initializeRequiredStatefulSetPVCTarget(
	ctx context.Context,
	ref cleanupResourceRef,
	templates []string,
	current map[string]*corev1.PersistentVolumeClaim,
) error {
	if c.job.Status == config.StatusRunning {
		existing, err := findExistingVersionUpdateCleanupJobInfo(ctx, c.store, c.job)
		if err != nil {
			return fmt.Errorf("load required StatefulSet PVC identity checkpoint: %w", err)
		}
		if existing == nil {
			return fmt.Errorf("precreated version update cleanup job info not found while loading required StatefulSet PVC identity")
		}
		if err := c.ensureVersionUpdateCleanupJobInfoOwned(existing, "loading required StatefulSet PVC identity checkpoint"); err != nil {
			return err
		}
		checkpoint, found, err := parseRequiredStatefulSetPVCDeletionCheckpoint(existing.InternalInfo)
		if err != nil {
			return fmt.Errorf("parse required StatefulSet PVC identity checkpoint: %w", err)
		}
		if found {
			target, err := requiredStatefulSetPVCTargetFromCheckpoint(ref, templates, checkpoint)
			if err != nil {
				return err
			}
			c.requiredStatefulSetPVCTarget = target
			c.job.InternalInfo = existing.InternalInfo
			return nil
		}
	}

	target := &requiredStatefulSetPVCDeletionTarget{
		ref:       ref,
		templates: append([]string(nil), templates...),
		pvcUIDs:   make(map[string]types.UID, len(current)),
	}
	for name, pvc := range current {
		if c.job.Status == config.StatusRunning && pvc.UID == "" {
			return requiredStatefulSetPVCConflict(ref, name, "PVC has an empty UID during the persisted identity scan")
		}
		target.pvcUIDs[name] = pvc.UID
	}
	c.requiredStatefulSetPVCTarget = target
	return nil
}

func (c *CleanupResourcesJobCtl) persistRequiredStatefulSetPVCTarget(ctx context.Context) error {
	target := c.requiredStatefulSetPVCTarget
	if target == nil {
		return nil
	}
	if c.job == nil || c.job.Status != config.StatusRunning {
		target.checkpointPersisted = true
		return nil
	}
	existing, err := findExistingVersionUpdateCleanupJobInfo(ctx, c.store, c.job)
	if err != nil {
		return fmt.Errorf("load required StatefulSet PVC identity checkpoint before persist: %w", err)
	}
	if existing == nil {
		return fmt.Errorf("precreated version update cleanup job info not found while persisting required StatefulSet PVC identity")
	}
	if err := c.ensureVersionUpdateCleanupJobInfoOwned(existing, "persisting required StatefulSet PVC identity checkpoint"); err != nil {
		return err
	}
	status := config.Status(strings.TrimSpace(existing.Status))
	if status != config.StatusRunning {
		return c.requiredStatefulSetPVCCheckpointStatusError(status, existing.Status)
	}
	c.job.InternalInfo = existing.InternalInfo
	checkpoint := requiredStatefulSetPVCCheckpointFromTarget(target)
	if current, found, err := parseRequiredStatefulSetPVCDeletionCheckpoint(existing.InternalInfo); err != nil {
		return fmt.Errorf("parse required StatefulSet PVC identity checkpoint before persist: %w", err)
	} else if found {
		if requiredStatefulSetPVCCheckpointsEqual(current, checkpoint) {
			target.checkpointPersisted = true
			c.job.InternalInfo = existing.InternalInfo
			return nil
		}
		return requiredStatefulSetPVCConflict(target.ref, target.ref.name, "persisted PVC identity checkpoint changed concurrently")
	}
	if target.checkpointPersisted {
		return requiredStatefulSetPVCConflict(target.ref, target.ref.name, "persisted PVC identity checkpoint disappeared")
	}
	nextInternalInfo, err := marshalRequiredStatefulSetPVCDeletionCheckpoint(existing.InternalInfo, checkpoint)
	if err != nil {
		return fmt.Errorf("marshal required StatefulSet PVC identity checkpoint: %w", err)
	}
	if err := c.ensureRequiredStatefulSetWorkflowTaskActive(ctx, "persisting required StatefulSet PVC identity checkpoint"); err != nil {
		return err
	}
	conditionalStore, ok := c.store.(datastore.ConditionalCompareAndSwap)
	if !ok {
		return fmt.Errorf("persist required StatefulSet PVC identity checkpoint: datastore does not support conditional compare-and-swap")
	}
	updated, err := conditionalStore.CompareAndSwapWithConditions(ctx, existing, versionUpdateCleanupJobInfoConditions(existing), map[string]interface{}{
		"internal_info": nextInternalInfo,
	})
	if err != nil {
		return fmt.Errorf("persist required StatefulSet PVC identity checkpoint: %w", err)
	}
	if !updated {
		latest, loadErr := findExistingVersionUpdateCleanupJobInfo(ctx, c.store, c.job)
		if loadErr != nil {
			return fmt.Errorf("reload required StatefulSet PVC identity checkpoint: %w", loadErr)
		}
		if latest != nil {
			if ownershipErr := c.ensureVersionUpdateCleanupJobInfoOwned(latest, "reloading required StatefulSet PVC identity checkpoint"); ownershipErr != nil {
				return ownershipErr
			}
			c.job.InternalInfo = latest.InternalInfo
			latestStatus := config.Status(strings.TrimSpace(latest.Status))
			if latestStatus != config.StatusRunning {
				return c.requiredStatefulSetPVCCheckpointStatusError(latestStatus, latest.Status)
			}
			current, found, parseErr := parseRequiredStatefulSetPVCDeletionCheckpoint(latest.InternalInfo)
			if parseErr != nil {
				return fmt.Errorf("parse concurrent required StatefulSet PVC identity checkpoint: %w", parseErr)
			}
			if found && requiredStatefulSetPVCCheckpointsEqual(current, checkpoint) {
				target.checkpointPersisted = true
				c.job.InternalInfo = latest.InternalInfo
				return nil
			}
		}
		return requiredStatefulSetPVCConflict(target.ref, target.ref.name, "PVC identity checkpoint changed concurrently")
	}
	target.checkpointPersisted = true
	c.job.InternalInfo = nextInternalInfo
	return nil
}

func (c *CleanupResourcesJobCtl) requiredStatefulSetPVCCheckpointStatusError(status config.Status, rawStatus string) error {
	if isTerminalVersionUpdateCleanupStatus(status) {
		return c.abortTerminalizedVersionUpdateCleanup(status, rawStatus)
	}
	return fmt.Errorf("version update cleanup job info for task %s component %s is %s while persisting required StatefulSet PVC identity, expected running", c.job.TaskID, resolveJobServiceName(c.job), rawStatus)
}

func requiredStatefulSetPVCCheckpointFromTarget(target *requiredStatefulSetPVCDeletionTarget) requiredStatefulSetPVCDeletionCheckpoint {
	checkpoint := requiredStatefulSetPVCDeletionCheckpoint{
		Namespace:       target.ref.namespace,
		StatefulSetName: target.ref.name,
		Templates:       append([]string(nil), target.templates...),
		PVCs:            make([]requiredStatefulSetPVCIdentityCheckpoint, 0, len(target.pvcUIDs)),
	}
	for _, name := range sortedRequiredStatefulSetPVCUIDNames(target.pvcUIDs) {
		checkpoint.PVCs = append(checkpoint.PVCs, requiredStatefulSetPVCIdentityCheckpoint{Name: name, UID: target.pvcUIDs[name]})
	}
	return checkpoint
}

func requiredStatefulSetPVCTargetFromCheckpoint(
	ref cleanupResourceRef,
	templates []string,
	checkpoint requiredStatefulSetPVCDeletionCheckpoint,
) (*requiredStatefulSetPVCDeletionTarget, error) {
	checkpoint.Namespace = namespaceOrDefault(checkpoint.Namespace)
	checkpoint.StatefulSetName = strings.TrimSpace(checkpoint.StatefulSetName)
	checkpoint.Templates = normalizedVersionUpdateCleanupPVCTemplates(checkpoint.Templates)
	if checkpoint.Namespace != ref.namespace || checkpoint.StatefulSetName != ref.name || !equalStringSlices(checkpoint.Templates, templates) {
		return nil, requiredStatefulSetPVCConflict(ref, ref.name, fmt.Sprintf("PVC identity checkpoint targets %s/%s templates %v instead of %s templates %v", checkpoint.Namespace, checkpoint.StatefulSetName, checkpoint.Templates, cleanupResourceDisplayName(ref), templates))
	}
	target := &requiredStatefulSetPVCDeletionTarget{
		ref:                 ref,
		templates:           append([]string(nil), templates...),
		pvcUIDs:             make(map[string]types.UID, len(checkpoint.PVCs)),
		checkpointPersisted: true,
	}
	for _, pvc := range checkpoint.PVCs {
		name := strings.TrimSpace(pvc.Name)
		if name == "" {
			return nil, fmt.Errorf("required StatefulSet %s PVC identity checkpoint has an empty PVC name", cleanupResourceDisplayName(ref))
		}
		if _, duplicate := target.pvcUIDs[name]; duplicate {
			return nil, fmt.Errorf("required StatefulSet %s PVC identity checkpoint repeats PVC %s", cleanupResourceDisplayName(ref), name)
		}
		if !matchesStatefulSetTemplatePVCName(templates, ref.name, name) {
			return nil, fmt.Errorf("required StatefulSet %s PVC identity checkpoint contains unrelated PVC %s", cleanupResourceDisplayName(ref), name)
		}
		if pvc.UID == "" {
			return nil, fmt.Errorf("required StatefulSet %s PVC identity checkpoint has an empty UID for PVC %s", cleanupResourceDisplayName(ref), name)
		}
		target.pvcUIDs[name] = pvc.UID
	}
	return target, nil
}

func parseRequiredStatefulSetPVCDeletionCheckpoint(raw string) (requiredStatefulSetPVCDeletionCheckpoint, bool, error) {
	var document map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &document); err != nil {
		return requiredStatefulSetPVCDeletionCheckpoint{}, false, err
	}
	encoded, found := document[requiredStatefulSetPVCCheckpointKey]
	if !found {
		return requiredStatefulSetPVCDeletionCheckpoint{}, false, nil
	}
	var checkpoint requiredStatefulSetPVCDeletionCheckpoint
	if err := json.Unmarshal(encoded, &checkpoint); err != nil {
		return requiredStatefulSetPVCDeletionCheckpoint{}, false, err
	}
	return checkpoint, true, nil
}

func marshalRequiredStatefulSetPVCDeletionCheckpoint(raw string, checkpoint requiredStatefulSetPVCDeletionCheckpoint) (string, error) {
	var document map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &document); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(checkpoint)
	if err != nil {
		return "", err
	}
	document[requiredStatefulSetPVCCheckpointKey] = encoded
	result, err := json.Marshal(document)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

func requiredStatefulSetPVCCheckpointsEqual(a, b requiredStatefulSetPVCDeletionCheckpoint) bool {
	a.Namespace = namespaceOrDefault(a.Namespace)
	b.Namespace = namespaceOrDefault(b.Namespace)
	a.StatefulSetName = strings.TrimSpace(a.StatefulSetName)
	b.StatefulSetName = strings.TrimSpace(b.StatefulSetName)
	a.Templates = normalizedVersionUpdateCleanupPVCTemplates(a.Templates)
	b.Templates = normalizedVersionUpdateCleanupPVCTemplates(b.Templates)
	if a.Namespace != b.Namespace || a.StatefulSetName != b.StatefulSetName || !equalStringSlices(a.Templates, b.Templates) || len(a.PVCs) != len(b.PVCs) {
		return false
	}
	aPVCs := make(map[string]types.UID, len(a.PVCs))
	for _, pvc := range a.PVCs {
		aPVCs[strings.TrimSpace(pvc.Name)] = pvc.UID
	}
	for _, pvc := range b.PVCs {
		uid, exists := aPVCs[strings.TrimSpace(pvc.Name)]
		if !exists || uid != pvc.UID {
			return false
		}
	}
	return true
}

func (c *CleanupResourcesJobCtl) getRequiredStatefulSetPVCTarget(ctx context.Context, name string) (*corev1.PersistentVolumeClaim, bool, error) {
	target := c.requiredStatefulSetPVCTarget
	if target == nil {
		return nil, false, fmt.Errorf("required StatefulSet PVC identity scan was not initialized")
	}
	expectedUID, remembered := target.pvcUIDs[name]
	if !remembered {
		return nil, false, fmt.Errorf("required StatefulSet PVC %s/%s was not remembered", target.ref.namespace, name)
	}
	pvc, err := c.client.CoreV1().PersistentVolumeClaims(target.ref.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get required StatefulSet PVC %s/%s: %w", target.ref.namespace, name, err)
	}
	if pvc.UID != expectedUID {
		return nil, false, requiredStatefulSetPVCConflict(target.ref, name, fmt.Sprintf("PVC UID changed from %q to %q", expectedUID, pvc.UID))
	}
	return pvc, true, nil
}

func (c *CleanupResourcesJobCtl) requiredStatefulSetPVCNames() []string {
	if c == nil || c.requiredStatefulSetPVCTarget == nil {
		return nil
	}
	return sortedRequiredStatefulSetPVCUIDNames(c.requiredStatefulSetPVCTarget.pvcUIDs)
}

func sortedRequiredStatefulSetPVCUIDNames(pvcUIDs map[string]types.UID) []string {
	names := make([]string, 0, len(pvcUIDs))
	for name := range pvcUIDs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (target *requiredStatefulSetPVCDeletionTarget) refForPVC(name string) cleanupResourceRef {
	ref, _ := newCleanupResourceRef(config.ResourcePVC, target.ref.namespace, name, false)
	return ref
}

func matchesStatefulSetTemplatePVCName(templates []string, statefulSetName, pvcName string) bool {
	for _, template := range templates {
		if isStatefulSetTemplatePVCName(template, statefulSetName, pvcName) {
			return true
		}
	}
	return false
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func requiredStatefulSetPVCConflict(ref cleanupResourceRef, name, detail string) error {
	return k8serrors.NewConflict(
		schema.GroupResource{Resource: "persistentvolumeclaims"},
		name,
		fmt.Errorf("required StatefulSet %s PVC %s/%s identity conflict: %s", cleanupResourceDisplayName(ref), ref.namespace, name, detail),
	)
}

func (c *CleanupResourcesJobCtl) requiredStatefulSetPVCDeletionProtectedError(ref cleanupResourceRef) error {
	componentName := "<unknown>"
	if c != nil && c.job != nil {
		if name := strings.TrimSpace(resolveJobServiceName(c.job)); name != "" {
			componentName = name
		}
	}
	return fmt.Errorf("required StatefulSet PVC deletion blocked for component %s: %s is protected by live share labels", componentName, cleanupResourceDisplayName(ref))
}
