package jobcleanup

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

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
	if c.Job.Status == config.StatusRunning {
		existing, err := jobinfo.FindExistingVersionUpdateCleanupJobInfo(ctx, c.Store, c.Job)
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
			c.Job.InternalInfo = existing.InternalInfo
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
	if c.Job == nil || c.Job.Status != config.StatusRunning {
		// The production path reaches this helper only after markRunning. Direct
		// package-level helper calls do not have a persistent execution to resume.
		target.checkpointPersisted = true
		return nil
	}
	conditionalStore, ok := c.Store.(datastore.ConditionalCompareAndSwap)
	if !ok {
		return fmt.Errorf("persist required StatefulSet pod identity checkpoint: datastore does not support conditional compare-and-swap")
	}

	for attempt := 1; attempt <= requiredStatefulSetPodCheckpointPersistMaxAttempts; attempt++ {
		existing, err := jobinfo.FindExistingVersionUpdateCleanupJobInfo(ctx, c.Store, c.Job)
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
		c.Job.InternalInfo = existing.InternalInfo
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
				c.Job.InternalInfo = existing.InternalInfo
				return nil
			case !persistedIsSubset:
				return k8serrors.NewConflict(
					schema.GroupResource{Resource: "jobinfos"},
					c.Job.TaskID,
					fmt.Errorf("required StatefulSet %s pod identity checkpoint forked concurrently", cleanupResourceDisplayName(target.ref)),
				)
			}
		} else if target.checkpointEverPersisted {
			return k8serrors.NewConflict(
				schema.GroupResource{Resource: "jobinfos"},
				c.Job.TaskID,
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
		updated, err := conditionalStore.CompareAndSwapWithConditions(ctx, existing, jobinfo.VersionUpdateCleanupJobInfoConditions(existing), map[string]interface{}{
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
		c.Job.InternalInfo = nextInternalInfo
		return nil
	}
	return k8serrors.NewConflict(
		schema.GroupResource{Resource: "jobinfos"},
		c.Job.TaskID,
		fmt.Errorf("required StatefulSet %s pod identity checkpoint did not converge after %d attempts", cleanupResourceDisplayName(target.ref), requiredStatefulSetPodCheckpointPersistMaxAttempts),
	)
}

func (c *CleanupResourcesJobCtl) requiredStatefulSetPodCheckpointStatusError(status config.Status, rawStatus string) error {
	if isTerminalVersionUpdateCleanupStatus(status) {
		return c.abortTerminalizedVersionUpdateCleanup(status, rawStatus)
	}
	return fmt.Errorf("version update cleanup job info for task %s component %s is %s while persisting required StatefulSet pod identity, expected running", c.Job.TaskID, jobinfo.ResolveJobServiceName(c.Job), rawStatus)
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

	subsetPods, valid := requiredStatefulSetCheckpointPodUIDs(subset)
	if !valid {
		return false
	}
	supersetPods, valid := requiredStatefulSetCheckpointPodUIDs(superset)
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

func requiredStatefulSetCheckpointPodUIDs(checkpoint requiredStatefulSetPodDeletionCheckpoint) (map[string]types.UID, bool) {
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
