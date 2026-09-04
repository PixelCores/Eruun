package application

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"

	"github.com/PixelCores/Eruun/pkg/apiserver/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/schedulelock"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type adoptedLifecycleOperation string

const (
	adoptedLifecycleStop    adoptedLifecycleOperation = "stop"
	adoptedLifecycleStart   adoptedLifecycleOperation = "start"
	adoptedLifecycleRestart adoptedLifecycleOperation = "restart"
)

type adoptedLifecycleTarget struct {
	component      *model.ApplicationComponent
	kind           string
	namespace      string
	name           string
	uid            string
	liveReplicas   int32
	resumeReplicas int32
}

func isAdoptedApplication(app *model.Applications) bool {
	return app != nil && app.EffectiveManagementMode() == config.ManagementModeAdopted
}

func (c *applicationsServiceImpl) withAdoptedLifecycleLock(
	ctx context.Context,
	appID string,
	operation adoptedLifecycleOperation,
	run func(context.Context, *model.Applications, []*model.ApplicationComponent) error,
) (*model.Applications, error) {
	runLocked := func(lockCtx context.Context) (*model.Applications, error) {
		app, err := c.AppRepo.FindByID(lockCtx, appID)
		if err != nil {
			return nil, err
		}
		if !isAdoptedApplication(app) {
			return nil, fmt.Errorf(
				"%w: application is no longer managed in adopted mode",
				bcode.ErrApplicationManagementMode,
			)
		}
		if err := EnsureAppWorkflowIdle(lockCtx, c.Store, app.ID); err != nil {
			return nil, err
		}
		components, err := c.ComponentRepo.FindByAppID(lockCtx, app.ID)
		if err != nil {
			return nil, err
		}
		setResourceAppNameForComponents(components, applicationResourceNameKey(app))
		if err := run(lockCtx, app, components); err != nil {
			return nil, err
		}
		return app, nil
	}
	if applicationMutationLockHeld(ctx, appID) {
		return runLocked(ctx)
	}

	lockProvider, err := c.appScheduleLocker()
	if err != nil {
		return nil, err
	}

	var currentApp *model.Applications
	err = schedulelock.WithAppScheduleLock(
		ctx,
		lockProvider,
		appID,
		"adopted-lifecycle-"+string(operation),
		true,
		func(lockCtx context.Context) error {
			var runErr error
			currentApp, runErr = runLocked(lockCtx)
			return runErr
		},
	)
	if err != nil {
		return nil, err
	}
	return currentApp, nil
}

func (c *applicationsServiceImpl) preflightAdoptedLifecycle(
	ctx context.Context,
	components []*model.ApplicationComponent,
	operation adoptedLifecycleOperation,
) ([]*adoptedLifecycleTarget, []*adoptedLifecycleTarget, error) {
	targets := make([]*adoptedLifecycleTarget, 0, len(components))
	skipped := make([]*adoptedLifecycleTarget, 0)
	for _, component := range components {
		if component == nil ||
			(component.ComponentType != config.ServerJob && component.ComponentType != config.StoreJob) {
			continue
		}

		_, shared := SharedLifecycleStrategyForComponent(component)
		shouldMutate := !shared
		switch operation {
		case adoptedLifecycleStart:
			if component.Status != string(config.ComponentStatusStopped) {
				shouldMutate = false
			}
		case adoptedLifecycleRestart:
			if component.Status == string(config.ComponentStatusStopped) {
				shouldMutate = false
			}
		}

		// Resolve every source identity before deciding whether the action is an
		// idempotent skip. A skipped component is still part of the operation's
		// ownership and StatefulSet data-safety preflight.
		target, err := c.resolveAdoptedLifecycleTarget(ctx, component, operation, shouldMutate)
		if err != nil {
			return nil, nil, err
		}
		if shouldMutate {
			targets = append(targets, target)
		} else {
			skipped = append(skipped, target)
		}
	}
	if err := c.validateNoAdoptedLifecycleHPAs(ctx, append(targets, skipped...), operation); err != nil {
		return nil, nil, err
	}
	return targets, skipped, nil
}

func (c *applicationsServiceImpl) validateNoAdoptedLifecycleHPAs(
	ctx context.Context,
	targets []*adoptedLifecycleTarget,
	operation adoptedLifecycleOperation,
) error {
	targetsByNamespace := make(map[string]map[string]struct{})
	for _, target := range targets {
		if target == nil {
			continue
		}
		namespaceTargets := targetsByNamespace[target.namespace]
		if namespaceTargets == nil {
			namespaceTargets = make(map[string]struct{})
			targetsByNamespace[target.namespace] = namespaceTargets
		}
		namespaceTargets[adoptedLifecycleKindNameKey(target.kind, target.name)] = struct{}{}
	}
	for namespace, namespaceTargets := range targetsByNamespace {
		opCtx, cancel := context.WithTimeout(ctx, adoptedLifecycleTimeout(operation))
		hpas, err := c.KubeClient.AutoscalingV2().HorizontalPodAutoscalers(namespace).List(
			opCtx,
			metav1.ListOptions{},
		)
		cancel()
		if err != nil {
			return fmt.Errorf("preflight adopted %s: list HorizontalPodAutoscalers in namespace %q: %w", operation, namespace, err)
		}
		for index := range hpas.Items {
			hpa := &hpas.Items[index]
			ref := hpa.Spec.ScaleTargetRef
			apiVersion := strings.TrimSpace(ref.APIVersion)
			if apiVersion != "" && apiVersion != appsv1.SchemeGroupVersion.String() {
				continue
			}
			if _, found := namespaceTargets[adoptedLifecycleKindNameKey(ref.Kind, ref.Name)]; !found {
				continue
			}
			return fmt.Errorf(
				"preflight adopted %s: HorizontalPodAutoscaler %s/%s targets %s; HPA coordination is unsupported",
				operation,
				namespace,
				hpa.Name,
				formatResource(ref.Kind, namespace, ref.Name),
			)
		}
	}
	return nil
}

func adoptedLifecycleKindNameKey(kind, name string) string {
	return strings.ToLower(strings.TrimSpace(kind)) + "/" + strings.TrimSpace(name)
}

func adoptedLifecycleReportTarget(component *model.ApplicationComponent) *adoptedLifecycleTarget {
	target := &adoptedLifecycleTarget{component: component}
	if component == nil {
		return target
	}
	target.namespace = pickNamespace(strings.TrimSpace(component.Namespace), config.DefaultNamespace)
	target.name = strings.TrimSpace(component.SourceWorkloadName)
	switch component.ComponentType {
	case config.ServerJob:
		target.kind = "Deployment"
	case config.StoreJob:
		target.kind = "StatefulSet"
	}
	return target
}

func (c *applicationsServiceImpl) resolveAdoptedLifecycleTarget(
	ctx context.Context,
	component *model.ApplicationComponent,
	operation adoptedLifecycleOperation,
	shouldMutate bool,
) (*adoptedLifecycleTarget, error) {
	if component == nil {
		return nil, fmt.Errorf("preflight adopted %s: component is nil", operation)
	}
	if !component.HasSourceWorkload() {
		return nil, fmt.Errorf("preflight adopted %s component %q: source workload reference is incomplete", operation, component.Name)
	}

	target := adoptedLifecycleReportTarget(component)
	target.uid = strings.TrimSpace(*component.SourceWorkloadUID)
	if apiVersion := strings.TrimSpace(component.SourceWorkloadAPIVersion); apiVersion != appsv1.SchemeGroupVersion.String() {
		return nil, fmt.Errorf(
			"preflight adopted %s component %q: unsupported source apiVersion %q",
			operation,
			component.Name,
			apiVersion,
		)
	}
	if sourceKind := strings.TrimSpace(component.SourceWorkloadKind); sourceKind != target.kind {
		return nil, fmt.Errorf(
			"preflight adopted %s component %q: source kind %q does not match component type (expected %s)",
			operation,
			component.Name,
			sourceKind,
			target.kind,
		)
	}

	opCtx, cancel := context.WithTimeout(ctx, adoptedLifecycleTimeout(operation))
	defer cancel()

	switch target.kind {
	case "Deployment":
		deployment, err := c.KubeClient.AppsV1().Deployments(target.namespace).Get(opCtx, target.name, metav1.GetOptions{})
		if err != nil {
			return nil, adoptedLifecycleGetError(operation, target, err)
		}
		if err := validateAdoptedLifecycleUID(target, deployment); err != nil {
			return nil, fmt.Errorf("preflight adopted %s: %w", operation, err)
		}
		if operation == adoptedLifecycleRestart && deployment.Spec.Paused {
			return nil, fmt.Errorf(
				"preflight adopted %s %s: Deployment is paused",
				operation,
				formatResource(target.kind, target.namespace, target.name),
			)
		}
		if operation == adoptedLifecycleStop {
			target.liveReplicas = adoptedLiveReplicas(deployment.Spec.Replicas)
			target.resumeReplicas, err = adoptedResumeReplicas(component, target.liveReplicas)
			if err != nil {
				return nil, fmt.Errorf("preflight adopted %s %s: %w", operation, formatResource(target.kind, target.namespace, target.name), err)
			}
		}
	case "StatefulSet":
		statefulSet, err := c.KubeClient.AppsV1().StatefulSets(target.namespace).Get(opCtx, target.name, metav1.GetOptions{})
		if err != nil {
			return nil, adoptedLifecycleGetError(operation, target, err)
		}
		if err := validateAdoptedLifecycleUID(target, statefulSet); err != nil {
			return nil, fmt.Errorf("preflight adopted %s: %w", operation, err)
		}
		if operation == adoptedLifecycleStop {
			target.liveReplicas = adoptedLiveReplicas(statefulSet.Spec.Replicas)
			target.resumeReplicas, err = adoptedResumeReplicas(component, target.liveReplicas)
			if err != nil {
				return nil, fmt.Errorf("preflight adopted %s %s: %w", operation, formatResource(target.kind, target.namespace, target.name), err)
			}
			if err := c.validateAdoptedStatefulSetStop(opCtx, statefulSet, target.resumeReplicas); err != nil {
				return nil, fmt.Errorf("preflight adopted %s %s: %w", operation, formatResource(target.kind, target.namespace, target.name), err)
			}
		} else if operation == adoptedLifecycleStart {
			if shouldMutate {
				if component.ResumeReplicas == nil || *component.ResumeReplicas <= 0 {
					return nil, fmt.Errorf(
						"preflight adopted %s %s: resume replicas must be greater than 0",
						operation,
						formatResource(target.kind, target.namespace, target.name),
					)
				}
				target.resumeReplicas = *component.ResumeReplicas
			} else {
				target.resumeReplicas, err = adoptedStatefulSetValidationReplicas(component, statefulSet)
				if err != nil {
					return nil, fmt.Errorf("preflight adopted %s %s: %w", operation, formatResource(target.kind, target.namespace, target.name), err)
				}
			}
			if err := c.validateAdoptedStatefulSetStop(opCtx, statefulSet, target.resumeReplicas); err != nil {
				return nil, fmt.Errorf("preflight adopted %s %s: %w", operation, formatResource(target.kind, target.namespace, target.name), err)
			}
		} else if operation == adoptedLifecycleRestart {
			if shouldMutate {
				if err := adoption.ValidateStatefulSetRestartStrategy(statefulSet); err != nil {
					return nil, fmt.Errorf("preflight adopted %s %s: %w", operation, formatResource(target.kind, target.namespace, target.name), err)
				}
			}
			target.resumeReplicas, err = adoptedStatefulSetValidationReplicas(component, statefulSet)
			if err != nil {
				return nil, fmt.Errorf("preflight adopted %s %s: %w", operation, formatResource(target.kind, target.namespace, target.name), err)
			}
			if err := c.validateAdoptedStatefulSetStop(opCtx, statefulSet, target.resumeReplicas); err != nil {
				return nil, fmt.Errorf("preflight adopted %s %s: %w", operation, formatResource(target.kind, target.namespace, target.name), err)
			}
		}
	default:
		return nil, fmt.Errorf("preflight adopted %s component %q: unsupported source kind %q", operation, component.Name, target.kind)
	}

	if operation == adoptedLifecycleStart && shouldMutate {
		if component.ResumeReplicas == nil || *component.ResumeReplicas <= 0 {
			return nil, fmt.Errorf(
				"preflight adopted %s %s: resume replicas must be greater than 0",
				operation,
				formatResource(target.kind, target.namespace, target.name),
			)
		}
		target.resumeReplicas = *component.ResumeReplicas
	}
	return target, nil
}

func adoptedLifecycleGetError(operation adoptedLifecycleOperation, target *adoptedLifecycleTarget, err error) error {
	resource := formatResource(target.kind, target.namespace, target.name)
	if k8serrors.IsNotFound(err) {
		return fmt.Errorf("preflight adopted %s %s: source workload not found: %w", operation, resource, err)
	}
	return fmt.Errorf("preflight adopted %s %s: get source workload: %w", operation, resource, err)
}

func validateAdoptedLifecycleUID(target *adoptedLifecycleTarget, object metav1.Object) error {
	if target == nil || object == nil {
		return fmt.Errorf("source workload identity is nil")
	}
	actualUID := strings.TrimSpace(string(object.GetUID()))
	if actualUID != target.uid {
		return fmt.Errorf(
			"%s source UID mismatch: expected %q, got %q",
			formatResource(target.kind, target.namespace, target.name),
			target.uid,
			actualUID,
		)
	}
	return nil
}

func adoptedLiveReplicas(liveReplicas *int32) int32 {
	if liveReplicas != nil {
		return *liveReplicas
	}
	return 1
}

func adoptedResumeReplicas(component *model.ApplicationComponent, replicas int32) (int32, error) {
	if replicas > 0 {
		return replicas, nil
	}
	if replicas == 0 && component != nil && component.ResumeReplicas != nil && *component.ResumeReplicas > 0 {
		return *component.ResumeReplicas, nil
	}
	return 0, fmt.Errorf("cannot capture a positive replica snapshot from live replicas %d", replicas)
}

func adoptedStatefulSetValidationReplicas(
	component *model.ApplicationComponent,
	statefulSet *appsv1.StatefulSet,
) (int32, error) {
	if statefulSet == nil {
		return 0, fmt.Errorf("statefulset is nil")
	}
	replicas := adoptedLiveReplicas(statefulSet.Spec.Replicas)
	if replicas > 0 {
		return replicas, nil
	}
	if component != nil && component.ResumeReplicas != nil && *component.ResumeReplicas > 0 {
		return *component.ResumeReplicas, nil
	}
	return 0, fmt.Errorf("replica snapshot must be greater than 0")
}

func (c *applicationsServiceImpl) validateAdoptedStatefulSetStop(
	ctx context.Context,
	statefulSet *appsv1.StatefulSet,
	resumeReplicas int32,
) error {
	if statefulSet == nil {
		return fmt.Errorf("statefulset is nil")
	}
	retention := statefulSet.Spec.PersistentVolumeClaimRetentionPolicy
	if retention != nil && retention.WhenScaled == appsv1.DeletePersistentVolumeClaimRetentionPolicyType {
		return fmt.Errorf("persistentVolumeClaimRetentionPolicy.whenScaled=Delete would remove PVCs")
	}

	for _, claimName := range adoptedStatefulSetPVCNames(statefulSet, resumeReplicas) {
		pvc, err := c.KubeClient.CoreV1().PersistentVolumeClaims(statefulSet.Namespace).Get(ctx, claimName, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return fmt.Errorf("referenced PVC %s/%s does not exist: %w", statefulSet.Namespace, claimName, err)
			}
			return fmt.Errorf("get referenced PVC %s/%s: %w", statefulSet.Namespace, claimName, err)
		}
		if pvc.DeletionTimestamp != nil {
			return fmt.Errorf("referenced PVC %s/%s is terminating", statefulSet.Namespace, claimName)
		}
		if pvc.Status.Phase != corev1.ClaimBound {
			return fmt.Errorf(
				"referenced PVC %s/%s must be Bound, got %s",
				statefulSet.Namespace,
				claimName,
				pvc.Status.Phase,
			)
		}
	}
	return nil
}

func adoptedStatefulSetPVCNames(statefulSet *appsv1.StatefulSet, replicas int32) []string {
	if statefulSet == nil {
		return nil
	}
	names := make(map[string]struct{})
	for _, volume := range statefulSet.Spec.Template.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		if name := strings.TrimSpace(volume.PersistentVolumeClaim.ClaimName); name != "" {
			names[name] = struct{}{}
		}
	}

	startOrdinal := int32(0)
	if statefulSet.Spec.Ordinals != nil {
		startOrdinal = statefulSet.Spec.Ordinals.Start
	}
	for _, template := range statefulSet.Spec.VolumeClaimTemplates {
		templateName := strings.TrimSpace(template.Name)
		if templateName == "" {
			continue
		}
		for offset := int32(0); offset < replicas; offset++ {
			name := fmt.Sprintf("%s-%s-%d", templateName, statefulSet.Name, startOrdinal+offset)
			names[name] = struct{}{}
		}
	}

	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func (c *applicationsServiceImpl) persistAdoptedResumeReplicas(
	ctx context.Context,
	targets []*adoptedLifecycleTarget,
) error {
	if len(targets) == 0 {
		return nil
	}
	txStore, ok := c.Store.(datastore.Transactional)
	if !ok {
		return fmt.Errorf("persist adopted replica snapshots: datastore does not support transactions")
	}
	if err := txStore.WithTransaction(ctx, func(tx datastore.DataStore) error {
		conditionalStore, ok := tx.(datastore.ConditionalCompareAndSwap)
		if !ok {
			return fmt.Errorf("persist adopted replica snapshots: transactional datastore does not support conditional updates")
		}
		for _, target := range targets {
			if target == nil || target.component == nil {
				return fmt.Errorf("persist adopted replica snapshots: target component is nil")
			}
			entity, conditions, err := adoptedLifecycleComponentIdentity(target.component)
			if err != nil {
				return err
			}
			updated, err := conditionalStore.CompareAndSwapWithConditions(ctx, entity, conditions, map[string]interface{}{
				"resume_replicas": target.resumeReplicas,
			})
			if err != nil {
				return fmt.Errorf("persist resume replicas for component %q: %w", target.component.Name, err)
			}
			if !updated {
				var persisted model.ApplicationComponent
				exists, err := tx.IsExistByCondition(ctx, entity.TableName(), conditions, &persisted)
				if err != nil {
					return fmt.Errorf("verify resume replicas for component %q: %w", target.component.Name, err)
				}
				if !exists {
					return fmt.Errorf("persist resume replicas for component %q: component identity changed", target.component.Name)
				}
				if persisted.ResumeReplicas == nil || *persisted.ResumeReplicas != target.resumeReplicas {
					return fmt.Errorf("persist resume replicas for component %q: conditional update did not apply", target.component.Name)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	for _, target := range targets {
		resumeReplicas := target.resumeReplicas
		target.component.ResumeReplicas = &resumeReplicas
	}
	return nil
}

func adoptedLifecycleComponentIdentity(
	component *model.ApplicationComponent,
) (*model.ApplicationComponent, map[string]interface{}, error) {
	if component == nil {
		return nil, nil, fmt.Errorf("persist adopted replica snapshots: component is nil")
	}
	appID := strings.TrimSpace(component.AppID)
	name := strings.TrimSpace(component.Name)
	if appID == "" {
		return nil, nil, fmt.Errorf("persist adopted replica snapshots: component app_id is empty")
	}
	if name == "" {
		return nil, nil, fmt.Errorf("persist adopted replica snapshots: component name is empty")
	}
	entity := &model.ApplicationComponent{
		ID:    component.ID,
		AppID: appID,
		Name:  name,
	}
	conditions := map[string]interface{}{"app_id": appID}
	if component.ID > 0 {
		conditions["id"] = component.ID
	} else {
		conditions["name"] = name
	}
	if component.SourceWorkloadUID == nil || strings.TrimSpace(*component.SourceWorkloadUID) == "" {
		return nil, nil, fmt.Errorf("persist adopted replica snapshots: component %q source UID is empty", name)
	}
	conditions["source_workload_uid"] = strings.TrimSpace(*component.SourceWorkloadUID)
	return entity, conditions, nil
}

func (c *applicationsServiceImpl) updateAdoptedLifecycleTarget(
	ctx context.Context,
	target *adoptedLifecycleTarget,
	operation adoptedLifecycleOperation,
	restartedAt string,
) error {
	if target == nil || target.component == nil {
		return fmt.Errorf("adopted %s target is nil", operation)
	}
	opCtx, cancel := context.WithTimeout(ctx, adoptedLifecycleTimeout(operation))
	defer cancel()

	switch target.kind {
	case "Deployment":
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			deployment, err := c.KubeClient.AppsV1().Deployments(target.namespace).Get(opCtx, target.name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if err := validateAdoptedLifecycleUID(target, deployment); err != nil {
				return err
			}
			if operation == adoptedLifecycleStop {
				if err := validateAdoptedStopReplicaSnapshot(target, deployment.Spec.Replicas); err != nil {
					return err
				}
			}
			changed := applyAdoptedDeploymentLifecycle(deployment, target, operation, restartedAt)
			if !changed {
				return nil
			}
			_, err = c.KubeClient.AppsV1().Deployments(target.namespace).Update(opCtx, deployment, metav1.UpdateOptions{})
			return err
		})
	case "StatefulSet":
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			statefulSet, err := c.KubeClient.AppsV1().StatefulSets(target.namespace).Get(opCtx, target.name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if err := validateAdoptedLifecycleUID(target, statefulSet); err != nil {
				return err
			}
			if operation == adoptedLifecycleStop {
				if err := validateAdoptedStopReplicaSnapshot(target, statefulSet.Spec.Replicas); err != nil {
					return err
				}
				if err := c.validateAdoptedStatefulSetStop(opCtx, statefulSet, target.resumeReplicas); err != nil {
					return err
				}
			}
			changed := applyAdoptedStatefulSetLifecycle(statefulSet, target, operation, restartedAt)
			if !changed {
				return nil
			}
			_, err = c.KubeClient.AppsV1().StatefulSets(target.namespace).Update(opCtx, statefulSet, metav1.UpdateOptions{})
			return err
		})
	default:
		return fmt.Errorf("adopted %s target kind %q is unsupported", operation, target.kind)
	}
}

func validateAdoptedStopReplicaSnapshot(target *adoptedLifecycleTarget, replicas *int32) error {
	if target == nil {
		return fmt.Errorf("adopted stop target is nil")
	}
	current := adoptedLiveReplicas(replicas)
	if current != target.liveReplicas {
		return fmt.Errorf(
			"%s replicas changed after preflight: expected %d, got %d",
			formatResource(target.kind, target.namespace, target.name),
			target.liveReplicas,
			current,
		)
	}
	return nil
}

func applyAdoptedDeploymentLifecycle(
	deployment *appsv1.Deployment,
	target *adoptedLifecycleTarget,
	operation adoptedLifecycleOperation,
	restartedAt string,
) bool {
	switch operation {
	case adoptedLifecycleStop:
		return setReplicas(&deployment.Spec.Replicas, 0)
	case adoptedLifecycleStart:
		return setReplicas(&deployment.Spec.Replicas, target.resumeReplicas)
	case adoptedLifecycleRestart:
		return applyAdoptedRestartMetadata(&deployment.Spec.Template.ObjectMeta, target.component, restartedAt)
	default:
		return false
	}
}

func applyAdoptedStatefulSetLifecycle(
	statefulSet *appsv1.StatefulSet,
	target *adoptedLifecycleTarget,
	operation adoptedLifecycleOperation,
	restartedAt string,
) bool {
	switch operation {
	case adoptedLifecycleStop:
		return setReplicas(&statefulSet.Spec.Replicas, 0)
	case adoptedLifecycleStart:
		return setReplicas(&statefulSet.Spec.Replicas, target.resumeReplicas)
	case adoptedLifecycleRestart:
		return applyAdoptedRestartMetadata(&statefulSet.Spec.Template.ObjectMeta, target.component, restartedAt)
	default:
		return false
	}
}

func setReplicas(current **int32, desired int32) bool {
	if *current != nil && **current == desired {
		return false
	}
	replicas := desired
	*current = &replicas
	return true
}

func applyAdoptedRestartMetadata(
	metadata *metav1.ObjectMeta,
	component *model.ApplicationComponent,
	restartedAt string,
) bool {
	changed := false
	if metadata.Labels == nil {
		metadata.Labels = make(map[string]string)
	}
	for key, value := range job.ApplyComponentManagedLabels(nil, component) {
		if metadata.Labels[key] == value {
			continue
		}
		metadata.Labels[key] = value
		changed = true
	}
	if metadata.Annotations == nil {
		metadata.Annotations = make(map[string]string)
	}
	if metadata.Annotations[config.AnnotationWorkloadRestartAt] != restartedAt {
		metadata.Annotations[config.AnnotationWorkloadRestartAt] = restartedAt
		changed = true
	}
	return changed
}

func adoptedLifecycleTimeout(operation adoptedLifecycleOperation) time.Duration {
	if operation == adoptedLifecycleRestart {
		return config.DefaultApplicationWorkloadRestartTimeout
	}
	return config.DefaultApplicationWorkloadScaleTimeout
}
