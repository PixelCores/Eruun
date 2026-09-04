package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/PixelCores/Eruun/pkg/apiserver/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/schedulelock"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/importsecret"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

const (
	adoptedCleanupRuntimeControllerRole = "runtime-controller"
	adoptedCleanupRuntimePodRole        = "runtime-pod"
)

type adoptedCleanupPlan struct {
	response              *apisv1.CleanupApplicationResourcesPlanResponse
	payload               []byte
	runtimeChildrenByRoot map[string][]apisv1.ImportNamespaceResourceResult
	blocked               bool
}

type cleanupFingerprintPayload struct {
	Version   int                                    `json:"version"`
	AppID     string                                 `json:"appId"`
	Namespace string                                 `json:"namespace"`
	Resources []apisv1.ImportNamespaceResourceResult `json:"resources"`
}

func (c *applicationsServiceImpl) PlanApplicationResourceCleanup(
	ctx context.Context,
	appID string,
) (*apisv1.CleanupApplicationResourcesPlanResponse, error) {
	app, err := c.applicationForCleanup(ctx, appID)
	if err != nil {
		return nil, err
	}
	if app.EffectiveManagementMode() != config.ManagementModeAdopted {
		return nil, fmt.Errorf("%w: cleanup plans are only available for adopted applications", bcode.ErrApplicationManagementMode)
	}
	keyring, err := c.importSecretKeyring()
	if err != nil {
		return nil, err
	}
	plan, err := c.buildAdoptedCleanupPlan(ctx, app, keyring)
	if err != nil {
		return nil, err
	}
	return plan.response, nil
}

func (c *applicationsServiceImpl) ApplyApplicationResourceCleanup(
	ctx context.Context,
	appID string,
	req apisv1.CleanupApplicationResourcesRequest,
) (*apisv1.CleanupApplicationResourcesResponse, error) {
	app, err := c.applicationForCleanup(ctx, appID)
	if err != nil {
		return nil, err
	}
	switch app.EffectiveManagementMode() {
	case config.ManagementModeNative:
		return c.CleanupApplicationResources(ctx, app.ID)
	case config.ManagementModeObserve:
		return nil, fmt.Errorf("%w: observe applications are read-only", bcode.ErrApplicationManagementMode)
	case config.ManagementModeAdopted:
	default:
		return nil, fmt.Errorf("%w: unsupported mode %q", bcode.ErrApplicationManagementMode, app.ManagementMode)
	}

	fingerprint := strings.TrimSpace(req.PlanFingerprint)
	if fingerprint == "" {
		return nil, fmt.Errorf("%w: planFingerprint is required", bcode.ErrApplicationConfig)
	}
	lockProvider, err := c.appScheduleLocker()
	if err != nil {
		return nil, err
	}

	var response *apisv1.CleanupApplicationResourcesResponse
	err = schedulelock.WithAppScheduleLock(ctx, lockProvider, app.ID, "cleanup-adopted-application-resources", true, func(lockCtx context.Context) error {
		current, err := c.applicationForCleanup(lockCtx, app.ID)
		if err != nil {
			return err
		}
		if current.EffectiveManagementMode() != config.ManagementModeAdopted {
			return fmt.Errorf("%w: application is no longer adopted", bcode.ErrApplicationManagementMode)
		}
		if err := EnsureAppWorkflowIdle(lockCtx, c.Store, current.ID); err != nil {
			return fmt.Errorf("cleanup adopted application: %w", err)
		}
		keyring, err := c.importSecretKeyring()
		if err != nil {
			return err
		}
		plan, err := c.buildAdoptedCleanupPlan(lockCtx, current, keyring)
		if err != nil {
			return err
		}
		if err := keyring.VerifyPlan(plan.payload, fingerprint); err != nil {
			return fmt.Errorf("%w: %v", bcode.ErrNamespaceImportPlanDrift, err)
		}
		if plan.blocked {
			return fmt.Errorf("%w: cleanup plan contains blocked resources", bcode.ErrApplicationManagementMode)
		}

		response = &apisv1.CleanupApplicationResourcesResponse{AppID: current.ID}
		var roots []apisv1.ImportNamespaceResourceResult
		var dependencies []apisv1.ImportNamespaceResourceResult
		for _, resource := range plan.response.ResourceResults {
			ref := cleanupResourceRef(resource)
			if resource.Disposition != adoption.DispositionManaged || resource.Status != "planned" {
				response.RetainedResources = append(response.RetainedResources, ref)
				continue
			}
			if cleanupResourceIsRootWorkload(resource) {
				roots = append(roots, resource)
			} else if !cleanupResourceIsRuntimeChild(resource) {
				dependencies = append(dependencies, resource)
			}
		}

		rootDeleteFailed := false
		acceptedRoots := make([]apisv1.ImportNamespaceResourceResult, 0, len(roots))
		for _, resource := range roots {
			ref := cleanupResourceRef(resource)
			children := plan.runtimeChildrenByRoot[ref]
			if err := c.quiesceAdoptedCleanupRoot(lockCtx, resource); err != nil {
				response.FailedResources = append(response.FailedResources, ref)
				response.RetainedResources = append(response.RetainedResources, ref)
				rootDeleteFailed = true
				for _, child := range children {
					response.RetainedResources = append(response.RetainedResources, cleanupResourceRef(child))
				}
				continue
			}
			quiescedChildren, err := c.planAdoptedCleanupRuntimeChildren(lockCtx, resource)
			if err != nil {
				response.FailedResources = append(response.FailedResources, ref)
				response.RetainedResources = append(response.RetainedResources, ref)
				rootDeleteFailed = true
				continue
			}
			unsignedChildren := unsignedAdoptedCleanupRuntimeChildren(children, quiescedChildren)
			if len(unsignedChildren) > 0 {
				response.RetainedResources = append(response.RetainedResources, ref)
				for _, child := range quiescedChildren {
					response.RetainedResources = append(response.RetainedResources, cleanupResourceRef(child))
				}
				for _, child := range unsignedChildren {
					response.FailedResources = append(response.FailedResources, cleanupResourceRef(child))
				}
				rootDeleteFailed = true
				continue
			}

			childDeleteFailed := false
			attemptedChildren := make(map[string]struct{}, len(children))
			for phase := 0; phase <= 1 && !childDeleteFailed; phase++ {
				acceptedChildren := make([]apisv1.ImportNamespaceResourceResult, 0, len(children))
				for _, child := range children {
					if adoptedCleanupRuntimeDeleteOrder(child) != phase {
						continue
					}
					childRef := cleanupResourceRef(child)
					attemptedChildren[childRef] = struct{}{}
					if err := c.deleteAdoptedCleanupRuntimeChild(lockCtx, child); err != nil {
						response.FailedResources = append(response.FailedResources, childRef)
						childDeleteFailed = true
						continue
					}
					acceptedChildren = append(acceptedChildren, child)
				}
				if len(acceptedChildren) > 0 {
					pending, waitErr := c.waitForAdoptedCleanupResourcesDeleted(lockCtx, acceptedChildren)
					for _, child := range acceptedChildren {
						childRef := cleanupResourceRef(child)
						if _, found := pending[childRef]; found {
							response.FailedResources = append(response.FailedResources, childRef)
							childDeleteFailed = true
							continue
						}
						response.DeletedResources = append(response.DeletedResources, childRef)
					}
					if waitErr != nil {
						childDeleteFailed = true
					}
				}
			}
			if childDeleteFailed {
				response.RetainedResources = append(response.RetainedResources, ref)
				for _, child := range children {
					childRef := cleanupResourceRef(child)
					if _, attempted := attemptedChildren[childRef]; !attempted {
						response.RetainedResources = append(response.RetainedResources, childRef)
					}
				}
				rootDeleteFailed = true
				continue
			}

			remainingChildren, err := c.planAdoptedCleanupRuntimeChildren(lockCtx, resource)
			if err != nil {
				response.FailedResources = append(response.FailedResources, ref)
				response.RetainedResources = append(response.RetainedResources, ref)
				rootDeleteFailed = true
				continue
			}
			if len(remainingChildren) > 0 {
				response.RetainedResources = append(response.RetainedResources, ref)
				for _, child := range remainingChildren {
					response.FailedResources = append(response.FailedResources, cleanupResourceRef(child))
				}
				rootDeleteFailed = true
				continue
			}

			refreshedRoot, err := c.refreshQuiescedAdoptedCleanupRoot(lockCtx, resource)
			if err != nil {
				response.FailedResources = append(response.FailedResources, ref)
				response.RetainedResources = append(response.RetainedResources, ref)
				rootDeleteFailed = true
				continue
			}
			if err := c.deleteAdoptedCleanupResource(lockCtx, refreshedRoot); err != nil {
				response.FailedResources = append(response.FailedResources, ref)
				response.RetainedResources = append(response.RetainedResources, ref)
				rootDeleteFailed = true
				continue
			}
			acceptedRoots = append(acceptedRoots, refreshedRoot)
		}
		if len(acceptedRoots) > 0 {
			pending, waitErr := c.waitForAdoptedCleanupResourcesDeleted(lockCtx, acceptedRoots)
			for _, resource := range acceptedRoots {
				ref := cleanupResourceRef(resource)
				if _, found := pending[ref]; found {
					response.FailedResources = append(response.FailedResources, ref)
					rootDeleteFailed = true
					continue
				}
				response.DeletedResources = append(response.DeletedResources, ref)
			}
			if waitErr != nil {
				rootDeleteFailed = true
			}
		}
		if rootDeleteFailed {
			for _, resource := range dependencies {
				response.RetainedResources = append(response.RetainedResources, cleanupResourceRef(resource))
			}
		} else {
			deletableDependencies := dependencies
			if len(dependencies) > 0 {
				refreshedPlan, refreshErr := c.buildAdoptedCleanupPlan(lockCtx, current, keyring)
				if refreshErr != nil {
					for _, resource := range dependencies {
						response.RetainedResources = append(response.RetainedResources, cleanupResourceRef(resource))
					}
					return fmt.Errorf("refresh adopted cleanup sharing before dependency deletion: %w", refreshErr)
				}
				refreshedByRef := make(map[string]apisv1.ImportNamespaceResourceResult, len(refreshedPlan.response.ResourceResults))
				for _, resource := range refreshedPlan.response.ResourceResults {
					refreshedByRef[cleanupResourceRef(resource)] = resource
				}
				deletableDependencies = make([]apisv1.ImportNamespaceResourceResult, 0, len(dependencies))
				for _, resource := range dependencies {
					ref := cleanupResourceRef(resource)
					refreshed, found := refreshedByRef[ref]
					if !found ||
						resource.Source == nil ||
						refreshed.Source == nil ||
						refreshed.Source.UID != resource.Source.UID ||
						refreshed.Disposition != adoption.DispositionManaged ||
						refreshed.Status != "planned" {
						response.RetainedResources = append(response.RetainedResources, ref)
						continue
					}
					deletableDependencies = append(deletableDependencies, resource)
				}
			}
			acceptedDependencies := make([]apisv1.ImportNamespaceResourceResult, 0, len(deletableDependencies))
			for _, resource := range deletableDependencies {
				ref := cleanupResourceRef(resource)
				if err := c.deleteAdoptedCleanupResource(lockCtx, resource); err != nil {
					response.FailedResources = append(response.FailedResources, ref)
					continue
				}
				acceptedDependencies = append(acceptedDependencies, resource)
			}
			if len(acceptedDependencies) > 0 {
				pending, _ := c.waitForAdoptedCleanupResourcesDeleted(lockCtx, acceptedDependencies)
				for _, resource := range acceptedDependencies {
					ref := cleanupResourceRef(resource)
					if _, found := pending[ref]; found {
						response.FailedResources = append(response.FailedResources, ref)
						continue
					}
					response.DeletedResources = append(response.DeletedResources, ref)
				}
			}
		}
		if len(response.FailedResources) > 0 {
			return fmt.Errorf("adopted cleanup completed with failures")
		}
		return nil
	})
	if err != nil {
		return response, err
	}
	return response, nil
}

func cleanupResourceIsRootWorkload(resource apisv1.ImportNamespaceResourceResult) bool {
	if !strings.EqualFold(strings.TrimSpace(resource.DependencyRole), "workload") {
		return false
	}
	return strings.EqualFold(resource.Kind, "Deployment") ||
		strings.EqualFold(resource.Kind, "StatefulSet")
}

func cleanupResourceIsRuntimeChild(resource apisv1.ImportNamespaceResourceResult) bool {
	role := strings.TrimSpace(resource.DependencyRole)
	return role == adoptedCleanupRuntimeControllerRole || role == adoptedCleanupRuntimePodRole
}

func (c *applicationsServiceImpl) quiesceAdoptedCleanupRoot(
	ctx context.Context,
	resource apisv1.ImportNamespaceResourceResult,
) error {
	if resource.Source == nil {
		return fmt.Errorf("cleanup root source identity is missing")
	}
	namespace := strings.TrimSpace(resource.Source.Namespace)
	name := strings.TrimSpace(resource.Source.Name)
	expectedUID := strings.TrimSpace(resource.Source.UID)
	zero := int32(0)

	switch strings.ToLower(strings.TrimSpace(resource.Source.Kind)) {
	case "deployment":
		deployment, err := c.KubeClient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get cleanup Deployment for quiesce: %w", err)
		}
		if strings.TrimSpace(string(deployment.UID)) != expectedUID {
			return fmt.Errorf("cleanup Deployment UID changed before quiesce")
		}
		if strings.TrimSpace(deployment.ResourceVersion) != strings.TrimSpace(resource.Source.ResourceVersion) {
			return fmt.Errorf("cleanup Deployment resourceVersion changed before quiesce")
		}
		if !deployment.Spec.Paused ||
			deployment.Spec.Replicas == nil ||
			*deployment.Spec.Replicas != 0 {
			deployment.Spec.Paused = true
			deployment.Spec.Replicas = &zero
			if _, err := c.KubeClient.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("quiesce cleanup Deployment: %w", err)
			}
		}

		replicaSets, err := c.KubeClient.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("list cleanup ReplicaSets for quiesce: %w", err)
		}
		for index := range replicaSets.Items {
			replicaSet := &replicaSets.Items[index]
			if !cleanupControllerOwnerMatches(metav1.GetControllerOf(replicaSet), "apps/v1", "Deployment", expectedUID) {
				continue
			}
			if replicaSet.Spec.Replicas != nil && *replicaSet.Spec.Replicas == 0 {
				continue
			}
			replicaSet.Spec.Replicas = &zero
			if _, err := c.KubeClient.AppsV1().ReplicaSets(namespace).Update(ctx, replicaSet, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("quiesce cleanup ReplicaSet %s: %w", replicaSet.Name, err)
			}
		}
		if err := c.waitForAdoptedCleanupRootQuiesced(ctx, resource); err != nil {
			return fmt.Errorf("wait for cleanup Deployment controllers to quiesce: %w", err)
		}
		return nil

	case "statefulset":
		statefulSet, err := c.KubeClient.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get cleanup StatefulSet for quiesce: %w", err)
		}
		if strings.TrimSpace(string(statefulSet.UID)) != expectedUID {
			return fmt.Errorf("cleanup StatefulSet UID changed before quiesce")
		}
		if strings.TrimSpace(statefulSet.ResourceVersion) != strings.TrimSpace(resource.Source.ResourceVersion) {
			return fmt.Errorf("cleanup StatefulSet resourceVersion changed before quiesce")
		}
		if err := validateAdoptedCleanupStatefulSetScaleSafety(statefulSet); err != nil {
			return err
		}
		if statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas != 0 {
			statefulSet.Spec.Replicas = &zero
			if _, err := c.KubeClient.AppsV1().StatefulSets(namespace).Update(ctx, statefulSet, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("quiesce cleanup StatefulSet: %w", err)
			}
		}
		if err := c.waitForAdoptedCleanupRootQuiesced(ctx, resource); err != nil {
			return fmt.Errorf("wait for cleanup StatefulSet controller to quiesce: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("cleanup root quiesce of kind %s is unsupported", resource.Source.Kind)
	}
}

func (c *applicationsServiceImpl) waitForAdoptedCleanupRootQuiesced(
	ctx context.Context,
	resource apisv1.ImportNamespaceResourceResult,
) error {
	if resource.Source == nil {
		return fmt.Errorf("cleanup root source identity is missing")
	}
	namespace := strings.TrimSpace(resource.Source.Namespace)
	name := strings.TrimSpace(resource.Source.Name)
	expectedUID := strings.TrimSpace(resource.Source.UID)
	timeout := config.DefaultApplicationCleanupTimeout
	return wait.PollUntilContextTimeout(
		ctx,
		config.DeleteApplicationTaskPollInterval,
		timeout,
		true,
		func(pollCtx context.Context) (bool, error) {
			switch strings.ToLower(strings.TrimSpace(resource.Source.Kind)) {
			case "deployment":
				deployment, err := c.KubeClient.AppsV1().Deployments(namespace).Get(pollCtx, name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				if strings.TrimSpace(string(deployment.UID)) != expectedUID {
					return false, fmt.Errorf("cleanup Deployment UID changed while waiting for quiesce")
				}
				if !adoptedCleanupDeploymentQuiesced(deployment) {
					return false, nil
				}
				replicaSets, err := c.KubeClient.AppsV1().ReplicaSets(namespace).List(pollCtx, metav1.ListOptions{})
				if err != nil {
					return false, err
				}
				for index := range replicaSets.Items {
					replicaSet := &replicaSets.Items[index]
					if !cleanupControllerOwnerMatches(metav1.GetControllerOf(replicaSet), "apps/v1", "Deployment", expectedUID) {
						continue
					}
					if !adoptedCleanupReplicaSetQuiesced(replicaSet) {
						return false, nil
					}
				}
				return true, nil
			case "statefulset":
				statefulSet, err := c.KubeClient.AppsV1().StatefulSets(namespace).Get(pollCtx, name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				if strings.TrimSpace(string(statefulSet.UID)) != expectedUID {
					return false, fmt.Errorf("cleanup StatefulSet UID changed while waiting for quiesce")
				}
				return adoptedCleanupStatefulSetQuiesced(statefulSet), nil
			default:
				return false, fmt.Errorf("cleanup root quiesce wait of kind %s is unsupported", resource.Source.Kind)
			}
		},
	)
}

func adoptedCleanupDeploymentQuiesced(deployment *appsv1.Deployment) bool {
	return deployment != nil &&
		deployment.Spec.Paused &&
		deployment.Spec.Replicas != nil &&
		*deployment.Spec.Replicas == 0 &&
		deployment.Status.ObservedGeneration >= deployment.Generation &&
		deployment.Status.Replicas == 0 &&
		deployment.Status.UpdatedReplicas == 0 &&
		deployment.Status.ReadyReplicas == 0 &&
		deployment.Status.AvailableReplicas == 0 &&
		deployment.Status.UnavailableReplicas == 0
}

func adoptedCleanupReplicaSetQuiesced(replicaSet *appsv1.ReplicaSet) bool {
	return replicaSet != nil &&
		replicaSet.Spec.Replicas != nil &&
		*replicaSet.Spec.Replicas == 0 &&
		replicaSet.Status.ObservedGeneration >= replicaSet.Generation &&
		replicaSet.Status.Replicas == 0 &&
		replicaSet.Status.FullyLabeledReplicas == 0 &&
		replicaSet.Status.ReadyReplicas == 0 &&
		replicaSet.Status.AvailableReplicas == 0
}

func adoptedCleanupStatefulSetQuiesced(statefulSet *appsv1.StatefulSet) bool {
	return statefulSet != nil &&
		statefulSet.Spec.Replicas != nil &&
		*statefulSet.Spec.Replicas == 0 &&
		statefulSet.Status.ObservedGeneration >= statefulSet.Generation &&
		statefulSet.Status.Replicas == 0 &&
		statefulSet.Status.CurrentReplicas == 0 &&
		statefulSet.Status.ReadyReplicas == 0 &&
		statefulSet.Status.AvailableReplicas == 0 &&
		statefulSet.Status.UpdatedReplicas == 0
}

func unsignedAdoptedCleanupRuntimeChildren(
	signed []apisv1.ImportNamespaceResourceResult,
	current []apisv1.ImportNamespaceResourceResult,
) []apisv1.ImportNamespaceResourceResult {
	signedIdentities := make(map[string]struct{}, len(signed))
	for _, child := range signed {
		signedIdentities[cleanupRuntimeResourceIdentity(child)] = struct{}{}
	}
	unsigned := make([]apisv1.ImportNamespaceResourceResult, 0)
	for _, child := range current {
		if _, found := signedIdentities[cleanupRuntimeResourceIdentity(child)]; !found {
			unsigned = append(unsigned, child)
		}
	}
	return unsigned
}

func cleanupRuntimeResourceIdentity(resource apisv1.ImportNamespaceResourceResult) string {
	uid := ""
	if resource.Source != nil {
		uid = strings.TrimSpace(resource.Source.UID)
	}
	return cleanupResourceRef(resource) + "/" + uid
}

func (c *applicationsServiceImpl) refreshQuiescedAdoptedCleanupRoot(
	ctx context.Context,
	resource apisv1.ImportNamespaceResourceResult,
) (apisv1.ImportNamespaceResourceResult, error) {
	if resource.Source == nil {
		return resource, fmt.Errorf("cleanup root source identity is missing")
	}
	source := adoption.ResourceIdentity{
		APIVersion: resource.Source.APIVersion,
		Kind:       resource.Source.Kind,
		Namespace:  resource.Source.Namespace,
		Name:       resource.Source.Name,
	}
	live, err := c.getAdoptedCleanupResource(ctx, source)
	if err != nil {
		return resource, fmt.Errorf("refresh quiesced cleanup root: %w", err)
	}
	if strings.TrimSpace(string(live.GetUID())) != strings.TrimSpace(resource.Source.UID) {
		return resource, fmt.Errorf("cleanup root UID changed after quiesce")
	}
	switch typed := live.(type) {
	case *appsv1.Deployment:
		if !typed.Spec.Paused || typed.Spec.Replicas == nil || *typed.Spec.Replicas != 0 {
			return resource, fmt.Errorf("cleanup Deployment is no longer quiesced")
		}
	case *appsv1.StatefulSet:
		if err := validateAdoptedCleanupStatefulSetScaleSafety(typed); err != nil {
			return resource, err
		}
		if typed.Spec.Replicas == nil || *typed.Spec.Replicas != 0 {
			return resource, fmt.Errorf("cleanup StatefulSet is no longer quiesced")
		}
	default:
		return resource, fmt.Errorf("cleanup root kind %s is unsupported", resource.Source.Kind)
	}
	children, err := c.planAdoptedCleanupRuntimeChildren(ctx, resource)
	if err != nil {
		return resource, err
	}
	if len(children) > 0 {
		return resource, fmt.Errorf("cleanup root gained new runtime children after quiesce")
	}
	refreshed := resource
	refreshed.Source = &apisv1.ImportNamespaceResourceIdentity{
		APIVersion:      resource.Source.APIVersion,
		Kind:            resource.Source.Kind,
		Namespace:       resource.Source.Namespace,
		Name:            resource.Source.Name,
		UID:             resource.Source.UID,
		ResourceVersion: live.GetResourceVersion(),
		SpecDigest:      resource.Source.SpecDigest,
	}
	return refreshed, nil
}

func validateAdoptedCleanupStatefulSetScaleSafety(statefulSet *appsv1.StatefulSet) error {
	if statefulSet == nil || statefulSet.Spec.PersistentVolumeClaimRetentionPolicy == nil {
		return nil
	}
	retention := statefulSet.Spec.PersistentVolumeClaimRetentionPolicy
	if retention.WhenDeleted == appsv1.DeletePersistentVolumeClaimRetentionPolicyType {
		return fmt.Errorf("StatefulSet persistentVolumeClaimRetentionPolicy.whenDeleted=Delete would remove PVCs")
	}
	if retention.WhenScaled == appsv1.DeletePersistentVolumeClaimRetentionPolicyType {
		return fmt.Errorf("StatefulSet persistentVolumeClaimRetentionPolicy.whenScaled=Delete would remove PVCs during cleanup quiesce")
	}
	return nil
}

func (c *applicationsServiceImpl) waitForAdoptedCleanupResourcesDeleted(
	ctx context.Context,
	resources []apisv1.ImportNamespaceResourceResult,
) (map[string]struct{}, error) {
	pending := make(map[string]apisv1.ImportNamespaceResourceResult, len(resources))
	for _, resource := range resources {
		pending[cleanupResourceRef(resource)] = resource
	}
	timeout := time.Duration(config.DefaultDeleteApplicationWaitSeconds) * time.Second
	err := wait.PollUntilContextTimeout(
		ctx,
		config.DeleteApplicationTaskPollInterval,
		timeout,
		true,
		func(pollCtx context.Context) (bool, error) {
			for ref, resource := range pending {
				if resource.Source == nil {
					return false, fmt.Errorf("wait cleanup resource %s: source identity is missing", ref)
				}
				_, err := c.getAdoptedCleanupResource(pollCtx, adoption.ResourceIdentity{
					APIVersion: resource.Source.APIVersion,
					Kind:       resource.Source.Kind,
					Namespace:  resource.Source.Namespace,
					Name:       resource.Source.Name,
				})
				if apierrors.IsNotFound(err) {
					delete(pending, ref)
					continue
				}
				if err != nil {
					return false, fmt.Errorf("wait cleanup resource %s deleted: %w", ref, err)
				}
			}
			return len(pending) == 0, nil
		},
	)
	pendingRefs := make(map[string]struct{}, len(pending))
	for ref := range pending {
		pendingRefs[ref] = struct{}{}
	}
	return pendingRefs, err
}

func (c *applicationsServiceImpl) applicationForCleanup(ctx context.Context, appID string) (*model.Applications, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	if c.AppRepo == nil {
		return nil, fmt.Errorf("application repository is nil")
	}
	app, err := c.AppRepo.FindByID(ctx, appID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}
	return app, nil
}

func (c *applicationsServiceImpl) importSecretKeyring() (*importsecret.Keyring, error) {
	if c.Cfg == nil {
		return nil, fmt.Errorf("%w: server import secret keyring configuration is unavailable", bcode.ErrApplicationManagementMode)
	}
	keyring, err := importsecret.Load(c.Cfg.ImportSecretKeyring, c.Cfg.ImportSecretKeyringFile)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", bcode.ErrApplicationManagementMode, err)
	}
	if keyring == nil {
		return nil, fmt.Errorf("%w: import secret keyring is required", bcode.ErrApplicationManagementMode)
	}
	return keyring, nil
}

func (c *applicationsServiceImpl) buildAdoptedCleanupPlan(
	ctx context.Context,
	app *model.Applications,
	keyring *importsecret.Keyring,
) (*adoptedCleanupPlan, error) {
	if c.KubeClient == nil {
		return nil, fmt.Errorf("kube client is nil")
	}
	snapshot, err := decodeApplicationAdoptionSnapshot(app)
	if err != nil {
		return nil, err
	}
	sharing, err := c.scanAdoptedCleanupSharing(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	results := make([]apisv1.ImportNamespaceResourceResult, 0, len(snapshot.Resources))
	runtimeChildrenByRoot := make(map[string][]apisv1.ImportNamespaceResourceResult)
	blocked := false
	for _, saved := range snapshot.Resources {
		result, isBlocked := c.planAdoptedCleanupResource(ctx, saved, sharing)
		var runtimeChildren []apisv1.ImportNamespaceResourceResult
		if !isBlocked &&
			result.Disposition == adoption.DispositionManaged &&
			result.Status == "planned" &&
			cleanupResourceIsRootWorkload(result) {
			runtimeChildren, err = c.planAdoptedCleanupRuntimeChildren(ctx, result)
			if err != nil {
				result.Disposition = adoption.DispositionBlocked
				result.Status = "blocked"
				result.Error = err.Error()
				isBlocked = true
			} else if len(runtimeChildren) > 0 {
				runtimeChildrenByRoot[cleanupResourceRef(result)] = runtimeChildren
			}
		}
		blocked = blocked || isBlocked
		results = append(results, result)
		results = append(results, runtimeChildren...)
	}
	payload, err := json.Marshal(cleanupFingerprintPayload{
		Version:   1,
		AppID:     app.ID,
		Namespace: snapshot.Namespace,
		Resources: results,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal adopted cleanup fingerprint payload: %w", err)
	}
	fingerprint, err := keyring.SignPlan(payload)
	if err != nil {
		return nil, err
	}
	return &adoptedCleanupPlan{
		response: &apisv1.CleanupApplicationResourcesPlanResponse{
			AppID:           app.ID,
			PlanFingerprint: fingerprint,
			ResourceResults: results,
		},
		payload:               payload,
		runtimeChildrenByRoot: runtimeChildrenByRoot,
		blocked:               blocked,
	}, nil
}

func (c *applicationsServiceImpl) planAdoptedCleanupRuntimeChildren(
	ctx context.Context,
	root apisv1.ImportNamespaceResourceResult,
) ([]apisv1.ImportNamespaceResourceResult, error) {
	if root.Source == nil {
		return nil, fmt.Errorf("cleanup root source identity is missing")
	}
	namespace := strings.TrimSpace(root.Source.Namespace)
	rootUID := strings.TrimSpace(root.Source.UID)
	if namespace == "" || rootUID == "" {
		return nil, fmt.Errorf("cleanup root namespace or UID is missing")
	}

	children := make([]apisv1.ImportNamespaceResourceResult, 0)
	switch strings.ToLower(strings.TrimSpace(root.Source.Kind)) {
	case "deployment":
		replicaSets, err := c.KubeClient.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list Deployment runtime ReplicaSets: %w", err)
		}
		replicaSetUIDs := make(map[string]struct{})
		for index := range replicaSets.Items {
			replicaSet := &replicaSets.Items[index]
			if !cleanupControllerOwnerMatches(metav1.GetControllerOf(replicaSet), "apps/v1", "Deployment", rootUID) {
				continue
			}
			replicaSetUIDs[strings.TrimSpace(string(replicaSet.UID))] = struct{}{}
			child, err := buildAdoptedCleanupRuntimeResult(
				replicaSet,
				"apps/v1",
				"ReplicaSet",
				root.ComponentName,
				adoptedCleanupRuntimeControllerRole,
			)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
		pods, err := c.KubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list Deployment runtime Pods: %w", err)
		}
		for index := range pods.Items {
			pod := &pods.Items[index]
			owner := metav1.GetControllerOf(pod)
			ownedByReplicaSet := false
			if owner != nil {
				_, ownedByReplicaSet = replicaSetUIDs[strings.TrimSpace(string(owner.UID))]
			}
			if !ownedByReplicaSet &&
				!cleanupControllerOwnerMatches(owner, "apps/v1", "Deployment", rootUID) {
				continue
			}
			child, err := buildAdoptedCleanupRuntimeResult(
				pod,
				"v1",
				"Pod",
				root.ComponentName,
				adoptedCleanupRuntimePodRole,
			)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
	case "statefulset":
		revisions, err := c.KubeClient.AppsV1().ControllerRevisions(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list StatefulSet runtime ControllerRevisions: %w", err)
		}
		for index := range revisions.Items {
			revision := &revisions.Items[index]
			if !cleanupControllerOwnerMatches(metav1.GetControllerOf(revision), "apps/v1", "StatefulSet", rootUID) {
				continue
			}
			child, err := buildAdoptedCleanupRuntimeResult(
				revision,
				"apps/v1",
				"ControllerRevision",
				root.ComponentName,
				adoptedCleanupRuntimeControllerRole,
			)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
		pods, err := c.KubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list StatefulSet runtime Pods: %w", err)
		}
		for index := range pods.Items {
			pod := &pods.Items[index]
			if !cleanupControllerOwnerMatches(metav1.GetControllerOf(pod), "apps/v1", "StatefulSet", rootUID) {
				continue
			}
			child, err := buildAdoptedCleanupRuntimeResult(
				pod,
				"v1",
				"Pod",
				root.ComponentName,
				adoptedCleanupRuntimePodRole,
			)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
	default:
		return nil, fmt.Errorf("cleanup runtime discovery of kind %s is unsupported", root.Source.Kind)
	}

	sort.SliceStable(children, func(i, j int) bool {
		leftOrder := adoptedCleanupRuntimeDeleteOrder(children[i])
		rightOrder := adoptedCleanupRuntimeDeleteOrder(children[j])
		if leftOrder != rightOrder {
			return leftOrder < rightOrder
		}
		leftRef := cleanupResourceRef(children[i])
		rightRef := cleanupResourceRef(children[j])
		if leftRef != rightRef {
			return leftRef < rightRef
		}
		return children[i].Source.UID < children[j].Source.UID
	})
	return children, nil
}

func cleanupControllerOwnerMatches(
	owner *metav1.OwnerReference,
	apiVersion string,
	kind string,
	uid string,
) bool {
	return owner != nil &&
		strings.EqualFold(strings.TrimSpace(owner.APIVersion), strings.TrimSpace(apiVersion)) &&
		strings.EqualFold(strings.TrimSpace(owner.Kind), strings.TrimSpace(kind)) &&
		strings.TrimSpace(string(owner.UID)) == strings.TrimSpace(uid)
}

func buildAdoptedCleanupRuntimeResult(
	object runtime.Object,
	apiVersion string,
	kind string,
	componentName string,
	dependencyRole string,
) (apisv1.ImportNamespaceResourceResult, error) {
	accessor, err := meta.Accessor(object)
	if err != nil {
		return apisv1.ImportNamespaceResourceResult{}, fmt.Errorf("access cleanup runtime %s metadata: %w", kind, err)
	}
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		return apisv1.ImportNamespaceResourceResult{}, fmt.Errorf("convert cleanup runtime %s: %w", kind, err)
	}
	digest, err := adoption.DigestObject(&unstructured.Unstructured{Object: content})
	if err != nil {
		return apisv1.ImportNamespaceResourceResult{}, fmt.Errorf("digest cleanup runtime %s: %w", kind, err)
	}
	namespace := strings.TrimSpace(accessor.GetNamespace())
	name := strings.TrimSpace(accessor.GetName())
	uid := strings.TrimSpace(string(accessor.GetUID()))
	if namespace == "" || name == "" || uid == "" {
		return apisv1.ImportNamespaceResourceResult{}, fmt.Errorf("cleanup runtime %s identity is incomplete", kind)
	}
	source := &apisv1.ImportNamespaceResourceIdentity{
		APIVersion: apiVersion,
		Kind:       kind,
		Namespace:  namespace,
		Name:       name,
		UID:        uid,
		SpecDigest: digest,
	}
	return apisv1.ImportNamespaceResourceResult{
		Kind:           kind,
		Namespace:      source.Namespace,
		Name:           source.Name,
		ComponentName:  componentName,
		DependencyRole: dependencyRole,
		Ownership:      adoption.OwnershipExclusive,
		Disposition:    adoption.DispositionManaged,
		Status:         "planned",
		Source:         source,
	}, nil
}

func adoptedCleanupRuntimeDeleteOrder(resource apisv1.ImportNamespaceResourceResult) int {
	if strings.TrimSpace(resource.DependencyRole) == adoptedCleanupRuntimePodRole {
		return 0
	}
	return 1
}

func decodeApplicationAdoptionSnapshot(app *model.Applications) (*adoption.Snapshot, error) {
	if app == nil || app.AdoptionSnapshot == nil {
		return nil, fmt.Errorf("%w: application has no adoption snapshot", bcode.ErrApplicationManagementMode)
	}
	payload, err := json.Marshal(app.AdoptionSnapshot)
	if err != nil {
		return nil, fmt.Errorf("marshal application adoption snapshot: %w", err)
	}
	var snapshot adoption.Snapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return nil, fmt.Errorf("decode application adoption snapshot: %w", err)
	}
	if err := snapshot.Validate(); err != nil {
		return nil, fmt.Errorf("validate application adoption snapshot: %w", err)
	}
	return &snapshot, nil
}

func (c *applicationsServiceImpl) planAdoptedCleanupResource(
	ctx context.Context,
	saved adoption.ResourceSnapshot,
	sharing *adoptedCleanupSharingState,
) (apisv1.ImportNamespaceResourceResult, bool) {
	source := saved.Source
	result := apisv1.ImportNamespaceResourceResult{
		Kind:           source.Kind,
		Namespace:      source.Namespace,
		Name:           source.Name,
		ComponentName:  saved.ComponentName,
		DependencyRole: saved.DependencyRole,
		Ownership:      saved.Ownership,
		Disposition:    saved.Disposition,
		Status:         "retained",
		Source: &apisv1.ImportNamespaceResourceIdentity{
			APIVersion:      source.APIVersion,
			Kind:            source.Kind,
			Namespace:       source.Namespace,
			Name:            source.Name,
			UID:             source.UID,
			ResourceVersion: source.ResourceVersion,
			SpecDigest:      source.SpecDigest,
		},
	}
	if cleanupResourceIsAlwaysRetained(saved) {
		if strings.EqualFold(source.Kind, "PersistentVolumeClaim") || strings.EqualFold(source.Kind, "PersistentVolume") {
			result.Ownership = adoption.OwnershipDataProtected
			result.Disposition = adoption.DispositionDataProtected
		}
		return result, false
	}

	live, err := c.getAdoptedCleanupResource(ctx, source)
	if apierrors.IsNotFound(err) {
		result.Disposition = adoption.DispositionExcluded
		result.Status = "missing"
		result.Source.ResourceVersion = ""
		return result, false
	}
	if err != nil {
		result.Disposition = adoption.DispositionBlocked
		result.Status = "blocked"
		result.Error = err.Error()
		return result, true
	}
	if string(live.GetUID()) != strings.TrimSpace(source.UID) {
		result.Disposition = adoption.DispositionBlocked
		result.Status = "blocked"
		result.Error = fmt.Sprintf("source UID changed: expected %q, got %q", source.UID, live.GetUID())
		return result, true
	}
	unstructuredLive, err := runtime.DefaultUnstructuredConverter.ToUnstructured(live)
	if err != nil {
		result.Disposition = adoption.DispositionBlocked
		result.Status = "blocked"
		result.Error = fmt.Sprintf("convert live resource: %v", err)
		return result, true
	}
	digest, err := adoption.DigestObject(&unstructured.Unstructured{Object: unstructuredLive})
	if err != nil {
		result.Disposition = adoption.DispositionBlocked
		result.Status = "blocked"
		result.Error = err.Error()
		return result, true
	}
	result.Source.ResourceVersion = live.GetResourceVersion()
	result.Source.SpecDigest = digest

	if sharing != nil {
		if reason := sharing.blockReason(saved); reason != "" {
			result.Disposition = adoption.DispositionBlocked
			result.Status = "blocked"
			result.Error = reason
			return result, true
		}
		if reason := sharing.sharedReason(saved, live); reason != "" {
			result.Ownership = adoption.OwnershipShared
			result.Disposition = adoption.DispositionSharedPreserved
			result.Status = "retained"
			result.Error = reason
			return result, false
		}
	}
	if statefulSet, ok := live.(*appsv1.StatefulSet); ok {
		if err := validateAdoptedCleanupStatefulSetScaleSafety(statefulSet); err != nil {
			result.Disposition = adoption.DispositionBlocked
			result.Status = "blocked"
			result.Error = err.Error()
			return result, true
		}
	}
	result.Disposition = adoption.DispositionManaged
	result.Status = "planned"
	return result, false
}

func cleanupResourceIsAlwaysRetained(resource adoption.ResourceSnapshot) bool {
	if resource.Disposition != adoption.DispositionManaged ||
		resource.Ownership != adoption.OwnershipExclusive {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(resource.Source.Kind)) {
	case "persistentvolumeclaim", "persistentvolume", "namespace",
		"customresourcedefinition":
		return true
	}
	return strings.TrimSpace(resource.Source.Namespace) == ""
}

func (c *applicationsServiceImpl) getAdoptedCleanupResource(
	ctx context.Context,
	source adoption.ResourceIdentity,
) (metav1.Object, error) {
	namespace := source.Namespace
	name := source.Name
	switch strings.ToLower(source.Kind) {
	case "deployment":
		return c.KubeClient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	case "statefulset":
		return c.KubeClient.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	case "replicaset":
		return c.KubeClient.AppsV1().ReplicaSets(namespace).Get(ctx, name, metav1.GetOptions{})
	case "controllerrevision":
		return c.KubeClient.AppsV1().ControllerRevisions(namespace).Get(ctx, name, metav1.GetOptions{})
	case "pod":
		return c.KubeClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	case "service":
		return c.KubeClient.CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
	case "ingress":
		return c.KubeClient.NetworkingV1().Ingresses(namespace).Get(ctx, name, metav1.GetOptions{})
	case "configmap":
		return c.KubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	case "secret":
		return c.KubeClient.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	case "serviceaccount":
		return c.KubeClient.CoreV1().ServiceAccounts(namespace).Get(ctx, name, metav1.GetOptions{})
	case "role":
		return c.KubeClient.RbacV1().Roles(namespace).Get(ctx, name, metav1.GetOptions{})
	case "rolebinding":
		return c.KubeClient.RbacV1().RoleBindings(namespace).Get(ctx, name, metav1.GetOptions{})
	case "poddisruptionbudget":
		return c.KubeClient.PolicyV1().PodDisruptionBudgets(namespace).Get(ctx, name, metav1.GetOptions{})
	case "networkpolicy":
		return c.KubeClient.NetworkingV1().NetworkPolicies(namespace).Get(ctx, name, metav1.GetOptions{})
	default:
		return nil, fmt.Errorf("cleanup of kind %s is unsupported", source.Kind)
	}
}

func (c *applicationsServiceImpl) deleteAdoptedCleanupResource(
	ctx context.Context,
	resource apisv1.ImportNamespaceResourceResult,
) error {
	if resource.Source == nil {
		return fmt.Errorf("cleanup resource source identity is missing")
	}
	uid := types.UID(resource.Source.UID)
	resourceVersion := resource.Source.ResourceVersion
	propagation := metav1.DeletePropagationOrphan
	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{
		UID:             &uid,
		ResourceVersion: &resourceVersion,
	}, PropagationPolicy: &propagation}
	return c.deleteAdoptedCleanupResourceWithOptions(ctx, resource, options)
}

func (c *applicationsServiceImpl) deleteAdoptedCleanupRuntimeChild(
	ctx context.Context,
	resource apisv1.ImportNamespaceResourceResult,
) error {
	if resource.Source == nil {
		return fmt.Errorf("cleanup runtime resource source identity is missing")
	}
	uid := types.UID(strings.TrimSpace(resource.Source.UID))
	if uid == "" {
		return fmt.Errorf("cleanup runtime resource UID is missing")
	}
	// Quiescing the signed owner chain changes controller resourceVersions before
	// deletion. The immutable UID remains the runtime-child precondition.
	propagation := metav1.DeletePropagationOrphan
	options := metav1.DeleteOptions{
		Preconditions:     &metav1.Preconditions{UID: &uid},
		PropagationPolicy: &propagation,
	}
	return c.deleteAdoptedCleanupResourceWithOptions(ctx, resource, options)
}

func (c *applicationsServiceImpl) deleteAdoptedCleanupResourceWithOptions(
	ctx context.Context,
	resource apisv1.ImportNamespaceResourceResult,
	options metav1.DeleteOptions,
) error {
	if resource.Source == nil {
		return fmt.Errorf("cleanup resource source identity is missing")
	}
	namespace := resource.Source.Namespace
	name := resource.Source.Name
	var err error
	switch strings.ToLower(resource.Source.Kind) {
	case "deployment":
		err = c.KubeClient.AppsV1().Deployments(namespace).Delete(ctx, name, options)
	case "statefulset":
		err = c.KubeClient.AppsV1().StatefulSets(namespace).Delete(ctx, name, options)
	case "replicaset":
		err = c.KubeClient.AppsV1().ReplicaSets(namespace).Delete(ctx, name, options)
	case "controllerrevision":
		err = c.KubeClient.AppsV1().ControllerRevisions(namespace).Delete(ctx, name, options)
	case "pod":
		err = c.KubeClient.CoreV1().Pods(namespace).Delete(ctx, name, options)
	case "service":
		err = c.KubeClient.CoreV1().Services(namespace).Delete(ctx, name, options)
	case "ingress":
		err = c.KubeClient.NetworkingV1().Ingresses(namespace).Delete(ctx, name, options)
	case "configmap":
		err = c.KubeClient.CoreV1().ConfigMaps(namespace).Delete(ctx, name, options)
	case "secret":
		err = c.KubeClient.CoreV1().Secrets(namespace).Delete(ctx, name, options)
	case "serviceaccount":
		err = c.KubeClient.CoreV1().ServiceAccounts(namespace).Delete(ctx, name, options)
	case "role":
		err = c.KubeClient.RbacV1().Roles(namespace).Delete(ctx, name, options)
	case "rolebinding":
		err = c.KubeClient.RbacV1().RoleBindings(namespace).Delete(ctx, name, options)
	case "poddisruptionbudget":
		err = c.KubeClient.PolicyV1().PodDisruptionBudgets(namespace).Delete(ctx, name, options)
	case "networkpolicy":
		err = c.KubeClient.NetworkingV1().NetworkPolicies(namespace).Delete(ctx, name, options)
	default:
		return fmt.Errorf("cleanup of kind %s is unsupported", resource.Source.Kind)
	}
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func cleanupResourceRef(resource apisv1.ImportNamespaceResourceResult) string {
	namespace := strings.TrimSpace(resource.Namespace)
	if resource.Source != nil {
		namespace = strings.TrimSpace(resource.Source.Namespace)
	}
	if namespace == "" {
		return fmt.Sprintf("%s/%s", resource.Kind, resource.Name)
	}
	return fmt.Sprintf("%s/%s/%s", resource.Kind, namespace, resource.Name)
}
