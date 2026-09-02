package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

type adoptedDependencyJobSpec struct {
	jobType      config.JobType
	resourceKind config.ResourceKind
	priority     int
	apiVersion   string
}

var adoptedDependencyJobSpecs = map[string]adoptedDependencyJobSpec{
	"service": {
		jobType:      config.JobDeployService,
		resourceKind: config.ResourceService,
		priority:     config.JobPriorityHigh,
		apiVersion:   "v1",
	},
	"ingress": {
		jobType:      config.JobDeployIngress,
		resourceKind: config.ResourceIngress,
		priority:     config.JobPriorityHigh,
		apiVersion:   networkingv1.SchemeGroupVersion.String(),
	},
	"persistentvolumeclaim": {
		jobType:      config.JobDeployPVC,
		resourceKind: config.ResourcePVC,
		priority:     config.JobPriorityHigh,
		apiVersion:   "v1",
	},
	"configmap": {
		jobType:      config.JobDeployConfigMap,
		resourceKind: config.ResourceConfigMap,
		priority:     config.JobPriorityMaxHigh,
		apiVersion:   "v1",
	},
	"secret": {
		jobType:      config.JobDeploySecret,
		resourceKind: config.ResourceSecret,
		priority:     config.JobPriorityMaxHigh,
		apiVersion:   "v1",
	},
	"serviceaccount": {
		jobType:      config.JobDeployServiceAccount,
		resourceKind: config.ResourceServiceAccount,
		priority:     config.JobPriorityMaxHigh,
		apiVersion:   "v1",
	},
	"role": {
		jobType:      config.JobDeployRole,
		resourceKind: config.ResourceRole,
		priority:     config.JobPriorityMaxHigh,
		apiVersion:   rbacv1.SchemeGroupVersion.String(),
	},
	"rolebinding": {
		jobType:      config.JobDeployRoleBinding,
		resourceKind: config.ResourceRoleBinding,
		priority:     config.JobPriorityMaxHigh,
		apiVersion:   rbacv1.SchemeGroupVersion.String(),
	},
	"poddisruptionbudget": {
		jobType:      config.JobDeployPodDisruptionBudget,
		resourceKind: config.ResourcePodDisruptionBudget,
		priority:     config.JobPriorityHigh,
		apiVersion:   policyv1.SchemeGroupVersion.String(),
	},
	"networkpolicy": {
		jobType:      config.JobDeployNetworkPolicy,
		resourceKind: config.ResourceNetworkPolicy,
		priority:     config.JobPriorityHigh,
		apiVersion:   networkingv1.SchemeGroupVersion.String(),
	},
}

var adoptedDependencyKindByJobType = func() map[config.JobType]string {
	result := make(map[config.JobType]string, len(adoptedDependencyJobSpecs)+2)
	for kind, spec := range adoptedDependencyJobSpecs {
		result[spec.jobType] = kind
	}
	result[config.JobDeployClusterRole] = "clusterrole"
	result[config.JobDeployClusterRoleBinding] = "clusterrolebinding"
	return result
}()

// augmentAdoptedDependencyJobs turns the approved snapshot dependency closure
// into executable jobs. Imported components intentionally contain only root
// workloads, so resources such as ConfigMaps, RBAC and policies otherwise
// never reach the workflow controller.
//
// Existing component-generated Service/Ingress jobs are retained only when
// they resolve to a managed/exclusive snapshot identity. This preserves future
// version changes while ensuring shared, protected, blocked and cluster-scoped
// resources never enter the writable job set.
func augmentAdoptedDependencyJobs(
	ctx context.Context,
	stepGroups [][]StepExecution,
	task *model.WorkflowQueue,
	store datastore.DataStore,
	defaultJobTimeoutSeconds int64,
) ([][]StepExecution, error) {
	if task == nil || strings.TrimSpace(task.AppID) == "" {
		return stepGroups, nil
	}
	if store == nil {
		return nil, fmt.Errorf("load adopted dependency snapshot: datastore is nil")
	}

	app := &model.Applications{ID: strings.TrimSpace(task.AppID)}
	if err := store.Get(ctx, app); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			// A terminal callback/recovery workflow can outlive application
			// metadata. Runtime resource jobs still perform a strict,
			// fail-closed ownership check before touching Kubernetes.
			return stepGroups, nil
		}
		return nil, fmt.Errorf("load application %s for adopted dependency jobs: %w", task.AppID, err)
	}
	if app.EffectiveManagementMode() != config.ManagementModeAdopted {
		return stepGroups, nil
	}
	snapshot, err := workflowAdoptionSnapshot(app)
	if err != nil {
		return nil, err
	}
	if namespace := strings.TrimSpace(app.Namespace); namespace != "" && namespace != strings.TrimSpace(snapshot.Namespace) {
		return nil, fmt.Errorf(
			"adopted application %s namespace %q does not match snapshot namespace %q",
			app.ID,
			namespace,
			snapshot.Namespace,
		)
	}

	resourceAppName := naming.ApplicationResourceKey(app.Name, "", false)
	seen := make(map[string]struct{})
	protectedPVCRequests := make(map[string]string)
	for groupIndex := range stepGroups {
		for executionIndex := range stepGroups[groupIndex] {
			execution := &stepGroups[groupIndex][executionIndex]
			for _, priority := range orderedJobPriorities(execution.Jobs) {
				jobs := execution.Jobs[priority]
				filtered := make([]*model.JobTask, 0, len(jobs))
				for _, jobTask := range jobs {
					kind, dependency := adoptedDependencyKindByJobType[config.JobType(jobTask.JobType)]
					if !dependency {
						filtered = append(filtered, jobTask)
						continue
					}
					// Cluster-scoped RBAC is observable dependency context only
					// in the first adopted-management contract.
					if kind == "clusterrole" || kind == "clusterrolebinding" {
						continue
					}
					resource, err := findSnapshotResourceForGeneratedJob(snapshot, kind, jobTask)
					if err != nil {
						return nil, err
					}
					if resource == nil {
						return nil, fmt.Errorf(
							"adopted dependency job %s/%s (%s) has no approved snapshot source identity",
							jobTask.Namespace,
							jobTask.Name,
							jobTask.JobType,
						)
					}
					protectedPVC, err := adoptedResourceIsProtectedStandalonePVC(snapshot, resource)
					if err != nil {
						return nil, err
					}
					if protectedPVC {
						key := adoptedSnapshotResourceKey(snapshot, resource)
						protectedJob, err := adoptedProtectedPVCJob(
							resource,
							snapshot,
							task,
							resourceAppName,
							defaultJobTimeoutSeconds,
							jobTask,
						)
						if err != nil {
							return nil, err
						}
						request := adoptedPVCStorageRequest(protectedJob)
						if previous, duplicate := protectedPVCRequests[key]; duplicate {
							if previous != request {
								return nil, fmt.Errorf(
									"adopted protected pvc %s has conflicting requested sizes %q and %q",
									key,
									previous,
									request,
								)
							}
							continue
						}
						protectedPVCRequests[key] = request
						seen[key] = struct{}{}
						filtered = append(filtered, protectedJob)
						continue
					}
					if !adoptedResourceIsWritable(resource) {
						continue
					}
					key := adoptedSnapshotResourceKey(snapshot, resource)
					if _, duplicate := seen[key]; duplicate {
						continue
					}
					seen[key] = struct{}{}
					filtered = append(filtered, jobTask)
				}
				execution.Jobs[priority] = filtered
			}
		}
	}

	anchor, anchorsByComponent := adoptedWorkloadExecutionAnchors(stepGroups)
	if anchor == nil {
		return stepGroups, nil
	}

	resources := append([]adoption.ResourceSnapshot(nil), snapshot.Resources...)
	sort.SliceStable(resources, func(i, j int) bool {
		left := adoptedSnapshotResourceKey(snapshot, &resources[i])
		right := adoptedSnapshotResourceKey(snapshot, &resources[j])
		return left < right
	})
	for index := range resources {
		resource := &resources[index]
		protectedPVC, err := adoptedResourceIsProtectedStandalonePVC(snapshot, resource)
		if err != nil {
			return nil, err
		}
		if !adoptedResourceIsWritable(resource) && !protectedPVC {
			continue
		}
		kind := strings.ToLower(strings.TrimSpace(resource.Source.Kind))
		spec, supported := adoptedDependencyJobSpecs[kind]
		if !supported {
			continue
		}
		key := adoptedSnapshotResourceKey(snapshot, resource)
		if _, alreadyPresent := seen[key]; alreadyPresent {
			continue
		}
		jobTask, err := adoptedSnapshotDependencyJob(
			resource,
			snapshot,
			spec,
			task,
			resourceAppName,
			defaultJobTimeoutSeconds,
		)
		if err != nil {
			return nil, err
		}
		target := anchor
		if componentName := strings.TrimSpace(resource.ComponentName); componentName != "" {
			target = anchorsByComponent[componentName]
			if target == nil {
				continue
			}
		}
		target.Jobs[spec.priority] = append(target.Jobs[spec.priority], jobTask)
		seen[key] = struct{}{}
	}
	return stepGroups, nil
}

func workflowAdoptionSnapshot(app *model.Applications) (*adoption.Snapshot, error) {
	if app == nil || app.AdoptionSnapshot == nil {
		return nil, fmt.Errorf("adopted application %q has no adoption snapshot", applicationIdentifier(app))
	}
	payload, err := json.Marshal(app.AdoptionSnapshot)
	if err != nil {
		return nil, fmt.Errorf("marshal adopted application %s snapshot: %w", app.ID, err)
	}
	var snapshot adoption.Snapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return nil, fmt.Errorf("decode adopted application %s snapshot: %w", app.ID, err)
	}
	if err := snapshot.Validate(); err != nil {
		return nil, fmt.Errorf("validate adopted application %s snapshot: %w", app.ID, err)
	}
	return &snapshot, nil
}

func applicationIdentifier(app *model.Applications) string {
	if app == nil {
		return ""
	}
	return app.ID
}

func orderedJobPriorities(buckets map[int][]*model.JobTask) []int {
	priorities := make([]int, 0, len(buckets))
	for priority := range buckets {
		priorities = append(priorities, priority)
	}
	sort.Ints(priorities)
	return priorities
}

func findSnapshotResourceForGeneratedJob(
	snapshot *adoption.Snapshot,
	kind string,
	jobTask *model.JobTask,
) (*adoption.ResourceSnapshot, error) {
	if snapshot == nil || jobTask == nil {
		return nil, nil
	}
	namespace := strings.TrimSpace(jobTask.Namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(snapshot.Namespace)
	}
	name := strings.TrimSpace(jobTask.Name)
	var matches []*adoption.ResourceSnapshot
	for index := range snapshot.Resources {
		resource := &snapshot.Resources[index]
		if !strings.EqualFold(strings.TrimSpace(resource.Source.Kind), kind) {
			continue
		}
		sourceNamespace := adoptedSnapshotResourceNamespace(snapshot, resource)
		if sourceNamespace != namespace {
			continue
		}
		sourceName := strings.TrimSpace(resource.Source.Name)
		nameMatches := sourceName == name
		if !nameMatches && strings.EqualFold(kind, "Ingress") {
			nameMatches = workflowjob.BuildIngressName(sourceName, jobTask.ResourceAppNameOrID()) == name
		}
		if nameMatches {
			matches = append(matches, resource)
		}
	}
	switch len(matches) {
	case 0:
		return nil, nil
	case 1:
		return matches[0], nil
	default:
		return nil, fmt.Errorf(
			"adopted dependency job %s/%s (%s) has ambiguous snapshot source identities",
			namespace,
			name,
			jobTask.JobType,
		)
	}
}

func adoptedResourceIsWritable(resource *adoption.ResourceSnapshot) bool {
	return resource != nil &&
		strings.EqualFold(strings.TrimSpace(resource.Ownership), adoption.OwnershipExclusive) &&
		strings.EqualFold(strings.TrimSpace(resource.Disposition), adoption.DispositionManaged)
}

func adoptedResourceIsProtectedStandalonePVC(
	snapshot *adoption.Snapshot,
	resource *adoption.ResourceSnapshot,
) (bool, error) {
	if resource == nil ||
		!strings.EqualFold(strings.TrimSpace(resource.Source.Kind), "PersistentVolumeClaim") ||
		!strings.EqualFold(strings.TrimSpace(resource.Disposition), adoption.DispositionDataProtected) {
		return false, nil
	}
	ownership := strings.TrimSpace(resource.Ownership)
	if !strings.EqualFold(ownership, adoption.OwnershipDataProtected) &&
		!strings.EqualFold(ownership, adoption.OwnershipExclusive) {
		return false, nil
	}

	namespace := adoptedSnapshotResourceNamespace(snapshot, resource)
	name := strings.TrimSpace(resource.Source.Name)
	jobInfo, err := decodeAdoptedDependencyManifest(resource, namespace, name)
	if err != nil {
		return false, err
	}
	pvc, ok := jobInfo.(*corev1.PersistentVolumeClaim)
	if !ok {
		return false, fmt.Errorf("adopted protected pvc %s/%s has invalid snapshot payload", namespace, name)
	}
	for _, owner := range pvc.OwnerReferences {
		if strings.EqualFold(strings.TrimSpace(owner.Kind), "StatefulSet") {
			return false, nil
		}
	}
	if snapshot == nil {
		return true, nil
	}
	for index := range snapshot.Resources {
		workload := &snapshot.Resources[index]
		if !strings.EqualFold(strings.TrimSpace(workload.Source.Kind), "StatefulSet") ||
			adoptedSnapshotResourceNamespace(snapshot, workload) != namespace ||
			len(workload.Manifest) == 0 {
			continue
		}
		var statefulSet appsv1.StatefulSet
		if err := json.Unmarshal(workload.Manifest, &statefulSet); err != nil {
			return false, fmt.Errorf(
				"decode adopted StatefulSet %s/%s while classifying protected pvc: %w",
				namespace,
				workload.Source.Name,
				err,
			)
		}
		for _, template := range statefulSet.Spec.VolumeClaimTemplates {
			prefix := strings.TrimSpace(template.Name) + "-" + strings.TrimSpace(statefulSet.Name) + "-"
			if !strings.HasPrefix(name, prefix) {
				continue
			}
			ordinal := strings.TrimPrefix(name, prefix)
			if value, err := strconv.Atoi(ordinal); err == nil && value >= 0 {
				return false, nil
			}
		}
	}
	return true, nil
}

func adoptedProtectedPVCJob(
	resource *adoption.ResourceSnapshot,
	snapshot *adoption.Snapshot,
	task *model.WorkflowQueue,
	resourceAppName string,
	defaultJobTimeoutSeconds int64,
	generated *model.JobTask,
) (*model.JobTask, error) {
	jobTask, err := adoptedSnapshotDependencyJob(
		resource,
		snapshot,
		adoptedDependencyJobSpecs["persistentvolumeclaim"],
		task,
		resourceAppName,
		defaultJobTimeoutSeconds,
	)
	if err != nil {
		return nil, err
	}
	if generated == nil {
		return jobTask, nil
	}
	desired, ok := generated.JobInfo.(*corev1.PersistentVolumeClaim)
	if !ok || desired == nil {
		return nil, fmt.Errorf(
			"adopted protected pvc %s/%s has unsupported generated job payload %T",
			jobTask.Namespace,
			jobTask.Name,
			generated.JobInfo,
		)
	}
	safeDesired, ok := jobTask.JobInfo.(*corev1.PersistentVolumeClaim)
	if !ok || safeDesired == nil {
		return nil, fmt.Errorf("adopted protected pvc snapshot payload is invalid")
	}
	if desired.Spec.StorageClassName != nil &&
		!equalOptionalString(desired.Spec.StorageClassName, safeDesired.Spec.StorageClassName) {
		return nil, fmt.Errorf(
			"adopted protected pvc %s/%s cannot change storageClassName from %q to %q",
			jobTask.Namespace,
			jobTask.Name,
			optionalStringValue(safeDesired.Spec.StorageClassName),
			optionalStringValue(desired.Spec.StorageClassName),
		)
	}
	if requested, exists := desired.Spec.Resources.Requests[corev1.ResourceStorage]; exists {
		if safeDesired.Spec.Resources.Requests == nil {
			safeDesired.Spec.Resources.Requests = make(corev1.ResourceList)
		}
		safeDesired.Spec.Resources.Requests[corev1.ResourceStorage] = requested.DeepCopy()
	}
	jobTask.FailurePolicy = generated.FailurePolicy
	if generated.Timeout > 0 {
		jobTask.Timeout = generated.Timeout
	}
	return jobTask, nil
}

func equalOptionalString(left, right *string) bool {
	return optionalStringValue(left) == optionalStringValue(right)
}

func optionalStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func adoptedPVCStorageRequest(jobTask *model.JobTask) string {
	if jobTask == nil {
		return ""
	}
	pvc, ok := jobTask.JobInfo.(*corev1.PersistentVolumeClaim)
	if !ok || pvc == nil {
		return ""
	}
	requested, exists := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if !exists {
		return ""
	}
	return requested.String()
}

func adoptedSnapshotResourceNamespace(snapshot *adoption.Snapshot, resource *adoption.ResourceSnapshot) string {
	if resource == nil {
		return ""
	}
	if namespace := strings.TrimSpace(resource.Source.Namespace); namespace != "" {
		return namespace
	}
	if snapshot == nil {
		return ""
	}
	return strings.TrimSpace(snapshot.Namespace)
}

func adoptedSnapshotResourceKey(snapshot *adoption.Snapshot, resource *adoption.ResourceSnapshot) string {
	if resource == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(resource.Source.Kind)) + "/" +
		adoptedSnapshotResourceNamespace(snapshot, resource) + "/" +
		strings.TrimSpace(resource.Source.Name)
}

func adoptedWorkloadExecutionAnchors(stepGroups [][]StepExecution) (*StepExecution, map[string]*StepExecution) {
	anchorsByComponent := make(map[string]*StepExecution)
	var first *StepExecution
	for groupIndex := range stepGroups {
		for executionIndex := range stepGroups[groupIndex] {
			execution := &stepGroups[groupIndex][executionIndex]
			for _, jobs := range execution.Jobs {
				for _, jobTask := range jobs {
					if jobTask == nil {
						continue
					}
					switch config.JobType(jobTask.JobType) {
					case config.JobDeploy, config.JobDeployStore:
						if first == nil {
							first = execution
						}
						componentName := strings.TrimSpace(jobTask.Name)
						if componentName == "" {
							continue
						}
						if _, exists := anchorsByComponent[componentName]; !exists {
							anchorsByComponent[componentName] = execution
						}
					}
				}
			}
		}
	}
	return first, anchorsByComponent
}

func adoptedSnapshotDependencyJob(
	resource *adoption.ResourceSnapshot,
	snapshot *adoption.Snapshot,
	spec adoptedDependencyJobSpec,
	task *model.WorkflowQueue,
	resourceAppName string,
	defaultJobTimeoutSeconds int64,
) (*model.JobTask, error) {
	if resource == nil || snapshot == nil || task == nil {
		return nil, fmt.Errorf("adopted dependency job input is incomplete")
	}
	if strings.TrimSpace(resource.Source.APIVersion) != spec.apiVersion {
		return nil, fmt.Errorf(
			"adopted %s %s uses unsupported apiVersion %q",
			resource.Source.Kind,
			resource.Source.Name,
			resource.Source.APIVersion,
		)
	}
	namespace := adoptedSnapshotResourceNamespace(snapshot, resource)
	name := strings.TrimSpace(resource.Source.Name)
	if namespace == "" || name == "" {
		return nil, fmt.Errorf("adopted %s source namespace and name are required", resource.Source.Kind)
	}
	jobInfo, err := decodeAdoptedDependencyManifest(resource, namespace, name)
	if err != nil {
		return nil, err
	}

	jobTask := NewJobTask(
		name,
		namespace,
		task.WorkflowID,
		task.ProjectID,
		task.AppID,
		task.TaskID,
		defaultJobTimeoutSeconds,
		resourceAppName,
	)
	jobTask.JobType = string(spec.jobType)
	jobTask.JobInfo = jobInfo
	jobTask.Info = buildResourceInfo(spec.resourceKind, namespace, name)
	setDeployTimeout(jobTask)
	return jobTask, nil
}

func decodeAdoptedDependencyManifest(
	resource *adoption.ResourceSnapshot,
	namespace, name string,
) (interface{}, error) {
	if len(resource.Manifest) == 0 {
		return nil, fmt.Errorf("adopted %s %s/%s has no recreation manifest", resource.Source.Kind, namespace, name)
	}
	decode := func(target interface{}) error {
		if err := json.Unmarshal(resource.Manifest, target); err != nil {
			return fmt.Errorf(
				"decode adopted %s %s/%s snapshot manifest: %w",
				resource.Source.Kind,
				namespace,
				name,
				err,
			)
		}
		return nil
	}
	validate := func(actualNamespace, actualName string) error {
		if strings.TrimSpace(actualNamespace) != namespace || strings.TrimSpace(actualName) != name {
			return fmt.Errorf(
				"adopted %s snapshot manifest identity %s/%s does not match source %s/%s",
				resource.Source.Kind,
				actualNamespace,
				actualName,
				namespace,
				name,
			)
		}
		return nil
	}

	switch strings.ToLower(strings.TrimSpace(resource.Source.Kind)) {
	case "service":
		var object applyv1.ServiceApplyConfiguration
		if err := decode(&object); err != nil {
			return nil, err
		}
		if object.Namespace == nil || object.Name == nil {
			return nil, fmt.Errorf("adopted Service snapshot manifest identity is incomplete")
		}
		if err := validate(*object.Namespace, *object.Name); err != nil {
			return nil, err
		}
		return &object, nil
	case "ingress":
		var object networkingv1.Ingress
		if err := decode(&object); err != nil {
			return nil, err
		}
		if err := validate(object.Namespace, object.Name); err != nil {
			return nil, err
		}
		return &object, nil
	case "persistentvolumeclaim":
		var object corev1.PersistentVolumeClaim
		if err := decode(&object); err != nil {
			return nil, err
		}
		if err := validate(object.Namespace, object.Name); err != nil {
			return nil, err
		}
		return &object, nil
	case "configmap":
		var object corev1.ConfigMap
		if err := decode(&object); err != nil {
			return nil, err
		}
		if err := validate(object.Namespace, object.Name); err != nil {
			return nil, err
		}
		return &object, nil
	case "secret":
		var object corev1.Secret
		if err := decode(&object); err != nil {
			return nil, err
		}
		if err := validate(object.Namespace, object.Name); err != nil {
			return nil, err
		}
		// Snapshot manifests deliberately contain no Secret payload. Runtime
		// decryption is performed only inside the adopted Secret controller.
		object.Data = nil
		object.StringData = nil
		return &object, nil
	case "serviceaccount":
		var object corev1.ServiceAccount
		if err := decode(&object); err != nil {
			return nil, err
		}
		if err := validate(object.Namespace, object.Name); err != nil {
			return nil, err
		}
		return &object, nil
	case "role":
		var object rbacv1.Role
		if err := decode(&object); err != nil {
			return nil, err
		}
		if err := validate(object.Namespace, object.Name); err != nil {
			return nil, err
		}
		return &object, nil
	case "rolebinding":
		var object rbacv1.RoleBinding
		if err := decode(&object); err != nil {
			return nil, err
		}
		if err := validate(object.Namespace, object.Name); err != nil {
			return nil, err
		}
		return &object, nil
	case "poddisruptionbudget":
		var object policyv1.PodDisruptionBudget
		if err := decode(&object); err != nil {
			return nil, err
		}
		if err := validate(object.Namespace, object.Name); err != nil {
			return nil, err
		}
		return &object, nil
	case "networkpolicy":
		var object networkingv1.NetworkPolicy
		if err := decode(&object); err != nil {
			return nil, err
		}
		if err := validate(object.Namespace, object.Name); err != nil {
			return nil, err
		}
		return &object, nil
	default:
		return nil, fmt.Errorf("unsupported adopted dependency kind %q", resource.Source.Kind)
	}
}
