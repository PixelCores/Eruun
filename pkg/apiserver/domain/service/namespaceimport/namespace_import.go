package namespaceimport

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	applicationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	validationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/validation"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

const (
	importModeDryRun = "dry-run"
	importModeApply  = "apply"

	importResourceStatusPlanned = "planned"
	importResourceStatusLabeled = "labeled"
	importResourceStatusFailed  = "failed"
	importResourceStatusSkipped = "skipped"

	tryImportOrphanReasonMissingInNamespaceScan = "missing_in_namespace_scan"
)

const (
	importKindDeployments            = "deployments"
	importKindStatefulSets           = "statefulsets"
	importKindDaemonSets             = "daemonsets"
	importKindJobs                   = "jobs"
	importKindCronJobs               = "cronjobs"
	importKindConfigMaps             = "configmaps"
	importKindSecrets                = "secrets"
	importKindPersistentVolumeClaims = "persistentvolumeclaims"
	importKindServices               = "services"
	importKindIngresses              = "ingresses"
	importKindServiceAccounts        = "serviceaccounts"
	importKindRoles                  = "roles"
	importKindRoleBindings           = "rolebindings"
	importKindClusterRoles           = "clusterroles"
	importKindClusterRoleBindings    = "clusterrolebindings"
	importKindPodDisruptionBudgets   = "poddisruptionbudgets"
	importKindNetworkPolicies        = "networkpolicies"
	importKindPersistentVolumes      = "persistentvolumes"
)

var defaultImportKinds = []string{
	importKindDeployments,
	importKindStatefulSets,
	importKindDaemonSets,
	importKindJobs,
	importKindCronJobs,
	importKindConfigMaps,
	importKindSecrets,
	importKindPersistentVolumeClaims,
	importKindServices,
	importKindIngresses,
	importKindServiceAccounts,
	importKindRoles,
	importKindRoleBindings,
}

var allImportKinds = []string{
	importKindDeployments,
	importKindStatefulSets,
	importKindDaemonSets,
	importKindJobs,
	importKindCronJobs,
	importKindConfigMaps,
	importKindSecrets,
	importKindPersistentVolumeClaims,
	importKindServices,
	importKindIngresses,
	importKindServiceAccounts,
	importKindRoles,
	importKindRoleBindings,
	importKindClusterRoles,
	importKindClusterRoleBindings,
	importKindPodDisruptionBudgets,
	importKindNetworkPolicies,
	importKindPersistentVolumes,
}

var importKindAliases = map[string]string{
	"deploy":                 importKindDeployments,
	"deployment":             importKindDeployments,
	"deployments":            importKindDeployments,
	"daemonset":              importKindDaemonSets,
	"daemonsets":             importKindDaemonSets,
	"ds":                     importKindDaemonSets,
	"job":                    importKindJobs,
	"jobs":                   importKindJobs,
	"cronjob":                importKindCronJobs,
	"cronjobs":               importKindCronJobs,
	"cj":                     importKindCronJobs,
	"sts":                    importKindStatefulSets,
	"statefulset":            importKindStatefulSets,
	"statefulsets":           importKindStatefulSets,
	"configmap":              importKindConfigMaps,
	"configmaps":             importKindConfigMaps,
	"secret":                 importKindSecrets,
	"secrets":                importKindSecrets,
	"pvc":                    importKindPersistentVolumeClaims,
	"pvcs":                   importKindPersistentVolumeClaims,
	"persistentvolumeclaim":  importKindPersistentVolumeClaims,
	"persistentvolumeclaims": importKindPersistentVolumeClaims,
	"service":                importKindServices,
	"services":               importKindServices,
	"svc":                    importKindServices,
	"ingress":                importKindIngresses,
	"ingresses":              importKindIngresses,
	"serviceaccount":         importKindServiceAccounts,
	"serviceaccounts":        importKindServiceAccounts,
	"sa":                     importKindServiceAccounts,
	"role":                   importKindRoles,
	"roles":                  importKindRoles,
	"rolebinding":            importKindRoleBindings,
	"rolebindings":           importKindRoleBindings,
	"clusterrole":            importKindClusterRoles,
	"clusterroles":           importKindClusterRoles,
	"clusterrolebinding":     importKindClusterRoleBindings,
	"clusterrolebindings":    importKindClusterRoleBindings,
	"pdb":                    importKindPodDisruptionBudgets,
	"poddisruptionbudget":    importKindPodDisruptionBudgets,
	"poddisruptionbudgets":   importKindPodDisruptionBudgets,
	"networkpolicy":          importKindNetworkPolicies,
	"networkpolicies":        importKindNetworkPolicies,
	"netpol":                 importKindNetworkPolicies,
	"pv":                     importKindPersistentVolumes,
	"persistentvolume":       importKindPersistentVolumes,
	"persistentvolumes":      importKindPersistentVolumes,
}

type NamespaceImportService interface {
	ImportNamespaceResources(ctx context.Context, req apisv1.ImportNamespaceApplicationsRequest) (*apisv1.ImportNamespaceApplicationsResponse, error)
	TryImportNamespaceResources(ctx context.Context, req apisv1.TryImportNamespaceApplicationsRequest) (*apisv1.TryImportNamespaceApplicationsResponse, error)
}

type ValidationService interface {
	TryApplication(ctx context.Context, req apisv1.CreateApplicationsRequest) *apisv1.TryApplicationResponse
}

type applicationCreator interface {
	CreateApplications(
		context.Context,
		apisv1.CreateApplicationsRequest,
	) (*apisv1.ApplicationBase, error)
	CreateApplicationsWithMutation(
		context.Context,
		apisv1.CreateApplicationsRequest,
		applicationservice.ApplicationCreateMutation,
	) (*apisv1.ApplicationBase, error)
}

type namespaceImportServiceImpl struct {
	Cfg                 *config.Config       `inject:""`
	KubeClient          kubernetes.Interface `inject:"kubeClient"`
	Cache               cache.ICache         `inject:"cache"`
	AdoptedImportLocker locker.Locker
	ApplicationService  applicationCreator               `inject:""`
	ValidationService   ValidationService                `inject:""`
	AppRepo             repository.ApplicationRepository `inject:""`
	WorkflowRepo        repository.WorkflowRepository    `inject:""`
	ComponentRepo       repository.ComponentRepository   `inject:""`
}

func NewNamespaceImportService() NamespaceImportService {
	return &namespaceImportServiceImpl{}
}

type importResource struct {
	kindKey        string
	kind           string
	namespace      string
	name           string
	labels         map[string]string
	object         *unstructured.Unstructured
	appID          string
	componentName  string
	explicitAppID  bool
	dependencyRole string
	ownership      string
	disposition    string
	dispositionErr string
}

type workloadRef struct {
	appID          string
	componentName  string
	labels         map[string]string
	configMaps     map[string]struct{}
	pvcs           map[string]struct{}
	pvcPrefixes    map[string]struct{}
	secrets        map[string]struct{}
	serviceAccount string
}

type importAppPlan struct {
	appID                           string
	name                            string
	alias                           string
	resources                       []*importResource
	components                      []apisv1.CreateComponentRequest
	createReq                       apisv1.CreateApplicationsRequest
	componentNames                  []string
	resourceComponentByKey          map[string]string
	workloadComponentByOriginalName map[string]string
	existingComponentIDByName       map[string]int
	applyErrorStatus                string
	warnings                        []string
	err                             error
	adopted                         *adoptedAppPlanState
}

func (s *namespaceImportServiceImpl) ImportNamespaceResources(ctx context.Context, req apisv1.ImportNamespaceApplicationsRequest) (*apisv1.ImportNamespaceApplicationsResponse, error) {
	namespace := strings.TrimSpace(req.Namespace)
	if namespace == "" {
		return nil, bcode.ErrApplicationConfig
	}
	if s.KubeClient == nil {
		return nil, fmt.Errorf("kube client is nil")
	}
	if s.ApplicationService == nil {
		return nil, fmt.Errorf("application service is nil")
	}

	mode, err := normalizeImportMode(req.Mode)
	if err != nil {
		return nil, err
	}
	managementMode, err := normalizeImportManagementMode(req)
	if err != nil {
		return nil, err
	}
	if managementMode == config.ManagementModeAdopted && strings.TrimSpace(req.Mode) == "" {
		return nil, fmt.Errorf("%w: adopted namespace import requires explicit mode dry-run or apply", bcode.ErrApplicationConfig)
	}
	if managementMode == config.ManagementModeAdopted && mode == importModeApply {
		return s.withAdoptedNamespaceApplyLock(
			ctx,
			namespace,
			func(lockCtx context.Context) (*apisv1.ImportNamespaceApplicationsResponse, error) {
				return s.importNamespaceResources(lockCtx, req, namespace, mode, managementMode)
			},
		)
	}
	return s.importNamespaceResources(ctx, req, namespace, mode, managementMode)
}

func (s *namespaceImportServiceImpl) importNamespaceResources(
	ctx context.Context,
	req apisv1.ImportNamespaceApplicationsRequest,
	namespace string,
	mode string,
	managementMode config.ManagementMode,
) (*apisv1.ImportNamespaceApplicationsResponse, error) {
	run, err := s.prepareNamespaceImportRun(ctx, req, namespace, mode, managementMode)
	if err != nil {
		return nil, err
	}
	if err := run.buildResponse(); err != nil {
		return nil, err
	}
	if run.finishWithoutApply() {
		return run.response, nil
	}
	if err := run.verifyAdoptedApply(req); err != nil {
		return nil, err
	}
	if err := s.applyNamespaceImportRun(ctx, run); err != nil {
		return nil, err
	}
	return run.response, nil
}

func (s *namespaceImportServiceImpl) TryImportNamespaceResources(ctx context.Context, req apisv1.TryImportNamespaceApplicationsRequest) (*apisv1.TryImportNamespaceApplicationsResponse, error) {
	namespace := strings.TrimSpace(req.Namespace)
	if namespace == "" {
		return nil, bcode.ErrApplicationConfig
	}
	if s.KubeClient == nil {
		return nil, fmt.Errorf("kube client is nil")
	}
	if s.ComponentRepo == nil {
		return nil, fmt.Errorf("component repository is nil")
	}

	includeKinds, kindWarnings, err := normalizeImportKinds(req.IncludeKinds)
	if err != nil {
		return nil, err
	}
	resources, scanWarnings, err := s.scanNamespaceResources(ctx, namespace, includeKinds)
	if err != nil {
		return nil, err
	}

	grouped, appNames, appAliases, assignWarnings := assignResourcesToApps(namespace, resources)
	plans := s.buildImportPlans(grouped, appNames, appAliases, sharedAppIDForNamespace(namespace))
	existingAppIDMap, _, _, err := s.loadExistingAppIndex(ctx, namespace)
	if err != nil {
		return nil, err
	}

	resp := &apisv1.TryImportNamespaceApplicationsResponse{
		Namespace: namespace,
		Summary: apisv1.TryImportNamespaceSummary{
			ResourcesScanned: len(resources),
		},
		Warnings: append(append(append([]string{}, kindWarnings...), scanWarnings...), assignWarnings...),
	}

	for _, plan := range plans {
		appResult := apisv1.TryImportNamespaceAppResult{
			Name:              plan.name,
			ScannedComponents: append([]string(nil), plan.componentNames...),
		}
		if len(plan.warnings) > 0 {
			resp.Warnings = append(resp.Warnings, plan.warnings...)
		}
		if plan.err != nil {
			appResult.Error = plan.err.Error()
			resp.Apps = append(resp.Apps, appResult)
			continue
		}

		appKey := appNameNamespaceKey(plan.name, namespace)
		existingAppID := strings.TrimSpace(existingAppIDMap[appKey])
		if existingAppID == "" {
			resp.Apps = append(resp.Apps, appResult)
			continue
		}
		appResult.AppID = existingAppID
		resp.Summary.AppsMatched++

		existingComponents, err := s.ComponentRepo.FindByAppID(ctx, existingAppID)
		if err != nil {
			appResult.Error = fmt.Sprintf("list existing components failed: %v", err)
			resp.Apps = append(resp.Apps, appResult)
			continue
		}

		scannedSet := make(map[string]struct{}, len(plan.componentNames))
		for _, componentName := range plan.componentNames {
			normalized := strings.ToLower(strings.TrimSpace(componentName))
			if normalized == "" {
				continue
			}
			scannedSet[normalized] = struct{}{}
		}

		orphanNames := make([]string, 0)
		for _, comp := range existingComponents {
			if comp == nil {
				continue
			}
			componentReq, err := convertComponentModelToCreateRequest(comp)
			if err != nil {
				resp.Warnings = append(resp.Warnings, fmt.Sprintf("convert existing component %s failed: %v", strings.TrimSpace(comp.Name), err))
				continue
			}
			normalizedName := strings.ToLower(strings.TrimSpace(componentReq.Name))
			if normalizedName == "" {
				continue
			}
			if _, exists := scannedSet[normalizedName]; exists {
				continue
			}

			matchedKinds := matchedIncludeKindsForTryImport(componentReq, includeKinds)
			if len(matchedKinds) == 0 {
				continue
			}
			orphanNames = append(orphanNames, strings.TrimSpace(componentReq.Name))
			resp.OrphanComponents = append(resp.OrphanComponents, apisv1.TryImportNamespaceOrphanComponent{
				AppID:               existingAppID,
				AppName:             plan.name,
				ComponentName:       strings.TrimSpace(componentReq.Name),
				ComponentType:       componentReq.ComponentType,
				Reason:              tryImportOrphanReasonMissingInNamespaceScan,
				MatchedIncludeKinds: matchedKinds,
			})
		}
		sort.Strings(orphanNames)
		appResult.OrphanComponentNames = orphanNames
		resp.Summary.OrphansDetected += len(orphanNames)
		resp.Apps = append(resp.Apps, appResult)
	}

	sort.Slice(resp.OrphanComponents, func(i, j int) bool {
		if resp.OrphanComponents[i].AppID != resp.OrphanComponents[j].AppID {
			return resp.OrphanComponents[i].AppID < resp.OrphanComponents[j].AppID
		}
		return resp.OrphanComponents[i].ComponentName < resp.OrphanComponents[j].ComponentName
	})
	return resp, nil
}

func matchedIncludeKindsForTryImport(component apisv1.CreateComponentRequest, includeKinds map[string]struct{}) []string {
	if len(includeKinds) == 0 {
		return nil
	}
	componentKinds := inferredPrimaryImportKindsForComponent(component)
	if len(componentKinds) == 0 {
		return nil
	}
	if !allComponentKindsIncluded(componentKinds, includeKinds) {
		return nil
	}
	result := make([]string, 0, len(componentKinds))
	for _, kind := range allImportKinds {
		if _, ok := componentKinds[kind]; ok {
			result = append(result, kind)
		}
	}
	return result
}

func normalizeImportMode(mode string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(mode))
	if normalized == "" {
		return importModeDryRun, nil
	}
	switch normalized {
	case importModeDryRun, importModeApply:
		return normalized, nil
	default:
		return "", bcode.ErrApplicationConfig
	}
}

func appNameNamespaceKey(name, namespace string) string {
	return strings.ToLower(strings.TrimSpace(namespace)) + "/" + strings.ToLower(strings.TrimSpace(name))
}

func (s *namespaceImportServiceImpl) prepareImportPlansForExecution(
	ctx context.Context,
	namespace string,
	plans []importAppPlan,
	includeKinds map[string]struct{},
	existingAppIDMap map[string]string,
	existingAppIDSet map[string]struct{},
	existingAppNameByID map[string]string,
) {
	for idx := range plans {
		plan := &plans[idx]
		if plan.err != nil {
			if strings.TrimSpace(plan.applyErrorStatus) == "" {
				plan.applyErrorStatus = importResourceStatusSkipped
			}
			continue
		}

		createReq, err := buildImportCreateRequest(namespace, *plan, existingAppIDMap, existingAppIDSet, existingAppNameByID)
		if err != nil {
			plan.err = err
			plan.applyErrorStatus = importResourceStatusFailed
			continue
		}
		if createReq.ID != "" && !isFullImportKinds(includeKinds) {
			mergedComponents, err := s.mergeCreateComponentsWithExisting(ctx, createReq.ID, createReq.Name, createReq.Namespace, createReq.Component, includeKinds)
			if err != nil {
				plan.err = err
				plan.applyErrorStatus = importResourceStatusFailed
				continue
			}
			// createReq.Component has already been sanitized for imported components in
			// buildImportCreateRequest. For partial imports we must keep omitted existing
			// components untouched to avoid mutating user-defined selectors/labels.
			createReq.Component = mergedComponents
		}
		plan.createReq = createReq
		if createReq.ID != "" && s.ComponentRepo != nil {
			existingComponents, err := s.ComponentRepo.FindByAppID(ctx, createReq.ID)
			if err != nil {
				plan.err = fmt.Errorf("load existing import component IDs: %w", err)
				plan.applyErrorStatus = importResourceStatusFailed
				continue
			}
			plan.existingComponentIDByName = make(map[string]int, len(existingComponents))
			for _, component := range existingComponents {
				if component != nil && strings.TrimSpace(component.Name) != "" && component.ID > 0 {
					plan.existingComponentIDByName[component.Name] = component.ID
				}
			}
		}

		if err := s.tryValidateImportCreateRequest(ctx, createReq); err != nil {
			plan.err = err
			plan.applyErrorStatus = importResourceStatusFailed
		}
	}
}

func buildImportCreateRequest(
	namespace string,
	plan importAppPlan,
	existingAppIDMap map[string]string,
	existingAppIDSet map[string]struct{},
	existingAppNameByID map[string]string,
) (apisv1.CreateApplicationsRequest, error) {
	createReq := apisv1.CreateApplicationsRequest{
		Name:        plan.name,
		Namespace:   namespace,
		Alias:       plan.alias,
		Version:     "imported",
		Project:     "imported",
		Description: fmt.Sprintf("imported from namespace %s", namespace),
		Component:   sanitizeImportComponentsForCreate(plan.components),
	}

	appKey := appNameNamespaceKey(plan.name, namespace)
	selectorAppID, selectorManaged, selectorErr := resolveSelectorManagedAppID(plan.resources)
	if selectorErr != nil {
		return apisv1.CreateApplicationsRequest{}, selectorErr
	}
	if selectorManaged {
		if existingIDByName := strings.TrimSpace(existingAppIDMap[appKey]); existingIDByName != "" && !strings.EqualFold(existingIDByName, selectorAppID) {
			return apisv1.CreateApplicationsRequest{}, fmt.Errorf("selector-managed appID %q conflicts with existing appID %q for app name %q", selectorAppID, existingIDByName, plan.name)
		}
		if _, exists := existingAppIDSet[selectorAppID]; !exists {
			return apisv1.CreateApplicationsRequest{}, fmt.Errorf("selector-managed appID %q not found in database; refusing to create a new appID for immutable selectors", selectorAppID)
		}
		existingName := strings.TrimSpace(existingAppNameByID[selectorAppID])
		if existingName == "" {
			return apisv1.CreateApplicationsRequest{}, fmt.Errorf("selector-managed appID %q found in database without an application name", selectorAppID)
		}
		if !strings.EqualFold(existingName, strings.TrimSpace(plan.name)) {
			return apisv1.CreateApplicationsRequest{}, fmt.Errorf("selector-managed appID %q belongs to app name %q, but import plan resolved %q; refusing fallback rename", selectorAppID, existingName, plan.name)
		}
		createReq.ID = selectorAppID
		createReq.Name = existingName
	} else if existingID := existingAppIDMap[appKey]; existingID != "" {
		createReq.ID = existingID
	}
	return createReq, nil
}

func isFullImportKinds(includeKinds map[string]struct{}) bool {
	if len(includeKinds) != len(allImportKinds) {
		return false
	}
	for _, kind := range allImportKinds {
		if _, ok := includeKinds[kind]; !ok {
			return false
		}
	}
	return true
}

func (s *namespaceImportServiceImpl) mergeCreateComponentsWithExisting(
	ctx context.Context,
	appID string,
	appName string,
	namespace string,
	current []apisv1.CreateComponentRequest,
	includeKinds map[string]struct{},
) ([]apisv1.CreateComponentRequest, error) {
	if s.ComponentRepo == nil {
		return nil, fmt.Errorf("component repository is nil")
	}

	existingComponents, err := s.ComponentRepo.FindByAppID(ctx, appID)
	if err != nil {
		return nil, fmt.Errorf("find existing components for app %s: %w", appID, err)
	}

	merged := make(map[string]apisv1.CreateComponentRequest, len(existingComponents)+len(current))
	order := make([]string, 0, len(existingComponents)+len(current))
	currentByName := make(map[string]struct{}, len(current))
	currentResourceKeys := importComponentResourceKeys(current, appName, namespace)
	for _, req := range current {
		name := strings.ToLower(strings.TrimSpace(req.Name))
		if name == "" {
			continue
		}
		currentByName[name] = struct{}{}
	}

	for _, comp := range existingComponents {
		if comp == nil {
			continue
		}
		req, err := convertComponentModelToCreateRequest(comp)
		if err != nil {
			return nil, fmt.Errorf("convert existing component %s: %w", comp.Name, err)
		}
		req = sanitizeRetainedImportComponentForCreate(req)
		name := strings.ToLower(strings.TrimSpace(req.Name))
		if name == "" {
			continue
		}
		if _, exists := currentByName[name]; exists {
			continue
		}
		if importComponentConflictsWithResourceKeys(req, appName, namespace, currentResourceKeys) {
			continue
		}
		if !shouldRetainExistingComponentForPartialImport(req, includeKinds) {
			continue
		}
		if _, exists := merged[name]; !exists {
			order = append(order, name)
		}
		merged[name] = req
	}
	for _, req := range current {
		name := strings.ToLower(strings.TrimSpace(req.Name))
		if name == "" {
			continue
		}
		if _, exists := merged[name]; !exists {
			order = append(order, name)
		}
		merged[name] = req
	}

	result := make([]apisv1.CreateComponentRequest, 0, len(order))
	for _, name := range order {
		component, ok := merged[name]
		if !ok {
			continue
		}
		result = append(result, component)
	}
	return result, nil
}

func (s *namespaceImportServiceImpl) mergeCreateComponentsWithAllExisting(
	ctx context.Context,
	appID string,
	appName string,
	namespace string,
	current []apisv1.CreateComponentRequest,
) ([]apisv1.CreateComponentRequest, error) {
	if s.ComponentRepo == nil {
		return nil, fmt.Errorf("component repository is nil")
	}

	existingComponents, err := s.ComponentRepo.FindByAppID(ctx, appID)
	if err != nil {
		return nil, fmt.Errorf("find existing components for app %s: %w", appID, err)
	}

	merged := make(map[string]apisv1.CreateComponentRequest, len(existingComponents)+len(current))
	order := make([]string, 0, len(existingComponents)+len(current))
	currentResourceKeys := importComponentResourceKeys(current, appName, namespace)
	for _, comp := range existingComponents {
		if comp == nil {
			continue
		}
		req, err := convertComponentModelToCreateRequest(comp)
		if err != nil {
			return nil, fmt.Errorf("convert existing component %s: %w", comp.Name, err)
		}
		req = sanitizeRetainedImportComponentForCreate(req)
		name := strings.ToLower(strings.TrimSpace(req.Name))
		if name == "" {
			continue
		}
		if importComponentConflictsWithResourceKeys(req, appName, namespace, currentResourceKeys) {
			continue
		}
		merged[name] = req
		order = append(order, name)
	}

	for _, req := range current {
		name := strings.ToLower(strings.TrimSpace(req.Name))
		if name == "" {
			continue
		}
		if _, exists := merged[name]; !exists {
			order = append(order, name)
		}
		merged[name] = req
	}

	result := make([]apisv1.CreateComponentRequest, 0, len(order))
	seen := make(map[string]struct{}, len(order))
	for _, name := range order {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, merged[name])
	}
	return result, nil
}

func importComponentResourceKeys(components []apisv1.CreateComponentRequest, appName, namespace string) map[string]struct{} {
	if len(components) == 0 {
		return nil
	}
	keys := make(map[string]struct{})
	resourceAppName := naming.ApplicationResourceKey(appName, "", false)
	for _, component := range components {
		for _, key := range importComponentResolvedResourceKeys(component, resourceAppName, namespace) {
			keys[key] = struct{}{}
		}
	}
	if len(keys) == 0 {
		return nil
	}
	return keys
}

func importComponentConflictsWithResourceKeys(component apisv1.CreateComponentRequest, appName, namespace string, keys map[string]struct{}) bool {
	if len(keys) == 0 {
		return false
	}
	resourceAppName := naming.ApplicationResourceKey(appName, "", false)
	for _, key := range importComponentResolvedResourceKeys(component, resourceAppName, namespace) {
		if _, exists := keys[key]; exists {
			return true
		}
	}
	return false
}

func importComponentResolvedResourceKeys(component apisv1.CreateComponentRequest, resourceAppName, namespace string) []string {
	componentName := strings.TrimSpace(component.Name)
	if componentName == "" {
		return nil
	}
	namespace = applicationservice.ServiceNamespaceOrDefault(namespace)
	componentResourceAppName := resourceAppName
	if applicationservice.CreateComponentShareEnabled(component) {
		componentResourceAppName = componentName
	}

	keys := make([]string, 0, 4+len(component.Traits.Service)+len(component.Traits.Ingress))
	add := func(kind domainspec.ResourceKind, name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		keys = append(keys, importResourceCollisionKey(kind, namespace, name))
	}

	switch component.ComponentType {
	case config.ConfJob:
		add(domainspec.ResourceConfigMap, componentName)
	case config.SecretJob:
		add(domainspec.ResourceSecret, componentName)
	case config.ServerJob:
		add(domainspec.ResourceDeployment, naming.WebServiceName(componentName, componentResourceAppName))
	case config.StoreJob:
		add(domainspec.ResourceStatefulSet, naming.StoreServerName(componentName, componentResourceAppName))
	case config.InstantJob:
		add(domainspec.ResourceJob, naming.JobName(componentName, componentResourceAppName))
	case config.ScheduledJob:
		if strings.TrimSpace(component.Properties.Schedule) != "" {
			add(domainspec.ResourceCronJob, naming.CronJobName(componentName, componentResourceAppName))
		}
	}

	for _, name := range applicationservice.ResolvedServiceResourceNames(component, componentResourceAppName) {
		add(domainspec.ResourceService, name)
	}
	for _, name := range applicationservice.ResolvedIngressResourceNames(component, componentResourceAppName) {
		add(domainspec.ResourceIngress, name)
	}
	return keys
}

func importResourceCollisionKey(kind domainspec.ResourceKind, namespace, name string) string {
	return string(kind) + "\x00" + applicationservice.ServiceNamespaceOrDefault(namespace) + "\x00" + strings.TrimSpace(name)
}

func shouldRetainExistingComponentForPartialImport(component apisv1.CreateComponentRequest, includeKinds map[string]struct{}) bool {
	if len(includeKinds) == 0 {
		return true
	}
	kinds := inferredPrimaryImportKindsForComponent(component)
	if len(kinds) == 0 {
		return true
	}
	if !allComponentKindsIncluded(kinds, includeKinds) {
		return true
	}
	return false
}

func allComponentKindsIncluded(componentKinds, includeKinds map[string]struct{}) bool {
	for kind := range componentKinds {
		if _, included := includeKinds[kind]; !included {
			return false
		}
	}
	return true
}

func inferredPrimaryImportKindsForComponent(component apisv1.CreateComponentRequest) map[string]struct{} {
	kinds := make(map[string]struct{})
	add := func(kind string) {
		if kind == "" {
			return
		}
		kinds[kind] = struct{}{}
	}

	switch component.ComponentType {
	case config.ConfJob:
		add(importKindConfigMaps)
	case config.SecretJob:
		add(importKindSecrets)
	case config.StoreJob:
		add(importKindStatefulSets)
		add(importKindPersistentVolumeClaims)
	case config.InstantJob:
		add(importKindJobs)
	case config.ScheduledJob:
		add(importKindCronJobs)
	case config.ServerJob:
		add(importKindDeployments)
		add(importKindDaemonSets)
	case config.Service:
		add(importKindServices)
	}
	if len(applicationservice.ResolvedPVCResourceNames(component)) > 0 {
		add(importKindPersistentVolumeClaims)
	}

	if len(kinds) == 0 {
		return nil
	}
	return kinds
}

func convertComponentModelToCreateRequest(comp *model.ApplicationComponent) (apisv1.CreateComponentRequest, error) {
	dto, err := assembler.ConvertComponentModelToDTO(comp)
	if err != nil {
		return apisv1.CreateComponentRequest{}, err
	}
	if dto == nil {
		return apisv1.CreateComponentRequest{}, fmt.Errorf("component dto is nil")
	}
	return apisv1.CreateComponentRequest{
		Name:          dto.Name,
		ComponentType: dto.ComponentType,
		Image:         dto.Image,
		Namespace:     dto.Namespace,
		Replicas:      dto.Replicas,
		Properties:    dto.Properties,
		Traits:        dto.Traits,
	}, nil
}

func (s *namespaceImportServiceImpl) tryValidateImportCreateRequest(ctx context.Context, req apisv1.CreateApplicationsRequest) error {
	if s.ValidationService == nil {
		return fmt.Errorf("validation service is nil")
	}
	validationService := s.ValidationService
	if binder, ok := validationService.(interface {
		WithRepositories(repository.ApplicationRepository, repository.ComponentRepository) validationservice.ValidationService
	}); ok {
		validationService = binder.WithRepositories(s.AppRepo, s.ComponentRepo)
	}
	resp := validationService.TryApplication(ctx, req)
	if resp == nil {
		return fmt.Errorf("try application validation returned nil response")
	}
	if resp.Valid {
		return nil
	}
	return fmt.Errorf("try application validation failed: %s", summarizeValidationErrors(resp.Errors))
}

func summarizeValidationErrors(errors []apisv1.ValidationError) string {
	if len(errors) == 0 {
		return "invalid request"
	}
	const maxErrors = 3
	parts := make([]string, 0, maxErrors)
	for _, item := range errors {
		if len(parts) >= maxErrors {
			break
		}
		field := strings.TrimSpace(item.Field)
		message := strings.TrimSpace(item.Message)
		switch {
		case field != "" && message != "":
			parts = append(parts, fmt.Sprintf("%s: %s", field, message))
		case message != "":
			parts = append(parts, message)
		case field != "":
			parts = append(parts, field)
		}
	}
	if len(parts) == 0 {
		return "invalid request"
	}
	return strings.Join(parts, "; ")
}

func resolveImportedAppID(created *apisv1.ApplicationBase, fallback string) string {
	if created != nil {
		if id := strings.TrimSpace(created.ID); id != "" {
			return id
		}
	}
	return strings.TrimSpace(fallback)
}

func sanitizeImportComponentsForCreate(components []apisv1.CreateComponentRequest) []apisv1.CreateComponentRequest {
	return sanitizeImportComponents(components, true)
}

func sanitizeAdoptedImportComponentsForCreate(components []apisv1.CreateComponentRequest) []apisv1.CreateComponentRequest {
	return sanitizeImportComponents(components, false)
}

func sanitizeImportComponents(components []apisv1.CreateComponentRequest, sanitizeSelectors bool) []apisv1.CreateComponentRequest {
	if len(components) == 0 {
		return nil
	}
	sanitized := make([]apisv1.CreateComponentRequest, len(components))
	for i := range components {
		sanitized[i] = components[i]
		sanitized[i].Properties.Labels = sanitizeImportComponentLabels(components[i].Properties.Labels)
		sanitized[i].Traits = sanitizeImportTraits(components[i].Traits, sanitizeSelectors)
	}
	return sanitized
}

func sanitizeRetainedImportComponentForCreate(component apisv1.CreateComponentRequest) apisv1.CreateComponentRequest {
	component.Properties.Labels = sanitizeImportComponentLabels(component.Properties.Labels)
	component.Traits = sanitizeImportTraits(component.Traits, false)
	return component
}

var importManagedLabelKeys = map[string]struct{}{
	config.LabelManagedBy:     {},
	config.LabelAppID:         {},
	config.LabelComponentID:   {},
	config.LabelComponentName: {},
	config.LabelImportAppKey:  {},
}

func sanitizeImportTraits(traits apisv1.Traits, sanitizeSelectors bool) apisv1.Traits {
	sanitized := traits
	if len(traits.Init) > 0 {
		sanitized.Init = make([]domainspec.InitTraitSpec, len(traits.Init))
		copy(sanitized.Init, traits.Init)
		for i := range sanitized.Init {
			sanitized.Init[i].Properties.Labels = sanitizeImportComponentLabels(sanitized.Init[i].Properties.Labels)
			sanitized.Init[i].Traits = sanitizeImportTraits(sanitized.Init[i].Traits, sanitizeSelectors)
		}
	}
	if len(traits.Sidecar) > 0 {
		sanitized.Sidecar = make([]domainspec.SidecarTraitsSpec, len(traits.Sidecar))
		copy(sanitized.Sidecar, traits.Sidecar)
		for i := range sanitized.Sidecar {
			sanitized.Sidecar[i].Traits = sanitizeImportTraits(sanitized.Sidecar[i].Traits, sanitizeSelectors)
		}
	}
	if len(traits.Ingress) > 0 {
		sanitized.Ingress = make([]domainspec.IngressTraitsSpec, len(traits.Ingress))
		copy(sanitized.Ingress, traits.Ingress)
		for i := range sanitized.Ingress {
			sanitized.Ingress[i].Label = sanitizeImportManagedLabels(sanitized.Ingress[i].Label)
		}
	}
	if len(traits.Service) > 0 {
		sanitized.Service = make([]domainspec.ServiceTraitSpec, len(traits.Service))
		copy(sanitized.Service, traits.Service)
		for i := range sanitized.Service {
			if sanitizeSelectors {
				sanitized.Service[i].Selector = sanitizeImportManagedLabels(sanitized.Service[i].Selector)
			}
			sanitized.Service[i].Labels = sanitizeImportManagedLabels(sanitized.Service[i].Labels)
		}
	}
	if len(traits.RBAC) > 0 {
		sanitized.RBAC = make([]domainspec.RBACPolicySpec, len(traits.RBAC))
		copy(sanitized.RBAC, traits.RBAC)
		for i := range sanitized.RBAC {
			sanitized.RBAC[i].ServiceAccountLabels = sanitizeImportManagedLabels(sanitized.RBAC[i].ServiceAccountLabels)
			sanitized.RBAC[i].RoleLabels = sanitizeImportManagedLabels(sanitized.RBAC[i].RoleLabels)
			sanitized.RBAC[i].BindingLabels = sanitizeImportManagedLabels(sanitized.RBAC[i].BindingLabels)
		}
	}
	return sanitized
}

func sanitizeImportComponentLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	reservedComponentLabelKeys := applicationservice.ReservedComponentLabelKeys()
	filtered := make(map[string]string, len(labels))
	for key, val := range labels {
		if _, reserved := reservedComponentLabelKeys[key]; reserved {
			continue
		}
		filtered[key] = val
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func sanitizeImportManagedLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	filtered := make(map[string]string, len(labels))
	for key, val := range labels {
		if _, managed := importManagedLabelKeys[key]; managed {
			continue
		}
		filtered[key] = val
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func (s *namespaceImportServiceImpl) loadExistingAppIDMap(ctx context.Context, namespace string) (map[string]string, error) {
	indexByName, _, _, err := s.loadExistingAppIndex(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return indexByName, nil
}

func (s *namespaceImportServiceImpl) loadExistingAppIndex(ctx context.Context, namespace string) (map[string]string, map[string]struct{}, map[string]string, error) {
	if s.AppRepo == nil {
		return nil, nil, nil, fmt.Errorf("application repository is nil")
	}

	apps, err := s.AppRepo.List(ctx, datastore.ListOptions{
		Page:     0,
		PageSize: 0,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list applications from repository: %w", err)
	}
	targetNamespace := applicationservice.PickNamespace(strings.TrimSpace(namespace), config.DefaultNamespace)
	result := make(map[string]string, len(apps))
	idSet := make(map[string]struct{}, len(apps))
	nameByID := make(map[string]string, len(apps))
	for _, app := range apps {
		if app == nil {
			continue
		}
		id := strings.TrimSpace(app.ID)
		name := strings.TrimSpace(app.Name)
		if id == "" || name == "" {
			continue
		}
		appNamespace := applicationservice.PickNamespace(strings.TrimSpace(app.Namespace), config.DefaultNamespace)
		if appNamespace != targetNamespace {
			continue
		}
		idSet[id] = struct{}{}
		if _, exists := nameByID[id]; !exists {
			nameByID[id] = name
		}
		key := appNameNamespaceKey(name, appNamespace)
		if existingID, exists := result[key]; exists {
			if existingID != id {
				return nil, nil, nil, fmt.Errorf("duplicate application name %q found in namespace %q for app IDs %q and %q", name, appNamespace, existingID, id)
			}
			continue
		}
		result[key] = id
	}
	return result, idSet, nameByID, nil
}

func normalizeImportKinds(includeKinds []string) (map[string]struct{}, []string, error) {
	if len(includeKinds) == 0 {
		set := make(map[string]struct{}, len(defaultImportKinds))
		for _, kind := range defaultImportKinds {
			set[kind] = struct{}{}
		}
		return set, nil, nil
	}

	set := make(map[string]struct{}, len(includeKinds))
	warnings := make([]string, 0)
	for _, raw := range includeKinds {
		normalized := strings.ToLower(strings.TrimSpace(raw))
		if normalized == "" {
			continue
		}
		kind, ok := importKindAliases[normalized]
		if !ok {
			return nil, nil, bcode.ErrApplicationConfig
		}
		if _, exists := set[kind]; exists {
			warnings = append(warnings, fmt.Sprintf("duplicate includeKind %q ignored", raw))
			continue
		}
		set[kind] = struct{}{}
	}
	if len(set) == 0 {
		return nil, nil, bcode.ErrApplicationConfig
	}
	return set, warnings, nil
}

func (s *namespaceImportServiceImpl) scanNamespaceResources(ctx context.Context, namespace string, includeKinds map[string]struct{}) ([]*importResource, []string, error) {
	return s.scanNamespaceResourcesTableDriven(ctx, namespace, includeKinds)
}

func (s *namespaceImportServiceImpl) collectAssociatedClusterRBAC(ctx context.Context, namespace string) (map[string]struct{}, map[string]struct{}, []string, error) {
	associatedRoles := make(map[string]struct{})
	associatedBindings := make(map[string]struct{})
	warnings := make([]string, 0)

	roleBindings, err := s.KubeClient.RbacV1().RoleBindings(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list rolebindings for cluster-rbac association: %w", err)
	}
	for i := range roleBindings.Items {
		binding := &roleBindings.Items[i]
		if strings.EqualFold(strings.TrimSpace(binding.RoleRef.Kind), "ClusterRole") && strings.TrimSpace(binding.RoleRef.Name) != "" {
			associatedRoles[binding.RoleRef.Name] = struct{}{}
		}
	}

	clusterBindings, err := s.KubeClient.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list clusterrolebindings for association: %w", err)
	}
	for i := range clusterBindings.Items {
		binding := &clusterBindings.Items[i]
		referencesNamespace, crossNamespaceReferences := clusterRoleBindingServiceAccountReferenceState(binding, namespace)
		if !referencesNamespace {
			continue
		}
		if crossNamespaceReferences {
			warnings = append(warnings, fmt.Sprintf(
				"skip clusterrolebinding %q during namespace %q import: references serviceaccounts across namespaces",
				strings.TrimSpace(binding.Name), namespace,
			))
			continue
		}
		associatedBindings[binding.Name] = struct{}{}
		if strings.EqualFold(strings.TrimSpace(binding.RoleRef.Kind), "ClusterRole") && strings.TrimSpace(binding.RoleRef.Name) != "" {
			associatedRoles[binding.RoleRef.Name] = struct{}{}
		}
	}

	return associatedRoles, associatedBindings, warnings, nil
}

func clusterRoleBindingServiceAccountReferenceState(binding *rbacv1.ClusterRoleBinding, namespace string) (bool, bool) {
	if binding == nil {
		return false, false
	}
	subjectNamespaces := make(map[string]struct{})
	referencesNamespace := false
	for _, subject := range binding.Subjects {
		if subject.Kind != rbacv1.ServiceAccountKind {
			continue
		}
		subjectNamespace := strings.TrimSpace(subject.Namespace)
		if subjectNamespace == namespace {
			referencesNamespace = true
		}
		subjectNamespaces[subjectNamespace] = struct{}{}
	}
	return referencesNamespace, len(subjectNamespaces) > 1
}

func isCronJobOwnedJob(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
	for _, owner := range job.OwnerReferences {
		if strings.EqualFold(strings.TrimSpace(owner.Kind), "CronJob") {
			return true
		}
	}
	return false
}

func toUnstructured(obj runtime.Object, kind, apiVersion string) (*unstructured.Unstructured, error) {
	if obj == nil {
		return nil, fmt.Errorf("nil runtime object")
	}
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, fmt.Errorf("convert %s to unstructured: %w", kind, err)
	}
	u := &unstructured.Unstructured{Object: raw}
	u.SetKind(kind)
	u.SetAPIVersion(apiVersion)
	return u, nil
}

func sharedAppIDForNamespace(namespace string) string {
	return boundedRFC1123LabelValue("shared-" + namespace)
}

func boundedRFC1123AppName(value string) string {
	const (
		hashLen  = 10
		fallback = "app"
	)
	maxNameLen := datastore.PrimaryKeyMaxLength
	normalized := utils.ToRFC1123Name(value)
	if normalized == "" {
		return fallback
	}
	if len(normalized) <= maxNameLen {
		return normalized
	}
	hashPart := stableHashSuffix(normalized, hashLen)
	prefixLen := maxNameLen - len(hashPart) - 1
	if prefixLen < 1 {
		if len(hashPart) > maxNameLen {
			return hashPart[:maxNameLen]
		}
		return hashPart
	}
	prefix := strings.Trim(normalized[:prefixLen], "-")
	if prefix == "" {
		prefix = fallback
		if len(prefix) > prefixLen {
			prefix = prefix[:prefixLen]
		}
	}
	return prefix + "-" + hashPart
}

func boundedRFC1123LabelValue(value string) string {
	const maxLabelLen = 63
	const hashLen = 10
	normalized := utils.ToRFC1123Name(value)
	if len(normalized) <= maxLabelLen {
		return normalized
	}
	hashPart := stableHashSuffix(normalized, hashLen)
	prefixLen := maxLabelLen - len(hashPart) - 1
	if prefixLen < 1 {
		return hashPart
	}
	prefix := strings.Trim(normalized[:prefixLen], "-")
	if prefix == "" {
		prefix = "shared"
	}
	return prefix + "-" + hashPart
}

func stableHashSuffix(value string, size int) string {
	if size <= 0 {
		return ""
	}
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(value))
	hash := strconv.FormatUint(hasher.Sum64(), 36)
	if len(hash) >= size {
		return hash[:size]
	}
	return strings.Repeat("0", size-len(hash)) + hash
}

func isClusterScopedImportKind(kindKey string) bool {
	switch kindKey {
	case importKindClusterRoles, importKindClusterRoleBindings, importKindPersistentVolumes:
		return true
	default:
		return false
	}
}

func supportsNameBasedAppInference(kindKey string) bool {
	switch kindKey {
	case importKindDeployments,
		importKindStatefulSets,
		importKindDaemonSets,
		importKindJobs,
		importKindCronJobs,
		importKindConfigMaps,
		importKindSecrets,
		importKindPersistentVolumeClaims,
		importKindServices,
		importKindIngresses:
		return true
	default:
		return false
	}
}

func parseStrictResourceName(name string) (prefix, appID, component string, ok bool) {
	parts := strings.Split(strings.TrimSpace(name), "-")
	if len(parts) < 3 {
		return "", "", "", false
	}
	for idx := 1; idx < len(parts)-1; idx++ {
		candidate := strings.TrimSpace(parts[idx])
		if !looksLikeGeneratedAppID(candidate) {
			continue
		}
		prefix = strings.TrimSpace(strings.Join(parts[:idx], "-"))
		component = strings.TrimSpace(strings.Join(parts[idx+1:], "-"))
		if prefix == "" || component == "" {
			continue
		}
		return prefix, candidate, component, true
	}
	return "", "", "", false
}

func looksLikeGeneratedAppID(appID string) bool {
	id := strings.TrimSpace(strings.ToLower(appID))
	// Keep namespace import strict to avoid false positives like redis-master-svc.
	// Current ecosystems use generated IDs of length 16 or 24.
	if len(id) != 16 && len(id) != 24 {
		return false
	}
	hasDigit := false
	hasLetter := false
	for _, ch := range id {
		if ch >= '0' && ch <= '9' {
			hasDigit = true
			continue
		}
		if ch >= 'a' && ch <= 'z' {
			hasLetter = true
			continue
		}
		return false
	}
	return hasDigit && hasLetter
}

func pickTopPrefix(votes map[string]int) string {
	if len(votes) == 0 {
		return ""
	}
	type kv struct {
		key string
		cnt int
	}
	items := make([]kv, 0, len(votes))
	for k, c := range votes {
		items = append(items, kv{key: k, cnt: c})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].cnt != items[j].cnt {
			return items[i].cnt > items[j].cnt
		}
		return items[i].key < items[j].key
	})
	return items[0].key
}

func buildWorkloadRef(res *importResource) (workloadRef, error) {
	componentName := res.name
	if trimmed := strings.TrimSpace(res.componentName); trimmed != "" {
		componentName = trimmed
	}
	ref := workloadRef{
		appID:         res.appID,
		componentName: componentName,
		labels:        map[string]string{},
		configMaps:    make(map[string]struct{}),
		pvcs:          make(map[string]struct{}),
		pvcPrefixes:   make(map[string]struct{}),
		secrets:       make(map[string]struct{}),
	}
	switch res.kindKey {
	case importKindDeployments:
		var deploy appsv1.Deployment
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(res.object.Object, &deploy); err != nil {
			return ref, err
		}
		for k, v := range deploy.Spec.Template.Labels {
			ref.labels[k] = v
		}
		ref.serviceAccount = strings.TrimSpace(deploy.Spec.Template.Spec.ServiceAccountName)
		collectPodSpecReferences(&deploy.Spec.Template.Spec, ref.configMaps, ref.pvcs, ref.secrets)
	case importKindStatefulSets:
		var sts appsv1.StatefulSet
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(res.object.Object, &sts); err != nil {
			return ref, err
		}
		for k, v := range sts.Spec.Template.Labels {
			ref.labels[k] = v
		}
		ref.serviceAccount = strings.TrimSpace(sts.Spec.Template.Spec.ServiceAccountName)
		collectPodSpecReferences(&sts.Spec.Template.Spec, ref.configMaps, ref.pvcs, ref.secrets)
		for _, tpl := range sts.Spec.VolumeClaimTemplates {
			name := strings.TrimSpace(tpl.Name)
			if name != "" {
				if prefix := statefulSetPVCNamePrefix(name, sts.Name); prefix != "" {
					ref.pvcPrefixes[prefix] = struct{}{}
				}
			}
		}
	case importKindDaemonSets:
		var ds appsv1.DaemonSet
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(res.object.Object, &ds); err != nil {
			return ref, err
		}
		for k, v := range ds.Spec.Template.Labels {
			ref.labels[k] = v
		}
		ref.serviceAccount = strings.TrimSpace(ds.Spec.Template.Spec.ServiceAccountName)
		collectPodSpecReferences(&ds.Spec.Template.Spec, ref.configMaps, ref.pvcs, ref.secrets)
	case importKindJobs:
		var job batchv1.Job
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(res.object.Object, &job); err != nil {
			return ref, err
		}
		for k, v := range job.Spec.Template.Labels {
			ref.labels[k] = v
		}
		ref.serviceAccount = strings.TrimSpace(job.Spec.Template.Spec.ServiceAccountName)
		collectPodSpecReferences(&job.Spec.Template.Spec, ref.configMaps, ref.pvcs, ref.secrets)
	case importKindCronJobs:
		var cron batchv1.CronJob
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(res.object.Object, &cron); err != nil {
			return ref, err
		}
		for k, v := range cron.Spec.JobTemplate.Spec.Template.Labels {
			ref.labels[k] = v
		}
		ref.serviceAccount = strings.TrimSpace(cron.Spec.JobTemplate.Spec.Template.Spec.ServiceAccountName)
		collectPodSpecReferences(&cron.Spec.JobTemplate.Spec.Template.Spec, ref.configMaps, ref.pvcs, ref.secrets)
	default:
		return ref, fmt.Errorf("unsupported workload kind %s", res.kindKey)
	}
	if ref.serviceAccount == "" {
		ref.serviceAccount = "default"
	}
	return ref, nil
}

func resolveSelectorManagedAppID(resources []*importResource) (string, bool, error) {
	candidates := make(map[string]struct{})
	for _, res := range resources {
		if res == nil {
			continue
		}
		switch res.kindKey {
		case importKindDeployments, importKindStatefulSets, importKindDaemonSets:
		default:
			continue
		}
		selector := selectorMatchLabelsForImportResource(res)
		if len(selector) == 0 {
			continue
		}
		appID := strings.TrimSpace(selector[config.LabelAppID])
		if appID == "" {
			continue
		}
		candidates[appID] = struct{}{}
	}
	if len(candidates) == 0 {
		return "", false, nil
	}
	if len(candidates) > 1 {
		ids := make([]string, 0, len(candidates))
		for id := range candidates {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		return "", false, fmt.Errorf("multiple selector-managed appIDs detected in one import plan: %s", strings.Join(ids, ", "))
	}
	for id := range candidates {
		return id, true, nil
	}
	return "", false, nil
}

func statefulSetPVCNamePrefix(claimTemplateName, statefulSetName string) string {
	claimTemplateName = strings.TrimSpace(claimTemplateName)
	statefulSetName = strings.TrimSpace(statefulSetName)
	if claimTemplateName == "" || statefulSetName == "" {
		return ""
	}
	return claimTemplateName + "-" + statefulSetName + "-"
}

func collectPodSpecReferences(spec *corev1.PodSpec, configMaps, pvcs, secrets map[string]struct{}) {
	if spec == nil {
		return
	}
	for _, imagePullSecret := range spec.ImagePullSecrets {
		if name := strings.TrimSpace(imagePullSecret.Name); name != "" {
			secrets[name] = struct{}{}
		}
	}
	for _, vol := range spec.Volumes {
		if vol.ConfigMap != nil {
			name := strings.TrimSpace(vol.ConfigMap.Name)
			if name != "" {
				configMaps[name] = struct{}{}
			}
		}
		if vol.PersistentVolumeClaim != nil {
			name := strings.TrimSpace(vol.PersistentVolumeClaim.ClaimName)
			if name != "" {
				pvcs[name] = struct{}{}
			}
		}
		if vol.Secret != nil {
			name := strings.TrimSpace(vol.Secret.SecretName)
			if name != "" {
				secrets[name] = struct{}{}
			}
		}
		if vol.Projected != nil {
			for _, projection := range vol.Projected.Sources {
				if projection.ConfigMap != nil {
					if name := strings.TrimSpace(projection.ConfigMap.Name); name != "" {
						configMaps[name] = struct{}{}
					}
				}
				if projection.Secret != nil {
					if name := strings.TrimSpace(projection.Secret.Name); name != "" {
						secrets[name] = struct{}{}
					}
				}
			}
		}
		if vol.CSI != nil && vol.CSI.NodePublishSecretRef != nil {
			if name := strings.TrimSpace(vol.CSI.NodePublishSecretRef.Name); name != "" {
				secrets[name] = struct{}{}
			}
		}
		if vol.AzureFile != nil {
			if name := strings.TrimSpace(vol.AzureFile.SecretName); name != "" {
				secrets[name] = struct{}{}
			}
		}
		for _, secretRef := range []*corev1.LocalObjectReference{
			localSecretReference(vol.RBD),
			localSecretReference(vol.CephFS),
			localSecretReference(vol.FlexVolume),
			localSecretReference(vol.Cinder),
			localSecretReference(vol.ScaleIO),
			localSecretReference(vol.StorageOS),
		} {
			if secretRef != nil {
				if name := strings.TrimSpace(secretRef.Name); name != "" {
					secrets[name] = struct{}{}
				}
			}
		}
	}
	for _, c := range spec.Containers {
		collectContainerEnvReferences(c.EnvFrom, c.Env, configMaps, secrets)
	}
	for _, c := range spec.InitContainers {
		collectContainerEnvReferences(c.EnvFrom, c.Env, configMaps, secrets)
	}
	for _, c := range spec.EphemeralContainers {
		collectContainerEnvReferences(c.EnvFrom, c.Env, configMaps, secrets)
	}
}

func localSecretReference(volume interface{}) *corev1.LocalObjectReference {
	switch source := volume.(type) {
	case *corev1.RBDVolumeSource:
		if source != nil {
			return source.SecretRef
		}
	case *corev1.CephFSVolumeSource:
		if source != nil {
			return source.SecretRef
		}
	case *corev1.FlexVolumeSource:
		if source != nil {
			return source.SecretRef
		}
	case *corev1.CinderVolumeSource:
		if source != nil {
			return source.SecretRef
		}
	case *corev1.ScaleIOVolumeSource:
		if source != nil {
			return source.SecretRef
		}
	case *corev1.StorageOSVolumeSource:
		if source != nil {
			return source.SecretRef
		}
	}
	return nil
}

func collectContainerEnvReferences(
	envFromSources []corev1.EnvFromSource,
	envVars []corev1.EnvVar,
	configMaps, secrets map[string]struct{},
) {
	for _, envFrom := range envFromSources {
		if envFrom.ConfigMapRef != nil {
			name := strings.TrimSpace(envFrom.ConfigMapRef.Name)
			if name != "" {
				configMaps[name] = struct{}{}
			}
		}
		if envFrom.SecretRef != nil {
			name := strings.TrimSpace(envFrom.SecretRef.Name)
			if name != "" {
				secrets[name] = struct{}{}
			}
		}
	}
	for _, env := range envVars {
		if env.ValueFrom == nil {
			continue
		}
		if env.ValueFrom.ConfigMapKeyRef != nil {
			name := strings.TrimSpace(env.ValueFrom.ConfigMapKeyRef.Name)
			if name != "" {
				configMaps[name] = struct{}{}
			}
		}
		if env.ValueFrom.SecretKeyRef != nil {
			name := strings.TrimSpace(env.ValueFrom.SecretKeyRef.Name)
			if name != "" {
				secrets[name] = struct{}{}
			}
		}
	}
}

func collectReferenceOwners(workloadsByApp map[string][]workloadRef, refKind string) (map[string]map[string]struct{}, map[string]map[string]struct{}) {
	owners := make(map[string]map[string]struct{})
	componentOwners := make(map[string]map[string]struct{})
	for appID, refs := range workloadsByApp {
		for _, ref := range refs {
			names := workloadReferenceMap(ref, refKind)
			for name := range names {
				if _, ok := owners[name]; !ok {
					owners[name] = make(map[string]struct{})
				}
				owners[name][appID] = struct{}{}

				key := componentRefKey(appID, name)
				if _, ok := componentOwners[key]; !ok {
					componentOwners[key] = make(map[string]struct{})
				}
				componentOwners[key][ref.componentName] = struct{}{}
			}
		}
	}
	return owners, componentOwners
}

func workloadReferenceMap(ref workloadRef, refKind string) map[string]struct{} {
	switch refKind {
	case importKindConfigMaps:
		return ref.configMaps
	case importKindPersistentVolumeClaims:
		return ref.pvcs
	case importKindSecrets:
		return ref.secrets
	default:
		return nil
	}
}

func collectPVCPrefixOwners(workloadsByApp map[string][]workloadRef) (map[string]map[string]struct{}, map[string]map[string]struct{}) {
	owners := make(map[string]map[string]struct{})
	componentOwners := make(map[string]map[string]struct{})
	for appID, refs := range workloadsByApp {
		for _, ref := range refs {
			for prefix := range ref.pvcPrefixes {
				if strings.TrimSpace(prefix) == "" {
					continue
				}
				if _, ok := owners[prefix]; !ok {
					owners[prefix] = make(map[string]struct{})
				}
				owners[prefix][appID] = struct{}{}

				key := componentRefKey(appID, prefix)
				if _, ok := componentOwners[key]; !ok {
					componentOwners[key] = make(map[string]struct{})
				}
				componentOwners[key][ref.componentName] = struct{}{}
			}
		}
	}
	return owners, componentOwners
}

func matchPVCPrefixOwners(
	pvcName string,
	prefixOwners map[string]map[string]struct{},
	prefixComponents map[string]map[string]struct{},
) (map[string]struct{}, map[string]map[string]struct{}) {
	owners := make(map[string]struct{})
	componentCandidates := make(map[string]map[string]struct{})
	trimmedName := strings.TrimSpace(pvcName)
	if trimmedName == "" {
		return owners, componentCandidates
	}

	bestPrefixLen := -1
	for prefix, appSet := range prefixOwners {
		if prefix == "" || !strings.HasPrefix(trimmedName, prefix) {
			continue
		}
		currentLen := len(prefix)
		if currentLen > bestPrefixLen {
			owners = make(map[string]struct{})
			componentCandidates = make(map[string]map[string]struct{})
			bestPrefixLen = currentLen
		}
		if currentLen != bestPrefixLen {
			continue
		}
		for appID := range appSet {
			owners[appID] = struct{}{}
			key := componentRefKey(appID, prefix)
			for componentName := range prefixComponents[key] {
				if _, ok := componentCandidates[appID]; !ok {
					componentCandidates[appID] = make(map[string]struct{})
				}
				componentCandidates[appID][componentName] = struct{}{}
			}
		}
	}
	return owners, componentCandidates
}

func collectServiceAccountOwners(workloadsByApp map[string][]workloadRef) (map[string]map[string]struct{}, map[string]map[string]struct{}) {
	owners := make(map[string]map[string]struct{})
	componentOwners := make(map[string]map[string]struct{})
	for appID, refs := range workloadsByApp {
		for _, ref := range refs {
			serviceAccount := strings.TrimSpace(ref.serviceAccount)
			if serviceAccount == "" {
				continue
			}
			if _, ok := owners[serviceAccount]; !ok {
				owners[serviceAccount] = make(map[string]struct{})
			}
			owners[serviceAccount][appID] = struct{}{}

			key := componentRefKey(appID, serviceAccount)
			if _, ok := componentOwners[key]; !ok {
				componentOwners[key] = make(map[string]struct{})
			}
			componentOwners[key][ref.componentName] = struct{}{}
		}
	}
	return owners, componentOwners
}

func componentRefKey(appID, resourceName string) string {
	return appID + "/" + resourceName
}

func pickSingleComponent(values map[string]struct{}, fallback string) string {
	if len(values) == 1 {
		for v := range values {
			if strings.TrimSpace(v) != "" {
				return v
			}
		}
	}
	return fallback
}

func matchServiceOwners(res *importResource, workloadsByApp map[string][]workloadRef) (map[string]struct{}, map[string]struct{}) {
	owners := make(map[string]struct{})
	components := make(map[string]struct{})
	if res == nil || res.object == nil {
		return owners, components
	}
	var svc corev1.Service
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(res.object.Object, &svc); err != nil {
		return owners, components
	}
	if len(svc.Spec.Selector) == 0 {
		return owners, components
	}
	for appID, refs := range workloadsByApp {
		for _, ref := range refs {
			if selectorMatch(svc.Spec.Selector, ref.labels) {
				owners[appID] = struct{}{}
				components[ref.componentName] = struct{}{}
			}
		}
	}
	return owners, components
}

func selectorMatch(selector, labels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for key, val := range selector {
		if labels[key] != val {
			return false
		}
	}
	return true
}

func matchIngressOwners(res *importResource, serviceByName map[string]*importResource) (map[string]struct{}, map[string]struct{}) {
	owners := make(map[string]struct{})
	components := make(map[string]struct{})
	if res == nil || res.object == nil {
		return owners, components
	}
	var ing networkingv1.Ingress
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(res.object.Object, &ing); err != nil {
		return owners, components
	}
	serviceNames := ingressBackendServiceNames(&ing)
	for _, svcName := range serviceNames {
		svcRes, ok := serviceByName[svcName]
		if !ok || svcRes == nil {
			continue
		}
		if strings.TrimSpace(svcRes.appID) == "" {
			continue
		}
		owners[svcRes.appID] = struct{}{}
		if strings.TrimSpace(svcRes.componentName) != "" {
			components[svcRes.componentName] = struct{}{}
		}
	}
	return owners, components
}

func matchRoleBindingOwners(
	namespace string,
	res *importResource,
	serviceAccountOwners map[string]map[string]struct{},
	serviceAccountComponents map[string]map[string]struct{},
) (map[string]struct{}, map[string]map[string]struct{}, string, string) {
	owners := make(map[string]struct{})
	componentCandidates := make(map[string]map[string]struct{})
	if res == nil || res.object == nil {
		return owners, componentCandidates, "", ""
	}

	var binding rbacv1.RoleBinding
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(res.object.Object, &binding); err != nil {
		return owners, componentCandidates, "", ""
	}
	for _, subject := range binding.Subjects {
		if subject.Kind != rbacv1.ServiceAccountKind {
			continue
		}
		subjectNamespace := strings.TrimSpace(subject.Namespace)
		if subjectNamespace != "" && subjectNamespace != namespace {
			continue
		}
		name := strings.TrimSpace(subject.Name)
		if name == "" {
			continue
		}
		ownerSet := serviceAccountOwners[name]
		for owner := range ownerSet {
			owners[owner] = struct{}{}
			key := componentRefKey(owner, res.name)
			if _, ok := componentCandidates[key]; !ok {
				componentCandidates[key] = make(map[string]struct{})
			}
			for comp := range serviceAccountComponents[componentRefKey(owner, name)] {
				componentCandidates[key][comp] = struct{}{}
			}
		}
	}
	roleName := ""
	clusterRoleName := ""
	if strings.EqualFold(strings.TrimSpace(binding.RoleRef.Kind), "Role") {
		roleName = strings.TrimSpace(binding.RoleRef.Name)
	} else if strings.EqualFold(strings.TrimSpace(binding.RoleRef.Kind), "ClusterRole") {
		clusterRoleName = strings.TrimSpace(binding.RoleRef.Name)
	}
	return owners, componentCandidates, roleName, clusterRoleName
}

func matchClusterRoleBindingOwners(
	namespace string,
	res *importResource,
	serviceAccountOwners map[string]map[string]struct{},
	serviceAccountComponents map[string]map[string]struct{},
) (map[string]struct{}, map[string]map[string]struct{}, string) {
	owners := make(map[string]struct{})
	componentCandidates := make(map[string]map[string]struct{})
	if res == nil || res.object == nil {
		return owners, componentCandidates, ""
	}
	var binding rbacv1.ClusterRoleBinding
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(res.object.Object, &binding); err != nil {
		return owners, componentCandidates, ""
	}
	for _, subject := range binding.Subjects {
		if subject.Kind != rbacv1.ServiceAccountKind {
			continue
		}
		if strings.TrimSpace(subject.Namespace) != namespace {
			continue
		}
		name := strings.TrimSpace(subject.Name)
		if name == "" {
			continue
		}
		ownerSet := serviceAccountOwners[name]
		for owner := range ownerSet {
			owners[owner] = struct{}{}
			key := componentRefKey(owner, res.name)
			if _, ok := componentCandidates[key]; !ok {
				componentCandidates[key] = make(map[string]struct{})
			}
			for comp := range serviceAccountComponents[componentRefKey(owner, name)] {
				componentCandidates[key][comp] = struct{}{}
			}
		}
	}
	clusterRoleName := ""
	if strings.EqualFold(strings.TrimSpace(binding.RoleRef.Kind), "ClusterRole") {
		clusterRoleName = strings.TrimSpace(binding.RoleRef.Name)
	}
	return owners, componentCandidates, clusterRoleName
}

func ingressBackendServiceNames(ing *networkingv1.Ingress) []string {
	if ing == nil {
		return nil
	}
	seen := make(map[string]struct{})
	appendName := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		seen[name] = struct{}{}
	}
	if ing.Spec.DefaultBackend != nil && ing.Spec.DefaultBackend.Service != nil {
		appendName(ing.Spec.DefaultBackend.Service.Name)
	}
	for _, rule := range ing.Spec.Rules {
		if rule.HTTP == nil {
			continue
		}
		for _, path := range rule.HTTP.Paths {
			if path.Backend.Service == nil {
				continue
			}
			appendName(path.Backend.Service.Name)
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *namespaceImportServiceImpl) buildImportPlans(grouped map[string][]*importResource, appNames, appAliases map[string]string, sharedAppID string) []importAppPlan {
	appIDs := sortedImportAppIDs(grouped)
	sharedResourceTemplates, sharedSourceComponents, sharedTemplateWarnings, sharedSourceWarnings := buildSharedImportInputs(grouped[sharedAppID])

	plans := make([]importAppPlan, 0, len(appIDs))
	for _, appID := range appIDs {
		resources := sortedImportPlanResources(grouped[appID])
		plan := newImportAppPlan(appID, appNames, appAliases, resources)
		objects := importPlanObjects(appID, sharedAppID, resources)

		resourceSourceComponents, sourceComponents, warnings, err := convertImportPlanComponents(appID, sharedAppID, objects, sharedResourceTemplates, sharedTemplateWarnings, sharedSourceWarnings)
		appendImportPlanWarnings(&plan, appID, warnings)
		if err != nil {
			plan.err = err
			plans = append(plans, plan)
			continue
		}

		finalizeImportPlanComponents(&plan, appID, sharedAppID, resources, sourceComponents, resourceSourceComponents, sharedSourceComponents)
		plans = append(plans, plan)
	}
	return plans
}

func sortedImportAppIDs(grouped map[string][]*importResource) []string {
	appIDs := make([]string, 0, len(grouped))
	for appID := range grouped {
		appIDs = append(appIDs, appID)
	}
	sort.Strings(appIDs)
	return appIDs
}

func buildSharedImportInputs(resources []*importResource) ([]*unstructured.Unstructured, []apisv1.CreateComponentRequest, []string, []string) {
	sharedResourceTemplates, _, sharedTemplateWarnings := buildSharedResourceTemplates(resources)
	filteredTemplates, skippedWarnings := filterUnsafeStatefulSetVolumeClaimTemplateImports(sharedResourceTemplates)
	sharedTemplateWarnings = append(sharedTemplateWarnings, skippedWarnings...)
	sharedResourceTemplates = filteredTemplates
	sharedSourceComponents, sharedSourceWarnings := convertSharedTemplateSourceComponents(sharedResourceTemplates)
	return sharedResourceTemplates, sharedSourceComponents, sharedTemplateWarnings, sharedSourceWarnings
}

func sortedImportPlanResources(resources []*importResource) []*importResource {
	sort.SliceStable(resources, func(i, j int) bool {
		if resources[i].kind != resources[j].kind {
			return resources[i].kind < resources[j].kind
		}
		return resources[i].name < resources[j].name
	})
	return resources
}

func newImportAppPlan(appID string, appNames, appAliases map[string]string, resources []*importResource) importAppPlan {
	return importAppPlan{
		appID:     appID,
		name:      appNames[appID],
		alias:     appAliases[appID],
		resources: resources,
	}
}

func importPlanObjects(appID, sharedAppID string, resources []*importResource) []*unstructured.Unstructured {
	objects := make([]*unstructured.Unstructured, 0, len(resources))
	for _, res := range resources {
		if res != nil && res.object != nil {
			objects = append(objects, res.object.DeepCopy())
		}
	}
	if appID == sharedAppID {
		ensureShareLabelsForResources(objects, resources)
	}
	return objects
}

func convertImportPlanComponents(
	appID string,
	sharedAppID string,
	objects []*unstructured.Unstructured,
	sharedResourceTemplates []*unstructured.Unstructured,
	sharedTemplateWarnings []string,
	sharedSourceWarnings []string,
) ([]apisv1.CreateComponentRequest, []apisv1.CreateComponentRequest, []string, error) {
	filteredObjects, skippedWarnings := filterUnsafeStatefulSetVolumeClaimTemplateImports(objects)
	resourceSourceComponents, resourceWarnings, err := convertKubeObjectsToComponents(filteredObjects)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("convert resources for app %s: %w", appID, err)
	}

	sourceComponents := resourceSourceComponents
	warnings := append([]string(nil), skippedWarnings...)
	if appID != sharedAppID && len(sharedResourceTemplates) > 0 {
		fullObjects := importObjectsWithSharedTemplates(filteredObjects, sharedResourceTemplates)
		warnings = append(warnings, sharedTemplateWarnings...)
		warnings = append(warnings, sharedSourceWarnings...)
		components, convertWarnings, err := convertKubeObjectsToComponents(fullObjects)
		warnings = append(warnings, convertWarnings...)
		if err != nil {
			return resourceSourceComponents, nil, warnings, fmt.Errorf("convert resources for app %s: %w", appID, err)
		}
		sourceComponents = components
	} else {
		warnings = append(warnings, resourceWarnings...)
	}
	return resourceSourceComponents, sourceComponents, warnings, nil
}

func filterUnsafeStatefulSetVolumeClaimTemplateImports(objects []*unstructured.Unstructured) ([]*unstructured.Unstructured, []string) {
	if len(objects) == 0 {
		return nil, nil
	}
	filtered := make([]*unstructured.Unstructured, 0, len(objects))
	warnings := make([]string, 0)
	for _, obj := range objects {
		if obj == nil {
			continue
		}
		if obj.GetKind() == string(domainspec.KubeKindStatefulSet) && statefulSetImportHasVolumeClaimTemplates(obj) {
			warnings = append(warnings, fmt.Sprintf("statefulset %s/%s has volumeClaimTemplates and was skipped; importing it safely requires explicit PVC data migration", obj.GetNamespace(), obj.GetName()))
			continue
		}
		filtered = append(filtered, obj)
	}
	return filtered, warnings
}

func statefulSetImportHasVolumeClaimTemplates(obj *unstructured.Unstructured) bool {
	templates, found, err := unstructured.NestedSlice(obj.Object, "spec", "volumeClaimTemplates")
	return err == nil && found && len(templates) > 0
}

func importObjectsWithSharedTemplates(objects, sharedResourceTemplates []*unstructured.Unstructured) []*unstructured.Unstructured {
	fullObjects := make([]*unstructured.Unstructured, 0, len(objects)+len(sharedResourceTemplates))
	fullObjects = append(fullObjects, objects...)
	for _, obj := range sharedResourceTemplates {
		if obj == nil {
			continue
		}
		fullObjects = append(fullObjects, obj.DeepCopy())
	}
	return fullObjects
}

func appendImportPlanWarnings(plan *importAppPlan, appID string, warnings []string) {
	for _, warning := range warnings {
		plan.warnings = append(plan.warnings, fmt.Sprintf("app=%s: %s", appID, warning))
	}
}

func finalizeImportPlanComponents(plan *importAppPlan, appID, sharedAppID string, resources []*importResource, sourceComponents, resourceSourceComponents, sharedSourceComponents []apisv1.CreateComponentRequest) {
	deduped := dedupeImportComponents(sourceComponents)
	plan.resourceComponentByKey = buildResourceComponentNameMapping(resources, sourceComponents, deduped, resourceSourceComponents)
	plan.workloadComponentByOriginalName = buildWorkloadComponentNameMapping(resources, plan.resourceComponentByKey)
	if appID != sharedAppID && len(sharedSourceComponents) > 0 {
		ensureSharedComponentsOnApp(sourceComponents, deduped, sharedSourceComponents, plan.resourceComponentByKey)
	}
	plan.components = deduped
	plan.componentNames = componentNames(deduped)
	if len(plan.components) == 0 && appID != sharedAppID {
		plan.err = fmt.Errorf("no convertible components discovered")
		plan.applyErrorStatus = importResourceStatusSkipped
	}
}

func buildSharedResourceTemplates(resources []*importResource) ([]*unstructured.Unstructured, map[string]struct{}, []string) {
	if len(resources) == 0 {
		return nil, nil, nil
	}
	templates := make([]*unstructured.Unstructured, 0, len(resources))
	componentNames := make(map[string]struct{})
	warnings := make([]string, 0)
	for _, res := range resources {
		if res == nil || res.object == nil {
			continue
		}
		obj := res.object.DeepCopy()
		if obj == nil {
			continue
		}
		if err := ensureShareLabelsOnResourceObject(res.kindKey, obj, res.name); err != nil {
			warnings = append(warnings, fmt.Sprintf("inject shared resource %s/%s failed: %v", res.kind, res.name, err))
			continue
		}
		templates = append(templates, obj)
		if isShareComponentCarrierKind(res.kindKey) {
			name := strings.ToLower(strings.TrimSpace(res.name))
			if name != "" {
				componentNames[name] = struct{}{}
			}
		}
	}
	return templates, componentNames, warnings
}

func convertSharedTemplateSourceComponents(templates []*unstructured.Unstructured) ([]apisv1.CreateComponentRequest, []string) {
	if len(templates) == 0 {
		return nil, nil
	}
	objects := make([]*unstructured.Unstructured, 0, len(templates))
	for _, obj := range templates {
		if obj == nil {
			continue
		}
		objects = append(objects, obj.DeepCopy())
	}
	if len(objects) == 0 {
		return nil, nil
	}
	components, warnings, err := convertKubeObjectsToComponents(objects)
	if err != nil {
		return nil, append(warnings, fmt.Sprintf("convert shared templates for source matching failed: %v", err))
	}
	return components, warnings
}

func ensureShareLabelsForResources(objects []*unstructured.Unstructured, resources []*importResource) {
	if len(objects) == 0 || len(resources) == 0 {
		return
	}
	for i := range objects {
		if i >= len(resources) {
			break
		}
		obj := objects[i]
		res := resources[i]
		if obj == nil || res == nil {
			continue
		}
		if err := ensureShareLabelsOnResourceObject(res.kindKey, obj, res.name); err != nil {
			klog.Warningf("inject share labels for %s/%s failed: %v", res.kind, res.name, err)
		}
	}
}

func ensureShareLabelsOnResourceObject(kindKey string, obj *unstructured.Unstructured, defaultShareName string) error {
	if obj == nil {
		return nil
	}
	metaLabels := ensureShareLabels(utils.CopyStringMap(obj.GetLabels()), defaultShareName)
	obj.SetLabels(metaLabels)

	switch kindKey {
	case importKindDeployments, importKindStatefulSets, importKindDaemonSets, importKindJobs:
		templateLabels, _, err := unstructured.NestedStringMap(obj.Object, "spec", "template", "metadata", "labels")
		if err != nil {
			return err
		}
		templateLabels = ensureShareLabels(templateLabels, defaultShareName)
		if err := unstructured.SetNestedStringMap(obj.Object, templateLabels, "spec", "template", "metadata", "labels"); err != nil {
			return err
		}
	case importKindCronJobs:
		templateLabels, _, err := unstructured.NestedStringMap(obj.Object, "spec", "jobTemplate", "spec", "template", "metadata", "labels")
		if err != nil {
			return err
		}
		templateLabels = ensureShareLabels(templateLabels, defaultShareName)
		if err := unstructured.SetNestedStringMap(obj.Object, templateLabels, "spec", "jobTemplate", "spec", "template", "metadata", "labels"); err != nil {
			return err
		}
	}
	return nil
}

func isShareComponentCarrierKind(kindKey string) bool {
	switch kindKey {
	case importKindDeployments,
		importKindStatefulSets,
		importKindDaemonSets,
		importKindJobs,
		importKindCronJobs,
		importKindConfigMaps,
		importKindSecrets:
		return true
	default:
		return false
	}
}

func ensureSharedComponentsOnApp(
	sourceComponents, dedupedComponents, sharedSourceComponents []apisv1.CreateComponentRequest,
	resourceComponentByKey map[string]string,
) {
	if len(sourceComponents) == 0 || len(dedupedComponents) == 0 || len(sharedSourceComponents) == 0 {
		return
	}
	localComponentNames := make(map[string]struct{}, len(resourceComponentByKey))
	for _, name := range resourceComponentByKey {
		normalized := strings.ToLower(strings.TrimSpace(name))
		if normalized == "" {
			continue
		}
		localComponentNames[normalized] = struct{}{}
	}
	sharedBudget := buildComponentSignatureBudget(sharedSourceComponents)
	for i := range sourceComponents {
		if i >= len(dedupedComponents) {
			break
		}
		dedupedName := strings.ToLower(strings.TrimSpace(dedupedComponents[i].Name))
		if dedupedName != "" {
			if _, isLocal := localComponentNames[dedupedName]; isLocal {
				continue
			}
		}
		signature := componentSourceSignature(sourceComponents[i])
		if signature == "" || sharedBudget[signature] <= 0 {
			continue
		}
		sharedBudget[signature]--
		if dedupedComponents[i].Traits.Share == nil {
			dedupedComponents[i].Traits.Share = &domainspec.ShareTraitSpec{Strategy: string(domainspec.ShareStrategyDefault)}
			continue
		}
		if strings.TrimSpace(dedupedComponents[i].Traits.Share.Strategy) == "" {
			dedupedComponents[i].Traits.Share.Strategy = string(domainspec.ShareStrategyDefault)
		}
	}
}

func dedupeImportComponents(components []apisv1.CreateComponentRequest) []apisv1.CreateComponentRequest {
	if len(components) == 0 {
		return nil
	}
	const (
		maxNameLen   = 63
		nameFallback = "component"
	)

	sanitize := func(name string) string {
		normalized := utils.ToRFC1123Name(strings.TrimSpace(name))
		if len(normalized) > maxNameLen {
			normalized = strings.Trim(normalized[:maxNameLen], "-")
		}
		if normalized == "" {
			return nameFallback
		}
		return normalized
	}

	trimForSuffix := func(name string, suffix string) string {
		name = sanitize(name)
		maxPrefixLen := maxNameLen - len(suffix) - 1
		if maxPrefixLen < 1 {
			maxPrefixLen = 1
		}
		if len(name) <= maxPrefixLen {
			return name
		}
		trimmed := strings.Trim(name[:maxPrefixLen], "-")
		if trimmed == "" {
			return nameFallback
		}
		return trimmed
	}

	buildIndexedName := func(baseName string, idx int) string {
		suffix := strconv.Itoa(idx)
		prefix := trimForSuffix(baseName, suffix)
		return sanitize(prefix + "-" + suffix)
	}

	used := make(map[string]struct{}, len(components))
	nextIndexByBase := make(map[string]int, len(components))
	result := make([]apisv1.CreateComponentRequest, 0, len(components))
	for _, component := range components {
		baseName := sanitize(component.Name)
		name := baseName
		if _, exists := used[name]; exists {
			baseKey := strings.ToLower(baseName)
			nextIdx := nextIndexByBase[baseKey]
			if nextIdx < 2 {
				nextIdx = 2
			}
			for {
				candidate := buildIndexedName(baseName, nextIdx)
				nextIdx++
				if _, taken := used[candidate]; taken {
					continue
				}
				name = candidate
				break
			}
			nextIndexByBase[baseKey] = nextIdx
		}

		component.Name = name
		used[name] = struct{}{}
		result = append(result, component)
	}
	return result
}

func buildResourceComponentNameMapping(resources []*importResource, sourceComponents, dedupedComponents, resourceSourceComponents []apisv1.CreateComponentRequest) map[string]string {
	if len(resources) == 0 || len(sourceComponents) == 0 || len(sourceComponents) != len(dedupedComponents) {
		return nil
	}
	resourceBudget := buildComponentSignatureBudget(resourceSourceComponents)
	if len(resourceBudget) == 0 {
		return nil
	}
	renameQueueByName := make(map[string][]string, len(sourceComponents))
	for i := range sourceComponents {
		signature := componentSourceSignature(sourceComponents[i])
		if signature == "" || resourceBudget[signature] <= 0 {
			continue
		}
		resourceBudget[signature]--
		sourceName := strings.ToLower(strings.TrimSpace(sourceComponents[i].Name))
		dedupedName := strings.TrimSpace(dedupedComponents[i].Name)
		if sourceName == "" || dedupedName == "" {
			continue
		}
		renameQueueByName[sourceName] = append(renameQueueByName[sourceName], dedupedName)
	}
	if len(renameQueueByName) == 0 {
		return nil
	}

	mapping := make(map[string]string, len(resources))
	for _, res := range resourcesInConvertComponentOrder(resources) {
		if res == nil {
			continue
		}
		sourceName := strings.ToLower(strings.TrimSpace(res.name))
		if sourceName == "" {
			sourceName = strings.ToLower(strings.TrimSpace(res.componentName))
		}
		if sourceName == "" {
			continue
		}
		queue := renameQueueByName[sourceName]
		if len(queue) == 0 {
			continue
		}
		key := resourceResultKey(res)
		if key == "" {
			continue
		}
		mapping[key] = queue[0]
		renameQueueByName[sourceName] = queue[1:]
	}
	if len(mapping) == 0 {
		return nil
	}
	return mapping
}

func buildWorkloadComponentNameMapping(resources []*importResource, resourceComponentByKey map[string]string) map[string]string {
	if len(resources) == 0 || len(resourceComponentByKey) == 0 {
		return nil
	}
	mapping := make(map[string]string, len(resources))
	ambiguous := make(map[string]struct{})
	for _, res := range resources {
		if res == nil || !isImportWorkloadKind(res.kindKey) {
			continue
		}
		original := strings.TrimSpace(res.componentName)
		if original == "" {
			continue
		}
		mapped := strings.TrimSpace(resourceComponentByKey[resourceResultKey(res)])
		if mapped == "" {
			mapped = original
		}
		key := strings.ToLower(original)
		if existing, ok := mapping[key]; !ok {
			mapping[key] = mapped
			continue
		} else if !strings.EqualFold(existing, mapped) {
			delete(mapping, key)
			ambiguous[key] = struct{}{}
		}
	}
	for key := range ambiguous {
		delete(mapping, key)
	}
	if len(mapping) == 0 {
		return nil
	}
	return mapping
}

func isImportWorkloadKind(kindKey string) bool {
	switch kindKey {
	case importKindDeployments,
		importKindStatefulSets,
		importKindDaemonSets,
		importKindJobs,
		importKindCronJobs:
		return true
	default:
		return false
	}
}

func buildComponentSignatureBudget(components []apisv1.CreateComponentRequest) map[string]int {
	if len(components) == 0 {
		return nil
	}
	budget := make(map[string]int, len(components))
	for i := range components {
		signature := componentSourceSignature(components[i])
		if signature == "" {
			continue
		}
		budget[signature]++
	}
	if len(budget) == 0 {
		return nil
	}
	return budget
}

func componentSourceSignature(component apisv1.CreateComponentRequest) string {
	normalized := struct {
		Name          string            `json:"name"`
		Namespace     string            `json:"namespace"`
		ComponentType config.JobType    `json:"componentType"`
		Image         string            `json:"image"`
		Properties    apisv1.Properties `json:"properties"`
		Traits        apisv1.Traits     `json:"traits"`
	}{
		Name:          strings.TrimSpace(component.Name),
		Namespace:     strings.TrimSpace(component.Namespace),
		ComponentType: component.ComponentType,
		Image:         strings.TrimSpace(component.Image),
		Properties:    component.Properties,
		Traits:        component.Traits,
	}
	data, err := json.Marshal(normalized)
	if err != nil {
		return ""
	}
	return string(data)
}

func resourcesInConvertComponentOrder(resources []*importResource) []*importResource {
	if len(resources) == 0 {
		return nil
	}
	configMaps := make([]*importResource, 0)
	secrets := make([]*importResource, 0)
	workloads := make([]*importResource, 0)
	jobs := make([]*importResource, 0)
	cronJobs := make([]*importResource, 0)

	for _, res := range resources {
		if res == nil {
			continue
		}
		switch res.kindKey {
		case importKindConfigMaps:
			configMaps = append(configMaps, res)
		case importKindSecrets:
			secrets = append(secrets, res)
		case importKindDeployments, importKindStatefulSets, importKindDaemonSets:
			workloads = append(workloads, res)
		case importKindJobs:
			jobs = append(jobs, res)
		case importKindCronJobs:
			cronJobs = append(cronJobs, res)
		}
	}

	ordered := make([]*importResource, 0, len(configMaps)+len(secrets)+len(workloads)+len(jobs)+len(cronJobs))
	ordered = append(ordered, configMaps...)
	ordered = append(ordered, secrets...)
	ordered = append(ordered, workloads...)
	ordered = append(ordered, jobs...)
	ordered = append(ordered, cronJobs...)
	return ordered
}

func componentNames(components []apisv1.CreateComponentRequest) []string {
	names := make([]string, 0, len(components))
	for _, component := range components {
		name := strings.TrimSpace(component.Name)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func resourceResultKey(res *importResource) string {
	if res == nil {
		return ""
	}
	return fmt.Sprintf("%s/%s/%s", res.kind, res.namespace, res.name)
}

func (s *namespaceImportServiceImpl) markPlanResourcesSkipped(resp *apisv1.ImportNamespaceApplicationsResponse, resultIndex map[string]int, resources []*importResource, err error) {
	if resp == nil {
		return
	}
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	for _, res := range resources {
		idx, ok := resultIndex[resourceResultKey(res)]
		if !ok {
			continue
		}
		resp.ResourceResults[idx].Status = importResourceStatusSkipped
		resp.ResourceResults[idx].Error = errText
	}
}

func (s *namespaceImportServiceImpl) markPlanResourcesWithStatus(
	resp *apisv1.ImportNamespaceApplicationsResponse,
	resultIndex map[string]int,
	resources []*importResource,
	err error,
	status string,
) {
	if strings.EqualFold(strings.TrimSpace(status), importResourceStatusFailed) {
		s.markPlanResourcesFailed(resp, resultIndex, resources, err)
		return
	}
	s.markPlanResourcesSkipped(resp, resultIndex, resources, err)
}

func (s *namespaceImportServiceImpl) updatePlanResourceResultAppID(
	resp *apisv1.ImportNamespaceApplicationsResponse,
	resultIndex map[string]int,
	resources []*importResource,
	appID string,
) {
	if resp == nil {
		return
	}
	for _, res := range resources {
		idx, ok := resultIndex[resourceResultKey(res)]
		if !ok {
			continue
		}
		resp.ResourceResults[idx].AppID = appID
	}
}

func (s *namespaceImportServiceImpl) markPlanResourcesFailed(resp *apisv1.ImportNamespaceApplicationsResponse, resultIndex map[string]int, resources []*importResource, err error) {
	if resp == nil {
		return
	}
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	for _, res := range resources {
		idx, ok := resultIndex[resourceResultKey(res)]
		if !ok {
			continue
		}
		if resp.ResourceResults[idx].Status == importResourceStatusFailed {
			continue
		}
		resp.ResourceResults[idx].Status = importResourceStatusFailed
		resp.ResourceResults[idx].Error = errText
		resp.Summary.ResourcesLabeledFailed++
	}
}

func (s *namespaceImportServiceImpl) loadComponentIDMap(ctx context.Context, appID string) (map[string]int, error) {
	if s.ComponentRepo == nil {
		return nil, fmt.Errorf("component repository is nil")
	}
	components, err := s.ComponentRepo.FindByAppID(ctx, appID)
	if err != nil {
		return nil, err
	}
	result := make(map[string]int, len(components))
	for _, component := range components {
		if component == nil {
			continue
		}
		name := strings.TrimSpace(component.Name)
		if name == "" {
			continue
		}
		result[strings.ToLower(name)] = component.ID
	}
	return result, nil
}

func resolveResourceComponentName(res *importResource, componentIDByName map[string]int, resourceComponentByKey, workloadComponentByOriginalName map[string]string) string {
	if res == nil {
		return ""
	}

	resolveIfKnown := func(name string) string {
		name = strings.TrimSpace(name)
		if name == "" {
			return ""
		}
		if _, ok := componentIDByName[strings.ToLower(name)]; !ok {
			return ""
		}
		return name
	}

	if resourceComponentByKey != nil {
		if name := resolveIfKnown(resourceComponentByKey[resourceResultKey(res)]); name != "" {
			return name
		}
	}
	if name := resolveIfKnown(resolveDependentResourceTargetName(res, workloadComponentByOriginalName)); name != "" {
		return name
	}
	if name := resolveIfKnown(res.componentName); name != "" {
		return name
	}
	if name := resolveIfKnown(res.name); name != "" {
		return name
	}
	if len(componentIDByName) == 1 {
		for name := range componentIDByName {
			return name
		}
	}
	return resolveIfKnown(res.labels[config.LabelComponentName])
}

func resolveDependentResourceTargetName(res *importResource, workloadComponentByOriginalName map[string]string) string {
	if res == nil || len(workloadComponentByOriginalName) == 0 {
		return ""
	}
	switch res.kindKey {
	case importKindServices,
		importKindIngresses,
		importKindServiceAccounts,
		importKindRoleBindings,
		importKindClusterRoleBindings:
	default:
		return ""
	}
	original := strings.ToLower(strings.TrimSpace(res.componentName))
	if original == "" {
		return ""
	}
	return strings.TrimSpace(workloadComponentByOriginalName[original])
}

func resolveComponentID(componentName string, res *importResource, componentIDByName map[string]int) int {
	if componentName != "" {
		if id, ok := componentIDByName[strings.ToLower(componentName)]; ok {
			return id
		}
	}
	if res == nil {
		return 0
	}
	raw := strings.TrimSpace(res.labels[config.LabelComponentID])
	if raw == "" {
		return 0
	}
	id, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return id
}

func buildImportLabels(res *importResource, appID, stableAppKey, componentName string, componentID int, forceShareLabels bool) map[string]string {
	labels := make(map[string]string, 5)
	stableAppKey = strings.TrimSpace(stableAppKey)
	if stableAppKey == "" {
		stableAppKey = appID
	}
	labels[config.LabelAppID] = appID
	labels[config.LabelImportAppKey] = stableAppKey

	if componentName == "" && res != nil {
		componentName = strings.TrimSpace(res.labels[config.LabelComponentName])
	}
	if componentName == "" && res != nil {
		componentName = strings.TrimSpace(res.name)
	}
	if componentName != "" {
		componentName = boundedRFC1123LabelValue(componentName)
	}

	if componentName != "" {
		labels[config.LabelComponentName] = componentName
	}

	labels[config.LabelManagedBy] = config.ManagedByEruun

	if componentID > 0 {
		labels[config.LabelComponentID] = strconv.Itoa(componentID)
	}

	shareName := ""
	shareStrategy := ""
	if res != nil {
		shareName = strings.TrimSpace(res.labels[config.LabelShareName])
		shareStrategy = strings.TrimSpace(res.labels[config.LabelShareStrategy])
	}
	if forceShareLabels && shareName == "" {
		switch {
		case componentName != "":
			shareName = componentName
		case res != nil:
			shareName = strings.TrimSpace(res.name)
		}
	}
	if shareName != "" && shareStrategy == "" {
		shareStrategy = string(domainspec.ShareStrategyDefault)
	}
	if shareStrategy != "" {
		normalized, ok := domainspec.NormalizeShareStrategy(shareStrategy)
		if !ok {
			normalized = domainspec.ShareStrategyDefault
		}
		shareStrategy = string(normalized)
	}
	if shareName != "" {
		labels[config.LabelShareName] = shareName
	}
	if shareStrategy != "" {
		labels[config.LabelShareStrategy] = shareStrategy
	}

	return labels
}

func isImportRBACKind(res *importResource) bool {
	if res == nil {
		return false
	}
	switch res.kindKey {
	case importKindServiceAccounts,
		importKindRoles,
		importKindRoleBindings,
		importKindClusterRoles,
		importKindClusterRoleBindings:
		return true
	default:
		return false
	}
}

func (s *namespaceImportServiceImpl) patchResourceLabels(ctx context.Context, res *importResource, labels map[string]string) error {
	if res == nil {
		return fmt.Errorf("resource is nil")
	}
	if len(labels) == 0 {
		return nil
	}
	patch, err := buildLabelPatch(res.kindKey, labels, selectorMatchLabelsForImportResource(res))
	if err != nil {
		return err
	}
	return s.patchResourceLabelBytes(ctx, res, patch)
}

func (s *namespaceImportServiceImpl) patchResourceMetadataLabels(ctx context.Context, res *importResource, labels map[string]string) error {
	if res == nil {
		return fmt.Errorf("resource is nil")
	}
	if len(labels) == 0 {
		return nil
	}
	patch, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": utils.CopyStringMap(labels),
		},
	})
	if err != nil {
		return fmt.Errorf("marshal metadata-only label patch: %w", err)
	}
	return s.patchResourceLabelBytes(ctx, res, patch)
}

func (s *namespaceImportServiceImpl) patchResourceLabelBytes(ctx context.Context, res *importResource, patch []byte) error {
	var err error
	switch res.kindKey {
	case importKindDeployments:
		_, err = s.KubeClient.AppsV1().Deployments(res.namespace).Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	case importKindStatefulSets:
		_, err = s.KubeClient.AppsV1().StatefulSets(res.namespace).Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	case importKindDaemonSets:
		_, err = s.KubeClient.AppsV1().DaemonSets(res.namespace).Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	case importKindJobs:
		_, err = s.KubeClient.BatchV1().Jobs(res.namespace).Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	case importKindCronJobs:
		_, err = s.KubeClient.BatchV1().CronJobs(res.namespace).Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	case importKindConfigMaps:
		_, err = s.KubeClient.CoreV1().ConfigMaps(res.namespace).Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	case importKindSecrets:
		_, err = s.KubeClient.CoreV1().Secrets(res.namespace).Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	case importKindPersistentVolumeClaims:
		_, err = s.KubeClient.CoreV1().PersistentVolumeClaims(res.namespace).Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	case importKindServices:
		_, err = s.KubeClient.CoreV1().Services(res.namespace).Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	case importKindIngresses:
		_, err = s.KubeClient.NetworkingV1().Ingresses(res.namespace).Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	case importKindServiceAccounts:
		_, err = s.KubeClient.CoreV1().ServiceAccounts(res.namespace).Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	case importKindRoles:
		_, err = s.KubeClient.RbacV1().Roles(res.namespace).Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	case importKindRoleBindings:
		_, err = s.KubeClient.RbacV1().RoleBindings(res.namespace).Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	case importKindClusterRoles:
		_, err = s.KubeClient.RbacV1().ClusterRoles().Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	case importKindClusterRoleBindings:
		_, err = s.KubeClient.RbacV1().ClusterRoleBindings().Patch(ctx, res.name, types.MergePatchType, patch, metav1.PatchOptions{})
	default:
		return fmt.Errorf("unsupported resource kind %s", res.kind)
	}
	if err != nil {
		return fmt.Errorf("patch labels for %s/%s: %w", res.kind, res.name, err)
	}
	return nil
}

func selectorMatchLabelsForImportResource(res *importResource) map[string]string {
	if res == nil || res.object == nil {
		return nil
	}
	switch res.kindKey {
	case importKindDeployments, importKindStatefulSets, importKindDaemonSets:
	default:
		return nil
	}
	selector, found, err := unstructured.NestedStringMap(res.object.Object, "spec", "selector", "matchLabels")
	if err != nil || !found || len(selector) == 0 {
		return nil
	}
	return selector
}

func buildLabelPatch(kindKey string, labels, selectorMatchLabels map[string]string) ([]byte, error) {
	metadataLabels := alignManagedLabelsWithSelector(labels, selectorMatchLabels)
	if kindKey == importKindDeployments || kindKey == importKindStatefulSets || kindKey == importKindDaemonSets {
		templateLabels := utils.CopyStringMap(metadataLabels)
		for key, val := range selectorMatchLabels {
			templateLabels[key] = val
		}
		payload := map[string]interface{}{
			"metadata": map[string]interface{}{
				"labels": metadataLabels,
			},
			"spec": map[string]interface{}{
				"template": map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": templateLabels,
					},
				},
			},
		}
		return json.Marshal(payload)
	}
	if kindKey == importKindJobs {
		payload := map[string]interface{}{
			"metadata": map[string]interface{}{
				"labels": metadataLabels,
			},
		}
		return json.Marshal(payload)
	}
	if kindKey == importKindCronJobs {
		payload := map[string]interface{}{
			"metadata": map[string]interface{}{
				"labels": metadataLabels,
			},
			"spec": map[string]interface{}{
				"jobTemplate": map[string]interface{}{
					"spec": map[string]interface{}{
						"template": map[string]interface{}{
							"metadata": map[string]interface{}{
								"labels": metadataLabels,
							},
						},
					},
				},
			},
		}
		return json.Marshal(payload)
	}
	payload := map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": metadataLabels,
		},
	}
	return json.Marshal(payload)
}

var selectorManagedLabelKeys = map[string]struct{}{
	config.LabelManagedBy: {},
	config.LabelAppID:     {},
}

func alignManagedLabelsWithSelector(labels, selectorMatchLabels map[string]string) map[string]string {
	result := utils.CopyStringMap(labels)
	result[config.LabelManagedBy] = config.ManagedByEruun
	if len(selectorMatchLabels) == 0 {
		return result
	}
	for key, val := range selectorMatchLabels {
		if _, managed := selectorManagedLabelKeys[key]; !managed {
			continue
		}
		normalized := strings.TrimSpace(val)
		if normalized == "" {
			continue
		}
		if key == config.LabelManagedBy {
			result[key] = config.ManagedByEruun
			continue
		}
		result[key] = normalized
	}
	return result
}
