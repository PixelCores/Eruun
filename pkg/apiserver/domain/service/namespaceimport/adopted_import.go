package namespaceimport

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	klabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/PixelCores/Eruun/pkg/apiserver/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	applicationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/importsecret"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

const (
	adoptedDependencyRoleWorkload       = "workload"
	adoptedDependencyRoleService        = "service"
	adoptedDependencyRoleIngress        = "ingress"
	adoptedDependencyRolePVC            = "pvc"
	adoptedDependencyRoleConfigMap      = "configmap"
	adoptedDependencyRoleSecret         = "secret"
	adoptedDependencyRoleServiceAccount = "service-account"
	adoptedDependencyRoleRBAC           = "rbac"
	adoptedDependencyRolePDB            = "pdb"
	adoptedDependencyRoleNetworkPolicy  = "network-policy"
)

type adoptedAppPlanState struct {
	mapping                    apisv1.ImportNamespaceApplicationMapping
	snapshot                   adoption.Snapshot
	sourceWorkloadByComponent  map[string]*importResource
	secretResourcesByComponent map[string][]*importResource
	existingComponentIDByName  map[string]int
	targetState                adoptedCanonicalTargetState
}

type adoptedImportPlanning struct {
	plans       []importAppPlan
	excluded    []*importResource
	payload     []byte
	fingerprint string
	keyring     *importsecret.Keyring
	warnings    []string
}

type adoptedRoot struct {
	planKey       string
	componentName string
	resource      *importResource
	ref           workloadRef
	serviceNames  map[string]struct{}
}

type adoptedMembership struct {
	resource          *importResource
	appComponents     map[string]map[string]struct{}
	externalWorkloads map[string]struct{}
}

type adoptedCanonicalPlan struct {
	Version        int                       `json:"version"`
	Namespace      string                    `json:"namespace"`
	ManagementMode config.ManagementMode     `json:"managementMode"`
	Applications   []adoptedCanonicalPlanApp `json:"applications"`
}

type adoptedCanonicalPlanApp struct {
	Name        string                      `json:"name"`
	Alias       string                      `json:"alias,omitempty"`
	TargetAppID string                      `json:"targetAppId,omitempty"`
	TargetState adoptedCanonicalTargetState `json:"targetState"`
	Components  []adoptedCanonicalComponent `json:"components"`
	Snapshot    adoption.Snapshot           `json:"snapshot"`
}

type adoptedCanonicalTargetState struct {
	Exists               bool                  `json:"exists"`
	ID                   string                `json:"id,omitempty"`
	Name                 string                `json:"name,omitempty"`
	Namespace            string                `json:"namespace,omitempty"`
	ManagementMode       config.ManagementMode `json:"managementMode,omitempty"`
	UpdatedAtUnixNano    int64                 `json:"updatedAtUnixNano,omitempty"`
	ComponentStateDigest string                `json:"componentStateDigest,omitempty"`
	WorkflowStateDigest  string                `json:"workflowStateDigest,omitempty"`
}

type adoptedCanonicalComponent struct {
	Name     string                    `json:"name"`
	Workload adoption.ResourceIdentity `json:"workload"`
}

func (s *namespaceImportServiceImpl) tryAdoptedApplyReplay(
	ctx context.Context,
	namespace string,
	req apisv1.ImportNamespaceApplicationsRequest,
	resources []*importResource,
	existingAppIDMap map[string]string,
	existingAppIDSet map[string]struct{},
	existingAppNameByID map[string]string,
	keyring *importsecret.Keyring,
) (*adoptedImportPlanning, string, bool, error) {
	if !strings.EqualFold(strings.TrimSpace(req.Mode), importModeApply) ||
		strings.TrimSpace(req.PlanFingerprint) == "" ||
		len(req.Applications) != 1 {
		return nil, "", false, nil
	}
	mapping := req.Applications[0]
	targetAppID := strings.TrimSpace(mapping.TargetAppID)
	if targetAppID == "" {
		targetAppID = strings.TrimSpace(existingAppIDMap[appNameNamespaceKey(mapping.Name, namespace)])
	}
	if targetAppID == "" {
		return nil, "", false, nil
	}
	if s.AppRepo == nil || s.ComponentRepo == nil {
		return nil, "", true, fmt.Errorf("%w: adopted replay repositories are not initialized", bcode.ErrNamespaceImportPlanDrift)
	}
	app, err := s.AppRepo.FindByID(ctx, targetAppID)
	if err != nil {
		return nil, "", true, fmt.Errorf("%w: load adopted replay target %q: %v", bcode.ErrNamespaceImportPlanDrift, targetAppID, err)
	}
	if app.EffectiveManagementMode() != config.ManagementModeAdopted {
		if strings.TrimSpace(mapping.TargetAppID) != "" {
			return nil, "", false, nil
		}
		return nil, "", true, fmt.Errorf("%w: an application now occupies the requested adopted name", bcode.ErrNamespaceImportPlanDrift)
	}
	if !strings.EqualFold(strings.TrimSpace(app.Name), strings.TrimSpace(mapping.Name)) ||
		strings.TrimSpace(app.Namespace) != namespace ||
		strings.TrimSpace(app.Alias) != strings.TrimSpace(mapping.Alias) {
		return nil, "", true, fmt.Errorf("%w: persisted adopted application identity no longer matches the request", bcode.ErrNamespaceImportPlanDrift)
	}
	persistedSnapshot, err := decodeAdoptionSnapshot(app.AdoptionSnapshot)
	if err != nil {
		return nil, "", true, fmt.Errorf("%w: decode persisted adoption snapshot: %v", bcode.ErrNamespaceImportPlanDrift, err)
	}
	if err := persistedSnapshot.Validate(); err != nil {
		return nil, "", true, fmt.Errorf("%w: invalid persisted adoption snapshot: %v", bcode.ErrNamespaceImportPlanDrift, err)
	}
	if strings.TrimSpace(persistedSnapshot.PlanFingerprint) != strings.TrimSpace(req.PlanFingerprint) {
		if strings.TrimSpace(mapping.TargetAppID) != "" {
			return nil, "", false, nil
		}
		return nil, "", true, fmt.Errorf("%w: submitted fingerprint does not match the persisted adoption snapshot", bcode.ErrNamespaceImportPlanDrift)
	}

	replayMapping := mapping
	replayMapping.TargetAppID = targetAppID
	planning, err := s.buildAdoptedImportPlanning(
		ctx,
		namespace,
		resources,
		[]apisv1.ImportNamespaceApplicationMapping{replayMapping},
		existingAppIDMap,
		existingAppIDSet,
		existingAppNameByID,
		keyring,
	)
	if err != nil {
		return nil, "", true, fmt.Errorf("%w: rebuild adopted replay plan: %v", bcode.ErrNamespaceImportPlanDrift, err)
	}
	if len(planning.plans) != 1 || planning.plans[0].adopted == nil || planning.plans[0].err != nil {
		return nil, "", true, fmt.Errorf("%w: current dependency plan is no longer adoptable", bcode.ErrNamespaceImportPlanDrift)
	}
	currentSnapshot := planning.plans[0].adopted.snapshot
	if !equalAdoptionReplaySnapshots(persistedSnapshot, currentSnapshot) {
		return nil, "", true, fmt.Errorf("%w: current Kubernetes identity or dependency graph differs from the persisted adoption snapshot", bcode.ErrNamespaceImportPlanDrift)
	}
	components, err := s.ComponentRepo.FindByAppID(ctx, targetAppID)
	if err != nil {
		return nil, "", true, fmt.Errorf("%w: load adopted replay components: %v", bcode.ErrNamespaceImportPlanDrift, err)
	}
	if err := validateAdoptedReplayComponentBindings(
		targetAppID,
		replayMapping,
		planning.plans[0].adopted.sourceWorkloadByComponent,
		components,
	); err != nil {
		return nil, "", true, fmt.Errorf("%w: %v", bcode.ErrNamespaceImportPlanDrift, err)
	}

	planning.fingerprint = strings.TrimSpace(req.PlanFingerprint)
	planning.plans[0].adopted.snapshot = persistedSnapshot
	return planning, targetAppID, true, nil
}

func decodeAdoptionSnapshot(value *model.JSONStruct) (adoption.Snapshot, error) {
	if value == nil {
		return adoption.Snapshot{}, fmt.Errorf("snapshot is missing")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return adoption.Snapshot{}, err
	}
	var snapshot adoption.Snapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return adoption.Snapshot{}, err
	}
	return snapshot, nil
}

func equalAdoptionReplaySnapshots(left, right adoption.Snapshot) bool {
	normalize := func(snapshot adoption.Snapshot) ([]byte, error) {
		snapshot.PlanFingerprint = ""
		// Snapshot v2 only adds the write-ahead recreation claim. A persisted
		// v1 snapshot without such state describes the same import contract as
		// the v2 snapshot rebuilt from Kubernetes after an upgrade.
		if snapshot.Version == 1 {
			snapshot.Version = adoption.SnapshotVersion
		}
		snapshot.Resources = append([]adoption.ResourceSnapshot(nil), snapshot.Resources...)
		for index := range snapshot.Resources {
			snapshot.Resources[index].Manifest = nil
			snapshot.Resources[index].SecretKeys = append([]string(nil), snapshot.Resources[index].SecretKeys...)
		}
		snapshot.Sort()
		return json.Marshal(snapshot)
	}
	leftPayload, leftErr := normalize(left)
	rightPayload, rightErr := normalize(right)
	return leftErr == nil && rightErr == nil && string(leftPayload) == string(rightPayload)
}

func validateAdoptedReplayComponentBindings(
	appID string,
	mapping apisv1.ImportNamespaceApplicationMapping,
	sources map[string]*importResource,
	components []*model.ApplicationComponent,
) error {
	if len(components) != len(mapping.Components) {
		return fmt.Errorf("persisted component count changed for app %q", appID)
	}
	byName := make(map[string]*model.ApplicationComponent, len(components))
	for _, component := range components {
		if component == nil || strings.TrimSpace(component.Name) == "" {
			return fmt.Errorf("persisted component binding is incomplete")
		}
		byName[component.Name] = component
	}
	for _, requested := range mapping.Components {
		component := byName[strings.TrimSpace(requested.Name)]
		source := sources[strings.TrimSpace(requested.Name)]
		if component == nil || source == nil || source.object == nil || component.ID <= 0 {
			return fmt.Errorf("persisted source binding for component %q is missing", requested.Name)
		}
		uid := ""
		if component.SourceWorkloadUID != nil {
			uid = strings.TrimSpace(*component.SourceWorkloadUID)
		}
		if component.AppID != appID ||
			component.SourceWorkloadAPIVersion != strings.TrimSpace(requested.Workload.APIVersion) ||
			component.SourceWorkloadKind != canonicalAdoptedWorkloadKind(requested.Workload.Kind) ||
			component.SourceWorkloadName != strings.TrimSpace(requested.Workload.Name) ||
			uid == "" ||
			uid != strings.TrimSpace(string(source.object.GetUID())) {
			return fmt.Errorf("persisted source binding for component %q changed", requested.Name)
		}
	}
	return nil
}

func normalizeImportManagementMode(req apisv1.ImportNamespaceApplicationsRequest) (config.ManagementMode, error) {
	raw := strings.TrimSpace(string(req.ManagementMode))
	if raw == "" {
		if len(req.Applications) > 0 || strings.TrimSpace(req.PlanFingerprint) != "" {
			return "", bcode.ErrApplicationConfig
		}
		return config.ManagementModeObserve, nil
	}
	mode, ok := config.NormalizeManagementMode(raw)
	if !ok || mode == config.ManagementModeNative {
		return "", bcode.ErrApplicationConfig
	}
	if mode == config.ManagementModeAdopted {
		if len(req.Applications) != 1 {
			return "", fmt.Errorf(
				"%w: adopted namespace import requires exactly one application mapping",
				bcode.ErrApplicationConfig,
			)
		}
		if len(req.IncludeKinds) > 0 {
			return "", fmt.Errorf(
				"%w: adopted namespace import cannot be combined with includeKinds",
				bcode.ErrApplicationConfig,
			)
		}
		return mode, nil
	}
	if len(req.Applications) > 0 || strings.TrimSpace(req.PlanFingerprint) != "" {
		return "", bcode.ErrApplicationConfig
	}
	return mode, nil
}

func (s *namespaceImportServiceImpl) loadAdoptedImportKeyring() (*importsecret.Keyring, error) {
	if s.Cfg == nil {
		return nil, nil
	}
	return importsecret.Load(s.Cfg.ImportSecretKeyring, s.Cfg.ImportSecretKeyringFile)
}

func (s *namespaceImportServiceImpl) prepareAdoptedPlansForExecution(
	ctx context.Context,
	namespace string,
	plans []importAppPlan,
) {
	for index := range plans {
		plan := &plans[index]
		if plan.err != nil {
			if strings.TrimSpace(plan.applyErrorStatus) == "" {
				plan.applyErrorStatus = importResourceStatusSkipped
			}
			continue
		}
		if plan.adopted == nil {
			plan.err = fmt.Errorf("%w: adopted plan state is missing", bcode.ErrAdoptedResourceConflict)
			plan.applyErrorStatus = importResourceStatusSkipped
			continue
		}
		targetAppID := strings.TrimSpace(plan.adopted.mapping.TargetAppID)
		if targetAppID != "" {
			if s.AppRepo == nil || s.ComponentRepo == nil {
				plan.err = fmt.Errorf("adopted target repositories are not initialized")
				plan.applyErrorStatus = importResourceStatusFailed
				continue
			}
			existing, err := s.AppRepo.FindByID(ctx, targetAppID)
			if err != nil {
				plan.err = fmt.Errorf("%w: load target app %q: %v", bcode.ErrNamespaceImportPlanDrift, targetAppID, err)
				plan.applyErrorStatus = importResourceStatusFailed
				continue
			}
			if existing.EffectiveManagementMode() == config.ManagementModeNative {
				plan.err = fmt.Errorf(
					"%w: native target app %q cannot be replaced by namespace adoption",
					bcode.ErrAdoptedResourceConflict,
					targetAppID,
				)
				plan.applyErrorStatus = importResourceStatusSkipped
				continue
			}
			components, err := s.ComponentRepo.FindByAppID(ctx, targetAppID)
			if err != nil {
				plan.err = fmt.Errorf("%w: load target app %q components: %v", bcode.ErrNamespaceImportPlanDrift, targetAppID, err)
				plan.applyErrorStatus = importResourceStatusFailed
				continue
			}
			plan.adopted.existingComponentIDByName = make(map[string]int, len(components))
			for _, component := range components {
				if component != nil && strings.TrimSpace(component.Name) != "" && component.ID > 0 {
					plan.adopted.existingComponentIDByName[component.Name] = component.ID
				}
			}
		}
		createReq := apisv1.CreateApplicationsRequest{
			ID:          targetAppID,
			Name:        plan.adopted.mapping.Name,
			Namespace:   namespace,
			Alias:       plan.adopted.mapping.Alias,
			Version:     "imported",
			Project:     "imported",
			Description: fmt.Sprintf("adopted from namespace %s", namespace),
			Component:   sanitizeAdoptedImportComponentsForCreate(plan.components),
		}
		if err := s.tryValidateImportCreateRequest(ctx, createReq); err != nil {
			plan.err = err
			plan.applyErrorStatus = importResourceStatusFailed
			continue
		}
		plan.createReq = createReq
	}
}

func (s *namespaceImportServiceImpl) buildAdoptedImportPlanning(
	ctx context.Context,
	namespace string,
	resources []*importResource,
	mappings []apisv1.ImportNamespaceApplicationMapping,
	existingAppIDMap map[string]string,
	existingAppIDSet map[string]struct{},
	existingAppNameByID map[string]string,
	keyring *importsecret.Keyring,
) (*adoptedImportPlanning, error) {
	membership := make(map[string]*adoptedMembership)
	resourceByWorkload := indexAdoptedWorkloads(resources)
	roots, planByKey, warnings, err := buildAdoptedRoots(
		namespace,
		mappings,
		resourceByWorkload,
		existingAppIDMap,
		existingAppIDSet,
		existingAppNameByID,
	)
	if err != nil {
		return nil, err
	}
	warnings = append(
		warnings,
		"Namespace, CustomResourceDefinition, and custom-resource inventory is outside the adopted import boundary; those resources remain external",
	)

	hpaTargets, err := s.scanAdoptedHPATargets(ctx, namespace)
	if err != nil {
		return nil, err
	}
	resourceByKindAndName := indexAdoptedResources(resources)
	for _, root := range roots {
		addAdoptedMembership(membership, root.resource, root.planKey, root.componentName)
		collectAdoptedRootDependencies(root, resources, resourceByKindAndName, membership)
	}
	collectAdoptedIngressDependencies(roots, resources, resourceByKindAndName, membership)
	collectAdoptedRBACDependencies(namespace, roots, resources, resourceByKindAndName, membership)
	collectAdoptedPolicyDependencies(roots, resources, membership)
	externalWarnings := collectAdoptedExternalWorkloadUsage(roots, resources, membership)
	warnings = append(warnings, externalWarnings...)
	externalPodWarnings, podLabelConflictReasons, err := s.collectAdoptedExternalPodAndReplicaSetUsage(
		ctx,
		namespace,
		roots,
		membership,
	)
	if err != nil {
		return nil, err
	}
	warnings = append(warnings, externalPodWarnings...)
	propagateAdoptedSharedDependencies(namespace, resources, membership)

	blockedPlanReasons := make(map[string][]string)
	for planKey, reasons := range podLabelConflictReasons {
		blockedPlanReasons[planKey] = append(blockedPlanReasons[planKey], reasons...)
	}
	uidConflictReasons, err := s.findAdoptedOwnershipConflicts(ctx, namespace, roots, membership, mappings)
	if err != nil {
		return nil, err
	}
	for planKey, reasons := range uidConflictReasons {
		blockedPlanReasons[planKey] = append(blockedPlanReasons[planKey], reasons...)
	}
	for planKey, reasons := range adoptedOwnerReferenceConflicts(membership) {
		blockedPlanReasons[planKey] = append(blockedPlanReasons[planKey], reasons...)
	}
	for _, root := range roots {
		key := adoptedWorkloadIdentityKey(root.resource.object.GetAPIVersion(), root.resource.kind, root.resource.name)
		if hpas := hpaTargets[key]; len(hpas) > 0 {
			blockedPlanReasons[root.planKey] = append(
				blockedPlanReasons[root.planKey],
				fmt.Sprintf("workload %s/%s is targeted by HPA %s", root.resource.kind, root.resource.name, strings.Join(hpas, ", ")),
			)
		}
		if strings.TrimSpace(string(root.resource.object.GetUID())) == "" ||
			strings.TrimSpace(root.resource.object.GetResourceVersion()) == "" {
			blockedPlanReasons[root.planKey] = append(
				blockedPlanReasons[root.planKey],
				fmt.Sprintf("workload %s/%s has incomplete UID/resourceVersion identity", root.resource.kind, root.resource.name),
			)
		}
	}
	for _, member := range membership {
		if len(member.appComponents) <= 1 {
			continue
		}
		apps := sortedStringMapKeys(member.appComponents)
		reason := fmt.Sprintf(
			"resource %s/%s is shared across explicitly mapped applications %s",
			member.resource.kind,
			member.resource.name,
			strings.Join(apps, ", "),
		)
		for _, appKey := range apps {
			blockedPlanReasons[appKey] = append(blockedPlanReasons[appKey], reason)
		}
	}
	if keyring == nil {
		for planKey := range planByKey {
			blockedPlanReasons[planKey] = append(
				blockedPlanReasons[planKey],
				"adopted namespace import requires an import secret keyring for authenticated plan fingerprints",
			)
		}
	}

	assignAdoptedResourceSemantics(membership, blockedPlanReasons)
	excluded := markExcludedAdoptedResources(resources, membership)

	plans := make([]importAppPlan, 0, len(planByKey))
	for _, mapping := range mappings {
		planKey := adoptedPlanKey(mapping)
		plan := planByKey[planKey]
		planResources := adoptedPlanResources(planKey, membership)
		plan.resources = sortedImportPlanResources(planResources)

		components, conversionWarnings, conversionErr := buildAdoptedComponents(mapping, roots, plan.resources)
		appendImportPlanWarnings(&plan, planKey, conversionWarnings)
		if conversionErr != nil {
			blockedPlanReasons[planKey] = append(blockedPlanReasons[planKey], conversionErr.Error())
		}
		plan.components = components
		plan.componentNames = componentNames(components)
		plan.resourceComponentByKey = adoptedResourceComponentMapping(planKey, membership)
		plan.workloadComponentByOriginalName = adoptedWorkloadComponentMapping(mapping)
		plan.adopted = &adoptedAppPlanState{
			mapping:                    mapping,
			sourceWorkloadByComponent:  adoptedSourceWorkloads(planKey, roots),
			secretResourcesByComponent: adoptedSecretResourcesByComponent(planKey, membership),
		}

		if reasons := uniqueSortedStrings(blockedPlanReasons[planKey]); len(reasons) > 0 {
			for _, resource := range plan.resources {
				resource.disposition = adoption.DispositionBlocked
				resource.dispositionErr = strings.Join(reasons, "; ")
			}
			plan.err = fmt.Errorf("%w: %s", bcode.ErrAdoptedResourceConflict, strings.Join(reasons, "; "))
			plan.applyErrorStatus = importResourceStatusSkipped
			plan.warnings = append(plan.warnings, reasons...)
		}
		if len(plan.components) == 0 && plan.err == nil {
			plan.err = fmt.Errorf("%w: no convertible mapped workload components", bcode.ErrAdoptedResourceConflict)
			plan.applyErrorStatus = importResourceStatusSkipped
		}

		snapshotResources := make([]adoption.ResourceSnapshot, 0, len(plan.resources))
		for _, resource := range plan.resources {
			snapshot, snapshotErr := adoption.ResourceSnapshotFromObject(
				resource.object,
				resource.componentName,
				resource.dependencyRole,
				resource.ownership,
				resource.disposition,
			)
			if snapshotErr != nil {
				return nil, snapshotErr
			}
			snapshotResources = append(snapshotResources, snapshot)
		}
		plan.adopted.snapshot = adoption.NewSnapshot(namespace, snapshotResources)
		plans = append(plans, plan)
	}

	canonical, err := s.buildAdoptedCanonicalPlan(ctx, namespace, plans)
	if err != nil {
		return nil, err
	}
	planning := &adoptedImportPlanning{
		plans:    plans,
		excluded: excluded,
		payload:  canonical,
		keyring:  keyring,
		warnings: warnings,
	}
	if keyring != nil {
		fingerprint, err := keyring.SignPlan(canonical)
		if err != nil {
			return nil, fmt.Errorf("sign adopted namespace import plan: %w", err)
		}
		planning.fingerprint = fingerprint
		for index := range planning.plans {
			if planning.plans[index].adopted == nil {
				continue
			}
			planning.plans[index].adopted.snapshot.PlanFingerprint = fingerprint
		}
	}
	return planning, nil
}

func (s *namespaceImportServiceImpl) findAdoptedOwnershipConflicts(
	ctx context.Context,
	namespace string,
	roots []*adoptedRoot,
	membership map[string]*adoptedMembership,
	mappings []apisv1.ImportNamespaceApplicationMapping,
) (map[string][]string, error) {
	if s.AppRepo == nil || s.ComponentRepo == nil {
		return nil, fmt.Errorf("adopted ownership repositories are not initialized")
	}
	rootsByUID := make(map[string][]*adoptedRoot)
	for _, root := range roots {
		if root == nil || root.resource == nil || root.resource.object == nil {
			continue
		}
		uid := strings.TrimSpace(string(root.resource.object.GetUID()))
		if uid != "" {
			rootsByUID[uid] = append(rootsByUID[uid], root)
		}
	}
	if len(rootsByUID) == 0 && len(membership) == 0 {
		return nil, nil
	}
	allowedOwnerByPlan := make(map[string]string, len(mappings))
	for _, mapping := range mappings {
		allowedOwnerByPlan[adoptedPlanKey(mapping)] = strings.TrimSpace(mapping.TargetAppID)
	}
	plannedDependencies := make(map[string]*adoptedMembership, len(membership))
	for _, member := range membership {
		if member == nil || member.resource == nil {
			continue
		}
		plannedDependencies[adoptedOwnershipIdentityKey(
			member.resource.kind,
			member.resource.namespace,
			member.resource.name,
		)] = member
	}
	apps, err := s.AppRepo.List(ctx, datastore.ListOptions{Page: 0, PageSize: 0})
	if err != nil {
		return nil, fmt.Errorf("list applications for adopted UID ownership: %w", err)
	}
	sort.SliceStable(apps, func(i, j int) bool {
		if apps[i] == nil {
			return false
		}
		if apps[j] == nil {
			return true
		}
		return apps[i].ID < apps[j].ID
	})
	conflicts := make(map[string][]string)
	targetNamespace := applicationservice.PickNamespace(strings.TrimSpace(namespace), config.DefaultNamespace)
	blockUnverifiableSnapshot := func(appID string, snapshotErr error) {
		for planKey, allowedOwner := range allowedOwnerByPlan {
			if allowedOwner == appID {
				continue
			}
			conflicts[planKey] = append(
				conflicts[planKey],
				fmt.Sprintf("cannot verify adopted ownership for app %s: %v", appID, snapshotErr),
			)
		}
	}
	for _, app := range apps {
		if app == nil || strings.TrimSpace(app.ID) == "" {
			continue
		}
		appNamespace := applicationservice.PickNamespace(strings.TrimSpace(app.Namespace), config.DefaultNamespace)
		if appNamespace != targetNamespace {
			continue
		}
		components, err := s.ComponentRepo.FindByAppID(ctx, app.ID)
		if err != nil {
			return nil, fmt.Errorf("list components for adopted UID ownership app %q: %w", app.ID, err)
		}
		sort.SliceStable(components, func(i, j int) bool {
			if components[i] == nil {
				return false
			}
			if components[j] == nil {
				return true
			}
			return components[i].Name < components[j].Name
		})
		for _, component := range components {
			if component == nil || component.SourceWorkloadUID == nil {
				continue
			}
			uid := strings.TrimSpace(*component.SourceWorkloadUID)
			for _, root := range rootsByUID[uid] {
				if root == nil || allowedOwnerByPlan[root.planKey] == app.ID {
					continue
				}
				conflicts[root.planKey] = append(
					conflicts[root.planKey],
					fmt.Sprintf(
						"workload %s/%s UID %s is already adopted by app %s component %s",
						root.resource.kind,
						root.resource.name,
						uid,
						app.ID,
						component.Name,
					),
				)
			}
		}
		if app.EffectiveManagementMode() != config.ManagementModeAdopted {
			continue
		}
		snapshot, err := decodeAdoptionSnapshot(app.AdoptionSnapshot)
		if err != nil {
			blockUnverifiableSnapshot(app.ID, err)
			continue
		}
		if err := snapshot.Validate(); err != nil {
			blockUnverifiableSnapshot(app.ID, err)
			continue
		}
		for _, persisted := range snapshot.Resources {
			if persisted.Ownership != adoption.OwnershipExclusive ||
				persisted.Disposition != adoption.DispositionManaged {
				continue
			}
			persistedNamespace := strings.TrimSpace(persisted.Source.Namespace)
			if persistedNamespace == "" {
				persistedNamespace = strings.TrimSpace(snapshot.Namespace)
			}
			member := plannedDependencies[adoptedOwnershipIdentityKey(
				persisted.Source.Kind,
				persistedNamespace,
				persisted.Source.Name,
			)]
			if member == nil || member.resource == nil {
				continue
			}
			for planKey := range member.appComponents {
				if allowedOwnerByPlan[planKey] == app.ID {
					continue
				}
				conflicts[planKey] = append(
					conflicts[planKey],
					fmt.Sprintf(
						"resource %s/%s is already managed exclusively by adopted app %s (snapshot UID %s)",
						member.resource.kind,
						member.resource.name,
						app.ID,
						strings.TrimSpace(persisted.Source.UID),
					),
				)
			}
		}
	}
	for planKey := range conflicts {
		conflicts[planKey] = uniqueSortedStrings(conflicts[planKey])
	}
	return conflicts, nil
}

func adoptedOwnershipIdentityKey(kind, namespace, name string) string {
	return strings.ToLower(strings.TrimSpace(kind)) + "/" +
		strings.TrimSpace(namespace) + "/" +
		strings.TrimSpace(name)
}

func buildAdoptedRoots(
	namespace string,
	mappings []apisv1.ImportNamespaceApplicationMapping,
	resourceByWorkload map[string]*importResource,
	existingAppIDMap map[string]string,
	existingAppIDSet map[string]struct{},
	existingAppNameByID map[string]string,
) ([]*adoptedRoot, map[string]importAppPlan, []string, error) {
	roots := make([]*adoptedRoot, 0)
	plans := make(map[string]importAppPlan, len(mappings))
	seenApps := make(map[string]struct{}, len(mappings))
	seenWorkloads := make(map[string]struct{})
	var warnings []string

	for _, mapping := range mappings {
		mapping.Name = strings.TrimSpace(mapping.Name)
		mapping.Alias = strings.TrimSpace(mapping.Alias)
		mapping.TargetAppID = strings.TrimSpace(mapping.TargetAppID)
		if mapping.Name == "" || len(mapping.Components) == 0 {
			return nil, nil, nil, bcode.ErrApplicationConfig
		}
		appNameKey := appNameNamespaceKey(mapping.Name, namespace)
		if _, exists := seenApps[appNameKey]; exists {
			return nil, nil, nil, bcode.ErrApplicationConfig
		}
		seenApps[appNameKey] = struct{}{}
		if mapping.TargetAppID != "" {
			if _, exists := existingAppIDSet[mapping.TargetAppID]; !exists {
				return nil, nil, nil, fmt.Errorf("%w: target app %q does not exist", bcode.ErrAdoptedResourceConflict, mapping.TargetAppID)
			}
			if existingName := strings.TrimSpace(existingAppNameByID[mapping.TargetAppID]); !strings.EqualFold(existingName, mapping.Name) {
				return nil, nil, nil, fmt.Errorf(
					"%w: target app %q is named %q, not %q",
					bcode.ErrAdoptedResourceConflict,
					mapping.TargetAppID,
					existingName,
					mapping.Name,
				)
			}
		} else if existingID := strings.TrimSpace(existingAppIDMap[appNameKey]); existingID != "" {
			return nil, nil, nil, fmt.Errorf(
				"%w: application %q already exists as %q; targetAppId is required",
				bcode.ErrAdoptedResourceConflict,
				mapping.Name,
				existingID,
			)
		}

		planKey := adoptedPlanKey(mapping)
		componentNames := make(map[string]struct{}, len(mapping.Components))
		for _, component := range mapping.Components {
			component.Name = strings.TrimSpace(component.Name)
			apiVersion := strings.TrimSpace(component.Workload.APIVersion)
			kind := canonicalAdoptedWorkloadKind(component.Workload.Kind)
			name := strings.TrimSpace(component.Workload.Name)
			if component.Name == "" || apiVersion != appsv1.SchemeGroupVersion.String() || kind == "" || name == "" {
				return nil, nil, nil, bcode.ErrApplicationConfig
			}
			componentNameKey := strings.ToLower(component.Name)
			if _, exists := componentNames[componentNameKey]; exists {
				return nil, nil, nil, bcode.ErrApplicationConfig
			}
			componentNames[componentNameKey] = struct{}{}

			workloadKey := adoptedWorkloadIdentityKey(apiVersion, kind, name)
			if _, exists := seenWorkloads[workloadKey]; exists {
				return nil, nil, nil, fmt.Errorf("%w: workload %s is mapped more than once", bcode.ErrAdoptedResourceConflict, workloadKey)
			}
			seenWorkloads[workloadKey] = struct{}{}
			resource := resourceByWorkload[workloadKey]
			if resource == nil {
				return nil, nil, nil, fmt.Errorf("%w: workload %s was not found in namespace %s", bcode.ErrAdoptedResourceConflict, workloadKey, namespace)
			}
			resource.appID = planKey
			resource.componentName = component.Name
			resource.explicitAppID = true
			ref, err := buildWorkloadRef(resource)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("extract adopted workload %s dependencies: %w", workloadKey, err)
			}
			roots = append(roots, &adoptedRoot{
				planKey:       planKey,
				componentName: component.Name,
				resource:      resource,
				ref:           ref,
				serviceNames:  make(map[string]struct{}),
			})
		}

		plans[planKey] = importAppPlan{
			appID: planKey,
			name:  mapping.Name,
			alias: mapping.Alias,
		}
	}
	return roots, plans, warnings, nil
}

func canonicalAdoptedWorkloadKind(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "deployment", "deployments":
		return string(domainspec.KubeKindDeployment)
	case "statefulset", "statefulsets":
		return string(domainspec.KubeKindStatefulSet)
	default:
		return ""
	}
}

func adoptedWorkloadIdentityKey(apiVersion, kind, name string) string {
	return strings.ToLower(strings.TrimSpace(apiVersion)) + "/" +
		strings.ToLower(strings.TrimSpace(kind)) + "/" +
		strings.TrimSpace(name)
}

func adoptedPlanKey(mapping apisv1.ImportNamespaceApplicationMapping) string {
	if target := strings.TrimSpace(mapping.TargetAppID); target != "" {
		return target
	}
	name := strings.ToLower(strings.TrimSpace(mapping.Name))
	return boundedRFC1123LabelValue("adopted-" + name + "-" + stableHashSuffix(name, 8))
}

func indexAdoptedWorkloads(resources []*importResource) map[string]*importResource {
	result := make(map[string]*importResource)
	for _, resource := range resources {
		if resource == nil || resource.object == nil {
			continue
		}
		switch resource.kindKey {
		case importKindDeployments, importKindStatefulSets:
		default:
			continue
		}
		key := adoptedWorkloadIdentityKey(resource.object.GetAPIVersion(), resource.kind, resource.name)
		result[key] = resource
	}
	return result
}

func indexAdoptedResources(resources []*importResource) map[string]*importResource {
	result := make(map[string]*importResource, len(resources))
	for _, resource := range resources {
		if resource == nil {
			continue
		}
		result[adoptedResourceLookupKey(resource.kindKey, resource.name)] = resource
	}
	return result
}

func adoptedResourceLookupKey(kindKey, name string) string {
	return strings.ToLower(strings.TrimSpace(kindKey)) + "/" + strings.TrimSpace(name)
}

func addAdoptedMembership(members map[string]*adoptedMembership, resource *importResource, planKey, componentName string) {
	if resource == nil {
		return
	}
	key := resourceResultKey(resource)
	member := members[key]
	if member == nil {
		member = &adoptedMembership{
			resource:          resource,
			appComponents:     make(map[string]map[string]struct{}),
			externalWorkloads: make(map[string]struct{}),
		}
		members[key] = member
	}
	if member.appComponents[planKey] == nil {
		member.appComponents[planKey] = make(map[string]struct{})
	}
	if componentName = strings.TrimSpace(componentName); componentName != "" {
		member.appComponents[planKey][componentName] = struct{}{}
	}
}

func collectAdoptedRootDependencies(
	root *adoptedRoot,
	resources []*importResource,
	resourceByKindAndName map[string]*importResource,
	membership map[string]*adoptedMembership,
) {
	if root == nil || root.resource == nil {
		return
	}
	addNamed := func(kindKey string, names map[string]struct{}) {
		for name := range names {
			addAdoptedMembership(
				membership,
				resourceByKindAndName[adoptedResourceLookupKey(kindKey, name)],
				root.planKey,
				root.componentName,
			)
		}
	}
	addNamed(importKindConfigMaps, root.ref.configMaps)
	addNamed(importKindSecrets, root.ref.secrets)
	addNamed(importKindPersistentVolumeClaims, root.ref.pvcs)
	if root.ref.serviceAccount != "" && root.ref.serviceAccount != "default" {
		serviceAccountResource := resourceByKindAndName[adoptedResourceLookupKey(
			importKindServiceAccounts,
			root.ref.serviceAccount,
		)]
		addAdoptedMembership(
			membership,
			serviceAccountResource,
			root.planKey,
			root.componentName,
		)
		if serviceAccountResource != nil && serviceAccountResource.object != nil {
			var serviceAccount corev1.ServiceAccount
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
				serviceAccountResource.object.Object,
				&serviceAccount,
			); err == nil {
				for _, secretName := range adoptedServiceAccountSecretNames(&serviceAccount) {
					addAdoptedMembership(
						membership,
						resourceByKindAndName[adoptedResourceLookupKey(importKindSecrets, secretName)],
						root.planKey,
						root.componentName,
					)
				}
			}
		}
	}
	for _, resource := range resources {
		if resource == nil {
			continue
		}
		if resource.kindKey == importKindPersistentVolumeClaims {
			for prefix := range root.ref.pvcPrefixes {
				if strings.HasPrefix(resource.name, prefix) {
					addAdoptedMembership(membership, resource, root.planKey, root.componentName)
				}
			}
			continue
		}
		if resource.kindKey != importKindServices {
			continue
		}
		if adoptedServiceSelectsRoot(resource, root) || adoptedStatefulSetServiceName(root.resource) == resource.name {
			root.serviceNames[resource.name] = struct{}{}
			addAdoptedMembership(membership, resource, root.planKey, root.componentName)
		}
	}
}

func adoptedServiceSelectsRoot(resource *importResource, root *adoptedRoot) bool {
	if resource == nil || resource.object == nil || root == nil {
		return false
	}
	var service corev1.Service
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &service); err != nil {
		return false
	}
	return selectorMatch(service.Spec.Selector, root.ref.labels)
}

func adoptedStatefulSetServiceName(resource *importResource) string {
	if resource == nil || resource.object == nil || resource.kindKey != importKindStatefulSets {
		return ""
	}
	var statefulSet appsv1.StatefulSet
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &statefulSet); err != nil {
		return ""
	}
	return strings.TrimSpace(statefulSet.Spec.ServiceName)
}

func collectAdoptedIngressDependencies(
	roots []*adoptedRoot,
	resources []*importResource,
	resourceByKindAndName map[string]*importResource,
	membership map[string]*adoptedMembership,
) {
	for _, resource := range resources {
		if resource == nil || resource.kindKey != importKindIngresses || resource.object == nil {
			continue
		}
		var ingress networkingv1.Ingress
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &ingress); err != nil {
			continue
		}
		backends := ingressBackendServiceNames(&ingress)
		for _, root := range roots {
			for _, serviceName := range backends {
				if _, selected := root.serviceNames[serviceName]; selected {
					addAdoptedMembership(membership, resource, root.planKey, root.componentName)
					for _, tls := range ingress.Spec.TLS {
						secretName := strings.TrimSpace(tls.SecretName)
						if secretName == "" {
							continue
						}
						addAdoptedMembership(
							membership,
							resourceByKindAndName[adoptedResourceLookupKey(importKindSecrets, secretName)],
							root.planKey,
							root.componentName,
						)
					}
					break
				}
			}
		}
	}
}

func adoptedServiceAccountSecretNames(serviceAccount *corev1.ServiceAccount) []string {
	if serviceAccount == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(serviceAccount.Secrets)+len(serviceAccount.ImagePullSecrets))
	for _, reference := range serviceAccount.Secrets {
		if name := strings.TrimSpace(reference.Name); name != "" {
			seen[name] = struct{}{}
		}
	}
	for _, reference := range serviceAccount.ImagePullSecrets {
		if name := strings.TrimSpace(reference.Name); name != "" {
			seen[name] = struct{}{}
		}
	}
	return sortedStringMapKeys(seen)
}

func collectAdoptedRBACDependencies(
	namespace string,
	roots []*adoptedRoot,
	resources []*importResource,
	resourceByKindAndName map[string]*importResource,
	membership map[string]*adoptedMembership,
) {
	for _, root := range roots {
		serviceAccount := strings.TrimSpace(root.ref.serviceAccount)
		if serviceAccount == "" || serviceAccount == "default" {
			continue
		}
		for _, resource := range resources {
			if resource == nil || resource.object == nil {
				continue
			}
			switch resource.kindKey {
			case importKindRoleBindings:
				var binding rbacv1.RoleBinding
				if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &binding); err != nil ||
					!roleBindingReferencesServiceAccount(&binding, namespace, serviceAccount) {
					continue
				}
				addAdoptedMembership(membership, resource, root.planKey, root.componentName)
				switch strings.ToLower(strings.TrimSpace(binding.RoleRef.Kind)) {
				case "role":
					addAdoptedMembership(
						membership,
						resourceByKindAndName[adoptedResourceLookupKey(importKindRoles, binding.RoleRef.Name)],
						root.planKey,
						root.componentName,
					)
				case "clusterrole":
					addAdoptedMembership(
						membership,
						resourceByKindAndName[adoptedResourceLookupKey(importKindClusterRoles, binding.RoleRef.Name)],
						root.planKey,
						root.componentName,
					)
				}
			case importKindClusterRoleBindings:
				var binding rbacv1.ClusterRoleBinding
				if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &binding); err != nil ||
					!clusterRoleBindingReferencesServiceAccount(&binding, namespace, serviceAccount) {
					continue
				}
				addAdoptedMembership(membership, resource, root.planKey, root.componentName)
				if strings.EqualFold(strings.TrimSpace(binding.RoleRef.Kind), "ClusterRole") {
					addAdoptedMembership(
						membership,
						resourceByKindAndName[adoptedResourceLookupKey(importKindClusterRoles, binding.RoleRef.Name)],
						root.planKey,
						root.componentName,
					)
				}
			}
		}
	}
}

func roleBindingReferencesServiceAccount(binding *rbacv1.RoleBinding, namespace, name string) bool {
	if binding == nil {
		return false
	}
	for _, subject := range binding.Subjects {
		if subject.Kind != rbacv1.ServiceAccountKind || subject.Name != name {
			continue
		}
		if subject.Namespace == "" || subject.Namespace == namespace {
			return true
		}
	}
	return false
}

func clusterRoleBindingReferencesServiceAccount(binding *rbacv1.ClusterRoleBinding, namespace, name string) bool {
	if binding == nil {
		return false
	}
	for _, subject := range binding.Subjects {
		if subject.Kind == rbacv1.ServiceAccountKind && subject.Name == name && subject.Namespace == namespace {
			return true
		}
	}
	return false
}

func collectAdoptedPolicyDependencies(
	roots []*adoptedRoot,
	resources []*importResource,
	membership map[string]*adoptedMembership,
) {
	for _, resource := range resources {
		if resource == nil || resource.object == nil {
			continue
		}
		switch resource.kindKey {
		case importKindPodDisruptionBudgets, importKindNetworkPolicies:
		default:
			continue
		}
		for _, root := range roots {
			if root == nil || !adoptedPolicySelectsLabels(resource, root.ref.labels) {
				continue
			}
			addAdoptedMembership(membership, resource, root.planKey, root.componentName)
		}
	}
}

func adoptedPolicySelectsLabels(resource *importResource, podLabels map[string]string) bool {
	if resource == nil || resource.object == nil {
		return false
	}
	var selector *metav1.LabelSelector
	switch resource.kindKey {
	case importKindPodDisruptionBudgets:
		var pdb policyv1.PodDisruptionBudget
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &pdb); err != nil ||
			pdb.Spec.Selector == nil {
			return false
		}
		selector = pdb.Spec.Selector
	case importKindNetworkPolicies:
		var policy networkingv1.NetworkPolicy
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &policy); err != nil {
			return false
		}
		selector = &policy.Spec.PodSelector
	default:
		return false
	}
	compiled, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return false
	}
	return compiled.Matches(klabels.Set(podLabels))
}

func collectAdoptedExternalWorkloadUsage(
	roots []*adoptedRoot,
	resources []*importResource,
	membership map[string]*adoptedMembership,
) []string {
	rootKeys := make(map[string]struct{}, len(roots))
	for _, root := range roots {
		if root != nil && root.resource != nil {
			rootKeys[resourceResultKey(root.resource)] = struct{}{}
		}
	}

	var warnings []string
	for _, workload := range resources {
		if workload == nil || workload.object == nil || !isImportWorkloadKind(workload.kindKey) {
			continue
		}
		if _, mapped := rootKeys[resourceResultKey(workload)]; mapped {
			continue
		}
		ref, err := buildWorkloadRef(workload)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf(
				"cannot inspect external workload %s/%s references: %v",
				workload.kind,
				workload.name,
				err,
			))
			continue
		}
		workloadIdentity := strings.TrimSpace(workload.kind) + "/" + strings.TrimSpace(workload.name)
		warnings = append(
			warnings,
			collectAdoptedExternalUsageForRef(workloadIdentity, workload, ref, membership)...,
		)
	}
	return uniqueSortedStrings(warnings)
}

func (s *namespaceImportServiceImpl) collectAdoptedExternalPodAndReplicaSetUsage(
	ctx context.Context,
	namespace string,
	roots []*adoptedRoot,
	membership map[string]*adoptedMembership,
) ([]string, map[string][]string, error) {
	replicaSets, err := s.KubeClient.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list replicasets for adopted external-use scan: %w", err)
	}

	labelConflicts := make(map[string][]string)
	for _, root := range roots {
		if root == nil || root.resource == nil {
			continue
		}
		appendAdoptedPodLabelConflict(
			labelConflicts,
			root,
			strings.TrimSpace(root.resource.kind)+"/"+strings.TrimSpace(root.resource.name)+" PodTemplate",
			root.ref.labels,
		)
	}

	targetReplicaSetRoots := make(map[string]*adoptedRoot)
	var warnings []string
	for index := range replicaSets.Items {
		replicaSet := &replicaSets.Items[index]
		if root := adoptedControllingRoot(replicaSet, roots, importKindDeployments); root != nil {
			if uid := strings.TrimSpace(string(replicaSet.UID)); uid != "" {
				targetReplicaSetRoots[uid] = root
			}
			appendAdoptedPodLabelConflict(
				labelConflicts,
				root,
				"ReplicaSet/"+strings.TrimSpace(replicaSet.Name)+" PodTemplate",
				replicaSet.Spec.Template.Labels,
			)
			continue
		}
		ref := adoptedPodSpecWorkloadRef(replicaSet.Spec.Template.Labels, &replicaSet.Spec.Template.Spec)
		warnings = append(
			warnings,
			collectAdoptedExternalUsageForRef(
				"ReplicaSet/"+strings.TrimSpace(replicaSet.Name),
				nil,
				ref,
				membership,
			)...,
		)
	}

	pods, err := s.KubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list pods for adopted external-use scan: %w", err)
	}
	for index := range pods.Items {
		pod := &pods.Items[index]
		root := adoptedControllingRoot(pod, roots, importKindDeployments, importKindStatefulSets)
		if root == nil {
			root = adoptedRootControlledByUID(pod, targetReplicaSetRoots)
		}
		if root != nil {
			appendAdoptedPodLabelConflict(
				labelConflicts,
				root,
				"Pod/"+strings.TrimSpace(pod.Name),
				pod.Labels,
			)
			continue
		}
		ref := adoptedPodSpecWorkloadRef(pod.Labels, &pod.Spec)
		warnings = append(
			warnings,
			collectAdoptedExternalUsageForRef(
				"Pod/"+strings.TrimSpace(pod.Name),
				nil,
				ref,
				membership,
			)...,
		)
	}
	for planKey, reasons := range labelConflicts {
		labelConflicts[planKey] = uniqueSortedStrings(reasons)
	}
	return uniqueSortedStrings(warnings), labelConflicts, nil
}

func appendAdoptedPodLabelConflict(
	conflicts map[string][]string,
	root *adoptedRoot,
	object string,
	labels map[string]string,
) {
	if root == nil || len(labels) == 0 {
		return
	}
	expected := map[string]string{
		config.LabelAppID:         strings.TrimSpace(root.planKey),
		config.LabelComponentName: boundedRFC1123LabelValue(root.componentName),
	}
	for _, key := range []string{config.LabelAppID, config.LabelComponentName} {
		value := strings.TrimSpace(labels[key])
		if value != "" && value != expected[key] {
			conflicts[root.planKey] = append(conflicts[root.planKey], fmt.Sprintf(
				"%s has conflicting managed label %s=%q; expected %q",
				object,
				key,
				value,
				expected[key],
			))
		}
	}
	componentID := strings.TrimSpace(labels[config.LabelComponentID])
	if componentID != "" &&
		(strings.TrimSpace(labels[config.LabelAppID]) == "" || strings.TrimSpace(labels[config.LabelComponentName]) == "") {
		conflicts[root.planKey] = append(conflicts[root.planKey], fmt.Sprintf(
			"%s has partial managed labels including %s=%q; remove the incomplete Eruun label set before adoption",
			object,
			config.LabelComponentID,
			componentID,
		))
	}
}

func adoptedObjectIsControlledByRoot(
	object metav1.Object,
	roots []*adoptedRoot,
	allowedKindKeys ...string,
) bool {
	return adoptedControllingRoot(object, roots, allowedKindKeys...) != nil
}

func adoptedControllingRoot(
	object metav1.Object,
	roots []*adoptedRoot,
	allowedKindKeys ...string,
) *adoptedRoot {
	if object == nil {
		return nil
	}
	for _, root := range roots {
		if root == nil || root.resource == nil || root.resource.object == nil {
			continue
		}
		kindAllowed := false
		for _, kindKey := range allowedKindKeys {
			if root.resource.kindKey == kindKey {
				kindAllowed = true
				break
			}
		}
		if !kindAllowed {
			continue
		}
		if strings.TrimSpace(string(root.resource.object.GetUID())) == "" {
			continue
		}
		if metav1.IsControlledBy(object, root.resource.object) {
			return root
		}
	}
	return nil
}

func adoptedRootControlledByUID(object metav1.Object, controllerRoots map[string]*adoptedRoot) *adoptedRoot {
	if object == nil || len(controllerRoots) == 0 {
		return nil
	}
	controller := metav1.GetControllerOf(object)
	if controller == nil {
		return nil
	}
	return controllerRoots[strings.TrimSpace(string(controller.UID))]
}

func adoptedPodSpecWorkloadRef(labels map[string]string, podSpec *corev1.PodSpec) workloadRef {
	ref := workloadRef{
		labels:      copyStringMap(labels),
		configMaps:  make(map[string]struct{}),
		pvcs:        make(map[string]struct{}),
		pvcPrefixes: make(map[string]struct{}),
		secrets:     make(map[string]struct{}),
	}
	if podSpec != nil {
		ref.serviceAccount = strings.TrimSpace(podSpec.ServiceAccountName)
		collectPodSpecReferences(podSpec, ref.configMaps, ref.pvcs, ref.secrets)
	}
	if ref.serviceAccount == "" {
		ref.serviceAccount = "default"
	}
	return ref
}

func collectAdoptedExternalUsageForRef(
	workloadIdentity string,
	workload *importResource,
	ref workloadRef,
	membership map[string]*adoptedMembership,
) []string {
	var warnings []string
	for _, member := range membership {
		if member == nil || member.resource == nil ||
			!externalWorkloadUsesAdoptedResource(workload, ref, member.resource) {
			continue
		}
		addAdoptedExternalUse(member, workloadIdentity)
		warnings = append(warnings, fmt.Sprintf(
			"resource %s/%s is also used by target-external workload %s and will be preserved as shared",
			member.resource.kind,
			member.resource.name,
			workloadIdentity,
		))
	}
	return warnings
}

func externalWorkloadUsesAdoptedResource(
	workload *importResource,
	ref workloadRef,
	dependency *importResource,
) bool {
	if dependency == nil {
		return false
	}
	switch dependency.kindKey {
	case importKindConfigMaps:
		_, found := ref.configMaps[dependency.name]
		return found
	case importKindSecrets:
		_, found := ref.secrets[dependency.name]
		return found
	case importKindPersistentVolumeClaims:
		if _, found := ref.pvcs[dependency.name]; found {
			return true
		}
		for prefix := range ref.pvcPrefixes {
			if strings.HasPrefix(dependency.name, prefix) {
				return true
			}
		}
		return false
	case importKindServiceAccounts:
		return ref.serviceAccount != "" &&
			ref.serviceAccount != "default" &&
			ref.serviceAccount == dependency.name
	case importKindServices:
		return adoptedServiceSelectsLabels(dependency, ref.labels) ||
			(workload != nil && adoptedStatefulSetServiceName(workload) == dependency.name)
	case importKindPodDisruptionBudgets, importKindNetworkPolicies:
		return adoptedPolicySelectsLabels(dependency, ref.labels)
	default:
		return false
	}
}

func adoptedServiceSelectsLabels(resource *importResource, podLabels map[string]string) bool {
	if resource == nil || resource.object == nil {
		return false
	}
	var service corev1.Service
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &service); err != nil {
		return false
	}
	return selectorMatch(service.Spec.Selector, podLabels)
}

func addAdoptedExternalUse(member *adoptedMembership, identity string) {
	if member == nil {
		return
	}
	if member.externalWorkloads == nil {
		member.externalWorkloads = make(map[string]struct{})
	}
	if identity = strings.TrimSpace(identity); identity != "" {
		member.externalWorkloads[identity] = struct{}{}
	}
}

func propagateAdoptedSharedDependencies(
	namespace string,
	resources []*importResource,
	membership map[string]*adoptedMembership,
) {
	resourceByKindAndName := indexAdoptedResources(resources)
	propagateAdoptedIngressSharing(resourceByKindAndName, membership)
	propagateAdoptedIngressSecretSharing(resources, resourceByKindAndName, membership)
	propagateAdoptedRBACSharing(namespace, resources, resourceByKindAndName, membership)
	propagateAdoptedServiceAccountSecretSharing(resourceByKindAndName, membership)
}

func propagateAdoptedIngressSharing(
	resourceByKindAndName map[string]*importResource,
	membership map[string]*adoptedMembership,
) {
	for _, member := range membership {
		if member == nil || member.resource == nil ||
			member.resource.kindKey != importKindIngresses ||
			member.resource.object == nil {
			continue
		}
		var ingress networkingv1.Ingress
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(member.resource.object.Object, &ingress); err != nil {
			continue
		}
		for _, serviceName := range ingressBackendServiceNames(&ingress) {
			service := resourceByKindAndName[adoptedResourceLookupKey(importKindServices, serviceName)]
			serviceMember := membership[resourceResultKey(service)]
			if service == nil || serviceMember == nil {
				addAdoptedExternalUse(member, "external-service/"+serviceName)
				continue
			}
			if adoptedMembershipRequiresSharedPreservation(serviceMember) {
				addAdoptedExternalUse(member, "shared-service/"+serviceName)
			}
		}
	}
}

func propagateAdoptedIngressSecretSharing(
	resources []*importResource,
	resourceByKindAndName map[string]*importResource,
	membership map[string]*adoptedMembership,
) {
	for _, resource := range resources {
		if resource == nil || resource.kindKey != importKindIngresses || resource.object == nil {
			continue
		}
		var ingress networkingv1.Ingress
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &ingress); err != nil {
			continue
		}
		ingressMember := membership[resourceResultKey(resource)]
		externalIngress := ingressMember == nil
		sharedIngress := adoptedMembershipRequiresSharedPreservation(ingressMember)
		if !externalIngress && !sharedIngress {
			continue
		}
		for _, tls := range ingress.Spec.TLS {
			secretName := strings.TrimSpace(tls.SecretName)
			if secretName == "" {
				continue
			}
			secret := resourceByKindAndName[adoptedResourceLookupKey(importKindSecrets, secretName)]
			secretMember := membership[resourceResultKey(secret)]
			if secretMember == nil {
				continue
			}
			if externalIngress {
				addAdoptedExternalUse(secretMember, "external-ingress/"+resource.name)
			} else {
				addAdoptedExternalUse(secretMember, "shared-ingress/"+resource.name)
			}
		}
	}
}

func propagateAdoptedServiceAccountSecretSharing(
	resourceByKindAndName map[string]*importResource,
	membership map[string]*adoptedMembership,
) {
	for _, member := range membership {
		if member == nil || member.resource == nil ||
			member.resource.kindKey != importKindServiceAccounts ||
			member.resource.object == nil ||
			!adoptedMembershipRequiresSharedPreservation(member) {
			continue
		}
		var serviceAccount corev1.ServiceAccount
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
			member.resource.object.Object,
			&serviceAccount,
		); err != nil {
			continue
		}
		for _, secretName := range adoptedServiceAccountSecretNames(&serviceAccount) {
			secret := resourceByKindAndName[adoptedResourceLookupKey(importKindSecrets, secretName)]
			secretMember := membership[resourceResultKey(secret)]
			if secretMember != nil {
				addAdoptedExternalUse(secretMember, "shared-service-account/"+member.resource.name)
			}
		}
	}
}

func propagateAdoptedRBACSharing(
	namespace string,
	resources []*importResource,
	resourceByKindAndName map[string]*importResource,
	membership map[string]*adoptedMembership,
) {
	selectedServiceAccounts := make(map[string]*adoptedMembership)
	for _, member := range membership {
		if member != nil && member.resource != nil && member.resource.kindKey == importKindServiceAccounts {
			selectedServiceAccounts[member.resource.name] = member
		}
	}

	// ClusterRoleBindings are always external to the adopted ownership
	// boundary. Collect their ServiceAccount references before classifying
	// namespaced RoleBindings so the result cannot depend on map iteration.
	for _, resource := range resources {
		if resource == nil || resource.kindKey != importKindClusterRoleBindings || resource.object == nil {
			continue
		}
		var binding rbacv1.ClusterRoleBinding
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &binding); err != nil {
			continue
		}
		for _, subject := range binding.Subjects {
			if subject.Kind != rbacv1.ServiceAccountKind ||
				strings.TrimSpace(subject.Namespace) != namespace {
				continue
			}
			serviceAccountMember := selectedServiceAccounts[strings.TrimSpace(subject.Name)]
			if serviceAccountMember != nil {
				addAdoptedExternalUse(
					serviceAccountMember,
					"cluster-role-binding/"+resource.name,
				)
			}
		}
		if member := membership[resourceResultKey(resource)]; member != nil &&
			adoptedBindingHasExternalSubject(namespace, binding.Subjects, selectedServiceAccounts) {
			addAdoptedExternalUse(member, "external-rbac-subject")
		}
	}

	for _, member := range membership {
		if member == nil || member.resource == nil ||
			member.resource.kindKey != importKindRoleBindings ||
			member.resource.object == nil {
			continue
		}
		var binding rbacv1.RoleBinding
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(member.resource.object.Object, &binding); err != nil {
			continue
		}
		if adoptedBindingHasExternalSubject(namespace, binding.Subjects, selectedServiceAccounts) {
			addAdoptedExternalUse(member, "external-rbac-subject")
		}
	}

	for _, resource := range resources {
		if resource == nil || resource.object == nil {
			continue
		}
		var roleKindKey string
		var roleName string
		switch resource.kindKey {
		case importKindRoleBindings:
			var binding rbacv1.RoleBinding
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &binding); err != nil {
				continue
			}
			roleName = binding.RoleRef.Name
			if strings.EqualFold(binding.RoleRef.Kind, "Role") {
				roleKindKey = importKindRoles
			} else if strings.EqualFold(binding.RoleRef.Kind, "ClusterRole") {
				roleKindKey = importKindClusterRoles
			}
		case importKindClusterRoleBindings:
			var binding rbacv1.ClusterRoleBinding
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &binding); err != nil {
				continue
			}
			if strings.EqualFold(binding.RoleRef.Kind, "ClusterRole") {
				roleKindKey = importKindClusterRoles
				roleName = binding.RoleRef.Name
			}
		default:
			continue
		}
		if roleKindKey == "" || strings.TrimSpace(roleName) == "" {
			continue
		}
		roleResource := resourceByKindAndName[adoptedResourceLookupKey(roleKindKey, roleName)]
		roleMember := membership[resourceResultKey(roleResource)]
		if roleMember == nil {
			continue
		}
		bindingMember := membership[resourceResultKey(resource)]
		if bindingMember == nil || adoptedMembershipRequiresSharedPreservation(bindingMember) {
			addAdoptedExternalUse(roleMember, "shared-binding/"+resource.name)
		}
	}
}

func adoptedBindingHasExternalSubject(
	namespace string,
	subjects []rbacv1.Subject,
	selectedServiceAccounts map[string]*adoptedMembership,
) bool {
	for _, subject := range subjects {
		if subject.Kind != rbacv1.ServiceAccountKind {
			return true
		}
		subjectNamespace := strings.TrimSpace(subject.Namespace)
		if subjectNamespace != "" && subjectNamespace != namespace {
			return true
		}
		member := selectedServiceAccounts[strings.TrimSpace(subject.Name)]
		if member == nil || adoptedMembershipRequiresSharedPreservation(member) {
			return true
		}
	}
	return false
}

func adoptedOwnerReferenceConflicts(membership map[string]*adoptedMembership) map[string][]string {
	managedByUID := make(map[string]*adoptedMembership, len(membership))
	for _, member := range membership {
		if adoptedMembershipRequiresSharedPreservation(member) ||
			member.resource == nil ||
			member.resource.object == nil ||
			adoptedDependencyRoleForResource(member.resource) == "" {
			continue
		}
		switch member.resource.kindKey {
		case importKindPersistentVolumeClaims, importKindClusterRoles, importKindClusterRoleBindings:
			continue
		}
		if uid := strings.TrimSpace(string(member.resource.object.GetUID())); uid != "" {
			managedByUID[uid] = member
		}
	}

	conflicts := make(map[string][]string)
	for _, member := range membership {
		if member == nil || member.resource == nil || member.resource.object == nil {
			continue
		}
		for _, owner := range member.resource.object.GetOwnerReferences() {
			managed := managedByUID[strings.TrimSpace(string(owner.UID))]
			if managed == nil || managed.resource == member.resource {
				continue
			}
			for planKey := range managed.appComponents {
				conflicts[planKey] = append(
					conflicts[planKey],
					fmt.Sprintf(
						"resource %s/%s has ownerReference to adopted managed resource %s/%s UID %s; remove or rebind the ownerReference before import",
						member.resource.kind,
						member.resource.name,
						managed.resource.kind,
						managed.resource.name,
						owner.UID,
					),
				)
			}
		}
	}
	for planKey := range conflicts {
		conflicts[planKey] = uniqueSortedStrings(conflicts[planKey])
	}
	return conflicts
}

func adoptedMembershipRequiresSharedPreservation(member *adoptedMembership) bool {
	if member == nil || len(member.externalWorkloads) > 0 || len(member.appComponents) > 1 {
		return true
	}
	return member.resource != nil &&
		(member.resource.kindKey == importKindClusterRoles ||
			member.resource.kindKey == importKindClusterRoleBindings)
}

func assignAdoptedResourceSemantics(
	membership map[string]*adoptedMembership,
	blocked map[string][]string,
) {
	for _, member := range membership {
		resource := member.resource
		resource.dependencyRole = adoptedDependencyRoleForResource(resource)
		// Cluster-scoped RBAC is reported as part of the dependency graph, but
		// the first adopted-management contract deliberately never claims or
		// reconciles it. A cluster role can affect workloads in namespaces
		// outside the imported application even when the current scan sees only
		// one namespaced subject.
		if resource.kindKey == importKindClusterRoles ||
			resource.kindKey == importKindClusterRoleBindings {
			resource.ownership = adoption.OwnershipExternal
			resource.disposition = adoption.DispositionSharedPreserved
			continue
		}
		appKeys := sortedStringMapKeys(member.appComponents)
		if len(appKeys) != 1 {
			resource.ownership = adoption.OwnershipExternal
			resource.disposition = adoption.DispositionBlocked
			continue
		}
		planKey := appKeys[0]
		resource.appID = planKey
		resource.componentName = pickSingleComponent(member.appComponents[planKey], resource.componentName)
		if len(blocked[planKey]) > 0 {
			resource.ownership = adoption.OwnershipExternal
			resource.disposition = adoption.DispositionBlocked
			resource.dispositionErr = strings.Join(uniqueSortedStrings(blocked[planKey]), "; ")
			continue
		}
		if len(member.externalWorkloads) > 0 {
			resource.ownership = adoption.OwnershipShared
			resource.disposition = adoption.DispositionSharedPreserved
			continue
		}
		if resource.kindKey == importKindPersistentVolumeClaims {
			resource.ownership = adoption.OwnershipDataProtected
			resource.disposition = adoption.DispositionDataProtected
			continue
		}
		resource.ownership = adoption.OwnershipExclusive
		resource.disposition = adoption.DispositionManaged
	}
}

func markExcludedAdoptedResources(resources []*importResource, membership map[string]*adoptedMembership) []*importResource {
	excluded := make([]*importResource, 0)
	for _, resource := range resources {
		if resource == nil {
			continue
		}
		if _, selected := membership[resourceResultKey(resource)]; selected {
			continue
		}
		resource.appID = ""
		resource.componentName = ""
		resource.dependencyRole = adoptedDependencyRoleForResource(resource)
		resource.ownership = adoption.OwnershipExternal
		resource.disposition = adoption.DispositionExcluded
		excluded = append(excluded, resource)
	}
	return sortedImportPlanResources(excluded)
}

func adoptedDependencyRoleForResource(resource *importResource) string {
	if resource == nil {
		return ""
	}
	switch resource.kindKey {
	case importKindDeployments, importKindStatefulSets:
		return adoptedDependencyRoleWorkload
	case importKindServices:
		return adoptedDependencyRoleService
	case importKindIngresses:
		return adoptedDependencyRoleIngress
	case importKindPersistentVolumeClaims:
		return adoptedDependencyRolePVC
	case importKindConfigMaps:
		return adoptedDependencyRoleConfigMap
	case importKindSecrets:
		return adoptedDependencyRoleSecret
	case importKindServiceAccounts:
		return adoptedDependencyRoleServiceAccount
	case importKindRoles, importKindRoleBindings, importKindClusterRoles, importKindClusterRoleBindings:
		return adoptedDependencyRoleRBAC
	case importKindPodDisruptionBudgets:
		return adoptedDependencyRolePDB
	case importKindNetworkPolicies:
		return adoptedDependencyRoleNetworkPolicy
	default:
		return ""
	}
}

func adoptedPlanResources(planKey string, membership map[string]*adoptedMembership) []*importResource {
	resources := make([]*importResource, 0)
	for _, member := range membership {
		if _, ok := member.appComponents[planKey]; ok {
			resources = append(resources, member.resource)
		}
	}
	return resources
}

func buildAdoptedComponents(
	mapping apisv1.ImportNamespaceApplicationMapping,
	roots []*adoptedRoot,
	resources []*importResource,
) ([]apisv1.CreateComponentRequest, []string, error) {
	planKey := adoptedPlanKey(mapping)
	rawObjects := make([]*unstructured.Unstructured, 0, len(resources))
	for _, resource := range resources {
		if resource == nil || resource.object == nil {
			continue
		}
		switch resource.kindKey {
		case importKindConfigMaps,
			importKindSecrets,
			importKindIngresses,
			importKindPodDisruptionBudgets,
			importKindNetworkPolicies,
			importKindPersistentVolumes:
			continue
		}
		rawObjects = append(rawObjects, resource.object.DeepCopy())
	}
	converted, warnings, err := convertKubeObjectsToComponents(rawObjects)
	if err != nil {
		return nil, warnings, fmt.Errorf("convert adopted application %s: %w", mapping.Name, err)
	}
	convertedBySourceName := make(map[string][]apisv1.CreateComponentRequest)
	for _, component := range converted {
		component.Properties.Secret = nil
		convertedBySourceName[component.Name] = append(convertedBySourceName[component.Name], component)
	}

	result := make([]apisv1.CreateComponentRequest, 0, len(mapping.Components))
	for _, componentMapping := range mapping.Components {
		var root *adoptedRoot
		for _, candidate := range roots {
			if candidate.planKey == planKey && candidate.componentName == componentMapping.Name {
				root = candidate
				break
			}
		}
		if root == nil {
			return nil, warnings, fmt.Errorf("mapped component %s source workload was not resolved", componentMapping.Name)
		}
		candidates := convertedBySourceName[root.resource.name]
		if len(candidates) != 1 {
			return nil, warnings, fmt.Errorf(
				"mapped workload %s/%s produced %d components",
				root.resource.kind,
				root.resource.name,
				len(candidates),
			)
		}
		component := candidates[0]
		component.Name = componentMapping.Name
		component.Replicas, err = adoptedSourceWorkloadReplicas(root.resource)
		if err != nil {
			return nil, warnings, fmt.Errorf(
				"read replicas from adopted workload %s/%s: %w",
				root.resource.kind,
				root.resource.name,
				err,
			)
		}
		component.Properties.Secret = nil
		attachAdoptedServiceTraits(&component, root, resources)
		attachAdoptedIngressTraits(&component, root, resources)
		result = append(result, component)
	}
	return result, warnings, nil
}

func adoptedSourceWorkloadReplicas(resource *importResource) (int32, error) {
	if resource == nil || resource.object == nil {
		return 0, fmt.Errorf("source workload is nil")
	}
	switch resource.kindKey {
	case importKindDeployments:
		var deployment appsv1.Deployment
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
			resource.object.Object,
			&deployment,
		); err != nil {
			return 0, err
		}
		if deployment.Spec.Replicas == nil {
			return 1, nil
		}
		return *deployment.Spec.Replicas, nil
	case importKindStatefulSets:
		var statefulSet appsv1.StatefulSet
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
			resource.object.Object,
			&statefulSet,
		); err != nil {
			return 0, err
		}
		if statefulSet.Spec.Replicas == nil {
			return 1, nil
		}
		return *statefulSet.Spec.Replicas, nil
	default:
		return 0, fmt.Errorf("unsupported adopted workload kind %s", resource.kind)
	}
}

func attachAdoptedServiceTraits(component *apisv1.CreateComponentRequest, root *adoptedRoot, resources []*importResource) {
	if component == nil || root == nil {
		return
	}
	existing := make(map[string]int, len(component.Traits.Service))
	for i, trait := range component.Traits.Service {
		existing[strings.TrimSpace(trait.Name)] = i
	}
	for _, resource := range resources {
		if resource == nil || resource.kindKey != importKindServices {
			continue
		}
		if _, selected := root.serviceNames[resource.name]; !selected {
			continue
		}
		var service corev1.Service
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &service); err != nil {
			continue
		}
		trait := domainspec.ServiceTraitSpec{
			Name:         service.Name,
			Type:         string(domainspec.ServiceAccessTypeFromKube(service.Spec.Type)),
			ExternalName: service.Spec.ExternalName,
			Headless:     service.Spec.ClusterIP == corev1.ClusterIPNone,
			Selector:     copyStringMap(service.Spec.Selector),
			Labels:       copyStringMap(service.Labels),
			Ports:        adoptedServicePorts(service.Spec.Ports),
		}
		if index, already := existing[resource.name]; already {
			component.Traits.Service[index] = trait
			continue
		}
		component.Traits.Service = append(component.Traits.Service, trait)
		existing[resource.name] = len(component.Traits.Service) - 1
	}
}

func adoptedServicePorts(ports []corev1.ServicePort) []domainspec.ServicePortTraitSpec {
	result := make([]domainspec.ServicePortTraitSpec, 0, len(ports))
	for _, port := range ports {
		var targetPort int32
		if port.TargetPort.Type == intstr.Int {
			targetPort = port.TargetPort.IntVal
		}
		result = append(result, domainspec.ServicePortTraitSpec{
			Name:       port.Name,
			Port:       port.Port,
			TargetPort: targetPort,
			Protocol:   string(port.Protocol),
		})
	}
	return result
}

func attachAdoptedIngressTraits(component *apisv1.CreateComponentRequest, root *adoptedRoot, resources []*importResource) {
	if component == nil || root == nil {
		return
	}
	existing := make(map[string]struct{}, len(component.Traits.Ingress))
	for _, trait := range component.Traits.Ingress {
		existing[strings.TrimSpace(trait.Name)] = struct{}{}
	}
	for _, resource := range resources {
		if resource == nil || resource.kindKey != importKindIngresses || resource.object == nil {
			continue
		}
		var ingress networkingv1.Ingress
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &ingress); err != nil {
			continue
		}
		selected := false
		for _, serviceName := range ingressBackendServiceNames(&ingress) {
			if _, ok := root.serviceNames[serviceName]; ok {
				selected = true
				break
			}
		}
		if !selected {
			continue
		}
		if _, already := existing[ingress.Name]; already {
			continue
		}
		if trait := adoptedIngressTrait(&ingress); trait != nil {
			component.Traits.Ingress = append(component.Traits.Ingress, *trait)
			existing[ingress.Name] = struct{}{}
		}
	}
}

func adoptedIngressTrait(ingress *networkingv1.Ingress) *domainspec.IngressTraitsSpec {
	if ingress == nil {
		return nil
	}
	trait := &domainspec.IngressTraitsSpec{
		Name:        ingress.Name,
		Namespace:   ingress.Namespace,
		Label:       copyStringMap(ingress.Labels),
		Annotations: copyStringMap(ingress.Annotations),
	}
	if ingress.Spec.IngressClassName != nil {
		trait.IngressClassName = *ingress.Spec.IngressClassName
	}
	for _, tls := range ingress.Spec.TLS {
		trait.TLS = append(trait.TLS, domainspec.IngressTLSConfig{
			SecretName: tls.SecretName,
			Hosts:      append([]string(nil), tls.Hosts...),
		})
	}
	appendBackend := func(host string, path networkingv1.HTTPIngressPath) {
		if path.Backend.Service == nil {
			return
		}
		// Named ingress backend ports require a broader trait contract change.
		// Keep that independent concern outside explicit namespace adoption; the
		// source Ingress remains protected and reconciled from its snapshot.
		if strings.TrimSpace(path.Backend.Service.Port.Name) != "" {
			return
		}
		pathType := ""
		if path.PathType != nil {
			pathType = string(*path.PathType)
		}
		trait.Routes = append(trait.Routes, domainspec.IngressRoutes{
			Path:     path.Path,
			PathType: pathType,
			Host:     host,
			Backend: domainspec.IngressRoute{
				ServiceName: path.Backend.Service.Name,
				ServicePort: path.Backend.Service.Port.Number,
			},
		})
	}
	for _, rule := range ingress.Spec.Rules {
		if rule.HTTP == nil {
			continue
		}
		for _, path := range rule.HTTP.Paths {
			appendBackend(rule.Host, path)
		}
	}
	if len(trait.Routes) == 0 {
		return nil
	}
	return trait
}

func copyStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func adoptedResourceComponentMapping(planKey string, membership map[string]*adoptedMembership) map[string]string {
	result := make(map[string]string)
	for key, member := range membership {
		components := member.appComponents[planKey]
		if len(components) == 1 {
			for componentName := range components {
				result[key] = componentName
			}
		}
	}
	return result
}

func adoptedWorkloadComponentMapping(mapping apisv1.ImportNamespaceApplicationMapping) map[string]string {
	result := make(map[string]string, len(mapping.Components))
	for _, component := range mapping.Components {
		result[strings.ToLower(strings.TrimSpace(component.Workload.Name))] = strings.TrimSpace(component.Name)
	}
	return result
}

func adoptedSourceWorkloads(planKey string, roots []*adoptedRoot) map[string]*importResource {
	result := make(map[string]*importResource)
	for _, root := range roots {
		if root.planKey == planKey {
			result[root.componentName] = root.resource
		}
	}
	return result
}

func adoptedSecretResourcesByComponent(planKey string, membership map[string]*adoptedMembership) map[string][]*importResource {
	result := make(map[string][]*importResource)
	for _, member := range membership {
		if member.resource == nil || member.resource.kindKey != importKindSecrets {
			continue
		}
		for componentName := range member.appComponents[planKey] {
			result[componentName] = append(result[componentName], member.resource)
		}
	}
	for componentName := range result {
		sort.SliceStable(result[componentName], func(i, j int) bool {
			return result[componentName][i].name < result[componentName][j].name
		})
	}
	return result
}

func (s *namespaceImportServiceImpl) buildAdoptedCanonicalPlan(
	ctx context.Context,
	namespace string,
	plans []importAppPlan,
) ([]byte, error) {
	canonical := adoptedCanonicalPlan{
		Version:        adoption.SnapshotVersion,
		Namespace:      namespace,
		ManagementMode: config.ManagementModeAdopted,
		Applications:   make([]adoptedCanonicalPlanApp, 0, len(plans)),
	}
	for index := range plans {
		plan := &plans[index]
		if plan.adopted == nil {
			continue
		}
		app := adoptedCanonicalPlanApp{
			Name:        plan.adopted.mapping.Name,
			Alias:       plan.adopted.mapping.Alias,
			TargetAppID: plan.adopted.mapping.TargetAppID,
			Snapshot:    plan.adopted.snapshot,
		}
		targetState, err := s.adoptedCanonicalTargetState(ctx, plan.adopted.mapping.TargetAppID)
		if err != nil {
			return nil, err
		}
		plan.adopted.targetState = targetState
		app.TargetState = targetState
		app.Snapshot.PlanFingerprint = ""
		for _, component := range plan.adopted.mapping.Components {
			source := plan.adopted.sourceWorkloadByComponent[component.Name]
			if source == nil {
				continue
			}
			snapshot, err := adoption.ResourceSnapshotFromObject(
				source.object,
				component.Name,
				adoptedDependencyRoleWorkload,
				source.ownership,
				source.disposition,
			)
			if err != nil {
				return nil, err
			}
			app.Components = append(app.Components, adoptedCanonicalComponent{
				Name:     component.Name,
				Workload: snapshot.Source,
			})
		}
		sort.SliceStable(app.Components, func(i, j int) bool {
			return app.Components[i].Name < app.Components[j].Name
		})
		canonical.Applications = append(canonical.Applications, app)
	}
	sort.SliceStable(canonical.Applications, func(i, j int) bool {
		left := canonical.Applications[i]
		right := canonical.Applications[j]
		if left.Name != right.Name {
			return left.Name < right.Name
		}
		return left.TargetAppID < right.TargetAppID
	})
	payload, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("marshal adopted namespace import plan: %w", err)
	}
	return payload, nil
}

func (s *namespaceImportServiceImpl) adoptedCanonicalTargetState(
	ctx context.Context,
	targetAppID string,
) (adoptedCanonicalTargetState, error) {
	targetAppID = strings.TrimSpace(targetAppID)
	if targetAppID == "" {
		return adoptedCanonicalTargetState{}, nil
	}
	if s.AppRepo == nil || s.ComponentRepo == nil || s.WorkflowRepo == nil {
		return adoptedCanonicalTargetState{}, fmt.Errorf("adopted target repositories are not initialized")
	}
	app, err := s.AppRepo.FindByID(ctx, targetAppID)
	if err != nil {
		return adoptedCanonicalTargetState{}, fmt.Errorf("load adopted target app %s: %w", targetAppID, err)
	}
	components, err := s.ComponentRepo.FindByAppID(ctx, targetAppID)
	if err != nil {
		return adoptedCanonicalTargetState{}, fmt.Errorf("load adopted target components %s: %w", targetAppID, err)
	}
	workflows, err := s.WorkflowRepo.FindByAppID(ctx, targetAppID)
	if err != nil {
		return adoptedCanonicalTargetState{}, fmt.Errorf("load adopted target workflows %s: %w", targetAppID, err)
	}
	return buildAdoptedCanonicalTargetState(app, components, workflows)
}

func buildAdoptedCanonicalTargetState(
	app *model.Applications,
	components []*model.ApplicationComponent,
	workflows []*model.Workflow,
) (adoptedCanonicalTargetState, error) {
	if app == nil {
		return adoptedCanonicalTargetState{}, fmt.Errorf("adopted target application is nil")
	}
	type componentState struct {
		ID                       int               `json:"id"`
		Name                     string            `json:"name"`
		Namespace                string            `json:"namespace"`
		Image                    string            `json:"image"`
		Replicas                 int32             `json:"replicas"`
		ComponentType            config.JobType    `json:"componentType"`
		Properties               *model.JSONStruct `json:"properties,omitempty"`
		Traits                   *model.JSONStruct `json:"traits,omitempty"`
		SourceWorkloadAPIVersion string            `json:"sourceWorkloadApiVersion,omitempty"`
		SourceWorkloadKind       string            `json:"sourceWorkloadKind,omitempty"`
		SourceWorkloadName       string            `json:"sourceWorkloadName,omitempty"`
		SourceWorkloadUID        string            `json:"sourceWorkloadUid,omitempty"`
		SourcePodSelector        *model.JSONStruct `json:"sourcePodSelector,omitempty"`
		ResumeReplicas           *int32            `json:"resumeReplicas,omitempty"`
		AdoptedSecretData        *model.JSONStruct `json:"adoptedSecretData,omitempty"`
	}
	states := make([]componentState, 0, len(components))
	for _, component := range components {
		if component == nil {
			continue
		}
		uid := ""
		if component.SourceWorkloadUID != nil {
			uid = strings.TrimSpace(*component.SourceWorkloadUID)
		}
		states = append(states, componentState{
			ID:                       component.ID,
			Name:                     strings.TrimSpace(component.Name),
			Namespace:                strings.TrimSpace(component.Namespace),
			Image:                    component.Image,
			Replicas:                 component.Replicas,
			ComponentType:            component.ComponentType,
			Properties:               component.Properties,
			Traits:                   component.Traits,
			SourceWorkloadAPIVersion: strings.TrimSpace(component.SourceWorkloadAPIVersion),
			SourceWorkloadKind:       strings.TrimSpace(component.SourceWorkloadKind),
			SourceWorkloadName:       strings.TrimSpace(component.SourceWorkloadName),
			SourceWorkloadUID:        uid,
			SourcePodSelector:        component.SourcePodSelector,
			ResumeReplicas:           component.ResumeReplicas,
			AdoptedSecretData:        component.AdoptedSecretData,
		})
	}
	sort.SliceStable(states, func(i, j int) bool {
		if states[i].Name != states[j].Name {
			return states[i].Name < states[j].Name
		}
		return states[i].ID < states[j].ID
	})
	payload, err := json.Marshal(states)
	if err != nil {
		return adoptedCanonicalTargetState{}, fmt.Errorf("marshal adopted target component state: %w", err)
	}
	sum := sha256.Sum256(payload)
	workflowDigest, err := adoptedWorkflowStateDigest(workflows)
	if err != nil {
		return adoptedCanonicalTargetState{}, err
	}
	return adoptedCanonicalTargetState{
		Exists:               true,
		ID:                   app.ID,
		Name:                 app.Name,
		Namespace:            app.Namespace,
		ManagementMode:       app.EffectiveManagementMode(),
		UpdatedAtUnixNano:    app.UpdateTime.UnixNano(),
		ComponentStateDigest: fmt.Sprintf("%x", sum[:]),
		WorkflowStateDigest:  workflowDigest,
	}, nil
}

func adoptedWorkflowStateDigest(workflows []*model.Workflow) (string, error) {
	type workflowState struct {
		ID           string                  `json:"id"`
		Name         string                  `json:"name"`
		Namespace    string                  `json:"namespace"`
		Alias        string                  `json:"alias"`
		Disabled     bool                    `json:"disabled"`
		ProjectID    string                  `json:"projectId"`
		AppID        string                  `json:"appId"`
		UserID       string                  `json:"userId"`
		Description  string                  `json:"description"`
		WorkflowType config.WorkflowTaskType `json:"workflowType"`
		Status       config.Status           `json:"status"`
		Steps        *model.JSONStruct       `json:"steps,omitempty"`
		Callback     *model.JSONStruct       `json:"callback,omitempty"`
		UpdatedAt    int64                   `json:"updatedAtUnixNano"`
	}
	states := make([]workflowState, 0, len(workflows))
	for _, workflow := range workflows {
		if workflow == nil {
			continue
		}
		states = append(states, workflowState{
			ID:           workflow.ID,
			Name:         workflow.Name,
			Namespace:    workflow.Namespace,
			Alias:        workflow.Alias,
			Disabled:     workflow.Disabled,
			ProjectID:    workflow.ProjectID,
			AppID:        workflow.AppID,
			UserID:       workflow.UserID,
			Description:  workflow.Description,
			WorkflowType: workflow.WorkflowType,
			Status:       workflow.Status,
			Steps:        workflow.Steps,
			Callback:     workflow.Callback,
			UpdatedAt:    workflow.UpdateTime.UnixNano(),
		})
	}
	sort.SliceStable(states, func(i, j int) bool {
		if states[i].ID != states[j].ID {
			return states[i].ID < states[j].ID
		}
		return states[i].Name < states[j].Name
	})
	payload, err := json.Marshal(states)
	if err != nil {
		return "", fmt.Errorf("marshal adopted target workflow state: %w", err)
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%x", sum[:]), nil
}

func (s *namespaceImportServiceImpl) scanAdoptedHPATargets(ctx context.Context, namespace string) (map[string][]string, error) {
	list, err := s.KubeClient.AutoscalingV2().HorizontalPodAutoscalers(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list horizontalpodautoscalers: %w", err)
	}
	result := make(map[string][]string)
	for index := range list.Items {
		hpa := &list.Items[index]
		key := hpaTargetKey(hpa)
		if key == "" {
			continue
		}
		result[key] = append(result[key], hpa.Name)
	}
	for key := range result {
		sort.Strings(result[key])
	}
	return result, nil
}

func hpaTargetKey(hpa *autoscalingv2.HorizontalPodAutoscaler) string {
	if hpa == nil {
		return ""
	}
	kind := canonicalAdoptedWorkloadKind(hpa.Spec.ScaleTargetRef.Kind)
	if kind == "" || strings.TrimSpace(hpa.Spec.ScaleTargetRef.Name) == "" {
		return ""
	}
	apiVersion := strings.TrimSpace(hpa.Spec.ScaleTargetRef.APIVersion)
	if apiVersion == "" {
		apiVersion = appsv1.SchemeGroupVersion.String()
	}
	return adoptedWorkloadIdentityKey(apiVersion, kind, hpa.Spec.ScaleTargetRef.Name)
}

func sortedStringMapKeys[T any](values map[string]T) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func uniqueSortedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			seen[value] = struct{}{}
		}
	}
	return sortedStringMapKeys(seen)
}

func adoptedResourceIdentity(resource *importResource) (*apisv1.ImportNamespaceResourceIdentity, error) {
	if resource == nil || resource.object == nil {
		return nil, nil
	}
	digest, err := adoption.DigestObject(resource.object)
	if err != nil {
		return nil, err
	}
	return &apisv1.ImportNamespaceResourceIdentity{
		APIVersion:      resource.object.GetAPIVersion(),
		Kind:            resource.kind,
		Namespace:       resource.namespace,
		Name:            resource.name,
		UID:             string(resource.object.GetUID()),
		ResourceVersion: resource.object.GetResourceVersion(),
		SpecDigest:      digest,
	}, nil
}

func adoptedPlanResponseAppID(plan importAppPlan) string {
	if plan.adopted == nil {
		return plan.appID
	}
	return strings.TrimSpace(plan.adopted.mapping.TargetAppID)
}

func initialImportResourceStatus(resource *importResource) string {
	if resource == nil {
		return importResourceStatusSkipped
	}
	switch resource.disposition {
	case adoption.DispositionBlocked, adoption.DispositionExcluded:
		return importResourceStatusSkipped
	default:
		return importResourceStatusPlanned
	}
}

func markLegacyUnsafeImportResources(plans []importAppPlan) {
	for planIndex := range plans {
		plan := &plans[planIndex]
		var protectedPrefixes []string
		for _, resource := range plan.resources {
			if resource == nil || resource.object == nil || resource.kindKey != importKindStatefulSets ||
				!statefulSetImportHasVolumeClaimTemplates(resource.object) {
				continue
			}
			resource.dependencyRole = adoptedDependencyRoleWorkload
			resource.ownership = adoption.OwnershipExternal
			resource.disposition = adoption.DispositionBlocked
			resource.dispositionErr = "statefulset volumeClaimTemplates are not represented by the legacy observe import plan"
			var statefulSet appsv1.StatefulSet
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &statefulSet); err != nil {
				continue
			}
			for _, template := range statefulSet.Spec.VolumeClaimTemplates {
				if prefix := statefulSetPVCNamePrefix(template.Name, statefulSet.Name); prefix != "" {
					protectedPrefixes = append(protectedPrefixes, prefix)
				}
			}
		}
		for _, resource := range plan.resources {
			if resource == nil || resource.kindKey != importKindPersistentVolumeClaims {
				continue
			}
			for _, prefix := range protectedPrefixes {
				if strings.HasPrefix(resource.name, prefix) {
					resource.dependencyRole = adoptedDependencyRolePVC
					resource.ownership = adoption.OwnershipDataProtected
					resource.disposition = adoption.DispositionDataProtected
					resource.dispositionErr = "PVC belongs to a skipped StatefulSet volumeClaimTemplate"
					break
				}
			}
		}
	}
}

func importResourceCanBeLabeled(mode config.ManagementMode, resource *importResource) bool {
	// Explicit adoption is a DB-side operation. Kubernetes metadata mutation is
	// delegated to the pod coordinator after the source UID binding commits.
	// Preserve the pre-existing legacy observe import labeling behavior.
	if mode == config.ManagementModeAdopted || resource == nil {
		return false
	}
	return resource.disposition != adoption.DispositionBlocked &&
		resource.disposition != adoption.DispositionDataProtected
}

func (s *namespaceImportServiceImpl) mutateAdoptedApplicationCreate(
	ctx context.Context,
	store datastore.DataStore,
	app *model.Applications,
	components []*model.ApplicationComponent,
	plan *importAppPlan,
	keyring *importsecret.Keyring,
) error {
	if app == nil {
		return fmt.Errorf("adopted application is nil")
	}
	if plan == nil || plan.adopted == nil {
		return fmt.Errorf("adopted import plan state is missing")
	}
	if keyring == nil {
		return importsecret.ErrKeyringNotConfigured
	}
	if plan.adopted.targetState.Exists {
		if err := validateAdoptedTargetStateInStore(ctx, store, plan.adopted.targetState); err != nil {
			return err
		}
	}
	snapshotJSON, err := model.NewJSONStructByStruct(plan.adopted.snapshot)
	if err != nil {
		return fmt.Errorf("encode adoption snapshot: %w", err)
	}
	app.ManagementMode = config.ManagementModeAdopted
	app.AdoptionSnapshot = snapshotJSON
	for _, component := range components {
		if component == nil {
			continue
		}
		if existingID := plan.adopted.existingComponentIDByName[component.Name]; existingID > 0 {
			component.ID = existingID
		}
	}
	if err := applyAdoptedComponentBindings(app.ID, components, plan, keyring); err != nil {
		return err
	}
	return nil
}

func validateAdoptedTargetStateInStore(
	ctx context.Context,
	store datastore.DataStore,
	expected adoptedCanonicalTargetState,
) error {
	if !expected.Exists {
		return nil
	}
	if store == nil {
		return fmt.Errorf("%w: transactional target datastore is unavailable", bcode.ErrNamespaceImportPlanDrift)
	}
	app, err := repository.ApplicationByID(ctx, store, expected.ID)
	if err != nil {
		return fmt.Errorf("%w: reload target application: %v", bcode.ErrNamespaceImportPlanDrift, err)
	}
	components, err := repository.FindComponentsByAppID(ctx, store, expected.ID)
	if err != nil {
		return fmt.Errorf("%w: reload target components: %v", bcode.ErrNamespaceImportPlanDrift, err)
	}
	workflows, err := repository.FindWorkflowsByAppID(ctx, store, expected.ID)
	if err != nil {
		return fmt.Errorf("%w: reload target workflows: %v", bcode.ErrNamespaceImportPlanDrift, err)
	}
	actual, err := buildAdoptedCanonicalTargetState(app, components, workflows)
	if err != nil {
		return fmt.Errorf("%w: rebuild target state: %v", bcode.ErrNamespaceImportPlanDrift, err)
	}
	if actual != expected {
		return fmt.Errorf("%w: target application, component, or workflow state changed since dry-run", bcode.ErrNamespaceImportPlanDrift)
	}
	return nil
}

func applyAdoptedComponentBindings(
	appID string,
	components []*model.ApplicationComponent,
	plan *importAppPlan,
	keyring *importsecret.Keyring,
) error {
	if plan == nil || plan.adopted == nil {
		return fmt.Errorf("adopted import plan state is missing")
	}
	byName := make(map[string]*model.ApplicationComponent, len(components))
	for _, component := range components {
		if component != nil {
			byName[component.Name] = component
		}
	}
	for componentName, source := range plan.adopted.sourceWorkloadByComponent {
		component := byName[componentName]
		if component == nil || source == nil || source.object == nil {
			return fmt.Errorf("persist adopted component %q: source binding is missing", componentName)
		}
		uid := strings.TrimSpace(string(source.object.GetUID()))
		if uid == "" {
			return fmt.Errorf("persist adopted component %q: source UID is empty", componentName)
		}
		selector := selectorMatchLabelsForImportResource(source)
		selectorJSON, err := model.NewJSONStructByStruct(selector)
		if err != nil {
			return fmt.Errorf("encode source selector for component %q: %w", componentName, err)
		}
		secretData, err := encryptAdoptedSecretResources(
			keyring,
			appID,
			source.namespace,
			plan.adopted.secretResourcesByComponent[componentName],
		)
		if err != nil {
			return fmt.Errorf("encrypt adopted secret data for component %q: %w", componentName, err)
		}
		component.SourceWorkloadAPIVersion = source.object.GetAPIVersion()
		component.SourceWorkloadKind = source.kind
		component.SourceWorkloadName = source.name
		component.SourceWorkloadUID = &uid
		component.SourcePodSelector = selectorJSON
		component.ResumeReplicas = nil
		component.AdoptedSecretData = secretData
	}
	return nil
}

func encryptAdoptedSecretResources(
	keyring *importsecret.Keyring,
	appID, namespace string,
	resources []*importResource,
) (*model.JSONStruct, error) {
	if len(resources) == 0 {
		return nil, nil
	}
	payload := make(map[string]map[string]importsecret.Envelope, len(resources))
	for _, resource := range resources {
		if resource == nil || resource.object == nil {
			continue
		}
		var secret corev1.Secret
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.object.Object, &secret); err != nil {
			return nil, err
		}
		envelopes := make(map[string]importsecret.Envelope, len(secret.Data))
		keys := make([]string, 0, len(secret.Data))
		for key := range secret.Data {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			aad := importsecret.ResourceAAD(appID, namespace, resource.object.GetAPIVersion(), resource.kind, resource.name, key)
			envelope, err := keyring.Encrypt(secret.Data[key], aad)
			if err != nil {
				return nil, err
			}
			envelopes[key] = envelope
		}
		payload[resource.name] = envelopes
	}
	return model.NewJSONStructByStruct(payload)
}

func adoptedResourceCanBeLabeled(resource *importResource) bool {
	if resource == nil || resource.disposition != adoption.DispositionManaged {
		return false
	}
	switch resource.kindKey {
	case importKindDeployments, importKindStatefulSets:
		return false
	default:
		return true
	}
}
