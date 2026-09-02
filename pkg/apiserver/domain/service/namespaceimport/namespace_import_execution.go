package namespaceimport

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type namespaceImportRun struct {
	namespace           string
	mode                string
	managementMode      config.ManagementMode
	resources           []*importResource
	includeKinds        map[string]struct{}
	plans               []importAppPlan
	excluded            []*importResource
	warnings            []string
	adoptedPlanning     *adoptedImportPlanning
	adoptedReplay       bool
	replayAppID         string
	existingAppIDMap    map[string]string
	existingAppIDSet    map[string]struct{}
	existingAppNameByID map[string]string
	response            *apisv1.ImportNamespaceApplicationsResponse
	resourceResultIndex map[string]int
	appResultIndex      map[string]int
}

func (s *namespaceImportServiceImpl) prepareNamespaceImportRun(
	ctx context.Context,
	req apisv1.ImportNamespaceApplicationsRequest,
	namespace string,
	mode string,
	managementMode config.ManagementMode,
) (*namespaceImportRun, error) {
	var (
		includeKinds map[string]struct{}
		kindWarnings []string
		err          error
	)
	if managementMode == config.ManagementModeAdopted {
		includeKinds = make(map[string]struct{}, len(allImportKinds))
		for _, kind := range allImportKinds {
			includeKinds[kind] = struct{}{}
		}
	} else {
		includeKinds, kindWarnings, err = normalizeImportKinds(req.IncludeKinds)
		if err != nil {
			return nil, err
		}
	}
	resources, scanWarnings, err := s.scanNamespaceResources(ctx, namespace, includeKinds)
	if err != nil {
		return nil, err
	}

	existingAppIDMap, existingAppIDSet, existingAppNameByID, err := s.loadExistingAppIndex(ctx, namespace)
	if err != nil {
		return nil, err
	}

	var (
		plans           []importAppPlan
		excluded        []*importResource
		assignWarnings  []string
		adoptedPlanning *adoptedImportPlanning
		adoptedReplay   bool
		replayAppID     string
	)
	if managementMode == config.ManagementModeAdopted {
		keyring, err := s.loadAdoptedImportKeyring()
		if err != nil {
			return nil, err
		}
		adoptedPlanning, replayAppID, adoptedReplay, err = s.tryAdoptedApplyReplay(
			ctx,
			namespace,
			req,
			resources,
			existingAppIDMap,
			existingAppIDSet,
			existingAppNameByID,
			keyring,
		)
		if err == nil && !adoptedReplay {
			adoptedPlanning, err = s.buildAdoptedImportPlanning(
				ctx,
				namespace,
				resources,
				req.Applications,
				existingAppIDMap,
				existingAppIDSet,
				existingAppNameByID,
				keyring,
			)
		}
		if err != nil {
			if mode == importModeApply && strings.TrimSpace(req.PlanFingerprint) != "" {
				return nil, fmt.Errorf("%w: %v", bcode.ErrNamespaceImportPlanDrift, err)
			}
			return nil, err
		}
		plans = adoptedPlanning.plans
		excluded = adoptedPlanning.excluded
		assignWarnings = append(assignWarnings, adoptedPlanning.warnings...)
		if !adoptedReplay {
			s.prepareAdoptedPlansForExecution(ctx, namespace, plans)
		}
	} else {
		grouped, appNames, appAliases, warnings := assignResourcesToApps(namespace, resources)
		assignWarnings = append(assignWarnings, warnings...)
		plans = s.buildImportPlans(grouped, appNames, appAliases, sharedAppIDForNamespace(namespace))
		markLegacyUnsafeImportResources(plans)
		s.prepareImportPlansForExecution(ctx, namespace, plans, includeKinds, existingAppIDMap, existingAppIDSet, existingAppNameByID)
	}
	return &namespaceImportRun{
		namespace:           namespace,
		mode:                mode,
		managementMode:      managementMode,
		resources:           resources,
		includeKinds:        includeKinds,
		plans:               plans,
		excluded:            excluded,
		warnings:            append(append(append([]string{}, kindWarnings...), scanWarnings...), assignWarnings...),
		adoptedPlanning:     adoptedPlanning,
		adoptedReplay:       adoptedReplay,
		replayAppID:         replayAppID,
		existingAppIDMap:    existingAppIDMap,
		existingAppIDSet:    existingAppIDSet,
		existingAppNameByID: existingAppNameByID,
	}, nil
}

func (r *namespaceImportRun) buildResponse() error {
	namespace := r.namespace
	mode := r.mode
	managementMode := r.managementMode
	resources := r.resources
	plans := r.plans
	excluded := r.excluded
	adoptedPlanning := r.adoptedPlanning
	resp := &apisv1.ImportNamespaceApplicationsResponse{
		Namespace:      namespace,
		Mode:           mode,
		ManagementMode: managementMode,
		Summary: apisv1.ImportNamespaceSummary{
			ResourcesScanned: len(resources),
		},
		Warnings: append([]string(nil), r.warnings...),
	}
	if adoptedPlanning != nil {
		resp.PlanFingerprint = adoptedPlanning.fingerprint
	}

	resourceResultIndex := make(map[string]int, len(resources))
	appResultIndex := make(map[string]int, len(plans))

	for _, plan := range plans {
		resp.Summary.AppsPlanned++
		resp.Summary.ComponentsPlanned += len(plan.components)

		appResult := apisv1.ImportNamespaceAppResult{
			AppID:      adoptedPlanResponseAppID(plan),
			Name:       plan.name,
			Components: append([]string(nil), plan.componentNames...),
		}
		if plan.err != nil {
			appResult.Error = plan.err.Error()
		}
		if len(plan.warnings) > 0 {
			resp.Warnings = append(resp.Warnings, plan.warnings...)
		}

		appResultIndex[plan.appID] = len(resp.Apps)
		resp.Apps = append(resp.Apps, appResult)

		for _, res := range plan.resources {
			source, identityErr := adoptedResourceIdentity(res)
			if identityErr != nil {
				return identityErr
			}
			result := apisv1.ImportNamespaceResourceResult{
				Kind:           res.kind,
				Namespace:      res.namespace,
				Name:           res.name,
				Source:         source,
				AppID:          adoptedPlanResponseAppID(plan),
				ComponentName:  res.componentName,
				DependencyRole: res.dependencyRole,
				Ownership:      res.ownership,
				Disposition:    res.disposition,
				Status:         initialImportResourceStatus(res),
				Error:          res.dispositionErr,
			}
			resourceResultIndex[resourceResultKey(res)] = len(resp.ResourceResults)
			resp.ResourceResults = append(resp.ResourceResults, result)
		}
	}
	for _, res := range excluded {
		source, identityErr := adoptedResourceIdentity(res)
		if identityErr != nil {
			return identityErr
		}
		resourceResultIndex[resourceResultKey(res)] = len(resp.ResourceResults)
		resp.ResourceResults = append(resp.ResourceResults, apisv1.ImportNamespaceResourceResult{
			Kind:           res.kind,
			Namespace:      res.namespace,
			Name:           res.name,
			Source:         source,
			DependencyRole: res.dependencyRole,
			Ownership:      res.ownership,
			Disposition:    res.disposition,
			Status:         importResourceStatusSkipped,
			Error:          res.dispositionErr,
		})
	}
	r.response = resp
	r.resourceResultIndex = resourceResultIndex
	r.appResultIndex = appResultIndex
	return nil
}

func (r *namespaceImportRun) finishWithoutApply() bool {
	if r.mode == importModeDryRun {
		return true
	}
	if !r.adoptedReplay {
		return false
	}
	for index := range r.response.Apps {
		r.response.Apps[index].AppID = r.replayAppID
	}
	for index := range r.response.ResourceResults {
		if r.response.ResourceResults[index].AppID != "" {
			r.response.ResourceResults[index].AppID = r.replayAppID
		}
		r.response.ResourceResults[index].Status = importResourceStatusSkipped
	}
	r.response.Summary.AppsApplied = len(r.plans)
	for _, plan := range r.plans {
		r.response.Summary.ComponentsApplied += len(plan.components)
	}
	r.response.Warnings = append(r.response.Warnings, "adopted namespace import was already applied; no database or Kubernetes changes were made")
	return true
}

func (r *namespaceImportRun) verifyAdoptedApply(req apisv1.ImportNamespaceApplicationsRequest) error {
	managementMode := r.managementMode
	adoptedPlanning := r.adoptedPlanning
	plans := r.plans
	resp := r.response
	if managementMode == config.ManagementModeAdopted {
		submittedFingerprint := strings.TrimSpace(req.PlanFingerprint)
		if adoptedPlanning == nil ||
			adoptedPlanning.keyring == nil ||
			submittedFingerprint == "" ||
			adoptedPlanning.keyring.VerifyPlan(adoptedPlanning.payload, submittedFingerprint) != nil {
			return fmt.Errorf("%w: adopted namespace import fingerprint verification failed", bcode.ErrNamespaceImportPlanDrift)
		}
		// A retained previous key may legitimately authenticate a dry-run plan
		// issued before key rotation. Persist the fingerprint the caller
		// actually approved so an exact apply retry remains idempotent. Secret
		// payloads are still encrypted below with the current active key.
		adoptedPlanning.fingerprint = submittedFingerprint
		resp.PlanFingerprint = submittedFingerprint
		for index := range plans {
			if plans[index].adopted != nil {
				plans[index].adopted.snapshot.PlanFingerprint = submittedFingerprint
			}
		}
		for _, plan := range plans {
			if plan.err != nil {
				return fmt.Errorf("%w: %v", bcode.ErrAdoptedResourceConflict, plan.err)
			}
		}
	}
	return nil
}

func (s *namespaceImportServiceImpl) applyNamespaceImportRun(ctx context.Context, r *namespaceImportRun) error {
	namespace := r.namespace
	managementMode := r.managementMode
	plans := r.plans
	includeKinds := r.includeKinds
	adoptedPlanning := r.adoptedPlanning
	resp := r.response
	resourceResultIndex := r.resourceResultIndex
	appResultIndex := r.appResultIndex
	existingAppIDMap := r.existingAppIDMap
	existingAppIDSet := r.existingAppIDSet
	existingAppNameByID := r.existingAppNameByID
	var err error
	sharedPlanID := sharedAppIDForNamespace(namespace)
	appliedAppIDs := make(map[string]struct{}, len(plans))

	for _, plan := range plans {
		appIdx := appResultIndex[plan.appID]
		if plan.err != nil {
			s.markPlanResourcesWithStatus(resp, resourceResultIndex, plan.resources, plan.err, plan.applyErrorStatus)
			continue
		}

		appKey := appNameNamespaceKey(plan.name, namespace)
		createReq := plan.createReq
		if managementMode != config.ManagementModeAdopted {
			refreshedReq, refreshErr := buildImportCreateRequest(namespace, plan, existingAppIDMap, existingAppIDSet, existingAppNameByID)
			if refreshErr != nil {
				resp.Apps[appIdx].Error = refreshErr.Error()
				s.markPlanResourcesFailed(resp, resourceResultIndex, plan.resources, refreshErr)
				continue
			}
			idRebound := refreshedReq.ID != "" && refreshedReq.ID != createReq.ID
			if idRebound {
				createReq.ID = refreshedReq.ID
				createReq.Name = refreshedReq.Name
			}
			_, alreadyApplied := appliedAppIDs[createReq.ID]
			if createReq.ID != "" && (idRebound || alreadyApplied) {
				var (
					mergedComponents []apisv1.CreateComponentRequest
					mergeErr         error
				)
				if alreadyApplied || isFullImportKinds(includeKinds) {
					mergedComponents, mergeErr = s.mergeCreateComponentsWithAllExisting(ctx, createReq.ID, createReq.Name, createReq.Namespace, createReq.Component)
				} else {
					mergedComponents, mergeErr = s.mergeCreateComponentsWithExisting(ctx, createReq.ID, createReq.Name, createReq.Namespace, createReq.Component, includeKinds)
				}
				if mergeErr != nil {
					resp.Apps[appIdx].Error = mergeErr.Error()
					s.markPlanResourcesFailed(resp, resourceResultIndex, plan.resources, mergeErr)
					continue
				}
				createReq.Component = mergedComponents
			}
		}

		var created *apisv1.ApplicationBase
		if managementMode == config.ManagementModeAdopted {
			created, err = s.ApplicationService.CreateApplicationsWithMutation(
				ctx,
				createReq,
				func(
					mutationCtx context.Context,
					store datastore.DataStore,
					app *model.Applications,
					components []*model.ApplicationComponent,
				) error {
					return s.mutateAdoptedApplicationCreate(
						mutationCtx,
						store,
						app,
						components,
						&plan,
						adoptedPlanning.keyring,
					)
				},
			)
		} else {
			createReq.ImportAsObserve = true
			created, err = s.ApplicationService.CreateApplications(ctx, createReq)
		}
		if err != nil {
			if managementMode == config.ManagementModeAdopted &&
				errors.Is(err, bcode.ErrNamespaceImportPlanDrift) {
				return err
			}
			resp.Apps[appIdx].Error = err.Error()
			s.markPlanResourcesFailed(resp, resourceResultIndex, plan.resources, err)
			continue
		}

		effectiveAppID := resolveImportedAppID(created, createReq.ID)
		if effectiveAppID == "" {
			err := fmt.Errorf("create app %s returned empty app id", plan.name)
			resp.Apps[appIdx].Error = err.Error()
			s.markPlanResourcesFailed(resp, resourceResultIndex, plan.resources, err)
			continue
		}
		resp.Apps[appIdx].AppID = effectiveAppID
		s.updatePlanResourceResultAppID(resp, resourceResultIndex, plan.resources, effectiveAppID)
		existingAppIDMap[appKey] = effectiveAppID
		existingAppIDSet[effectiveAppID] = struct{}{}
		existingAppNameByID[effectiveAppID] = createReq.Name
		appliedAppIDs[effectiveAppID] = struct{}{}

		if managementMode == config.ManagementModeObserve {
			resp.Apps[appIdx].WorkflowDisabled = true
		}

		var componentIDByName map[string]int
		for _, res := range plan.resources {
			if !importResourceCanBeLabeled(managementMode, res) {
				continue
			}
			componentIDByName, err = s.loadComponentIDMap(ctx, effectiveAppID)
			if err != nil {
				resp.Apps[appIdx].Error = err.Error()
				s.markPlanResourcesFailed(resp, resourceResultIndex, plan.resources, err)
			}
			break
		}
		if err != nil {
			continue
		}

		resp.Summary.AppsApplied++
		resp.Summary.ComponentsApplied += len(plan.components)
		forceShareLabels := strings.EqualFold(plan.appID, sharedPlanID)

		for _, res := range plan.resources {
			resultIdx, ok := resourceResultIndex[resourceResultKey(res)]
			if !ok {
				continue
			}
			if !importResourceCanBeLabeled(managementMode, res) {
				resp.ResourceResults[resultIdx].Status = importResourceStatusSkipped
				continue
			}
			componentName := resolveResourceComponentName(res, componentIDByName, plan.resourceComponentByKey, plan.workloadComponentByOriginalName)
			componentID := resolveComponentID(componentName, res, componentIDByName)

			if componentName != "" {
				resp.ResourceResults[resultIdx].ComponentName = componentName
			}

			labels := buildImportLabels(res, effectiveAppID, plan.appID, componentName, componentID, forceShareLabels)
			var patchErr error
			if managementMode == config.ManagementModeAdopted {
				patchErr = s.patchResourceMetadataLabels(ctx, res, labels)
			} else {
				patchErr = s.patchResourceLabels(ctx, res, labels)
			}
			if patchErr != nil {
				resp.ResourceResults[resultIdx].Status = importResourceStatusFailed
				resp.ResourceResults[resultIdx].Error = patchErr.Error()
				resp.Summary.ResourcesLabeledFailed++
				continue
			}
			resp.ResourceResults[resultIdx].Status = importResourceStatusLabeled
			resp.Summary.ResourcesLabeledSuccess++
		}
	}
	return nil
}
