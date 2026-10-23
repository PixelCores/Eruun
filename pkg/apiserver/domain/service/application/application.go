package application

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/schedulelock"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/traitvalidation"
	urlpolicy "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	workflowservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/workflow"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/cache"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

const (
	componentTypeColumn            = "component_type"
	operationTaskNameCleanup       = "cleanup"
	operationTaskNameRestart       = "restart"
	operationTaskNameStop          = "stop"
	operationTaskNameStart         = "start"
	operationTaskNameUpdateVersion = "update-version"
)

type ApplicationsService interface {
	CreateApplications(context.Context, apisv1.CreateApplicationsRequest) (*apisv1.ApplicationBase, error)
	MarkInitialDeployingWorkflowComponents(context.Context, string, string) error
	HasImmediateActiveVersionUpdateTask(context.Context, string, int64) (bool, error)
	GetApplicationSpec(ctx context.Context, appID string) (*apisv1.CreateApplicationsRequest, error)
	ListApplications(ctx context.Context, opts ListApplicationsOptions) ([]*apisv1.ApplicationBase, error)
	ListTemplateApplications(ctx context.Context, opts ListApplicationsOptions) ([]*apisv1.ApplicationBase, error)
	BatchGetApplications(ctx context.Context, appIDs []string) (*apisv1.BatchGetApplicationsResponse, error)
	DeleteApplicationCascade(ctx context.Context, appID string, req apisv1.DeleteApplicationRequest) (*apisv1.DeleteApplicationResponse, error)
	CleanupApplicationResources(ctx context.Context, appID string) (*apisv1.CleanupApplicationResourcesResponse, error)
	ResetApplicationDatabases(ctx context.Context, appID string, req apisv1.DatabaseResetRequest) (*apisv1.DatabaseResetResponse, error)
	DownloadLogArchive(ctx context.Context, appID string, req apisv1.LogArchiveDownloadRequest) (*ComponentFileArchiveStream, error)
	RestartApplicationWorkloads(ctx context.Context, appID string, req apisv1.ApplicationLifecycleRequest) (*apisv1.RestartApplicationWorkloadsResponse, error)
	StopApplicationDeployments(ctx context.Context, appID string, req apisv1.ApplicationLifecycleRequest) (*apisv1.StopApplicationDeploymentsResponse, error)
	StartApplicationDeployments(ctx context.Context, appID string, req apisv1.ApplicationLifecycleRequest) (*apisv1.StartApplicationDeploymentsResponse, error)
	UpdateApplicationWorkflow(ctx context.Context, appID string, req apisv1.UpdateApplicationWorkflowRequest) (*apisv1.UpdateWorkflowResponse, error)
	ListApplicationWorkflows(ctx context.Context, appID string) ([]*model.Workflow, error)
	ListApplicationComponents(ctx context.Context, appID string) ([]*model.ApplicationComponent, error)
	ListApplicationTasks(ctx context.Context, appID string) ([]*model.WorkflowQueue, error)
	ListCronJobs(ctx context.Context) ([]*apisv1.CronJobInfo, error)
	ListScheduledJobs(ctx context.Context) ([]*apisv1.ScheduledJobInfo, error)
	ListComponentContainers(ctx context.Context, appID, componentName string) (*apisv1.ComponentContainersResponse, error)
	StreamComponentLogs(ctx context.Context, appID, componentName, requestedContainer string) (*ComponentLogStream, error)
	ExportComponentFilesZip(ctx context.Context, appID, componentName string, req apisv1.ExportComponentFilesRequest) (*ComponentFileArchiveStream, error)
	ExecComponentShellScript(ctx context.Context, appID, componentName string, req apisv1.ExecComponentShellScriptRequest) (*apisv1.ExecComponentShellScriptResponse, error)
	StreamComponentShellScript(ctx context.Context, appID, componentName string, req apisv1.ExecComponentShellScriptRequest) (*ComponentShellScriptStream, error)
	// UpdateVersion 更新应用版本，支持组件的更新、新增、删除操作
	UpdateVersion(ctx context.Context, appID string, req apisv1.UpdateVersionRequest) (*apisv1.UpdateVersionResponse, error)
	DiffUpdateVersion(ctx context.Context, targetAppID string, req apisv1.DiffUpdateVersionRequest) (*apisv1.DiffUpdateVersionResponse, error)
}

type ListApplicationsOptions struct {
	Page     int
	PageSize int
}

func (o ListApplicationsOptions) FullScan() bool {
	return o.PageSize <= 0
}

func (o ListApplicationsOptions) NormalizedPage() int {
	if o.Page <= 0 {
		return 1
	}
	return o.Page
}

type applicationsServiceImpl struct {
	NodePoolRepo              repository.NodePoolRepository    `inject:""`
	KubeClient                kubernetes.Interface             `inject:"kubeClient"`
	KubeConfig                *rest.Config                     `inject:"kubeConfig"`
	Store                     datastore.DataStore              `inject:"datastore"`
	Cache                     cache.ICache                     `inject:"cache"`
	RedisClient               *redis.Client                    `inject:"redisClient"`
	Cfg                       *config.Config                   `inject:""`
	URLSecurityPolicyProvider *urlpolicy.Provider              `inject:""`
	ScheduleLocker            locker.Locker                    `inject:"appScheduleLocker"`
	AppRepo                   repository.ApplicationRepository `inject:""`
	WorkflowRepo              repository.WorkflowRepository    `inject:""`
	ComponentRepo             repository.ComponentRepository   `inject:""`
}

type workflowUpsertOptions struct {
	desiredName             string
	desiredAlias            string
	workflowType            config.WorkflowTaskType
	callback                *model.JSONStruct
	createErrLog            string
	alwaysUpdateAlias       bool
	setWorkflowTypeOnUpdate bool
	setCallbackOnUpdate     bool
	pickTarget              func([]*model.Workflow) *model.Workflow
}

func NewApplicationService() ApplicationsService {
	return &applicationsServiceImpl{}
}

func (c *applicationsServiceImpl) CreateApplications(ctx context.Context, req apisv1.CreateApplicationsRequest) (*apisv1.ApplicationBase, error) {
	if !spec.ValidAPIName(req.Name, datastore.PrimaryKeyMaxLength) {
		return nil, bcode.ErrApplicationConfig
	}
	apisv1.NormalizeApplicationNames(&req)
	return c.createApplicationsWithScheduleLock(ctx, req, "create-or-replace-application")
}

func (c *applicationsServiceImpl) createApplicationsWithScheduleLock(
	ctx context.Context,
	req apisv1.CreateApplicationsRequest,
	operation string,
) (*apisv1.ApplicationBase, error) {
	if scope, ok := access.FromContext(ctx); ok {
		if req.Namespace != "" && req.Namespace != scope.Namespace {
			return nil, bcode.ErrForbidden
		}
		req.Namespace = scope.Namespace
	}
	var err error
	req, err = c.resolveCreateApplicationReplacement(ctx, req)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.ID) == "" {
		return c.createApplications(ctx, req)
	}
	lockProvider, err := c.appScheduleLocker()
	if err != nil {
		return nil, err
	}
	var result *apisv1.ApplicationBase
	err = schedulelock.WithAppScheduleLock(ctx, lockProvider, req.ID, operation, true, func(lockCtx context.Context) error {
		lockCtx = context.WithValue(lockCtx, applicationMutationLockContextKey{}, strings.TrimSpace(req.ID))
		var createErr error
		result, createErr = c.createApplications(lockCtx, req)
		return createErr
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *applicationsServiceImpl) resolveCreateApplicationReplacement(ctx context.Context, req apisv1.CreateApplicationsRequest) (apisv1.CreateApplicationsRequest, error) {
	if strings.TrimSpace(req.ID) != "" {
		if c.AppRepo == nil {
			return req, fmt.Errorf("application repository is not initialized")
		}
		existing, err := c.AppRepo.FindByID(ctx, req.ID)
		if err != nil {
			if errors.Is(err, datastore.ErrRecordNotExist) {
				return req, bcode.ErrApplicationNotExist
			}
			return req, fmt.Errorf("find replacement application %q: %w", req.ID, err)
		}
		if existing == nil || strings.TrimSpace(existing.ID) == "" {
			return req, bcode.ErrApplicationNotExist
		}
		req.ID = existing.ID
		return req, nil
	}
	if req.TemplateEnabled == nil || !*req.TemplateEnabled {
		return req, nil
	}
	if strings.TrimSpace(req.Version) == "" {
		req.Version = "1.0.0"
	}
	existing, err := c.findTemplateApplication(ctx, req.Namespace, req.Name, req.Version)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return req, nil
		}
		return req, err
	}
	req.ID = existing.ID
	return req, nil
}

func (c *applicationsServiceImpl) createApplications(
	ctx context.Context,
	req apisv1.CreateApplicationsRequest,
) (*apisv1.ApplicationBase, error) {
	if req.Version == "" {
		req.Version = "1.0.0"
	}
	if err := validateWorkflowFailurePolicy(req.FailurePolicy); err != nil {
		return nil, err
	}

	if c.Store == nil {
		return nil, fmt.Errorf("datastore is not initialized")
	}
	application, createTransaction, err := c.prepareApplicationForCreate(ctx, req)
	if err != nil {
		return nil, err
	}
	refreshAppID := strings.TrimSpace(req.ID)

	callbackSelection, err := resolveCreateApplicationCallback(ctx, c.Cfg, c.URLSecurityPolicyProvider, req)
	if err != nil {
		return nil, err
	}
	if callbackSelection.setCallback {
		application.Callback = callbackSelection.callback
	}

	if err := componentValidationError(ValidateTemplateOverrides(req.Components)); err != nil {
		return nil, err
	}

	//分解所有的组件
	resolvedComponents, _, err := resolveComponentsWithSourceIndexes(ctx, c.AppRepo, c.ComponentRepo, application.Namespace, application.Name, req.Components, c.Cfg)
	if err != nil {
		return nil, err
	}
	if err := c.validateComponentNodePools(ctx, resolvedComponents); err != nil {
		return nil, err
	}
	if err := validateApplicationResourceNames(ctx, c.AppRepo, c.ComponentRepo, application, resolvedComponents); err != nil {
		return nil, err
	}

	components, err := prepareComponents(application.ID, application.Namespace, resolvedComponents)
	if err != nil {
		return nil, err
	}
	if len(req.Workflow) > 0 {
		if err := validateWorkflowComponentRefs(req.Workflow, workflowComponentTypesFromRequests(resolvedComponents)); err != nil {
			klog.Errorf("create application workflow steps validation failed app=%s: %v", req.Name, err)
			return nil, err
		}
	}

	var workflow *model.Workflow
	run := func(store datastore.DataStore) error {
		if refreshAppID != "" {
			if err := workflowservice.EnsureAppWorkflowIdle(ctx, store, application.ID); err != nil {
				return err
			}
			if err := workflowservice.EnsureNoPendingStatefulSetCleanup(ctx, store, application.ID); err != nil {
				return err
			}
			currentApplication, err := c.refreshExistingApplication(ctx, store, req)
			if err != nil {
				return err
			}
			if currentApplication.ID != refreshAppID {
				return bcode.ErrApplicationNotExist
			}
			if callbackSelection.setCallback {
				currentApplication.Callback = callbackSelection.callback
			}
			application = currentApplication
			components, err = prepareComponents(application.ID, application.Namespace, resolvedComponents)
			if err != nil {
				return err
			}
		}
		workflow, err = c.persistCreatedApplication(ctx, store, application, components, req, resolvedComponents, callbackSelection)
		return err
	}

	if err := createTransaction.WithTransaction(ctx, run); err != nil {
		return nil, err
	}

	base := assembler.ConvertAppModelToBase(application, workflow.ID)
	base.Resources = summarizeApplicationResourcesFromCreateComponents(resolvedComponents)
	c.invalidateApplicationListCaches(ctx)
	c.invalidateApplicationComponentsCache(ctx, application.ID)
	return base, nil
}

func (c *applicationsServiceImpl) prepareApplicationForCreate(ctx context.Context, req apisv1.CreateApplicationsRequest) (*model.Applications, datastore.Transactional, error) {
	var (
		application *model.Applications
		err         error
	)
	templateEnabled := req.TemplateEnabled != nil && *req.TemplateEnabled
	refreshAppID := strings.TrimSpace(req.ID)
	var createTransaction datastore.Transactional
	if refreshAppID != "" {
		var ok bool
		createTransaction, ok = c.Store.(datastore.Transactional)
		if !ok {
			return nil, nil, fmt.Errorf("application refresh requires transactional datastore")
		}
		if err := workflowservice.EnsureAppWorkflowIdle(ctx, c.Store, refreshAppID); err != nil {
			return nil, nil, err
		}
		if err := workflowservice.EnsureNoPendingStatefulSetCleanup(ctx, c.Store, refreshAppID); err != nil {
			return nil, nil, err
		}
		application, err = c.refreshExistingApplication(ctx, c.Store, req)
		if err != nil {
			return nil, nil, err
		}
		if application.ID != refreshAppID {
			return nil, nil, bcode.ErrApplicationNotExist
		}
	} else {
		var ok bool
		createTransaction, ok = c.Store.(datastore.Transactional)
		if !ok {
			return nil, nil, fmt.Errorf("application creation requires transactional datastore")
		}
		if !templateEnabled {
			if err := c.ensureStandardApplicationNameAvailable(ctx, req.Namespace, req.Name, ""); err != nil {
				return nil, nil, err
			}
		}
		application = model.NewApplications(
			utils.RandStringByNumLowercase(24),
			req.Name,
			req.Namespace,
			req.Version,
			req.Alias,
			req.Project,
			req.Description,
			req.Icon,
			templateEnabled,
		)
	}
	if application.Namespace == "" {
		return nil, nil, fmt.Errorf("%w: namespace is required", bcode.ErrApplicationConfig)
	}
	if scope, ok := access.FromContext(ctx); ok {
		application.WorkspaceID = scope.WorkspaceID
	}

	return application, createTransaction, nil
}

func (c *applicationsServiceImpl) persistCreatedApplication(ctx context.Context, store datastore.DataStore, application *model.Applications, components []*model.ApplicationComponent, req apisv1.CreateApplicationsRequest, resolvedComponents []apisv1.CreateComponentRequest, callbackSelection applicationCallbackSelection) (*model.Workflow, error) {
	if err := repository.CreateApplications(ctx, store, application); err != nil {
		return nil, err
	}
	if callbackSelection.setCallback && callbackSelection.callback == nil {
		if err := updateApplicationCallbackField(ctx, store, application.ID, nil); err != nil {
			return nil, err
		}
	}
	if err := repository.DelComponentsByAppID(ctx, store, application.ID); err != nil {
		klog.Errorf("pre-cleanup components for application %s failed: %v", application.ID, err)
		return nil, bcode.ErrComponentBuild
	}
	if err := batchAddComponents(ctx, store, components); err != nil {
		klog.Errorf("batch create components for application %s failed: %v", application.ID, err)
		return nil, bcode.ErrCreateComponents
	}
	wf, err := c.upsertDefaultWorkflow(ctx, store, application, req, resolvedComponents, callbackSelection)
	if err != nil {
		return nil, err
	}
	if _, err := c.upsertUpdateWorkflow(ctx, store, application, req, resolvedComponents, callbackSelection); err != nil {
		return nil, err
	}
	if callbackSelection.overwriteAll {
		if err := updateWorkflowCallbacksForApp(ctx, store, application.ID, callbackSelection.callback); err != nil {
			return nil, err
		}
	}
	return wf, nil
}

func batchAddComponents(ctx context.Context, store datastore.DataStore, components []*model.ApplicationComponent) error {
	if len(components) == 0 {
		return nil
	}
	entities := make([]datastore.Entity, len(components))
	for i, comp := range components {
		entities[i] = comp
	}
	return store.BatchAdd(ctx, entities)
}

func (c *applicationsServiceImpl) upsertDefaultWorkflow(ctx context.Context, store datastore.DataStore, app *model.Applications, req apisv1.CreateApplicationsRequest, resolvedComponents []apisv1.CreateComponentRequest, callbackSelection applicationCallbackSelection) (*model.Workflow, error) {
	workflowAliasBase := req.Alias
	if workflowAliasBase == "" {
		workflowAliasBase = req.Name
	}

	desiredAlias := fmt.Sprintf("%s-workflow", workflowAliasBase)
	desiredName := fmt.Sprintf("%s-workflow", req.Name)

	return c.upsertApplicationWorkflow(ctx, store, app, req, resolvedComponents, workflowUpsertOptions{
		desiredName:         desiredName,
		desiredAlias:        desiredAlias,
		workflowType:        config.WorkflowTaskTypeWorkflow,
		callback:            callbackSelection.callback,
		createErrLog:        "create workflow failed",
		setCallbackOnUpdate: callbackSelection.setCallback,
		pickTarget: func(workflows []*model.Workflow) *model.Workflow {
			return pickDefaultWorkflow(workflows, desiredName, desiredAlias)
		},
	})
}

func (c *applicationsServiceImpl) upsertUpdateWorkflow(ctx context.Context, store datastore.DataStore, app *model.Applications, req apisv1.CreateApplicationsRequest, resolvedComponents []apisv1.CreateComponentRequest, callbackSelection applicationCallbackSelection) (*model.Workflow, error) {
	appName := req.Name
	if appName == "" {
		appName = app.Name
	}
	desiredName := fmt.Sprintf("%s-update-workflow", appName)
	desiredAlias := fmt.Sprintf("%s-update-workflow", app.ID)

	return c.upsertApplicationWorkflow(ctx, store, app, req, resolvedComponents, workflowUpsertOptions{
		desiredName:             desiredName,
		desiredAlias:            desiredAlias,
		workflowType:            config.WorkflowTaskTypeUpdate,
		callback:                callbackSelection.callback,
		createErrLog:            "create update workflow failed",
		alwaysUpdateAlias:       true,
		setWorkflowTypeOnUpdate: true,
		setCallbackOnUpdate:     callbackSelection.setCallback,
		pickTarget: func(workflows []*model.Workflow) *model.Workflow {
			return pickWorkflowByType(workflows, config.WorkflowTaskTypeUpdate)
		},
	})
}

func (c *applicationsServiceImpl) upsertApplicationWorkflow(ctx context.Context, store datastore.DataStore, app *model.Applications, req apisv1.CreateApplicationsRequest, resolvedComponents []apisv1.CreateComponentRequest, opts workflowUpsertOptions) (*model.Workflow, error) {
	workflowBody := defaultWorkflowBodyForCreate(req, resolvedComponents)

	workflowStep, err := model.NewJSONStructByStruct(workflowBody)
	if err != nil {
		return nil, bcode.ErrCreateWorkflow
	}

	workflows, err := repository.FindWorkflowsByAppID(ctx, store, app.ID)
	if err != nil {
		return nil, err
	}

	target := opts.pickTarget(workflows)
	if target == nil {
		workflow := newApplicationWorkflow(app, workflows, workflowStep, opts)
		if err := repository.CreateWorkflow(ctx, store, workflow); err != nil {
			klog.Errorf("%s: %v", opts.createErrLog, err)
			return nil, bcode.ErrCreateWorkflow
		}
		return workflow, nil
	}

	updateApplicationWorkflowFields(target, app, workflows, workflowStep, opts)

	if err := store.Put(ctx, target); err != nil {
		return nil, err
	}
	if opts.setCallbackOnUpdate && opts.callback == nil {
		if err := updateWorkflowCallbackField(ctx, store, target.ID, nil); err != nil {
			return nil, err
		}
	}
	return target, nil
}

func newApplicationWorkflow(app *model.Applications, workflows []*model.Workflow, workflowStep *model.JSONStruct, opts workflowUpsertOptions) *model.Workflow {
	return &model.Workflow{
		ID:           utils.RandStringByNumLowercase(24),
		Name:         ensureUniqueWorkflowName(opts.desiredName, workflows),
		Namespace:    app.Namespace,
		AppID:        app.ID,
		Alias:        opts.desiredAlias,
		ProjectID:    app.Project,
		Description:  app.Description,
		WorkflowType: opts.workflowType,
		Status:       config.StatusCreated,
		Steps:        workflowStep,
		Callback:     opts.callback,
	}
}

func updateApplicationWorkflowFields(target *model.Workflow, app *model.Applications, workflows []*model.Workflow, workflowStep *model.JSONStruct, opts workflowUpsertOptions) {
	if opts.desiredName != "" && !strings.EqualFold(target.Name, opts.desiredName) {
		target.Name = ensureUniqueWorkflowNameExcluding(opts.desiredName, workflows, target.ID)
	}
	if opts.alwaysUpdateAlias || opts.desiredAlias != "" {
		target.Alias = opts.desiredAlias
	}
	target.Namespace = app.Namespace
	target.ProjectID = app.Project
	target.Description = app.Description
	if opts.setWorkflowTypeOnUpdate {
		target.WorkflowType = opts.workflowType
	}
	if opts.setCallbackOnUpdate {
		target.Callback = opts.callback
	}
	target.Steps = workflowStep
}

func updateWorkflowCallbacksForApp(ctx context.Context, store datastore.DataStore, appID string, callback *model.JSONStruct) error {
	workflows, err := repository.FindWorkflowsByAppID(ctx, store, appID)
	if err != nil {
		return err
	}
	for _, workflow := range workflows {
		if workflow == nil {
			continue
		}
		workflow.Callback = callback
		if callback == nil {
			if err := updateWorkflowCallbackField(ctx, store, workflow.ID, nil); err != nil {
				return err
			}
			continue
		}
		if err := store.Put(ctx, workflow); err != nil {
			return err
		}
	}
	return nil
}

func updateApplicationCallbackField(ctx context.Context, store datastore.DataStore, appID string, callback *model.JSONStruct) error {
	if appID == "" {
		return datastore.ErrPrimaryEmpty
	}
	app := &model.Applications{ID: appID}
	updated, err := store.CompareAndSwap(ctx, app, "id", appID, map[string]interface{}{"callback": callback})
	if err != nil {
		return err
	}
	if !updated {
		return datastore.ErrRecordNotExist
	}
	return nil
}

func updateWorkflowCallbackField(ctx context.Context, store datastore.DataStore, workflowID string, callback *model.JSONStruct) error {
	if workflowID == "" {
		return datastore.ErrPrimaryEmpty
	}
	workflow := &model.Workflow{ID: workflowID}
	updated, err := store.CompareAndSwap(ctx, workflow, "id", workflowID, map[string]interface{}{"callback": callback})
	if err != nil {
		return err
	}
	if !updated {
		return datastore.ErrRecordNotExist
	}
	return nil
}

func pickDefaultWorkflow(workflows []*model.Workflow, desiredName, desiredAlias string) *model.Workflow {
	best, bestRank := (*model.Workflow)(nil), -1
	for _, wf := range workflows {
		if wf == nil {
			continue
		}
		if wf.WorkflowType != "" && wf.WorkflowType != config.WorkflowTaskTypeWorkflow {
			continue
		}
		rank := 0
		if desiredName != "" && strings.EqualFold(wf.Name, desiredName) {
			rank = 2
		} else if desiredAlias != "" && strings.EqualFold(wf.Alias, desiredAlias) {
			rank = 1
		}
		if best == nil || rank > bestRank ||
			(rank == bestRank && wf.UpdateTime.After(best.UpdateTime)) ||
			(rank == bestRank && wf.UpdateTime.Equal(best.UpdateTime) && wf.CreateTime.After(best.CreateTime)) {
			best = wf
			bestRank = rank
		}
	}
	return best
}

func pickExecutableDefaultWorkflow(workflows []*model.Workflow, desiredName, desiredAlias string) *model.Workflow {
	filtered := make([]*model.Workflow, 0, len(workflows))
	for _, wf := range workflows {
		if wf == nil || wf.Disabled {
			continue
		}
		filtered = append(filtered, wf)
	}
	return pickDefaultWorkflow(filtered, desiredName, desiredAlias)
}

func ensureUniqueWorkflowNameExcluding(base string, workflows []*model.Workflow, excludeID string) string {
	if base == "" {
		return base
	}
	candidate := base
	suffix := 1
	for {
		inUse := false
		for _, wf := range workflows {
			if wf == nil || wf.ID == excludeID {
				continue
			}
			if strings.EqualFold(wf.Name, candidate) {
				inUse = true
				break
			}
		}
		if !inUse {
			return candidate
		}
		candidate = fmt.Sprintf("%s-%d", base, suffix)
		suffix++
	}
}

func pickWorkflowByType(workflows []*model.Workflow, workflowType config.WorkflowTaskType) *model.Workflow {
	var best *model.Workflow
	for _, wf := range workflows {
		if wf == nil || wf.WorkflowType != workflowType {
			continue
		}
		if best == nil || wf.UpdateTime.After(best.UpdateTime) ||
			(wf.UpdateTime.Equal(best.UpdateTime) && wf.CreateTime.After(best.CreateTime)) {
			best = wf
		}
	}
	return best
}

func (c *applicationsServiceImpl) refreshExistingApplication(ctx context.Context, store datastore.DataStore, req apisv1.CreateApplicationsRequest) (*model.Applications, error) {
	if store == nil {
		return nil, fmt.Errorf("datastore is not initialized")
	}
	application, err := repository.ApplicationByID(ctx, store, req.ID)
	if err != nil {
		return nil, bcode.ErrApplicationNotExist
	}
	targetTemplateEnabled := application.TemplateEnabled
	if req.TemplateEnabled != nil {
		targetTemplateEnabled = *req.TemplateEnabled
	}
	if targetTemplateEnabled {
		if !application.TemplateEnabled && application.Name != req.Name {
			return nil, fmt.Errorf("%w: application name is immutable", bcode.ErrApplicationConfig)
		}
		targetNamespace := application.Namespace
		if req.Namespace != "" {
			targetNamespace = req.Namespace
		}
		if err := ensureTemplateApplicationKeyAvailableInStore(ctx, store, targetNamespace, req.Name, req.Version, application.ID); err != nil {
			return nil, err
		}
	} else {
		if application.Name != req.Name {
			return nil, fmt.Errorf("%w: application name is immutable", bcode.ErrApplicationConfig)
		}
		targetNamespace := application.Namespace
		if req.Namespace != "" {
			targetNamespace = req.Namespace
		}
		if err := ensureStandardApplicationNameAvailableInStore(ctx, store, targetNamespace, req.Name, application.ID); err != nil {
			return nil, err
		}
	}

	application.Name = req.Name
	application.Version = req.Version
	application.Alias = req.Alias
	application.Project = req.Project
	application.Description = req.Description
	application.Icon = req.Icon
	if req.Namespace != "" {
		application.Namespace = req.Namespace
	}
	if req.TemplateEnabled != nil {
		application.TemplateEnabled = *req.TemplateEnabled
	}
	return application, nil
}

func ensureStandardApplicationNameAvailableInStore(ctx context.Context, store datastore.DataStore, namespace, name, currentAppID string) error {
	if store == nil {
		return fmt.Errorf("datastore is not initialized")
	}
	apps, err := repository.ListApplicationsByQuery(ctx, store, &model.Applications{Name: name}, datastore.ListOptions{})
	if err != nil {
		return err
	}
	for _, app := range apps {
		if app == nil || app.TemplateEnabled || app.ID == currentAppID || app.Namespace != namespace {
			continue
		}
		return bcode.ErrApplicationExist
	}
	return nil
}

func ensureTemplateApplicationKeyAvailableInStore(ctx context.Context, store datastore.DataStore, namespace, name, version, currentAppID string) error {
	existing, err := findTemplateApplicationInStore(ctx, store, namespace, name, version)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil
		}
		return err
	}
	if existing.ID != "" && existing.ID != currentAppID {
		return bcode.ErrApplicationExist
	}
	return nil
}

func findTemplateApplicationInStore(ctx context.Context, store datastore.DataStore, namespace, name, version string) (*model.Applications, error) {
	if store == nil {
		return nil, fmt.Errorf("datastore is not initialized")
	}
	if name == "" || version == "" {
		return nil, datastore.ErrRecordNotExist
	}
	apps, err := repository.ListApplicationsByQuery(ctx, store, &model.Applications{
		Name:            name,
		Version:         version,
		TemplateEnabled: true,
	}, datastore.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, app := range apps {
		if app != nil && app.Namespace == namespace {
			return app, nil
		}
	}
	return nil, datastore.ErrRecordNotExist
}

func (c *applicationsServiceImpl) ensureStandardApplicationNameAvailable(ctx context.Context, namespace, name, currentAppID string) error {
	return ensureStandardApplicationNameAvailableInStore(ctx, c.Store, namespace, name, currentAppID)
}

func (c *applicationsServiceImpl) ensureTemplateApplicationKeyAvailable(ctx context.Context, namespace, name, version, currentAppID string) error {
	return ensureTemplateApplicationKeyAvailableInStore(ctx, c.Store, namespace, name, version, currentAppID)
}

func (c *applicationsServiceImpl) findTemplateApplication(ctx context.Context, namespace, name, version string) (*model.Applications, error) {
	return findTemplateApplicationInStore(ctx, c.Store, namespace, name, version)
}

func prepareComponents(appID, namespace string, reqComponents []apisv1.CreateComponentRequest) ([]*model.ApplicationComponent, error) {
	components := make([]*model.ApplicationComponent, 0, len(reqComponents))
	names := make(map[string]bool, len(reqComponents))
	for idx, reqComponent := range reqComponents {
		if err := componentValidationError(ValidateComponent(&reqComponent, fmt.Sprintf("component[%d]", idx), names)); err != nil {
			return nil, err
		}
		normalizeJobFailurePolicy(&reqComponent.Properties)

		reqComponent.Namespace = namespace
		if reqComponent.ComponentType == config.SecretJob {
			reqComponent.Properties.Secret = copyStringMap(reqComponent.Properties.Secret)
		}
		component := workflowservice.ConvertComponent(&reqComponent, appID)

		properties, err := model.NewJSONStructByStruct(reqComponent.Properties)
		if err != nil {
			klog.Errorf("new properties failure,%s", err.Error())
			return nil, bcode.ErrInvalidProperties
		}
		component.Properties = properties

		traits, err := model.NewJSONStructByStruct(reqComponent.Traits)
		if err != nil {
			klog.Errorf("new trait failure,%s", err.Error())
			return nil, bcode.ErrInvalidProperties
		}
		component.Traits = traits

		components = append(components, component)
	}
	return components, nil
}

func copyStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	copied := make(map[string]string, len(source))
	for key, value := range source {
		copied[key] = value
	}
	return copied
}

// validateComponentTraitsForWrite checks a trait patch for an existing
// component with the same rules as Try and application creation.
func validateComponentTraitsForWrite(componentType config.JobType, traits apisv1.Traits, fieldPrefix string) error {
	if err := spec.NormalizeComponentEvaluation(string(componentType), "", spec.Properties{}, &traits); err != nil {
		return fmt.Errorf("%w: %s: %v", bcode.ErrApplicationConfig, fieldPrefix, err)
	}
	return componentValidationError(traitvalidation.ValidateTraits(traits, fieldPrefix, componentType))
}

func (c *applicationsServiceImpl) UpdateApplicationWorkflow(ctx context.Context, appID string, req apisv1.UpdateApplicationWorkflowRequest) (*apisv1.UpdateWorkflowResponse, error) {
	apisv1.NormalizeWorkflowSteps(req.Workflow)
	var response *apisv1.UpdateWorkflowResponse
	_, err := c.withWritableApplicationLock(ctx, appID, "update-application-workflow", func(lockCtx context.Context, app *model.Applications) error {
		var updateErr error
		response, updateErr = c.updateApplicationWorkflowLocked(lockCtx, appID, app, req)
		return updateErr
	})
	if err != nil {
		return response, err
	}
	return response, nil
}

func (c *applicationsServiceImpl) updateApplicationWorkflowLocked(ctx context.Context, appID string, app *model.Applications, req apisv1.UpdateApplicationWorkflowRequest) (*apisv1.UpdateWorkflowResponse, error) {
	workflowType, err := validateUpdateApplicationWorkflowRequest(appID, req)
	if err != nil {
		return nil, err
	}
	app, err = c.applicationForRequestedID(ctx, appID, app)
	if err != nil {
		return nil, err
	}
	if err := workflowservice.EnsureAppWorkflowIdle(ctx, c.Store, app.ID); err != nil {
		return nil, err
	}

	components, err := c.ComponentRepo.FindByAppID(ctx, app.ID)
	if err != nil {
		return nil, err
	}
	if err := validateWorkflowComponentRefs(req.Workflow, workflowComponentTypesFromModels(components)); err != nil {
		klog.Errorf("component validation failed for app=%s workflowId=%s: %v", appID, req.WorkflowID, err)
		return nil, err
	}

	callback, err := normalizeWorkflowCallbackForWrite(ctx, c.Cfg, c.URLSecurityPolicyProvider, req.Callback)
	if err != nil {
		return nil, err
	}
	var callbackStruct *model.JSONStruct
	if callback != nil {
		callbackStruct, err = model.NewJSONStructByStruct(callback)
		if err != nil {
			return nil, bcode.ErrWorkflowConfig
		}
	}

	workflows, err := c.WorkflowRepo.FindByAppID(ctx, app.ID)
	if err != nil {
		return nil, err
	}

	target, targetName, err := c.resolveWorkflowUpdateTarget(ctx, app, req, workflows)
	if err != nil {
		return nil, err
	}

	workflowSteps := convertWorkflowStepsFromRequest(req.Workflow)
	failurePolicy := req.FailurePolicy
	if target != nil && !updateWorkflowFailurePolicySpecified(req) {
		failurePolicy, err = storedWorkflowFailurePolicy(target.Steps)
		if err != nil {
			return nil, fmt.Errorf("read workflow failurePolicy: %w", err)
		}
	}
	applyWorkflowFailurePolicy(workflowSteps, failurePolicy)
	stepsStruct, err := model.NewJSONStructByStruct(workflowSteps)
	if err != nil {
		return nil, fmt.Errorf("marshal workflow steps: %w", err)
	}

	transactional, ok := c.Store.(datastore.Transactional)
	if !ok {
		return nil, fmt.Errorf("workflow update requires transactional datastore")
	}
	if target == nil {
		namespace, projectID, description := deriveWorkflowMetadata(app, workflows)
		if workflowType == "" {
			workflowType = config.WorkflowTaskTypeWorkflow
		}
		target = &model.Workflow{
			ID:           utils.RandStringByNumLowercase(24),
			Name:         targetName,
			Alias:        req.Alias,
			Namespace:    namespace,
			Disabled:     false,
			ProjectID:    projectID,
			AppID:        app.ID,
			Description:  description,
			WorkflowType: workflowType,
			Status:       config.StatusCreated,
			Callback:     callbackStruct,
		}
		target.Steps = stepsStruct
		if err := transactional.WithTransaction(ctx, func(tx datastore.DataStore) error {
			return repository.CreateWorkflow(ctx, tx, target)
		}); err != nil {
			return nil, err
		}
	} else {
		if targetName != "" {
			target.Name = targetName
		}
		if req.Alias != "" {
			target.Alias = req.Alias
		}
		if req.Callback != nil {
			target.Callback = callbackStruct
		}
		if workflowType != "" {
			target.WorkflowType = workflowType
		}
		target.Steps = stepsStruct
		// Put skips nil fields, so clearing the callback is a second write that
		// must commit together with the new steps.
		if err := transactional.WithTransaction(ctx, func(tx datastore.DataStore) error {
			if err := tx.Put(ctx, target); err != nil {
				return err
			}
			if req.Callback != nil && callbackStruct == nil {
				return updateWorkflowCallbackField(ctx, tx, target.ID, nil)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	c.invalidateApplicationListCaches(ctx)
	return &apisv1.UpdateWorkflowResponse{WorkflowID: target.ID}, nil
}

func validateUpdateApplicationWorkflowRequest(appID string, req apisv1.UpdateApplicationWorkflowRequest) (config.WorkflowTaskType, error) {
	if appID == "" {
		return "", bcode.ErrApplicationNotExist
	}
	if len(req.Workflow) == 0 {
		return "", bcode.ErrWorkflowConfig
	}
	workflowType := config.NormalizeWorkflowTaskType(req.WorkflowType)
	if workflowType != "" && !config.IsSupportedWorkflowTaskType(workflowType) {
		return "", bcode.ErrWorkflowConfig
	}
	if err := validateWorkflowFailurePolicy(req.FailurePolicy); err != nil {
		return "", err
	}
	return workflowType, nil
}

func (c *applicationsServiceImpl) resolveWorkflowUpdateTarget(ctx context.Context, app *model.Applications, req apisv1.UpdateApplicationWorkflowRequest, workflows []*model.Workflow) (*model.Workflow, string, error) {
	targetName := strings.ToLower(strings.TrimSpace(req.Name))
	var target *model.Workflow
	if req.WorkflowID != "" {
		wf, err := c.WorkflowRepo.FindByID(ctx, req.WorkflowID)
		if err != nil {
			if errors.Is(err, datastore.ErrRecordNotExist) {
				return nil, "", bcode.ErrWorkflowNotExist
			}
			return nil, "", err
		}
		if wf.AppID != app.ID {
			return nil, "", bcode.ErrWorkflowConfig
		}
		target = wf
		if targetName == "" {
			targetName = target.Name
		}
	} else {
		if targetName == "" {
			targetName = fmt.Sprintf("%s-workflow", strings.ToLower(app.Name))
		}
		targetName = ensureUniqueWorkflowName(targetName, workflows)
	}

	return target, targetName, nil
}

func (c *applicationsServiceImpl) validateComponentNodePools(ctx context.Context, components []apisv1.CreateComponentRequest) error {
	for i, component := range components {
		errors := traitvalidation.ValidateNodePoolRef(ctx, c.NodePoolRepo, component.Traits, fmt.Sprintf("component[%d].traits", i))
		if len(errors) > 0 {
			return fmt.Errorf("%w: %s: %s", bcode.ErrApplicationConfig, errors[0].Field, errors[0].Message)
		}
	}
	return nil
}
