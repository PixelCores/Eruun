package application

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/schedulelock"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

const (
	databaseResetWorkflowAlias = "database-reset"
	databaseResetWorkflowName  = "database-reset"
)

func (c *applicationsServiceImpl) ResetApplicationDatabases(ctx context.Context, appID string, req apisv1.DatabaseResetRequest) (*apisv1.DatabaseResetResponse, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	lockProvider, err := c.appScheduleLocker()
	if err != nil {
		return nil, err
	}
	var resp *apisv1.DatabaseResetResponse
	err = schedulelock.WithAppScheduleLock(ctx, lockProvider, appID, "reset-application-databases", true, func(lockCtx context.Context) error {
		var lockErr error
		resp, lockErr = c.resetApplicationDatabasesUnlocked(lockCtx, appID, req)
		return lockErr
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *applicationsServiceImpl) resetApplicationDatabasesUnlocked(ctx context.Context, appID string, req apisv1.DatabaseResetRequest) (*apisv1.DatabaseResetResponse, error) {

	app, err := c.AppRepo.FindByID(ctx, appID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}
	if app.EffectiveManagementMode() != config.ManagementModeNative {
		return nil, fmt.Errorf("%w: database reset is disabled for %s applications",
			bcode.ErrApplicationManagementMode, app.EffectiveManagementMode())
	}

	components, err := c.ComponentRepo.FindByAppID(ctx, app.ID)
	if err != nil {
		return nil, err
	}
	setResourceAppNameForComponents(components, applicationResourceNameKey(app))

	databaseComponents, err := selectDatabaseResetComponents(req.Components, components)
	if err != nil {
		return nil, err
	}
	initSQLURL, err := normalizeDatabaseResetInitSQLURL(req.InitSQLURL, req.InitSQLURLProvided())
	if err != nil {
		return nil, err
	}
	txStore, ok := c.Store.(datastore.Transactional)
	if !ok {
		return nil, fmt.Errorf("%w: database reset requires transactional datastore", bcode.ErrExecWorkflow)
	}
	var (
		workflow *model.Workflow
		task     *model.WorkflowQueue
	)
	lockedApp, err := c.AppRepo.FindByID(ctx, app.ID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}
	if lockedApp.EffectiveManagementMode() != config.ManagementModeNative {
		return nil, fmt.Errorf("%w: database reset is disabled for %s applications",
			bcode.ErrApplicationManagementMode, lockedApp.EffectiveManagementMode())
	}
	app = lockedApp
	err = txStore.WithTransaction(ctx, func(tx datastore.DataStore) error {
		if err := EnsureAppWorkflowIdle(ctx, tx, app.ID); err != nil {
			return err
		}
		if err := EnsureNoPendingStatefulSetCleanup(ctx, tx, app.ID); err != nil {
			return err
		}
		workflowSteps, err := model.NewJSONStructByStruct(buildDatabaseResetWorkflowSteps(databaseComponents, initSQLURL))
		if err != nil {
			return fmt.Errorf("%w: encode database reset workflow steps: %v", bcode.ErrCreateWorkflow, err)
		}
		workflow = buildDatabaseResetWorkflow(app, workflowSteps)
		if err := repository.CreateWorkflow(ctx, tx, workflow); err != nil {
			klog.ErrorS(err, "create database reset workflow failed", "appID", app.ID)
			return bcode.ErrCreateWorkflow
		}
		if err := validateWorkflowTaskEnqueue(ctx, tx, workflow, false); err != nil {
			return err
		}
		task, err = createWorkflowQueueTaskWithCleanupInfo(ctx, tx, workflow, 0, "", "")
		return err
	})
	if err != nil {
		return nil, err
	}
	if workflow == nil || task == nil {
		return nil, fmt.Errorf("%w: database reset workflow task was not created", bcode.ErrExecWorkflow)
	}

	return &apisv1.DatabaseResetResponse{
		AppID:              app.ID,
		WorkflowID:         workflow.ID,
		TaskID:             task.TaskID,
		DatabaseComponents: databaseComponents,
		RestartComponents:  []string{},
	}, nil
}

func selectDatabaseResetComponents(requested []string, components []*model.ApplicationComponent) ([]string, error) {
	if len(requested) == 0 {
		return nil, bcode.ErrApplicationConfig
	}
	componentsByName := make(map[string]*model.ApplicationComponent, len(components))
	for _, component := range components {
		if component == nil {
			continue
		}
		componentsByName[strings.ToLower(strings.TrimSpace(component.Name))] = component
	}

	seen := make(map[string]struct{}, len(requested))
	selected := make([]string, 0, len(requested))
	for _, raw := range requested {
		name := strings.TrimSpace(raw)
		if name == "" {
			return nil, fmt.Errorf("%w: database reset component name is required", bcode.ErrApplicationConfig)
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			continue
		}
		component := componentsByName[key]
		if component == nil {
			return nil, fmt.Errorf("%w: component %q not found", bcode.ErrApplicationConfig, name)
		}
		if component.ComponentType != config.StoreJob {
			return nil, fmt.Errorf("%w: component %q is not a store component", bcode.ErrApplicationConfig, name)
		}
		seen[key] = struct{}{}
		selected = append(selected, component.Name)
	}
	if len(selected) == 0 {
		return nil, bcode.ErrApplicationConfig
	}
	return selected, nil
}

func buildDatabaseResetWorkflow(app *model.Applications, steps *model.JSONStruct) *model.Workflow {
	namespace := app.Namespace
	if strings.TrimSpace(namespace) == "" {
		namespace = config.DefaultNamespace
	}
	return &model.Workflow{
		ID:           utils.RandStringByNumLowercase(24),
		Name:         fmt.Sprintf("%s-%s", databaseResetWorkflowName, utils.RandStringByNumLowercase(8)),
		Namespace:    namespace,
		Alias:        databaseResetWorkflowAlias,
		Disabled:     false,
		ProjectID:    app.Project,
		AppID:        app.ID,
		Description:  "database reset",
		WorkflowType: config.WorkflowTaskTypeDatabaseReset,
		Status:       config.StatusCreated,
		Steps:        steps,
	}
}

func buildDatabaseResetWorkflowSteps(databaseComponents []string, initSQLURL string) *model.WorkflowSteps {
	return &model.WorkflowSteps{Steps: []*model.WorkflowStep{{
		Name:         databaseResetWorkflowName,
		WorkflowType: config.JobDatabaseReset,
		Mode:         config.WorkflowModeStepByStep,
		Properties: []model.Policies{{
			Policies:   append([]string(nil), databaseComponents...),
			InitSQLURL: initSQLURL,
		}},
	}}}
}

func normalizeDatabaseResetInitSQLURL(raw string, provided bool) (string, error) {
	if !provided {
		return "", nil
	}
	value := strings.TrimSpace(raw)
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed == nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", fmt.Errorf("%w: initSqlUrl must be an absolute HTTP(S) URL", bcode.ErrApplicationConfig)
	}
	return value, nil
}
