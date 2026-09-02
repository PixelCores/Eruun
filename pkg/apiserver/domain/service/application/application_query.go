package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

const workflowAppIDColumn = "app_id"
const applicationsSortKeyUpdateTime = "update_time"
const applicationsSortKeyID = "id"

func normalizeBatchApplicationIDs(appIDs []string) ([]string, []string, error) {
	if len(appIDs) == 0 {
		return nil, nil, bcode.ErrApplicationConfig
	}

	normalized := make([]string, 0, len(appIDs))
	unique := make([]string, 0, len(appIDs))
	seen := make(map[string]struct{}, len(appIDs))
	for _, appID := range appIDs {
		trimmedID := strings.TrimSpace(appID)
		if trimmedID == "" {
			return nil, nil, bcode.ErrApplicationConfig
		}
		normalized = append(normalized, trimmedID)
		if _, ok := seen[trimmedID]; ok {
			continue
		}
		seen[trimmedID] = struct{}{}
		unique = append(unique, trimmedID)
	}
	return normalized, unique, nil
}

// ListApplications list applications
func (c *applicationsServiceImpl) ListApplications(ctx context.Context, opts ListApplicationsOptions) ([]*apisv1.ApplicationBase, error) {
	if opts.FullScan() {
		var cached []*apisv1.ApplicationBase
		if c.loadJSONCache(applicationListCacheKey, &cached) {
			return cached, nil
		}
	}

	listOptions := datastore.ListOptions{
		Page:     0,
		PageSize: 0,
		SortBy:   applicationListSortOptions(),
	}
	if !opts.FullScan() {
		listOptions.Page = opts.NormalizedPage()
		listOptions.PageSize = opts.PageSize
	}

	apps, err := c.AppRepo.List(ctx, listOptions)
	if err != nil {
		return nil, err
	}

	list, err := c.applicationBasesWithDefaultWorkflow(ctx, apps)
	if err != nil {
		return nil, err
	}
	if opts.FullScan() {
		c.storeJSONCache(applicationListCacheKey, list)
	}
	return list, nil
}

func (c *applicationsServiceImpl) BatchGetApplications(ctx context.Context, appIDs []string) (*apisv1.BatchGetApplicationsResponse, error) {
	orderedAppIDs, uniqueAppIDs, err := normalizeBatchApplicationIDs(appIDs)
	if err != nil {
		return nil, err
	}

	appsByID, err := c.applicationsByIDs(ctx, uniqueAppIDs)
	if err != nil {
		return nil, err
	}
	workflowIDs, err := c.defaultWorkflowIDsByAppIDs(ctx, uniqueAppIDs)
	if err != nil {
		return nil, err
	}

	componentsByAppID := make(map[string][]*apisv1.BatchApplicationComponent, len(uniqueAppIDs))
	resp := &apisv1.BatchGetApplicationsResponse{
		Applications: make([]*apisv1.ApplicationWithComponents, 0, len(orderedAppIDs)),
	}
	for _, appID := range orderedAppIDs {
		app, ok := appsByID[appID]
		if !ok || app == nil {
			return nil, bcode.ErrApplicationNotExist
		}
		components, ok := componentsByAppID[appID]
		if !ok {
			modelComponents, err := c.ListApplicationComponents(ctx, appID)
			if err != nil {
				return nil, err
			}
			components, err = convertBatchApplicationComponents(modelComponents)
			if err != nil {
				return nil, err
			}
			componentsByAppID[appID] = components
		}

		base := assembler.ConvertAppModelToBase(app, workflowIDs[appID])
		if base == nil {
			return nil, fmt.Errorf("convert application %s to dto failed", appID)
		}
		resp.Applications = append(resp.Applications, &apisv1.ApplicationWithComponents{
			ApplicationBase: *base,
			Components:      components,
		})
	}

	return resp, nil
}

func convertBatchApplicationComponents(components []*model.ApplicationComponent) ([]*apisv1.BatchApplicationComponent, error) {
	result := make([]*apisv1.BatchApplicationComponent, 0, len(components))
	for _, component := range components {
		if component == nil {
			continue
		}
		var properties model.Properties
		if err := decodeJSONStruct(component.Properties, &properties); err != nil {
			return nil, fmt.Errorf("convert component %s properties: %w", component.Name, err)
		}
		result = append(result, &apisv1.BatchApplicationComponent{
			ID:            component.ID,
			AppID:         component.AppID,
			Name:          component.Name,
			Namespace:     component.Namespace,
			Replicas:      component.Replicas,
			ComponentType: component.ComponentType,
			Properties: apisv1.BatchApplicationComponentProperties{
				Ports: properties.Ports,
			},
		})
	}
	return result, nil
}

func (c *applicationsServiceImpl) applicationsByIDs(ctx context.Context, appIDs []string) (map[string]*model.Applications, error) {
	if len(appIDs) == 0 {
		return map[string]*model.Applications{}, nil
	}
	if c.AppRepo == nil {
		return nil, fmt.Errorf("application repository is nil")
	}

	requested := make(map[string]struct{}, len(appIDs))
	for _, appID := range appIDs {
		requested[appID] = struct{}{}
	}

	applications, err := c.AppRepo.FindByIDs(ctx, appIDs)
	if err != nil {
		return nil, err
	}

	appsByID := make(map[string]*model.Applications, len(appIDs))
	for _, app := range applications {
		if app == nil {
			continue
		}
		appID := strings.TrimSpace(app.ID)
		if appID == "" {
			continue
		}
		if _, ok := requested[appID]; !ok {
			continue
		}
		appsByID[appID] = app
	}

	for _, appID := range appIDs {
		if _, ok := appsByID[appID]; !ok {
			return nil, bcode.ErrApplicationNotExist
		}
	}
	return appsByID, nil
}

// ListTemplateApplications lists applications marked as templates (templateEnabled=true)
func (c *applicationsServiceImpl) ListTemplateApplications(ctx context.Context, opts ListApplicationsOptions) ([]*apisv1.ApplicationBase, error) {
	if opts.FullScan() {
		var cached []*apisv1.ApplicationBase
		if c.loadJSONCache(templateApplicationListCacheKey, &cached) {
			return cached, nil
		}
	}

	listOptions := datastore.ListOptions{
		Page:     0,
		PageSize: 0,
		SortBy:   applicationListSortOptions(),
	}
	if !opts.FullScan() {
		listOptions.Page = opts.NormalizedPage()
		listOptions.PageSize = opts.PageSize
	}

	apps, err := c.AppRepo.ListByQuery(ctx, &model.Applications{TemplateEnabled: true}, listOptions)
	if err != nil {
		return nil, err
	}

	list, err := c.applicationBasesWithDefaultWorkflow(ctx, apps)
	if err != nil {
		return nil, err
	}
	if err := c.enrichTemplateApplicationResourceSummary(ctx, list); err != nil {
		return nil, err
	}
	if opts.FullScan() {
		c.storeJSONCache(templateApplicationListCacheKey, list)
	}
	return list, nil
}

func (c *applicationsServiceImpl) enrichTemplateApplicationResourceSummary(ctx context.Context, apps []*apisv1.ApplicationBase) error {
	appIDs := make([]string, 0, len(apps))
	for _, app := range apps {
		if app == nil {
			continue
		}
		appID := strings.TrimSpace(app.ID)
		if appID == "" {
			continue
		}
		appIDs = append(appIDs, appID)
	}
	componentsByAppID, err := c.componentsByAppIDs(ctx, appIDs)
	if err != nil {
		return err
	}
	for _, app := range apps {
		if app == nil {
			continue
		}
		component := selectTemplateMainComponent(componentsByAppID[strings.TrimSpace(app.ID)])
		if component == nil {
			continue
		}
		resources, err := summarizeApplicationResourcesFromModelComponent(component)
		if err != nil {
			return fmt.Errorf("convert template component %s traits: %w", component.Name, err)
		}
		app.Resources = resources
	}
	return nil
}

func (c *applicationsServiceImpl) componentsByAppIDs(ctx context.Context, appIDs []string) (map[string][]*model.ApplicationComponent, error) {
	normalized := make([]string, 0, len(appIDs))
	requested := make(map[string]struct{}, len(appIDs))
	for _, appID := range appIDs {
		appID = strings.TrimSpace(appID)
		if appID == "" {
			continue
		}
		if _, exists := requested[appID]; exists {
			continue
		}
		requested[appID] = struct{}{}
		normalized = append(normalized, appID)
	}
	if len(normalized) == 0 {
		return map[string][]*model.ApplicationComponent{}, nil
	}
	if c.Store == nil {
		return nil, fmt.Errorf("datastore is nil")
	}
	entities, err := c.Store.List(ctx, &model.ApplicationComponent{}, &datastore.ListOptions{
		FilterOptions: datastore.FilterOptions{
			In: []datastore.InQueryOption{
				{Key: workflowAppIDColumn, Values: normalized},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	result := make(map[string][]*model.ApplicationComponent, len(normalized))
	for _, entity := range entities {
		component, ok := entity.(*model.ApplicationComponent)
		if !ok {
			return nil, fmt.Errorf("unexpected component entity type: %T", entity)
		}
		if component == nil {
			continue
		}
		appID := strings.TrimSpace(component.AppID)
		if _, ok := requested[appID]; !ok {
			continue
		}
		result[appID] = append(result[appID], component)
	}
	return result, nil
}

func selectTemplateMainComponent(components []*model.ApplicationComponent) *model.ApplicationComponent {
	candidates := make([]*model.ApplicationComponent, 0, len(components))
	for _, component := range components {
		if component == nil || !isApplicationResourceSummaryComponentType(component.ComponentType) {
			continue
		}
		candidates = append(candidates, component)
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].ID != candidates[j].ID {
			return candidates[i].ID < candidates[j].ID
		}
		return strings.TrimSpace(candidates[i].Name) < strings.TrimSpace(candidates[j].Name)
	})
	return candidates[0]
}

func (c *applicationsServiceImpl) applicationBasesWithDefaultWorkflow(ctx context.Context, apps []*model.Applications) ([]*apisv1.ApplicationBase, error) {
	appIDs := make([]string, 0, len(apps))
	for _, app := range apps {
		if app == nil {
			continue
		}
		appIDs = append(appIDs, app.ID)
	}
	workflowIDs, err := c.defaultWorkflowIDsByAppIDs(ctx, appIDs)
	if err != nil {
		return nil, err
	}

	var list []*apisv1.ApplicationBase
	for _, app := range apps {
		if app == nil {
			continue
		}
		workflowID := workflowIDs[strings.TrimSpace(app.ID)]
		appBase := assembler.ConvertAppModelToBase(app, workflowID)
		list = append(list, appBase)
	}
	sort.Slice(list, func(i, j int) bool {
		left := list[i].UpdateTime.UnixNano()
		right := list[j].UpdateTime.UnixNano()
		if left != right {
			return left > right
		}
		return strings.TrimSpace(list[i].ID) < strings.TrimSpace(list[j].ID)
	})
	return list, nil
}

func applicationListSortOptions() []datastore.SortOption {
	return []datastore.SortOption{
		{
			Key:   applicationsSortKeyUpdateTime,
			Order: datastore.SortOrderDescending,
		},
		{
			Key:   applicationsSortKeyID,
			Order: datastore.SortOrderAscending,
		},
	}
}

func (c *applicationsServiceImpl) defaultWorkflowIDByAppID(ctx context.Context, appID string) (string, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return "", nil
	}

	workflowIDs, err := c.defaultWorkflowIDsByAppIDs(ctx, []string{appID})
	if err != nil {
		return "", err
	}
	return workflowIDs[appID], nil
}

func (c *applicationsServiceImpl) defaultWorkflowIDsByAppIDs(ctx context.Context, appIDs []string) (map[string]string, error) {
	uniqueAppIDs := make([]string, 0, len(appIDs))
	requested := make(map[string]struct{}, len(appIDs))
	for _, appID := range appIDs {
		trimmedID := strings.TrimSpace(appID)
		if trimmedID == "" {
			continue
		}
		if _, exists := requested[trimmedID]; exists {
			continue
		}
		requested[trimmedID] = struct{}{}
		uniqueAppIDs = append(uniqueAppIDs, trimmedID)
	}
	if len(uniqueAppIDs) == 0 {
		return map[string]string{}, nil
	}

	workflowsByAppID := make(map[string][]*model.Workflow, len(uniqueAppIDs))
	if c.Store != nil {
		entities, err := c.Store.List(ctx, &model.Workflow{}, &datastore.ListOptions{
			FilterOptions: datastore.FilterOptions{
				In: []datastore.InQueryOption{
					{Key: workflowAppIDColumn, Values: uniqueAppIDs},
				},
			},
		})
		if err != nil {
			return nil, err
		}
		for _, entity := range entities {
			workflow, ok := entity.(*model.Workflow)
			if !ok || workflow == nil {
				continue
			}
			appID := strings.TrimSpace(workflow.AppID)
			if appID == "" {
				continue
			}
			if _, exists := requested[appID]; !exists {
				continue
			}
			workflowsByAppID[appID] = append(workflowsByAppID[appID], workflow)
		}
	} else {
		for _, appID := range uniqueAppIDs {
			workflows, err := c.WorkflowRepo.FindByAppID(ctx, appID)
			if err != nil {
				return nil, err
			}
			workflowsByAppID[appID] = workflows
		}
	}

	workflowIDs := make(map[string]string, len(uniqueAppIDs))
	for _, appID := range uniqueAppIDs {
		workflow := pickDefaultWorkflow(workflowsByAppID[appID], "", "")
		if workflow == nil {
			continue
		}
		workflowIDs[appID] = workflow.ID
	}
	return workflowIDs, nil
}

// GetApplication get application model
func (c *applicationsServiceImpl) GetApplication(ctx context.Context, appName string) (*model.Applications, error) {
	app, err := c.AppRepo.FindByName(ctx, appName)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}
	return app, nil
}

// DeleteApplication delete application
func (c *applicationsServiceImpl) DeleteApplication(ctx context.Context, app *model.Applications) error {
	if err := c.AppRepo.Delete(ctx, app); err != nil {
		return err
	}
	c.invalidateApplicationListCaches()
	if app != nil {
		c.invalidateApplicationComponentsCache(app.ID)
	}
	return nil
}

func (c *applicationsServiceImpl) ListApplicationComponents(ctx context.Context, appID string) ([]*model.ApplicationComponent, error) {
	app, err := c.findApplicationForComponentRead(ctx, appID)
	if err != nil {
		return nil, err
	}
	appID = app.ID
	componentCacheKey := applicationComponentsCacheKey(appID)
	var cached []*model.ApplicationComponent
	if c.loadJSONCache(componentCacheKey, &cached) {
		result, _, err := c.prepareApplicationComponentsForRead(cached, false)
		if err != nil {
			return nil, err
		}
		setResourceAppNameForComponents(result, applicationResourceNameKey(app))
		return result, nil
	}

	components, err := c.ComponentRepo.FindByAppID(ctx, app.ID)
	if err != nil {
		return nil, err
	}
	setResourceAppNameForComponents(components, applicationResourceNameKey(app))
	if len(components) == 0 {
		c.storeJSONCache(componentCacheKey, []*model.ApplicationComponent(nil))
		return nil, nil
	}
	result, cacheValue, err := c.prepareApplicationComponentsForRead(components, true)
	if err != nil {
		return nil, err
	}
	c.storeJSONCache(componentCacheKey, cacheValue)
	return result, nil
}

// ListApplicationRuntimeComponents returns component runtime fields from the
// repository without consulting or updating the component detail cache.
func (c *applicationsServiceImpl) ListApplicationRuntimeComponents(ctx context.Context, appID string) ([]*model.ApplicationComponent, error) {
	app, err := c.findApplicationForComponentRead(ctx, appID)
	if err != nil {
		return nil, err
	}
	components, err := c.ComponentRepo.FindByAppID(ctx, app.ID)
	if err != nil {
		return nil, err
	}
	setResourceAppNameForComponents(components, applicationResourceNameKey(app))
	if len(components) == 0 {
		return nil, nil
	}
	result, _, err := c.prepareApplicationComponentsForRead(components, false)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *applicationsServiceImpl) findApplicationForComponentRead(ctx context.Context, appID string) (*model.Applications, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
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

func (c *applicationsServiceImpl) prepareApplicationComponentsForRead(components []*model.ApplicationComponent, buildCacheValue bool) ([]*model.ApplicationComponent, []*model.ApplicationComponent, error) {
	if len(components) == 0 {
		return nil, nil, nil
	}
	result := make([]*model.ApplicationComponent, len(components))
	var cacheValue []*model.ApplicationComponent
	if buildCacheValue {
		cacheValue = make([]*model.ApplicationComponent, len(components))
	}
	for i, component := range components {
		prepared, cacheEntry, err := c.prepareApplicationComponentForRead(component)
		if err != nil {
			return nil, nil, err
		}
		result[i] = prepared
		if buildCacheValue {
			cacheValue[i] = cacheEntry
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i] == nil || result[j] == nil {
			return result[j] != nil
		}
		return result[i].UpdateTime.Unix() > result[j].UpdateTime.Unix()
	})
	if buildCacheValue {
		sort.SliceStable(cacheValue, func(i, j int) bool {
			if cacheValue[i] == nil || cacheValue[j] == nil {
				return cacheValue[j] != nil
			}
			return cacheValue[i].UpdateTime.Unix() > cacheValue[j].UpdateTime.Unix()
		})
	}
	return result, cacheValue, nil
}

func (c *applicationsServiceImpl) prepareApplicationComponentForRead(component *model.ApplicationComponent) (*model.ApplicationComponent, *model.ApplicationComponent, error) {
	copied := cloneApplicationComponent(component)
	if copied == nil {
		return nil, nil, nil
	}
	if shouldCorrectPendingStatus(copied) || shouldCorrectUpdatingStatus(copied) || shouldCorrectDeployingStatus(copied) {
		copied.Status = string(config.ComponentStatusRunning)
		copied.LastAbnormal = ""
		klog.V(4).Infof("component %s status corrected for read path: %s -> %s", copied.Name, component.Status, copied.Status)
	}
	return copied, buildCachedApplicationComponent(component, copied), nil
}

func cloneApplicationComponent(component *model.ApplicationComponent) *model.ApplicationComponent {
	if component == nil {
		return nil
	}
	copied := *component
	return &copied
}

func buildCachedApplicationComponent(original, prepared *model.ApplicationComponent) *model.ApplicationComponent {
	if original == nil {
		return nil
	}
	cached := cloneApplicationComponent(original)
	if cached == nil || prepared == nil {
		return cached
	}
	cached.Status = prepared.Status
	cached.LastAbnormal = prepared.LastAbnormal
	return cached
}

func shouldCorrectPendingStatus(component *model.ApplicationComponent) bool {
	if component == nil {
		return false
	}
	if component.Status != string(config.ComponentStatusPending) {
		return false
	}
	if component.Replicas <= 0 {
		return false
	}
	if component.ReadyReplicas < component.Replicas {
		return false
	}
	if !componentUsesPods(component.ComponentType) {
		return false
	}
	return true
}

func shouldCorrectUpdatingStatus(component *model.ApplicationComponent) bool {
	if component == nil {
		return false
	}
	if component.Status != string(config.ComponentStatusUpdating) {
		return false
	}
	if component.Replicas <= 0 {
		return false
	}
	if component.ReadyReplicas < component.Replicas {
		return false
	}
	if !componentUsesPods(component.ComponentType) {
		return false
	}
	return true
}

func shouldCorrectDeployingStatus(component *model.ApplicationComponent) bool {
	if component == nil {
		return false
	}
	if component.Status != string(config.ComponentStatusDeploying) {
		return false
	}
	if component.Replicas <= 0 {
		return false
	}
	if component.ReadyReplicas < component.Replicas {
		return false
	}
	if !componentUsesPods(component.ComponentType) {
		return false
	}
	return true
}

func (c *applicationsServiceImpl) ListApplicationTasks(ctx context.Context, appID string) ([]*model.WorkflowQueue, error) {
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	app, err := c.AppRepo.FindByID(ctx, appID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}
	tasks, err := repository.FindWorkflowTasksByAppID(ctx, c.Store, app.ID)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return nil, nil
	}
	return tasks, nil
}

func (c *applicationsServiceImpl) HasImmediateActiveVersionUpdateTask(ctx context.Context, appID string, nowUnix int64) (bool, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return false, bcode.ErrApplicationNotExist
	}
	if c.Store == nil {
		return false, fmt.Errorf("datastore is nil")
	}
	if nowUnix <= 0 {
		nowUnix = time.Now().Unix()
	}
	tasks, err := repository.FindActiveWorkflowTasksByAppID(ctx, c.Store, appID)
	if err != nil {
		return false, err
	}
	return hasImmediateActiveVersionUpdateTask(tasks, nowUnix), nil
}

func hasImmediateActiveVersionUpdateTask(tasks []*model.WorkflowQueue, nowUnix int64) bool {
	for _, task := range tasks {
		if task == nil {
			continue
		}
		if task.ExecuteAt > nowUnix {
			continue
		}
		if !isActiveApplicationWorkflowTaskStatus(task.Status) {
			continue
		}
		if isVersionUpdateWorkflowTask(task) {
			return true
		}
	}
	return false
}

func isActiveApplicationWorkflowTaskStatus(status config.Status) bool {
	switch status {
	case "", config.StatusCreated, config.StatusRunning, config.StatusWaiting, config.StatusQueued,
		config.StatusBlocked, config.QueueItemPending, config.StatusPrepare, config.StatusWaitingApprove,
		config.StatusDistributed, config.StatusDebugBefore, config.StatusDebugAfter:
		return true
	default:
		return false
	}
}

func isVersionUpdateWorkflowTask(task *model.WorkflowQueue) bool {
	if task == nil {
		return false
	}
	raw := strings.TrimSpace(task.ResourceActionInfo)
	if raw == "" {
		return false
	}
	var marker struct {
		Source  string `json:"source"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal([]byte(raw), &marker); err != nil {
		return false
	}
	return marker.Source == config.JobInfoSourceVersionUpdateAction && marker.Version == 1
}

func (c *applicationsServiceImpl) ListCronJobs(ctx context.Context) ([]*apisv1.CronJobInfo, error) {
	if c.KubeClient == nil {
		return nil, fmt.Errorf("kube client is nil")
	}
	list, err := c.KubeClient.BatchV1().CronJobs("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return []*apisv1.CronJobInfo{}, nil
	}
	resp := make([]*apisv1.CronJobInfo, 0, len(list.Items))
	for i := range list.Items {
		cron := list.Items[i]
		suspend := false
		if cron.Spec.Suspend != nil {
			suspend = *cron.Spec.Suspend
		}
		var lastSchedule *time.Time
		if cron.Status.LastScheduleTime != nil {
			t := cron.Status.LastScheduleTime.Time
			lastSchedule = &t
		}
		var lastSuccess *time.Time
		if cron.Status.LastSuccessfulTime != nil {
			t := cron.Status.LastSuccessfulTime.Time
			lastSuccess = &t
		}
		resp = append(resp, &apisv1.CronJobInfo{
			Name:                       cron.Name,
			Namespace:                  cron.Namespace,
			Schedule:                   cron.Spec.Schedule,
			Suspend:                    suspend,
			ConcurrencyPolicy:          string(cron.Spec.ConcurrencyPolicy),
			SuccessfulJobsHistoryLimit: cron.Spec.SuccessfulJobsHistoryLimit,
			FailedJobsHistoryLimit:     cron.Spec.FailedJobsHistoryLimit,
			LastScheduleTime:           lastSchedule,
			LastSuccessfulTime:         lastSuccess,
			CreateTime:                 cron.CreationTimestamp.Time,
		})
	}
	sort.Slice(resp, func(i, j int) bool {
		if resp[i].Namespace == resp[j].Namespace {
			return resp[i].Name < resp[j].Name
		}
		return resp[i].Namespace < resp[j].Namespace
	})
	return resp, nil
}

func (c *applicationsServiceImpl) ListScheduledJobs(ctx context.Context) ([]*apisv1.ScheduledJobInfo, error) {
	if c.Store == nil {
		return nil, fmt.Errorf("datastore is nil")
	}
	if c.AppRepo == nil {
		return nil, fmt.Errorf("app repository is nil")
	}
	entities, err := c.Store.List(ctx, &model.ApplicationComponent{}, &datastore.ListOptions{
		FilterOptions: datastore.FilterOptions{
			In: []datastore.InQueryOption{
				{Key: componentTypeColumn, Values: []string{string(config.ScheduledJob), string(config.JobDeployScheduled)}},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(entities) == 0 {
		return []*apisv1.ScheduledJobInfo{}, nil
	}

	appCache := make(map[string]*model.Applications)
	resp := make([]*apisv1.ScheduledJobInfo, 0, len(entities))
	for _, entity := range entities {
		component, ok := entity.(*model.ApplicationComponent)
		if !ok {
			klog.Warningf("unexpected component entity type: %T", entity)
			continue
		}
		if component == nil {
			continue
		}

		if strings.TrimSpace(component.AppID) == "" {
			klog.Warningf("scheduled job component missing appID: %s", component.Name)
			continue
		}

		app, ok := appCache[component.AppID]
		if !ok {
			app, err = c.AppRepo.FindByID(ctx, component.AppID)
			if err != nil {
				if errors.Is(err, datastore.ErrRecordNotExist) {
					klog.Warningf("scheduled job component app not found appID=%s component=%s", component.AppID, component.Name)
					continue
				}
				return nil, err
			}
			appCache[component.AppID] = app
		}

		var props model.Properties
		if component.Properties != nil {
			data, err := json.Marshal(component.Properties)
			if err != nil {
				return nil, fmt.Errorf("marshal component %s properties: %w", component.Name, err)
			}
			if string(data) != "null" {
				if err := json.Unmarshal(data, &props); err != nil {
					return nil, fmt.Errorf("decode component %s properties: %w", component.Name, err)
				}
			}
		}

		resp = append(resp, &apisv1.ScheduledJobInfo{
			AppID:              app.ID,
			AppName:            app.Name,
			AppNamespace:       app.Namespace,
			ComponentName:      component.Name,
			ComponentNamespace: component.Namespace,
			Image:              component.Image,
			Schedule:           props.Schedule,
			StartTime:          props.StartTime,
			RunPolicy:          props.RunPolicy,
			CreateTime:         component.CreateTime,
			UpdateTime:         component.UpdateTime,
		})
	}

	sort.Slice(resp, func(i, j int) bool {
		if resp[i].AppNamespace == resp[j].AppNamespace {
			if resp[i].AppName == resp[j].AppName {
				return resp[i].ComponentName < resp[j].ComponentName
			}
			return resp[i].AppName < resp[j].AppName
		}
		return resp[i].AppNamespace < resp[j].AppNamespace
	})

	return resp, nil
}

func (c *applicationsServiceImpl) ListApplicationWorkflows(ctx context.Context, appID string) ([]*model.Workflow, error) {
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	app, err := c.AppRepo.FindByID(ctx, appID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}
	workflows, err := c.WorkflowRepo.FindByAppID(ctx, app.ID)
	if err != nil {
		return nil, err
	}
	if len(workflows) == 0 {
		return nil, nil
	}
	sort.SliceStable(workflows, func(i, j int) bool {
		if workflows[i] == nil || workflows[j] == nil {
			return workflows[j] != nil
		}
		return workflows[i].UpdateTime.Unix() > workflows[j].UpdateTime.Unix()
	})
	return workflows, nil
}
