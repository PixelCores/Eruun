package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

type countingAppRepo struct {
	*mockAppRepo
	listCalls    int
	failListOver int
	lastOptions  datastore.ListOptions
	lastQuery    *model.Applications
}

func (r *countingAppRepo) List(ctx context.Context, options datastore.ListOptions) ([]*model.Applications, error) {
	return r.ListByQuery(ctx, &model.Applications{}, options)
}

func (r *countingAppRepo) ListByQuery(ctx context.Context, query *model.Applications, options datastore.ListOptions) ([]*model.Applications, error) {
	r.listCalls++
	r.lastOptions = options
	if query != nil {
		cp := *query
		r.lastQuery = &cp
	} else {
		r.lastQuery = nil
	}
	if r.failListOver > 0 && r.listCalls > r.failListOver {
		return nil, errors.New("forced list failure")
	}
	return r.mockAppRepo.ListByQuery(ctx, query, options)
}

type countingComponentRepo struct {
	*mockComponentRepo
	findCalls    int
	failFindOver int
}

func (r *countingComponentRepo) FindByAppID(ctx context.Context, appID string) ([]*model.ApplicationComponent, error) {
	r.findCalls++
	if r.failFindOver > 0 && r.findCalls > r.failFindOver {
		return nil, errors.New("forced find components failure")
	}
	return r.mockComponentRepo.FindByAppID(ctx, appID)
}

type countingWorkflowRepo struct {
	*mockWorkflowRepo
	findByAppIDCalls int
}

func (r *countingWorkflowRepo) FindByAppID(ctx context.Context, appID string) ([]*model.Workflow, error) {
	r.findByAppIDCalls++
	return r.mockWorkflowRepo.FindByAppID(ctx, appID)
}

func TestListApplicationsUsesCache(t *testing.T) {
	store := newInMemoryAppStore()
	app := model.NewApplications("app-1", "demo", "default", "1.0.0", "", "", "", "", false)
	require.NoError(t, store.Add(context.Background(), app))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	countingRepo := &countingAppRepo{
		mockAppRepo:  &mockAppRepo{store: store},
		failListOver: 1,
	}
	svc.AppRepo = countingRepo

	first, err := svc.ListApplications(context.Background(), ListApplicationsOptions{})
	require.NoError(t, err)
	require.Len(t, first, 1)

	second, err := svc.ListApplications(context.Background(), ListApplicationsOptions{})
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, 1, countingRepo.listCalls)
}

func TestListApplicationsPopulatesDefaultWorkflowID(t *testing.T) {
	store := newInMemoryAppStore()
	app := model.NewApplications("app-1", "demo", "default", "1.0.0", "", "", "", "", false)
	require.NoError(t, store.Add(context.Background(), app))
	require.NoError(t, store.Add(context.Background(), &model.Workflow{
		ID:           "wf-update",
		AppID:        app.ID,
		Name:         "demo-update-workflow",
		WorkflowType: config.WorkflowTaskTypeUpdate,
	}))
	require.NoError(t, store.Add(context.Background(), &model.Workflow{
		ID:           "wf-default",
		AppID:        app.ID,
		Name:         "demo-workflow",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
	}))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)

	apps, err := svc.ListApplications(context.Background(), ListApplicationsOptions{})
	require.NoError(t, err)
	require.Len(t, apps, 1)
	require.Equal(t, "wf-default", apps[0].WorkflowID)
}

func TestListApplicationsIgnoresLegacyWorkflowIDCacheKey(t *testing.T) {
	store := newInMemoryAppStore()
	app := model.NewApplications("app-1", "demo", "default", "1.0.0", "", "", "", "", false)
	require.NoError(t, store.Add(context.Background(), app))
	require.NoError(t, store.Add(context.Background(), &model.Workflow{
		ID:           "wf-default",
		AppID:        app.ID,
		Name:         "demo-workflow",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
	}))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	require.NoError(t, svc.Cache.Store("app:list", `[{"id":"legacy-app","name":"legacy","workflow_id":"legacy-wf"}]`))
	countingRepo := &countingAppRepo{mockAppRepo: &mockAppRepo{store: store}}
	svc.AppRepo = countingRepo

	apps, err := svc.ListApplications(context.Background(), ListApplicationsOptions{})
	require.NoError(t, err)
	require.Len(t, apps, 1)
	require.Equal(t, app.ID, apps[0].ID)
	require.Equal(t, "wf-default", apps[0].WorkflowID)
	require.Equal(t, 1, countingRepo.listCalls)
	require.True(t, svc.Cache.Exists("app:list"))
	require.True(t, svc.Cache.Exists(applicationListCacheKey))
}

func TestListTemplateApplicationsUsesCache(t *testing.T) {
	store := newInMemoryAppStore()
	template := model.NewApplications("app-1", "template", "default", "1.0.0", "", "", "", "", true)
	normal := model.NewApplications("app-2", "normal", "default", "1.0.0", "", "", "", "", false)
	require.NoError(t, store.Add(context.Background(), template))
	require.NoError(t, store.Add(context.Background(), normal))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	countingRepo := &countingAppRepo{
		mockAppRepo:  &mockAppRepo{store: store},
		failListOver: 1,
	}
	svc.AppRepo = countingRepo

	first, err := svc.ListTemplateApplications(context.Background(), ListApplicationsOptions{})
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.Equal(t, template.ID, first[0].ID)

	second, err := svc.ListTemplateApplications(context.Background(), ListApplicationsOptions{})
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, template.ID, second[0].ID)
	require.Equal(t, 1, countingRepo.listCalls)
}

func TestListTemplateApplicationsPopulatesDefaultWorkflowID(t *testing.T) {
	store := newInMemoryAppStore()
	template := model.NewApplications("app-1", "template", "default", "1.0.0", "", "", "", "", true)
	require.NoError(t, store.Add(context.Background(), template))
	require.NoError(t, store.Add(context.Background(), &model.Workflow{
		ID:           "wf-template",
		AppID:        template.ID,
		Name:         "template-workflow",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
	}))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)

	templates, err := svc.ListTemplateApplications(context.Background(), ListApplicationsOptions{})
	require.NoError(t, err)
	require.Len(t, templates, 1)
	require.Equal(t, "wf-template", templates[0].WorkflowID)
}

func TestListTemplateApplicationsAddsMainComponentResourceSummary(t *testing.T) {
	store := newInMemoryAppStore()
	template := model.NewApplications("app-template", "redis", "default", "6.2.17", "", "", "", "", true)
	require.NoError(t, store.Add(context.Background(), template))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		ID:            1,
		AppID:         template.ID,
		Name:          "redis-config",
		ComponentType: config.ConfJob,
		Traits:        mustJSONStruct(apisv1.Traits{}),
	}))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		ID:            2,
		AppID:         template.ID,
		Name:          "redis-secret",
		ComponentType: config.SecretJob,
		Traits:        mustJSONStruct(apisv1.Traits{}),
	}))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		ID:            3,
		AppID:         template.ID,
		Name:          "redis",
		Replicas:      1,
		ComponentType: config.StoreJob,
		Traits: mustJSONStruct(apisv1.Traits{
			Resources: &spec.ResourceTraitsSpec{CPU: "160m", Memory: "260Mi", CPULimit: "300m", MemoryLimit: "600Mi"},
		}),
	}))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)

	templates, err := svc.ListTemplateApplications(context.Background(), ListApplicationsOptions{})
	require.NoError(t, err)
	require.Len(t, templates, 1)
	require.Equal(t, "160m", templates[0].Resources.CPUReq)
	require.Equal(t, "300m", templates[0].Resources.CPULimit)
	require.Equal(t, "260Mi", templates[0].Resources.MemReq)
	require.Equal(t, "600Mi", templates[0].Resources.MemLimit)
	require.EqualValues(t, 1, templates[0].Resources.Replicas)
}

func TestListTemplateApplicationsMainComponentSelectionIsStable(t *testing.T) {
	store := newInMemoryAppStore()
	template := model.NewApplications("app-template", "multi", "default", "1.0.0", "", "", "", "", true)
	require.NoError(t, store.Add(context.Background(), template))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		ID:            20,
		AppID:         template.ID,
		Name:          "api",
		Replicas:      2,
		ComponentType: config.ServerJob,
		Traits: mustJSONStruct(apisv1.Traits{
			Resources: &spec.ResourceTraitsSpec{CPU: "200m", Memory: "256Mi"},
		}),
	}))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		ID:            10,
		AppID:         template.ID,
		Name:          "worker",
		Replicas:      1,
		ComponentType: config.InstantJob,
		Traits: mustJSONStruct(apisv1.Traits{
			Resources: &spec.ResourceTraitsSpec{CPU: "100m", Memory: "128Mi"},
		}),
	}))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)

	templates, err := svc.ListTemplateApplications(context.Background(), ListApplicationsOptions{})
	require.NoError(t, err)
	require.Len(t, templates, 1)
	require.Equal(t, "100m", templates[0].Resources.CPUReq)
	require.Equal(t, "128Mi", templates[0].Resources.MemReq)
	require.EqualValues(t, 1, templates[0].Resources.Replicas)
}

func TestListTemplateApplicationsResourceSummaryKeepsZeroValuesWhenMissing(t *testing.T) {
	store := newInMemoryAppStore()
	noWorkload := model.NewApplications("app-no-workload", "no-workload", "default", "1.0.0", "", "", "", "", true)
	noResources := model.NewApplications("app-no-resources", "no-resources", "default", "1.0.0", "", "", "", "", true)
	require.NoError(t, store.Add(context.Background(), noWorkload))
	require.NoError(t, store.Add(context.Background(), noResources))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		ID:            1,
		AppID:         noWorkload.ID,
		Name:          "cfg",
		ComponentType: config.ConfJob,
		Traits:        mustJSONStruct(apisv1.Traits{}),
	}))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		ID:            2,
		AppID:         noResources.ID,
		Name:          "api",
		Replicas:      2,
		ComponentType: config.ServerJob,
		Traits:        mustJSONStruct(apisv1.Traits{}),
	}))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)

	templates, err := svc.ListTemplateApplications(context.Background(), ListApplicationsOptions{})
	require.NoError(t, err)
	require.Len(t, templates, 2)
	for _, template := range templates {
		require.Empty(t, template.Resources.CPUReq)
		require.Empty(t, template.Resources.CPULimit)
		require.Empty(t, template.Resources.MemReq)
		require.Empty(t, template.Resources.MemLimit)
		require.Zero(t, template.Resources.Replicas)
	}
}

func TestListTemplateApplicationsIgnoresLegacyWorkflowIDCacheKey(t *testing.T) {
	store := newInMemoryAppStore()
	template := model.NewApplications("app-1", "template", "default", "1.0.0", "", "", "", "", true)
	require.NoError(t, store.Add(context.Background(), template))
	require.NoError(t, store.Add(context.Background(), &model.Workflow{
		ID:           "wf-template",
		AppID:        template.ID,
		Name:         "template-workflow",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
	}))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	require.NoError(t, svc.Cache.Store("app:template:list", `[{"id":"legacy-template","name":"legacy","workflow_id":"legacy-wf","templateEnabled":true}]`))
	countingRepo := &countingAppRepo{mockAppRepo: &mockAppRepo{store: store}}
	svc.AppRepo = countingRepo

	templates, err := svc.ListTemplateApplications(context.Background(), ListApplicationsOptions{})
	require.NoError(t, err)
	require.Len(t, templates, 1)
	require.Equal(t, template.ID, templates[0].ID)
	require.Equal(t, "wf-template", templates[0].WorkflowID)
	require.Equal(t, 1, countingRepo.listCalls)
	require.True(t, svc.Cache.Exists("app:template:list"))
	require.True(t, svc.Cache.Exists(templateApplicationListCacheKey))
}

func TestListTemplateApplicationsAvoidsWorkflowRepoNPlusOne(t *testing.T) {
	store := newInMemoryAppStore()
	templateA := model.NewApplications("app-1", "template-a", "default", "1.0.0", "", "", "", "", true)
	templateB := model.NewApplications("app-2", "template-b", "default", "1.0.0", "", "", "", "", true)
	require.NoError(t, store.Add(context.Background(), templateA))
	require.NoError(t, store.Add(context.Background(), templateB))
	require.NoError(t, store.Add(context.Background(), &model.Workflow{
		ID:           "wf-template-a",
		AppID:        templateA.ID,
		Name:         "template-a-workflow",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
	}))
	require.NoError(t, store.Add(context.Background(), &model.Workflow{
		ID:           "wf-template-b",
		AppID:        templateB.ID,
		Name:         "template-b-workflow",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
	}))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	countingRepo := &countingWorkflowRepo{mockWorkflowRepo: &mockWorkflowRepo{store: store}}
	svc.WorkflowRepo = countingRepo

	templates, err := svc.ListTemplateApplications(context.Background(), ListApplicationsOptions{})
	require.NoError(t, err)
	require.Len(t, templates, 2)
	require.Equal(t, 0, countingRepo.findByAppIDCalls)

	workflowByAppID := make(map[string]string, len(templates))
	for _, app := range templates {
		workflowByAppID[app.ID] = app.WorkflowID
	}
	require.Equal(t, "wf-template-a", workflowByAppID[templateA.ID])
	require.Equal(t, "wf-template-b", workflowByAppID[templateB.ID])
}

func TestListApplicationsBypassesCacheWhenPaginated(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), model.NewApplications("app-1", "demo-1", "default", "1.0.0", "", "", "", "", false)))
	require.NoError(t, store.Add(context.Background(), model.NewApplications("app-2", "demo-2", "default", "1.0.0", "", "", "", "", false)))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	countingRepo := &countingAppRepo{
		mockAppRepo: &mockAppRepo{store: store},
	}
	svc.AppRepo = countingRepo

	first, err := svc.ListApplications(context.Background(), ListApplicationsOptions{Page: 0, PageSize: 1})
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.Equal(t, 1, countingRepo.listCalls)
	require.Equal(t, 1, countingRepo.lastOptions.Page)
	require.Equal(t, 1, countingRepo.lastOptions.PageSize)

	second, err := svc.ListApplications(context.Background(), ListApplicationsOptions{Page: 0, PageSize: 1})
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, 2, countingRepo.listCalls)
	require.False(t, svc.Cache.Exists(applicationListCacheKey))
}

func TestListApplicationsPaginatedUsesStableSecondarySortKey(t *testing.T) {
	store := newInMemoryAppStore()
	sharedUpdateTime := time.Unix(1700000000, 0)

	app1 := model.NewApplications("app-1", "demo-1", "default", "1.0.0", "", "", "", "", false)
	app1.UpdateTime = sharedUpdateTime
	app2 := model.NewApplications("app-2", "demo-2", "default", "1.0.0", "", "", "", "", false)
	app2.UpdateTime = sharedUpdateTime
	require.NoError(t, store.Add(context.Background(), app2))
	require.NoError(t, store.Add(context.Background(), app1))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	countingRepo := &countingAppRepo{mockAppRepo: &mockAppRepo{store: store}}
	svc.AppRepo = countingRepo

	page1, err := svc.ListApplications(context.Background(), ListApplicationsOptions{Page: 1, PageSize: 1})
	require.NoError(t, err)
	require.Len(t, page1, 1)
	require.Equal(t, "app-1", page1[0].ID)

	page2, err := svc.ListApplications(context.Background(), ListApplicationsOptions{Page: 2, PageSize: 1})
	require.NoError(t, err)
	require.Len(t, page2, 1)
	require.Equal(t, "app-2", page2[0].ID)

	require.Len(t, countingRepo.lastOptions.SortBy, 2)
	require.Equal(t, applicationsSortKeyUpdateTime, countingRepo.lastOptions.SortBy[0].Key)
	require.Equal(t, datastore.SortOrderDescending, countingRepo.lastOptions.SortBy[0].Order)
	require.Equal(t, applicationsSortKeyID, countingRepo.lastOptions.SortBy[1].Key)
	require.Equal(t, datastore.SortOrderAscending, countingRepo.lastOptions.SortBy[1].Order)
}

func TestListTemplateApplicationsPushesPaginationAndTemplateFilterToRepo(t *testing.T) {
	store := newInMemoryAppStore()
	sharedUpdateTime := time.Unix(1700000000, 0)

	template1 := model.NewApplications("app-1", "tmpl-1", "default", "1.0.0", "", "", "", "", true)
	template1.UpdateTime = sharedUpdateTime
	template2 := model.NewApplications("app-2", "tmpl-2", "default", "1.0.0", "", "", "", "", true)
	template2.UpdateTime = sharedUpdateTime
	normal := model.NewApplications("app-3", "normal", "default", "1.0.0", "", "", "", "", false)
	normal.UpdateTime = sharedUpdateTime
	require.NoError(t, store.Add(context.Background(), normal))
	require.NoError(t, store.Add(context.Background(), template2))
	require.NoError(t, store.Add(context.Background(), template1))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	countingRepo := &countingAppRepo{mockAppRepo: &mockAppRepo{store: store}}
	svc.AppRepo = countingRepo

	templates, err := svc.ListTemplateApplications(context.Background(), ListApplicationsOptions{Page: 1, PageSize: 1})
	require.NoError(t, err)
	require.Len(t, templates, 1)
	require.Equal(t, "app-1", templates[0].ID)
	require.True(t, templates[0].TemplateEnabled)

	require.NotNil(t, countingRepo.lastQuery)
	require.True(t, countingRepo.lastQuery.TemplateEnabled)
	require.Equal(t, 1, countingRepo.lastOptions.Page)
	require.Equal(t, 1, countingRepo.lastOptions.PageSize)
	require.Len(t, countingRepo.lastOptions.SortBy, 2)
	require.Equal(t, applicationsSortKeyID, countingRepo.lastOptions.SortBy[1].Key)
}

func TestListApplicationComponentsUsesCache(t *testing.T) {
	store := newInMemoryAppStore()
	app := model.NewApplications("app-1", "demo", "default", "1.0.0", "", "", "", "", false)
	require.NoError(t, store.Add(context.Background(), app))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		ID:            1,
		AppID:         app.ID,
		Name:          "web",
		Namespace:     "default",
		ComponentType: "server",
	}))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	countingRepo := &countingComponentRepo{
		mockComponentRepo: &mockComponentRepo{store: store},
		failFindOver:      1,
	}
	svc.ComponentRepo = countingRepo

	first, err := svc.ListApplicationComponents(context.Background(), app.ID)
	require.NoError(t, err)
	require.Len(t, first, 1)

	second, err := svc.ListApplicationComponents(context.Background(), app.ID)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, 1, countingRepo.findCalls)
}

func TestListApplicationRuntimeComponentsBypassesDetailCache(t *testing.T) {
	store := newInMemoryAppStore()
	app := model.NewApplications("app-1", "demo", "default", "1.0.0", "", "", "", "", false)
	require.NoError(t, store.Add(context.Background(), app))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		ID:            1,
		AppID:         app.ID,
		Name:          "web",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
		Replicas:      1,
		ReadyReplicas: 1,
	}))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	countingRepo := &countingComponentRepo{mockComponentRepo: &mockComponentRepo{store: store}}
	svc.ComponentRepo = countingRepo
	svc.storeJSONCache(applicationComponentsCacheKey(app.ID), []*model.ApplicationComponent{{
		ID:            1,
		AppID:         app.ID,
		Name:          "web",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusPending),
		Replicas:      1,
		ReadyReplicas: 0,
	}})

	runtimeComponents, err := svc.ListApplicationRuntimeComponents(context.Background(), app.ID)
	require.NoError(t, err)
	require.Len(t, runtimeComponents, 1)
	require.Equal(t, string(config.ComponentStatusRunning), runtimeComponents[0].Status)
	require.Equal(t, 1, countingRepo.findCalls)

	detailComponents, err := svc.ListApplicationComponents(context.Background(), app.ID)
	require.NoError(t, err)
	require.Len(t, detailComponents, 1)
	require.Equal(t, string(config.ComponentStatusPending), detailComponents[0].Status)
	require.Equal(t, 1, countingRepo.findCalls)
}

func TestListApplicationComponentsCacheStoresSecretValuesAsText(t *testing.T) {
	store := newInMemoryAppStore()
	app := model.NewApplications("app-1", "demo", "default", "1.0.0", "", "", "", "", false)
	require.NoError(t, store.Add(context.Background(), app))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		ID:            1,
		AppID:         app.ID,
		Name:          "legacy-secret",
		Namespace:     "default",
		ComponentType: config.SecretJob,
		Properties: mustJSONStruct(&model.Properties{
			Secret: map[string]string{"password": "c2VjcmV0LXB3ZA=="},
		}),
	}))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	countingRepo := &countingComponentRepo{
		mockComponentRepo: &mockComponentRepo{store: store},
	}
	svc.ComponentRepo = countingRepo

	components, err := svc.ListApplicationComponents(context.Background(), app.ID)
	require.NoError(t, err)
	require.Len(t, components, 1)
	require.Equal(t, 1, countingRepo.findCalls)

	var props model.Properties
	require.NoError(t, decodeJSONStruct(components[0].Properties, &props))
	require.Equal(t, "c2VjcmV0LXB3ZA==", props.Secret["password"])

	var cached []*model.ApplicationComponent
	require.True(t, svc.loadJSONCache(applicationComponentsCacheKey(app.ID), &cached))
	require.Len(t, cached, 1)
	require.NoError(t, decodeJSONStruct(cached[0].Properties, &props))
	require.Equal(t, "c2VjcmV0LXB3ZA==", props.Secret["password"])
}

func TestListApplicationComponentsCacheHitKeepsStoredSecretValuesWithoutKubeLookup(t *testing.T) {
	store := newInMemoryAppStore()
	app := model.NewApplications("app-1", "demo", "default", "1.0.0", "", "", "", "", false)
	require.NoError(t, store.Add(context.Background(), app))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	client := fake.NewSimpleClientset()
	svc.KubeClient = client

	svc.storeJSONCache(applicationComponentsCacheKey(app.ID), []*model.ApplicationComponent{{
		AppID:         app.ID,
		Name:          "legacy-secret",
		Namespace:     "default",
		ComponentType: config.SecretJob,
		Properties: mustJSONStruct(&model.Properties{
			Secret: map[string]string{"password": "c2VjcmV0LXB3ZA=="},
		}),
	}})

	components, err := svc.ListApplicationComponents(context.Background(), app.ID)
	require.NoError(t, err)
	require.Len(t, components, 1)
	require.Empty(t, client.Actions())

	var props model.Properties
	require.NoError(t, decodeJSONStruct(components[0].Properties, &props))
	require.Equal(t, "c2VjcmV0LXB3ZA==", props.Secret["password"])
}

func TestListApplicationComponentsCacheHitKeepsBase64LikeTextSecrets(t *testing.T) {
	store := newInMemoryAppStore()
	app := model.NewApplications("app-1", "demo", "default", "1.0.0", "", "", "", "", false)
	require.NoError(t, store.Add(context.Background(), app))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	client := fake.NewSimpleClientset()
	svc.KubeClient = client

	svc.storeJSONCache(applicationComponentsCacheKey(app.ID), []*model.ApplicationComponent{{
		AppID:         app.ID,
		Name:          "manual-secret",
		Namespace:     "default",
		ComponentType: config.SecretJob,
		Properties: mustJSONStruct(&model.Properties{
			Secret: map[string]string{"password": "dGVzdA=="},
		}),
	}})

	components, err := svc.ListApplicationComponents(context.Background(), app.ID)
	require.NoError(t, err)
	require.Len(t, components, 1)
	require.Empty(t, client.Actions())

	var props model.Properties
	require.NoError(t, decodeJSONStruct(components[0].Properties, &props))
	require.Equal(t, "dGVzdA==", props.Secret["password"])
}

func TestDeleteApplicationInvalidatesCache(t *testing.T) {
	store := newInMemoryAppStore()
	app := model.NewApplications("app-1", "demo", "default", "1.0.0", "", "", "", "", true)
	require.NoError(t, store.Add(context.Background(), app))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	svc.storeJSONCache(applicationListCacheKey, []*apisv1.ApplicationBase{{ID: app.ID}})
	svc.storeJSONCache(templateApplicationListCacheKey, []*apisv1.ApplicationBase{{ID: app.ID}})
	svc.storeJSONCache(applicationComponentsCacheKey(app.ID), []*model.ApplicationComponent{{AppID: app.ID, Name: "web"}})

	require.True(t, svc.Cache.Exists(applicationListCacheKey))
	require.True(t, svc.Cache.Exists(templateApplicationListCacheKey))
	require.True(t, svc.Cache.Exists(applicationComponentsCacheKey(app.ID)))

	require.NoError(t, svc.DeleteApplication(context.Background(), app))
	require.False(t, svc.Cache.Exists(applicationListCacheKey))
	require.False(t, svc.Cache.Exists(templateApplicationListCacheKey))
	require.False(t, svc.Cache.Exists(applicationComponentsCacheKey(app.ID)))
}

func TestUpdateVersionInvalidatesCache(t *testing.T) {
	store := newInMemoryAppStore()
	app := model.NewApplications("app-1", "demo", "default", "1.0.0", "", "", "", "", true)
	require.NoError(t, store.Add(context.Background(), app))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		ID:            1,
		AppID:         app.ID,
		Name:          "web",
		Namespace:     "default",
		ComponentType: "server",
	}))

	svc := newMockServiceWithStore(store)
	svc.Cache = cache.NewMemCache(false)
	svc.storeJSONCache(applicationListCacheKey, []*apisv1.ApplicationBase{{ID: app.ID}})
	svc.storeJSONCache(templateApplicationListCacheKey, []*apisv1.ApplicationBase{{ID: app.ID}})
	svc.storeJSONCache(applicationComponentsCacheKey(app.ID), []*model.ApplicationComponent{{AppID: app.ID, Name: "web"}})

	autoExec := false
	resp, err := svc.UpdateVersion(context.Background(), app.ID, apisv1.UpdateVersionRequest{
		Version:  "2.0.0",
		AutoExec: &autoExec,
	})
	require.NoError(t, err)
	require.Equal(t, "2.0.0", resp.Version)
	require.False(t, svc.Cache.Exists(applicationListCacheKey))
	require.False(t, svc.Cache.Exists(templateApplicationListCacheKey))
	require.False(t, svc.Cache.Exists(applicationComponentsCacheKey(app.ID)))
}
