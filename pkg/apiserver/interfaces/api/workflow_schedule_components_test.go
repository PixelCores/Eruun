package api

import (
	"context"
	"encoding/json"

	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"

	cacheutil "github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"

	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func TestUpsertWorkflowScheduleEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeWorkflowService{
		scheduleUpsertResp: &apis.UpsertWorkflowScheduleResponse{
			Schedule: apis.WorkflowSchedule{
				ID:         "sch-1",
				AppID:      "app-1",
				WorkflowID: "wf-1",
				Cron:       "*/5 * * * *",
				Enabled:    true,
			},
		},
	}
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    svc,
	}
	r := gin.New()
	r.POST("/applications/:appID/workflow/schedule", appHandler.upsertWorkflowSchedule)

	body := `{"workflowId":"wf-1","cron":"*/5 * * * *","enabled":true}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/workflow/schedule", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}
	var payload apis.UpsertWorkflowScheduleResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)

	if payload.Schedule.WorkflowID != "wf-1" || payload.Schedule.AppID != "app-1" {
		t.Fatalf("unexpected schedule payload: %+v", payload.Schedule)
	}
	if svc.scheduleUpsertApp != "app-1" || svc.scheduleUpsertReq.WorkflowID != "wf-1" {
		t.Fatalf("expected upsert workflow schedule to be invoked")
	}
}

func TestDeleteWorkflowScheduleEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeWorkflowService{}
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    svc,
	}
	r := gin.New()
	r.DELETE("/applications/:appID/workflow/schedule/:workflowID", appHandler.deleteWorkflowSchedule)

	req := httptest.NewRequest(http.MethodDelete, "/applications/app-1/workflow/schedule/wf-1", nil)
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}
	var payload apis.DeleteWorkflowScheduleResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.WorkflowID != "wf-1" {
		t.Fatalf("unexpected workflowID: %s", payload.WorkflowID)
	}
	if svc.scheduleDeleteApp != "app-1" || svc.scheduleDeleteID != "wf-1" {
		t.Fatalf("expected delete workflow schedule to be invoked")
	}
}

func TestListApplicationComponentsIncludesStatusAndLinks(t *testing.T) {
	gin.SetMode(gin.TestMode)
	traits := model.Traits{
		Sidecar: []model.SidecarSpec{
			{
				Name:  "log-agent",
				Image: "vector:0.36",
			},
		},
		Ingress: []model.IngressTraitsSpec{
			{
				Routes: []model.IngressRoutes{
					{Host: "example.com", Path: "/"},
				},
			},
		},
	}
	properties := model.Properties{
		Ports: []model.Ports{{Port: 80}},
	}
	traitsJSON, err := model.NewJSONStructByStruct(&traits)
	if err != nil {
		t.Fatalf("traits json: %v", err)
	}
	propsJSON, err := model.NewJSONStructByStruct(&properties)
	if err != nil {
		t.Fatalf("props json: %v", err)
	}
	components := []*model.ApplicationComponent{
		{
			ID:            1,
			AppID:         "app-1",
			Name:          "backend",
			Namespace:     "default",
			ComponentType: config.ServerJob,
			Properties:    propsJSON,
			Traits:        traitsJSON,
		},
	}
	appHandler := &applications{
		ApplicationService: componentListApplicationService{components: components},
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/components", appHandler.listApplicationComponents)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/components", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}

	var payload apis.ListApplicationComponentsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if len(payload.Components) != 1 {
		t.Fatalf("expected 1 component, got %d", len(payload.Components))
	}
	if payload.Components[0].Status != string(config.ComponentStatusNotDeploy) {
		t.Fatalf("unexpected status: %s", payload.Components[0].Status)
	}
	if len(payload.Components[0].ExternalLinks) != 1 || payload.Components[0].ExternalLinks[0].Type != "ingress" || payload.Components[0].ExternalLinks[0].Value != "example.com/" {
		t.Fatalf("unexpected external links: %+v", payload.Components[0].ExternalLinks)
	}
	if len(payload.Components[0].Sidecars) != 1 || payload.Components[0].Sidecars[0].Name != "log-agent" || payload.Components[0].Sidecars[0].Image != "vector:0.36" {
		t.Fatalf("unexpected sidecars: %+v", payload.Components[0].Sidecars)
	}
}

func TestListApplicationComponentsIncludesResourceDetailsAndCredentials(t *testing.T) {
	gin.SetMode(gin.TestMode)
	secretProps := model.Properties{
		Secret: map[string]string{
			"password": "secret-pwd",
			"username": "root",
		},
	}
	apiProps := model.Properties{
		Ports: []model.Ports{{Port: 8080}},
	}
	apiTraits := model.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name: "api-svc",
				Type: string(spec.ServiceAccessInternal),
				Ports: []spec.ServicePortTraitSpec{
					{Name: "http", Port: 80, TargetPort: 8080, Protocol: "TCP"},
				},
			},
		},
		Ingress: []spec.IngressTraitsSpec{
			{
				Routes: []spec.IngressRoutes{
					{Host: "api.example.com", Path: "/api"},
				},
			},
		},
		Resources: &spec.ResourceTraitsSpec{CPU: "500m", Memory: "256Mi"},
		Envs: []spec.SimplifiedEnvSpec{
			{
				Name: "DB_PASSWORD",
				ValueFrom: spec.ValueSource{
					Secret: &spec.SecretSelectorSpec{Name: "db-secret", Key: "password"},
				},
			},
		},
	}
	secretPropsJSON, err := model.NewJSONStructByStruct(&secretProps)
	require.NoError(t, err)
	apiPropsJSON, err := model.NewJSONStructByStruct(&apiProps)
	require.NoError(t, err)
	apiTraitsJSON, err := model.NewJSONStructByStruct(&apiTraits)
	require.NoError(t, err)
	components := []*model.ApplicationComponent{
		{
			ID:            1,
			AppID:         "app-1",
			Name:          "db-secret",
			Namespace:     "default",
			ComponentType: config.SecretJob,
			Properties:    secretPropsJSON,
		},
		{
			ID:            2,
			AppID:         "app-1",
			Name:          "api",
			Namespace:     "default",
			ComponentType: config.ServerJob,
			Properties:    apiPropsJSON,
			Traits:        apiTraitsJSON,
		},
	}
	appHandler := &applications{
		ApplicationService: componentListApplicationService{components: components},
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/components", appHandler.listApplicationComponents)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/components", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.ListApplicationComponentsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Components, 2)
	var apiComponent *apis.ApplicationComponent
	var secretComponent *apis.ApplicationComponent
	for _, component := range payload.Components {
		switch component.Name {
		case "api":
			apiComponent = component
		case "db-secret":
			secretComponent = component
		}
	}
	require.NotNil(t, secretComponent)
	require.Equal(t, map[string]string{"password": "secret-pwd", "username": "root"}, secretComponent.Properties.Secret)
	require.NotNil(t, apiComponent)
	require.Equal(t, []apis.ComponentServiceInfo{
		{
			Name:      "api-svc",
			Namespace: "default",
			Type:      string(spec.ServiceAccessInternal),
			Ports: []apis.ComponentServicePortInfo{
				{Name: "http", Port: 80, TargetPort: 8080, Protocol: "TCP"},
			},
		},
	}, apiComponent.Services)
	require.Len(t, apiComponent.Ingresses, 1)
	require.Equal(t, []apis.ComponentIngressRouteInfo{
		{Host: "api.example.com", Path: "/api", PathType: "Prefix", ServiceName: "api-svc", ServicePort: 80},
	}, apiComponent.Ingresses[0].Routes)
	require.Equal(t, []apis.ComponentResourceConfig{
		{Scope: "main", Name: "api", CPU: "500m", Memory: "256Mi"},
	}, apiComponent.ResourceConfigs)
	require.Equal(t, []apis.ComponentCredentialInfo{
		{Source: "component.envs", EnvName: "DB_PASSWORD", SecretName: "db-secret", Key: "password", Value: "secret-pwd", Resolved: true},
	}, apiComponent.Credentials)
}

func TestListApplicationComponentsTreatsEmptySecretValuesAsUnresolved(t *testing.T) {
	gin.SetMode(gin.TestMode)

	secretPropsJSON, err := model.NewJSONStructByStruct(&model.Properties{
		Secret: map[string]string{
			"password": "",
			"username": "root",
		},
	})
	require.NoError(t, err)
	apiTraitsJSON, err := model.NewJSONStructByStruct(&model.Traits{
		Envs: []spec.SimplifiedEnvSpec{
			{
				Name: "DB_PASSWORD",
				ValueFrom: spec.ValueSource{
					Secret: &spec.SecretSelectorSpec{Name: "db-secret", Key: "password"},
				},
			},
		},
		EnvFrom: []spec.EnvFromSourceSpec{
			{
				Type:       config.StorageTypeSecret,
				SourceName: "db-secret",
			},
		},
	})
	require.NoError(t, err)

	appHandler := &applications{
		ApplicationService: componentListApplicationService{
			components: []*model.ApplicationComponent{
				{
					ID:            1,
					AppID:         "app-1",
					Name:          "db-secret",
					Namespace:     "default",
					ComponentType: config.SecretJob,
					Properties:    secretPropsJSON,
				},
				{
					ID:            2,
					AppID:         "app-1",
					Name:          "api",
					Namespace:     "default",
					ComponentType: config.ServerJob,
					Traits:        apiTraitsJSON,
				},
			},
		},
		WorkflowService: &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/components", appHandler.listApplicationComponents)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/components", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.ListApplicationComponentsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Components, 2)
	var apiComponent *apis.ApplicationComponent
	for _, component := range payload.Components {
		if component.Name == "api" {
			apiComponent = component
			break
		}
	}
	require.NotNil(t, apiComponent)
	require.ElementsMatch(t, []apis.ComponentCredentialInfo{
		{Source: "component.envs", EnvName: "DB_PASSWORD", SecretName: "db-secret", Key: "password", Resolved: false},
		{Source: "component.envFrom", SecretName: "db-secret", Key: "password", Resolved: false},
		{Source: "component.envFrom", SecretName: "db-secret", Key: "username", Value: "root", Resolved: true},
	}, apiComponent.Credentials)

	var raw map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &raw))
	data, ok := raw["data"].(map[string]interface{})
	require.True(t, ok)
	components, ok := data["components"].([]interface{})
	require.True(t, ok)
	foundAPIComponent := false
	matchedPasswordRecords := 0
	for _, item := range components {
		component, ok := item.(map[string]interface{})
		require.True(t, ok)
		if component["name"] != "api" {
			continue
		}
		foundAPIComponent = true
		credentials, ok := component["credentials"].([]interface{})
		require.True(t, ok)
		for _, cred := range credentials {
			record, ok := cred.(map[string]interface{})
			require.True(t, ok)
			if record["secretName"] != "db-secret" || record["key"] != "password" {
				continue
			}
			matchedPasswordRecords++
			_, hasValue := record["value"]
			require.False(t, hasValue)
		}
	}
	require.True(t, foundAPIComponent)
	require.Equal(t, 2, matchedPasswordRecords)
}

func TestListApplicationComponentsSkipsSyntheticServicesAndNormalizesIngressDefaults(t *testing.T) {
	gin.SetMode(gin.TestMode)

	jobPropsJSON, err := model.NewJSONStructByStruct(&model.Properties{
		Ports: []model.Ports{{Port: 8080}},
	})
	require.NoError(t, err)
	jobTraitsJSON, err := model.NewJSONStructByStruct(&model.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name: "job-svc",
				Type: string(spec.ServiceAccessInternal),
				Ports: []spec.ServicePortTraitSpec{
					{Port: 80, TargetPort: 8080},
				},
			},
		},
	})
	require.NoError(t, err)
	apiTraitsJSON, err := model.NewJSONStructByStruct(&model.Traits{
		Ingress: []spec.IngressTraitsSpec{
			{
				Routes: []spec.IngressRoutes{
					{Host: "api.example.com"},
				},
			},
		},
	})
	require.NoError(t, err)

	appHandler := &applications{
		ApplicationService: componentListApplicationService{
			components: []*model.ApplicationComponent{
				{
					ID:            1,
					AppID:         "app-2",
					Name:          "job-runner",
					Namespace:     "default",
					ComponentType: config.InstantJob,
					Properties:    jobPropsJSON,
					Traits:        jobTraitsJSON,
				},
				{
					ID:            2,
					AppID:         "app-2",
					Name:          "api",
					Namespace:     "default",
					ComponentType: config.ServerJob,
					Traits:        apiTraitsJSON,
				},
			},
		},
		WorkflowService: &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/components", appHandler.listApplicationComponents)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-2/components", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.ListApplicationComponentsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Components, 2)

	var jobComponent *apis.ApplicationComponent
	var apiComponent *apis.ApplicationComponent
	for _, component := range payload.Components {
		switch component.Name {
		case "job-runner":
			jobComponent = component
		case "api":
			apiComponent = component
		}
	}

	require.NotNil(t, jobComponent)
	require.Empty(t, jobComponent.Services)
	require.Empty(t, jobComponent.ExternalLinks)

	require.NotNil(t, apiComponent)
	require.Len(t, apiComponent.Ingresses, 1)
	require.Equal(t, []apis.ComponentIngressRouteInfo{
		{
			Host:        "api.example.com",
			Path:        "/",
			PathType:    "Prefix",
			ServiceName: naming.ServiceName("api", "app-2"),
			ServicePort: 80,
		},
	}, apiComponent.Ingresses[0].Routes)
}

func TestListApplicationComponentsSkipsIngressForUnsupportedComponentTypes(t *testing.T) {
	gin.SetMode(gin.TestMode)

	configTraitsJSON, err := model.NewJSONStructByStruct(&model.Traits{
		Ingress: []spec.IngressTraitsSpec{
			{
				Routes: []spec.IngressRoutes{
					{Host: "config.example.com", Path: "/"},
				},
			},
		},
	})
	require.NoError(t, err)
	apiTraitsJSON, err := model.NewJSONStructByStruct(&model.Traits{
		Ingress: []spec.IngressTraitsSpec{
			{
				Routes: []spec.IngressRoutes{
					{Host: "api.example.com", Path: "/"},
				},
			},
		},
	})
	require.NoError(t, err)

	appHandler := &applications{
		ApplicationService: componentListApplicationService{
			components: []*model.ApplicationComponent{
				{
					ID:            1,
					AppID:         "app-3",
					Name:          "app-config",
					Namespace:     "default",
					ComponentType: config.ConfJob,
					Traits:        configTraitsJSON,
				},
				{
					ID:            2,
					AppID:         "app-3",
					Name:          "api",
					Namespace:     "default",
					ComponentType: config.ServerJob,
					Traits:        apiTraitsJSON,
				},
			},
		},
		WorkflowService: &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/components", appHandler.listApplicationComponents)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-3/components", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.ListApplicationComponentsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Components, 2)

	var configComponent *apis.ApplicationComponent
	var apiComponent *apis.ApplicationComponent
	for _, component := range payload.Components {
		switch component.Name {
		case "app-config":
			configComponent = component
		case "api":
			apiComponent = component
		}
	}

	require.NotNil(t, configComponent)
	require.Empty(t, configComponent.Ingresses)
	require.Empty(t, configComponent.ExternalLinks)

	require.NotNil(t, apiComponent)
	require.Len(t, apiComponent.Ingresses, 1)
	require.Equal(t, []apis.ExternalLink{
		{Type: "ingress", Value: "api.example.com/"},
	}, apiComponent.ExternalLinks)
}

func TestListApplicationComponentsUsesDefaultNamespaceForBlankComponentIngress(t *testing.T) {
	gin.SetMode(gin.TestMode)

	apiTraitsJSON, err := model.NewJSONStructByStruct(&model.Traits{
		Ingress: []spec.IngressTraitsSpec{
			{
				Namespace: "custom-ns",
				Routes: []spec.IngressRoutes{
					{Host: "api.example.com", Path: "/"},
				},
			},
		},
	})
	require.NoError(t, err)

	appHandler := &applications{
		ApplicationService: componentListApplicationService{
			components: []*model.ApplicationComponent{
				{
					ID:            1,
					AppID:         "app-4",
					Name:          "api",
					ComponentType: config.ServerJob,
					Traits:        apiTraitsJSON,
				},
			},
		},
		WorkflowService: &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/components", appHandler.listApplicationComponents)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-4/components", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.ListApplicationComponentsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Components, 1)
	require.Len(t, payload.Components[0].Ingresses, 1)
	require.Equal(t, config.DefaultNamespace, payload.Components[0].Ingresses[0].Namespace)
}

func TestListApplicationComponentsRefreshesAfterConfigJobStatusSync(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &componentCacheSyncStore{
		component: &model.ApplicationComponent{
			ID:            1,
			AppID:         "app-1",
			Name:          "app-config",
			Namespace:     "default",
			ComponentType: config.ConfJob,
			Status:        string(config.ComponentStatusFailed),
			LastAbnormal:  "old error",
		},
	}
	cacheStore := cacheutil.NewMemCache(false)
	appService := &cacheBackedComponentListService{
		cache: cacheStore,
		store: store,
	}
	appHandler := &applications{
		ApplicationService: appService,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/components", appHandler.listApplicationComponents)

	firstReq := httptest.NewRequest(http.MethodGet, "/applications/app-1/components", nil)
	firstResp := httptest.NewRecorder()
	r.ServeHTTP(firstResp, firstReq)
	if firstResp.Code != http.StatusOK {
		t.Fatalf("first request unexpected status code: %d", firstResp.Code)
	}
	var firstPayload apis.ListApplicationComponentsResponse
	requireSuccessResponse(t, firstResp.Body.Bytes(), &firstPayload)
	if len(firstPayload.Components) != 1 {
		t.Fatalf("expected 1 component in first response, got %d", len(firstPayload.Components))
	}
	if firstPayload.Components[0].Status != string(config.ComponentStatusFailed) {
		t.Fatalf("expected first status failed, got %s", firstPayload.Components[0].Status)
	}
	if firstPayload.Components[0].LastAbnormal != "old error" {
		t.Fatalf("expected first lastAbnormal old error, got %s", firstPayload.Components[0].LastAbnormal)
	}

	jobTask := &model.JobTask{
		Name:      "app-config",
		Namespace: "default",
		AppID:     "app-1",
		JobType:   string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "default",
			},
		},
	}
	job.RunJobs(context.Background(), []*model.JobTask{jobTask}, 1, fake.NewSimpleClientset(), nil, store, func() {}, false, cacheStore, nil, nil, nil, nil)

	secondReq := httptest.NewRequest(http.MethodGet, "/applications/app-1/components", nil)
	secondResp := httptest.NewRecorder()
	r.ServeHTTP(secondResp, secondReq)
	if secondResp.Code != http.StatusOK {
		t.Fatalf("second request unexpected status code: %d", secondResp.Code)
	}
	var secondPayload apis.ListApplicationComponentsResponse
	requireSuccessResponse(t, secondResp.Body.Bytes(), &secondPayload)
	if len(secondPayload.Components) != 1 {
		t.Fatalf("expected 1 component in second response, got %d", len(secondPayload.Components))
	}
	if secondPayload.Components[0].Status != string(config.ComponentStatusRunning) {
		t.Fatalf("expected second status running, got %s", secondPayload.Components[0].Status)
	}
	if secondPayload.Components[0].LastAbnormal != "" {
		t.Fatalf("expected second lastAbnormal empty, got %s", secondPayload.Components[0].LastAbnormal)
	}
	if appService.listCalls != 2 {
		t.Fatalf("expected datastore-backed list to be called twice, got %d", appService.listCalls)
	}
}
