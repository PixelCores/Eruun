package workflow

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	wfNaming "github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func routeDeleteTask(t *testing.T, appID string, components ...string) *model.WorkflowQueue {
	t.Helper()
	info, err := json.Marshal(model.VersionUpdateResourceActionInfo{
		Source:                config.JobInfoSourceVersionUpdateAction,
		Version:               1,
		RouteDeleteComponents: components,
	})
	require.NoError(t, err)
	return &model.WorkflowQueue{WorkflowID: "wf-route", ProjectID: "proj-route", AppID: appID, TaskID: "task-route", ResourceActionInfo: string(info)}
}

func routeJobs(buckets map[int][]*model.JobTask, jobType config.JobType) []*model.JobTask {
	var jobs []*model.JobTask
	for _, bucket := range buckets {
		for _, job := range bucket {
			if job.JobType == string(jobType) {
				jobs = append(jobs, job)
			}
		}
	}
	return jobs
}

// The delete job must identify the route by the same name and owner labels the
// deploy job wrote, or the executor would treat the route as another owner's.
func TestBuildJobsForComponent_RouteDeleteTargetsDeployedRoute(t *testing.T) {
	propsJSON, err := model.NewJSONStructByStruct(model.Properties{Ports: []model.Ports{{Port: 8080}}, Labels: map[string]string{"layer": "web"}})
	require.NoError(t, err)
	withGateway, err := model.NewJSONStructByStruct(spec.Traits{Gateway: &spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}}})
	require.NoError(t, err)
	component := &model.ApplicationComponent{
		ID:            7,
		Name:          "Api",
		AppID:         "App-Route",
		Namespace:     "nvg",
		ComponentType: config.ServerJob,
		Image:         "api:latest",
		Properties:    propsJSON,
		Traits:        withGateway,
	}
	deployed, err := buildJobsForComponent(context.Background(), nil, component, routeDeleteTask(t, component.AppID), testJobTimeoutSeconds, testIngressGateway, "")
	require.NoError(t, err)
	deployJobs := routeJobs(deployed, config.JobDeployRoute)
	require.Len(t, deployJobs, 1)
	deployedRoute, _, err := jobinfo.RouteFromJobInfo(deployJobs[0])
	require.NoError(t, err)

	component.Traits = nil
	buckets, err := buildJobsForComponent(context.Background(), nil, component, routeDeleteTask(t, component.AppID, "Api"), testJobTimeoutSeconds, testIngressGateway, "")
	require.NoError(t, err)
	require.Empty(t, routeJobs(buckets, config.JobDeployRoute))
	deleteJobs := routeJobs(map[int][]*model.JobTask{config.JobPriorityNormal: buckets[config.JobPriorityNormal]}, config.JobDeleteResource)
	require.Len(t, routeJobs(buckets, config.JobDeleteResource), 1, "the only route job runs with the workloads")
	require.Len(t, deleteJobs, 1)
	ref, resource, err := jobinfo.RouteFromJobInfo(deleteJobs[0])
	require.NoError(t, err)
	require.Equal(t, spec.ResourceIngress, resource.Kind)
	require.Equal(t, deployedRoute.GetName(), ref.GetName())
	require.Equal(t, deployedRoute.GetNamespace(), ref.GetNamespace())
	require.Equal(t, deployedRoute.GetLabels()[spec.LabelAppID], ref.GetLabels()[spec.LabelAppID])
	require.Equal(t, deployedRoute.GetLabels()[spec.LabelComponentName], ref.GetLabels()[spec.LabelComponentName])
}

// Without a recorded gateway removal, or without a platform gateway, a component
// that declares no gateway gets no route job at all.
func TestBuildJobsForComponent_NoRouteJobWithoutGatewayRemoval(t *testing.T) {
	component := &model.ApplicationComponent{Name: "api", AppID: "app-1", Namespace: "nvg", ComponentType: config.ServerJob, Image: "api:latest"}
	for _, tc := range []struct {
		name    string
		task    *model.WorkflowQueue
		gateway spec.GatewayConfig
	}{
		{name: "no removal recorded", task: routeDeleteTask(t, "app-1"), gateway: testIngressGateway},
		{name: "another component's removal", task: routeDeleteTask(t, "app-1", "web"), gateway: testIngressGateway},
		{name: "platform gateway disabled", task: routeDeleteTask(t, "app-1", "api")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buckets, err := buildJobsForComponent(context.Background(), nil, component, tc.task, testJobTimeoutSeconds, tc.gateway, "")
			require.NoError(t, err)
			require.Empty(t, routeJobs(buckets, config.JobDeployRoute))
			require.Empty(t, routeJobs(buckets, config.JobDeleteResource))
		})
	}
}

var testGatewayAPIGateway = spec.GatewayConfig{Provider: spec.GatewayProviderGatewayAPI, Domain: "apps.example.com", ProxyNamespace: "istio-ingress",
	ParentRef: spec.GatewayParentRef{Namespace: "istio-ingress", Name: "eruun", SectionName: spec.GatewayHTTPSListener}}

// Under the gatewayapi provider the complete route payload, with the labels and
// annotation the builder adds, is an HTTPRoute that passes the workspace write
// boundary; the delete payload names the same HTTPRoute.
func TestBuildJobsForComponent_GatewayAPIRoutePassesWorkspacePolicy(t *testing.T) {
	propsJSON, err := model.NewJSONStructByStruct(model.Properties{Ports: []model.Ports{{Port: 8080}}})
	require.NoError(t, err)
	traitsJSON, err := model.NewJSONStructByStruct(spec.Traits{Gateway: &spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{
		{}, {Path: "/api", Rewrite: "/", Timeout: "600s"},
	}}})
	require.NoError(t, err)
	component := &model.ApplicationComponent{ID: 3, Name: "chat", AppID: "app-1", Namespace: "nvg", ComponentType: config.ServerJob, Image: "chat:1", Properties: propsJSON, Traits: traitsJSON}

	buckets, err := buildJobsForComponent(context.Background(), nil, component, routeDeleteTask(t, component.AppID), testJobTimeoutSeconds, testGatewayAPIGateway, "")
	require.NoError(t, err)
	deployJobs := routeJobs(buckets, config.JobDeployRoute)
	require.Len(t, deployJobs, 1)
	require.Equal(t, "gateway: chat-nvg.apps.example.com/, chat-nvg.apps.example.com/api", deployJobs[0].Info)
	route, resource, err := jobinfo.RouteFromJobInfo(deployJobs[0])
	require.NoError(t, err)
	require.Equal(t, spec.ResourceHTTPRoute, resource.Kind)
	require.Equal(t, spec.HTTPRouteResource, resource.GVR)
	deploy, err := jobexec.PrepareTask(deployJobs[0], component.AppID, &model.Workspace{ID: "space", Namespace: "nvg"}, spec.WorkspaceConfig{Gateway: testGatewayAPIGateway})
	require.NoError(t, err)
	require.True(t, deploy)

	component.Traits = nil
	buckets, err = buildJobsForComponent(context.Background(), nil, component, routeDeleteTask(t, component.AppID, "chat"), testJobTimeoutSeconds, testGatewayAPIGateway, "")
	require.NoError(t, err)
	deleteJobs := routeJobs(buckets, config.JobDeleteResource)
	require.Len(t, deleteJobs, 1)
	ref, resource, err := jobinfo.RouteFromJobInfo(deleteJobs[0])
	require.NoError(t, err)
	require.Equal(t, spec.ResourceHTTPRoute, resource.Kind)
	require.Equal(t, route.GetName(), ref.GetName())
	require.Equal(t, "delete httproute: nvg/"+route.GetName(), deleteJobs[0].Info)
}

// A Service the update stopped rendering is deleted by name with the owner
// labels its deploy job wrote; Services the component still renders are not.
// Application IDs are generated lowercase, so the Service and route label paths
// write the same app-id value.
func TestBuildJobsForComponent_ServiceDeleteTargetsDeployedService(t *testing.T) {
	propsJSON, err := model.NewJSONStructByStruct(model.Properties{Ports: []model.Ports{{Port: 8080}}, Labels: map[string]string{"layer": "web"}})
	require.NoError(t, err)
	selector := map[string]string{"app": "api"}
	withMetrics, err := model.NewJSONStructByStruct(spec.Traits{Service: []spec.ServiceTraitSpec{
		{Selector: selector, Ports: []spec.ServicePortTraitSpec{{Port: 8080}}},
		{Name: "api-metrics", Selector: selector, Ports: []spec.ServicePortTraitSpec{{Port: 9100}}},
	}})
	require.NoError(t, err)
	component := &model.ApplicationComponent{ID: 7, Name: "Api", AppID: "app-route", Namespace: "nvg", ComponentType: config.ServerJob, Image: "api:latest", Properties: propsJSON, Traits: withMetrics}
	deployed, err := buildJobsForComponent(context.Background(), nil, component, routeDeleteTask(t, component.AppID), testJobTimeoutSeconds, spec.GatewayConfig{}, "")
	require.NoError(t, err)
	var metrics *corev1.Service
	for _, job := range routeJobs(deployed, config.JobDeployService) {
		if service := job.JobInfo.(*corev1.Service); service.Name == "api-metrics" {
			metrics = service
		}
	}
	require.NotNil(t, metrics)

	component.Traits, err = model.NewJSONStructByStruct(spec.Traits{Service: []spec.ServiceTraitSpec{
		{Selector: selector, Ports: []spec.ServicePortTraitSpec{{Port: 8080}}},
	}})
	require.NoError(t, err)
	info, err := json.Marshal(model.VersionUpdateResourceActionInfo{
		Source: config.JobInfoSourceVersionUpdateAction, Version: 1,
		DeletedServices: map[string][]string{"Api": {"api-metrics"}},
	})
	require.NoError(t, err)
	task := &model.WorkflowQueue{WorkflowID: "wf", ProjectID: "proj", AppID: component.AppID, TaskID: "task", ResourceActionInfo: string(info)}
	buckets, err := buildJobsForComponent(context.Background(), nil, component, task, testJobTimeoutSeconds, spec.GatewayConfig{}, "")
	require.NoError(t, err)
	deleteJobs := routeJobs(map[int][]*model.JobTask{config.JobPriorityNormal: buckets[config.JobPriorityNormal]}, config.JobDeleteResource)
	require.Len(t, routeJobs(buckets, config.JobDeleteResource), 1)
	require.Len(t, deleteJobs, 1)
	target, resource, err := jobinfo.DeletionTargetFromJobInfo(deleteJobs[0])
	require.NoError(t, err)
	require.Equal(t, spec.ResourceService, resource.Kind)
	require.Equal(t, "api-metrics", target.GetName())
	require.Equal(t, metrics.Namespace, target.GetNamespace())
	require.Equal(t, metrics.Labels[spec.LabelAppID], target.GetLabels()[spec.LabelAppID])
	require.Equal(t, metrics.Labels[spec.LabelComponentName], target.GetLabels()[spec.LabelComponentName])
	require.Equal(t, "delete service: nvg/api-metrics", deleteJobs[0].Info)
}

// A route and the default Service share one naming rule, so dropping both in
// one update yields two resource_delete jobs with the same name. Their job
// records are told apart only by the execution key, which must differ.
func TestBuildJobsForComponent_SameNamedDeleteJobsHaveDistinctExecutionKeys(t *testing.T) {
	component := &model.ApplicationComponent{ID: 9, Name: "api", AppID: "app-route", Namespace: "nvg", ComponentType: config.ServerJob, Image: "api:latest"}
	sharedName := wfNaming.ServiceName(component.Name, component.ResourceNameKey())
	require.Equal(t, sharedName, wfNaming.GatewayRouteName(component.Name, component.ResourceNameKey()))
	info, err := json.Marshal(model.VersionUpdateResourceActionInfo{
		Source: config.JobInfoSourceVersionUpdateAction, Version: 1,
		RouteDeleteComponents: []string{"api"},
		DeletedServices:       map[string][]string{"api": {sharedName}},
	})
	require.NoError(t, err)
	task := &model.WorkflowQueue{WorkflowID: "wf", ProjectID: "proj", AppID: component.AppID, TaskID: "task", RunGeneration: 1, ResourceActionInfo: string(info)}

	buckets, err := buildJobsForComponent(context.Background(), nil, component, task, testJobTimeoutSeconds, testIngressGateway, "")
	require.NoError(t, err)
	normal := buckets[config.JobPriorityNormal]
	applyWorkflowJobExecutionIdentity(normal, task, 0, config.JobPriorityNormal)
	deleteJobs := routeJobs(map[int][]*model.JobTask{config.JobPriorityNormal: normal}, config.JobDeleteResource)
	require.Len(t, deleteJobs, 2)
	require.Equal(t, deleteJobs[0].Name, deleteJobs[1].Name)
	require.NotEmpty(t, deleteJobs[0].ExecutionKey)
	require.NotEqual(t, deleteJobs[0].ExecutionKey, deleteJobs[1].ExecutionKey)
	kinds := []spec.ResourceKind{}
	for _, job := range deleteJobs {
		_, resource, err := jobinfo.DeletionTargetFromJobInfo(job)
		require.NoError(t, err)
		kinds = append(kinds, resource.Kind)
	}
	require.ElementsMatch(t, []spec.ResourceKind{spec.ResourceIngress, spec.ResourceService}, kinds)
}
