package workflow

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
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
	deleteJobs := routeJobs(map[int][]*model.JobTask{config.JobPriorityNormal: buckets[config.JobPriorityNormal]}, config.JobDeleteRoute)
	require.Len(t, routeJobs(buckets, config.JobDeleteRoute), 1, "the only route job runs with the workloads")
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
			require.Empty(t, routeJobs(buckets, config.JobDeleteRoute))
		})
	}
}
