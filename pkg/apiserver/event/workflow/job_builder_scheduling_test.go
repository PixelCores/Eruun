package workflow

import (
	"context"
	"strings"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/stretchr/testify/require"
)

func TestSchedulingClassesPreserveDependenciesAndSubstepOverride(t *testing.T) {
	for _, mode := range []config.WorkflowMode{config.WorkflowModeDAG, config.WorkflowModeStepByStep} {
		t.Run(string(mode), func(t *testing.T) {
			props, err := model.NewJSONStructByStruct(model.Properties{Ports: []model.Ports{{Port: 80}}})
			require.NoError(t, err)
			components := map[string]*model.ApplicationComponent{}
			for _, name := range []string{"first", "second", "third"} {
				components[name] = &model.ApplicationComponent{Name: name, AppID: "app", Namespace: "default", Image: "nginx:1.21", Replicas: 1, ComponentType: config.ServerJob, Properties: props}
			}
			steps := &model.WorkflowSteps{Steps: []*model.WorkflowStep{{Name: "deploy", Mode: mode, SchedulingClass: "high", SubSteps: []*model.WorkflowSubStep{
				{Name: "first", WorkflowType: config.JobDeploy, SchedulingClass: "background"},
				{Name: "second", WorkflowType: config.JobDeploy},
				{Name: "third", WorkflowType: config.JobDeploy, SchedulingClass: "normal"},
			}}}}
			groups := buildWorkflowStepExecutionGroups(context.Background(), steps, components, &model.WorkflowQueue{AppID: "app", TaskID: "task"}, 60)
			require.Len(t, groups, 1)
			found := map[string]bool{}
			for _, execution := range groups[0] {
				for priority, jobs := range execution.Jobs {
					for _, task := range jobs {
						if task.JobType == string(config.JobDeployService) {
							require.Equal(t, config.JobPriorityHigh, priority)
						}
						if task.JobType == string(config.JobDeploy) {
							require.Equal(t, config.JobPriorityNormal, priority)
						}
						// Generated resource names retain their component prefix.
						for _, component := range []string{"first", "second", "third"} {
							if !strings.Contains(task.Name, component) {
								continue
							}
							want := map[string]string{"first": "background", "second": "high", "third": "normal"}[component]
							require.Equal(t, want, task.SchedulingClass)
							found[component] = true
						}
					}
				}
			}
			require.Len(t, found, 3)
		})
	}
}
