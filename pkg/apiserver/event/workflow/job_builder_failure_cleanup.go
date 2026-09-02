package workflow

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func buildWorkflowFailureCleanupJobs(ctx context.Context, task *model.WorkflowQueue, ds datastore.DataStore, defaultJobTimeoutSeconds int64) ([]*model.JobTask, error) {
	if task == nil {
		return nil, nil
	}
	app, err := loadWorkflowTaskApplication(ctx, task, ds)
	if err != nil {
		return nil, err
	}
	if app.EffectiveManagementMode() != config.ManagementModeNative {
		return nil, nil
	}
	entities, err := ds.List(ctx, &model.ApplicationComponent{AppID: task.AppID}, &datastore.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list application components for app %s: %w", task.AppID, err)
	}
	if len(entities) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(app.Name) == "" {
		return nil, fmt.Errorf("application %s name is empty", task.AppID)
	}
	resourceAppName := naming.ApplicationResourceKey(app.Name, "", false)
	components := make([]*model.ApplicationComponent, 0, len(entities))
	for _, entity := range entities {
		component, ok := entity.(*model.ApplicationComponent)
		if !ok || component == nil {
			continue
		}
		componentCopy := *component
		componentCopy.ResourceAppName = resourceAppName
		components = append(components, &componentCopy)
	}
	sort.SliceStable(components, func(i, j int) bool {
		left := strings.ToLower(strings.TrimSpace(components[i].Namespace + "/" + components[i].Name))
		right := strings.ToLower(strings.TrimSpace(components[j].Namespace + "/" + components[j].Name))
		if left == right {
			return components[i].ID < components[j].ID
		}
		return left < right
	})

	cleanupJobs := make([]*model.JobTask, 0, len(components))
	for _, component := range components {
		buckets := buildCleanupJobsForComponent(component, task, defaultJobTimeoutSeconds)
		cleanupJobs = append(cleanupJobs, buckets[config.JobPriorityLow]...)
	}
	applyWorkflowJobExecutionIdentity(cleanupJobs, task, -1, config.JobPriorityLow)
	return cleanupJobs, nil
}
