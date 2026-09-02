package workflow

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
)

const versionUpdateRestartStepName = "restart-components"

func versionUpdateResourceActionInfoFromTask(task *model.WorkflowQueue) (model.VersionUpdateResourceActionInfo, bool, error) {
	if task == nil {
		return model.VersionUpdateResourceActionInfo{}, false, nil
	}
	raw := strings.TrimSpace(task.ResourceActionInfo)
	if raw == "" {
		return model.VersionUpdateResourceActionInfo{}, false, nil
	}
	var marker struct {
		Source  string `json:"source"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal([]byte(raw), &marker); err != nil {
		return model.VersionUpdateResourceActionInfo{}, true, err
	}
	if marker.Source != config.JobInfoSourceVersionUpdateAction {
		return model.VersionUpdateResourceActionInfo{}, true, fmt.Errorf("unsupported resource action info source %q", marker.Source)
	}
	if marker.Version != 1 {
		return model.VersionUpdateResourceActionInfo{}, true, fmt.Errorf("unsupported resource action info version %d", marker.Version)
	}
	var info model.VersionUpdateResourceActionInfo
	if err := json.Unmarshal([]byte(raw), &info); err != nil {
		return model.VersionUpdateResourceActionInfo{}, true, err
	}
	return info, true, nil
}

func buildVersionUpdateRestartExecution(
	info model.VersionUpdateResourceActionInfo,
	componentMap map[string]*model.ApplicationComponent,
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
) (*StepExecution, error) {
	buckets := buildVersionRestartJobs(info.RestartComponents, componentMap, task, defaultJobTimeoutSeconds, info.ImageReadyTimeoutSeconds)
	if bucketsEmpty(buckets) {
		return nil, nil
	}
	return &StepExecution{
		Name:     versionUpdateRestartStepName,
		Mode:     config.WorkflowModeStepByStep,
		StepType: config.WorkflowStepTypeComponent,
		Jobs:     buckets,
	}, nil
}

func buildVersionRestartJobs(
	componentNames []string,
	componentMap map[string]*model.ApplicationComponent,
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
	restartReadyTimeoutSeconds int64,
) map[int][]*model.JobTask {
	buckets := newJobBuckets()
	if task == nil || len(componentNames) == 0 {
		return buckets
	}
	componentsByName := make(map[string]*model.ApplicationComponent, len(componentMap))
	for name, component := range componentMap {
		key := strings.ToLower(strings.TrimSpace(name))
		if key != "" && component != nil {
			componentsByName[key] = component
		}
	}

	seen := make(map[string]struct{}, len(componentNames))
	for _, name := range componentNames {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		component := componentsByName[key]
		if component == nil {
			continue
		}
		componentCopy := cloneVersionRestartComponent(component)
		jobTask := NewJobTask(componentCopy.Name, componentCopy.Namespace, task.WorkflowID, task.ProjectID, task.AppID, task.TaskID, defaultJobTimeoutSeconds, componentCopy.ResourceNameKey())
		jobTask.JobType = string(config.JobVersionRestart)
		jobTask.JobInfo = &workflowjob.VersionRestartJobInfo{Component: componentCopy}
		jobTask.Info = fmt.Sprintf("restart component: %s/%s", componentCopy.Namespace, componentCopy.Name)
		setDeployTimeout(jobTask)
		if restartReadyTimeoutSeconds > 0 {
			jobTask.Timeout = restartReadyTimeoutSeconds
		}
		buckets[config.JobPriorityLow] = append(buckets[config.JobPriorityLow], jobTask)
	}
	return buckets
}

func versionUpdateImageReadyTimeoutForComponent(task *model.WorkflowQueue, componentName string) (int64, bool) {
	info, ok, err := versionUpdateResourceActionInfoFromTask(task)
	if err != nil || !ok || info.ImageReadyTimeoutSeconds <= 0 || len(info.ImageReadyComponents) == 0 {
		return 0, false
	}
	target := strings.ToLower(strings.TrimSpace(componentName))
	if target == "" {
		return 0, false
	}
	for _, name := range info.ImageReadyComponents {
		if strings.ToLower(strings.TrimSpace(name)) == target {
			return info.ImageReadyTimeoutSeconds, true
		}
	}
	return 0, false
}

func cloneVersionRestartComponent(component *model.ApplicationComponent) *model.ApplicationComponent {
	if component == nil {
		return nil
	}
	cp := *component
	if strings.TrimSpace(cp.Namespace) == "" {
		cp.Namespace = config.DefaultNamespace
	}
	return &cp
}
