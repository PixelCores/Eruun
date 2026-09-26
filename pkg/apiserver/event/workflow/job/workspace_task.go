package job

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// PrepareTask validates expanded jobs and writes policy defaults back to their
// typed payloads before comparisons, checkpointing and namespace initialization.
// The transport repeats validation at the actual Kubernetes write boundary.
// The boolean identifies jobs which deploy resources and need the baseline.
func PrepareTask(task *model.JobTask, appID string, w *model.Workspace, cfg spec.WorkspaceConfig) (bool, error) {
	if task == nil || w == nil || task.AppID != appID || task.Namespace != w.Namespace {
		return false, bcode.ErrForbidden
	}
	if (task.JobType == string(config.JobCommand) || task.JobType == string(config.JobEval)) &&
		(task.AppID != "" || task.TaskID == "" || task.WorkspaceID != w.ID) {
		return false, bcode.ErrForbidden
	}
	resource := ""
	switch config.JobType(task.JobType) {
	case config.JobDeploy:
		resource = "deployments"
	case config.JobDeployStore:
		resource = "statefulsets"
	case config.JobDeployService:
		resource = "services"
	case config.JobDeployPVC:
		resource = "persistentvolumeclaims"
	case config.JobDeployConfigMap:
		resource = "configmaps"
	case config.JobDeploySecret:
		resource = "secrets"
	case config.JobDeployIngress:
		resource = "ingresses"
	case config.JobDeployInstant, config.JobCommand, config.JobEval:
		resource = "jobs"
	case config.JobDeployScheduled:
		// Scheduled execution supports CronJob and one-shot Job payloads.
		resource = "cronjobs"
	case config.JobDeployCallback, config.JobCleanupResources, config.JobDatabaseReset, config.JobLogArchiveUpload, config.JobVersionRestart:
		return false, nil
	case config.JobResourceImportScan, config.JobResourceImportManage:
		return false, nil
	default:
		return false, bcode.ErrForbidden
	}
	raw, err := json.Marshal(task.JobInfo)
	if err != nil {
		return false, err
	}
	var obj map[string]interface{}
	if json.Unmarshal(raw, &obj) != nil || obj == nil {
		return false, bcode.ErrForbidden
	}
	if resource == "cronjobs" {
		spec, _ := obj["spec"].(map[string]interface{})
		template, _ := spec["jobTemplate"].(map[string]interface{})
		if template == nil {
			resource = "jobs"
		}
	}
	if err = workspace.PrepareResource(w.Namespace, resource, obj, cfg); err != nil {
		return false, err
	}
	raw, err = json.Marshal(obj)
	if err != nil {
		return false, fmt.Errorf("encode prepared workspace task: %w", err)
	}
	value := reflect.ValueOf(task.JobInfo)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return false, fmt.Errorf("workspace task payload must be a non-nil pointer")
	}
	// Unmarshal merges into existing structs and maps. Decode a fresh value so
	// policy-removed fields disappear, then preserve the caller's payload pointer.
	prepared := reflect.New(value.Type().Elem())
	if err = json.Unmarshal(raw, prepared.Interface()); err != nil {
		return false, fmt.Errorf("populate prepared workspace task: %w", err)
	}
	value.Elem().Set(prepared.Elem())
	return true, nil
}
