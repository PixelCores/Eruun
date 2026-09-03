package workspace

import (
	"encoding/json"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// ValidateTask validates expanded jobs before namespace initialization. The
// transport repeats payload validation at the actual Kubernetes write boundary.
// The boolean identifies jobs which deploy resources and need the baseline.
func ValidateTask(task *model.JobTask, appID string, w *model.Workspace, cfg spec.WorkspaceConfig) (bool, error) {
	if task == nil || w == nil || task.AppID != appID || task.Namespace != w.Namespace {
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
	case config.JobDeployInstant:
		resource = "jobs"
	case config.JobDeployScheduled:
		// Scheduled execution supports CronJob and one-shot Job payloads.
		resource = "cronjobs"
	case config.JobDeployCallback, config.JobCleanupResources, config.JobDatabaseReset, config.JobLogArchiveUpload, config.JobVersionRestart:
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
	if resource == "cronjobs" && mapAt(mapAt(obj, "spec"), "jobTemplate") == nil {
		resource = "jobs"
	}
	transport := &tenantTransport{namespace: w.Namespace, config: cfg}
	if err = transport.prepare(resource, obj); err != nil {
		return false, err
	}
	return true, nil
}
