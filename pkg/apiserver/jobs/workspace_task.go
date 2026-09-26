package jobs

import (
	"encoding/json"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// PrepareEvaluationTask validates task identity before applying trusted runner policy.
func PrepareEvaluationTask(task *model.JobTask, w *model.Workspace, cfg spec.WorkspaceConfig, image string) error {
	if task == nil || w == nil || task.JobType != string(config.JobEval) || task.EvaluationInfo == "" || task.WorkspaceID != w.ID || task.Namespace != w.Namespace {
		return bcode.ErrForbidden
	}
	raw, err := json.Marshal(task.JobInfo)
	if err != nil {
		return err
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return err
	}
	if err := workspace.PrepareEvaluationJob(w.Namespace, task.Name, image, obj); err != nil {
		return err
	}

	raw, err = json.Marshal(obj)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, task.JobInfo)
}
