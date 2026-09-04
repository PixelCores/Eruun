package job

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	importcontract "github.com/PixelCores/Eruun/pkg/apiserver/resourceimport/contract"
)

type ResourceImportJobCtl struct {
	job      *model.JobTask
	store    datastore.DataStore
	executor ResourceImportExecutor
}

func NewResourceImportJobCtl(
	job *model.JobTask,
	store datastore.DataStore,
	executor ResourceImportExecutor,
) *ResourceImportJobCtl {
	return &ResourceImportJobCtl{job: job, store: store, executor: executor}
}

func (c *ResourceImportJobCtl) Clean(context.Context) {}

func (c *ResourceImportJobCtl) SaveInfo(ctx context.Context) error {
	return saveJobInfo(ctx, c.store, c.job)
}

func (c *ResourceImportJobCtl) Run(ctx context.Context) error {
	if c.executor == nil {
		return fmt.Errorf("resource import executor is nil")
	}
	info, err := requiredJobInfo[*importcontract.TaskEnvelope](c.job)
	if err != nil {
		return err
	}
	if info.Version != importcontract.TaskEnvelopeVersion || info.Namespace == "" || len(info.Request) == 0 || !json.Valid(info.Request) {
		return fmt.Errorf("resource import task info is invalid")
	}
	taskType, ok := resourceImportTaskType(config.JobType(c.job.JobType))
	if !ok {
		return fmt.Errorf("unsupported resource import job type %q", c.job.JobType)
	}
	result, err := c.executor.ExecuteResourceImportJob(ctx, taskType, info.Request)
	if err != nil {
		return err
	}
	c.job.Info = string(result)
	return nil
}

func resourceImportTaskType(jobType config.JobType) (config.WorkflowTaskType, bool) {
	switch jobType {
	case config.JobResourceImportScan:
		return config.WorkflowTaskTypeResourceImportScan, true
	case config.JobResourceImportManage:
		return config.WorkflowTaskTypeResourceImportManage, true
	default:
		return "", false
	}
}

func isResourceImportJobType(jobType config.JobType) bool {
	_, ok := resourceImportTaskType(jobType)
	return ok
}
