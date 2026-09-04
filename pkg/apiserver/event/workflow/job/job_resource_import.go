package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	importcontract "github.com/PixelCores/Eruun/pkg/apiserver/resourceimport/contract"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
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
	var checkpoint json.RawMessage
	if taskType == config.WorkflowTaskTypeResourceImportManage {
		checkpoint, err = c.loadManageCheckpoint(ctx)
		if err != nil {
			return errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("load resource import management checkpoint: %w", err))
		}
		if len(checkpoint) == 0 {
			checkpoint, err = c.executor.PrepareResourceImportJob(ctx, taskType, info.Request)
			if err != nil {
				return err
			}
			if len(checkpoint) == 0 || !json.Valid(checkpoint) {
				return fmt.Errorf("resource import management checkpoint is invalid")
			}
			previousInternalInfo := c.job.InternalInfo
			c.job.InternalInfo = string(checkpoint)
			if err := c.SaveInfo(ctx); err != nil {
				c.job.InternalInfo = previousInternalInfo
				return errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("persist resource import management checkpoint: %w", err))
			}
		}
	}
	result, err := c.executor.ExecuteResourceImportJob(ctx, taskType, info.Request, checkpoint)
	if err != nil {
		return err
	}
	c.job.Info = string(result)
	return nil
}

func (c *ResourceImportJobCtl) loadManageCheckpoint(ctx context.Context) (json.RawMessage, error) {
	if c == nil || c.job == nil {
		return nil, fmt.Errorf("resource import job is nil")
	}
	if raw := strings.TrimSpace(c.job.InternalInfo); raw != "" {
		if !json.Valid([]byte(raw)) {
			return nil, fmt.Errorf("persisted resource import management checkpoint is invalid")
		}
		return json.RawMessage(raw), nil
	}
	jobInfos, err := loadJobInfos(ctx, c.store, c.job.TaskID, c.job.JobType, "")
	if err != nil {
		return nil, err
	}
	for _, jobInfo := range jobInfos {
		if jobInfo == nil {
			continue
		}
		raw := strings.TrimSpace(jobInfo.InternalInfo)
		if raw == "" {
			continue
		}
		if !json.Valid([]byte(raw)) {
			return nil, fmt.Errorf("persisted resource import management checkpoint is invalid")
		}
		c.job.InternalInfo = raw
		return json.RawMessage(raw), nil
	}
	return nil, nil
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
