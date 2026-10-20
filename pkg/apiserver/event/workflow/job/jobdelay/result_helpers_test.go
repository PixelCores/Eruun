package jobdelay

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobresult"
)

func testResultJobInfo(id int, payload *jobresult.JobResultPayload) *model.JobInfo {
	executionKey := payload.ExecutionKey
	return &model.JobInfo{
		ID:            id,
		TaskID:        payload.TaskID,
		Type:          payload.JobType,
		ServiceName:   payload.ServiceName,
		ExecutionKey:  &executionKey,
		RunGeneration: payload.RunGeneration,
	}
}
