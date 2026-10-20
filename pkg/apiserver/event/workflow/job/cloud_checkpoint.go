package job

import (
	"encoding/json"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	wfcloudcontract "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

type CloudJobInfo = wfcloudcontract.CloudJobInfo

type CloudJobRequest = wfcloudcontract.CloudJobRequest

type CloudJobResult = wfcloudcontract.CloudJobResult

type CloudJobRecord struct {
	Provider     string                 `json:"provider,omitempty"`
	Action       string                 `json:"action,omitempty"`
	ExecutionKey string                 `json:"executionKey,omitempty"`
	Status       config.Status          `json:"status,omitempty"`
	Request      *CloudJobRequest       `json:"request,omitempty"`
	State        map[string]interface{} `json:"state,omitempty"`
	Result       *CloudJobResult        `json:"result,omitempty"`
	Error        string                 `json:"error,omitempty"`
}

func cloudJobExecutionKey(info *CloudJobInfo) string {
	if info == nil {
		return ""
	}
	return strings.TrimSpace(info.ExecutionKey)
}

func cloudJobExecutionKeyFromJob(job *model.JobTask) string {
	if job == nil {
		return ""
	}
	info, ok := optionalJobInfo[*CloudJobInfo](job)
	if !ok {
		return ""
	}
	return cloudJobExecutionKey(info)
}

func cloudJobExecutionKeyFromRecord(record *CloudJobRecord) string {
	if record == nil {
		return ""
	}
	return strings.TrimSpace(record.ExecutionKey)
}

func parseCloudJobRecord(raw string) *CloudJobRecord {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var record CloudJobRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return nil
	}
	return &record
}

func parseCloudCheckpointFromJobInfo(jobInfo *model.JobInfo) *CloudJobRecord {
	if jobInfo == nil {
		return nil
	}
	if record := parseCloudJobRecord(jobInfo.InternalInfo); record != nil {
		return record
	}
	return parseCloudJobRecord(jobInfo.Info)
}

func selectCloudCheckpointJobInfo(job *model.JobTask, candidates []*model.JobInfo) *model.JobInfo {
	if len(candidates) == 0 {
		return nil
	}
	executionKey := cloudJobExecutionKeyFromJob(job)
	if executionKey == "" {
		return candidates[0]
	}
	for _, candidate := range candidates {
		record := parseCloudCheckpointFromJobInfo(candidate)
		if record == nil {
			continue
		}
		if candidateKey := cloudJobExecutionKeyFromRecord(record); candidateKey == executionKey {
			return candidate
		}
	}
	return nil
}
