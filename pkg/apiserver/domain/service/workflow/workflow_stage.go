package workflow

import (
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

const (
	stageUnknownName         = "unknown"
	stageMessageKeySeparator = "|"
)

func stageNameForJob(job *model.JobInfo) string {
	if job == nil {
		return stageUnknownName
	}
	name := strings.TrimSpace(job.ServiceName)
	if name != "" {
		return name
	}
	name = strings.TrimSpace(job.Type)
	if name == "" {
		return stageUnknownName
	}
	return name
}

type stageAggregate struct {
	detail        apis.TaskStageDetail
	typeOrder     []string
	typeSet       map[string]struct{}
	infoMessages  []apis.TaskStageMessage
	infoSeen      map[string]struct{}
	errorMessages []apis.TaskStageMessage
	errorSeen     map[string]struct{}
}

func newStageAggregate(name string) *stageAggregate {
	return &stageAggregate{
		detail: apis.TaskStageDetail{
			Name: name,
		},
		typeSet:   make(map[string]struct{}),
		infoSeen:  make(map[string]struct{}),
		errorSeen: make(map[string]struct{}),
	}
}

func (s *stageAggregate) add(job *model.JobInfo) {
	if job == nil {
		return
	}
	if s.detail.ID == 0 {
		s.detail.ID = job.ID
	}
	if s.detail.Status == "" {
		s.detail.Status = job.Status
	} else {
		s.detail.Status = chooseAggStatus(s.detail.Status, job.Status)
	}
	if job.StartTime != 0 && (s.detail.StartTime == 0 || job.StartTime < s.detail.StartTime) {
		s.detail.StartTime = job.StartTime
	}
	if job.EndTime > s.detail.EndTime {
		s.detail.EndTime = job.EndTime
	}
	if job.Type != "" {
		if _, ok := s.typeSet[job.Type]; !ok {
			s.typeSet[job.Type] = struct{}{}
			s.typeOrder = append(s.typeOrder, job.Type)
		}
	}
	appendUniqueMessage(&s.infoMessages, s.infoSeen, apis.TaskStageMessage{
		Type:    job.Type,
		Message: publicStageInfoMessage(job),
	})
	appendUniqueMessage(&s.errorMessages, s.errorSeen, apis.TaskStageMessage{
		Component: s.detail.Name,
		Message:   job.Error,
	})
}

func (s *stageAggregate) finalize() apis.TaskStageDetail {
	if len(s.typeOrder) > 0 {
		s.detail.Type = formatStageTypes(s.typeOrder)
	}
	if len(s.infoMessages) > 0 {
		s.detail.Info = s.infoMessages
	}
	if len(s.errorMessages) > 0 {
		s.detail.Error = s.errorMessages
	}
	return s.detail
}

func publicStageInfoMessage(job *model.JobInfo) string {
	if job == nil {
		return ""
	}
	if config.JobType(job.Type) == config.JobDeployCloud {
		raw := strings.TrimSpace(job.InternalInfo)
		if raw == "" {
			raw = strings.TrimSpace(job.Info)
		}
		return workflowjob.PublicCloudJobInfoMessage(raw)
	}
	return strings.TrimSpace(job.Info)
}

func formatStageTypes(types []string) string {
	if len(types) == 0 {
		return ""
	}
	return "[" + strings.Join(types, ",") + "]"
}

func appendUniqueMessage(messages *[]apis.TaskStageMessage, seen map[string]struct{}, msg apis.TaskStageMessage) {
	msg.Message = strings.TrimSpace(msg.Message)
	msg.Type = strings.TrimSpace(msg.Type)
	msg.Component = strings.TrimSpace(msg.Component)
	if msg.Message == "" {
		return
	}
	key := msg.Message
	if msg.Type != "" {
		key = msg.Type + stageMessageKeySeparator + key
	}
	if msg.Component != "" {
		key = msg.Component + stageMessageKeySeparator + key
	}
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}
	*messages = append(*messages, msg)
}
