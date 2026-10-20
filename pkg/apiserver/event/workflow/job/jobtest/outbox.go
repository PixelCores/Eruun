package jobtest

import (
	"context"
	"fmt"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func EnsureActiveTestContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func OutboxTransitionKey(from, to config.JobResultOutboxState) string {
	return string(from) + "->" + string(to)
}

func MatchOutboxFilters(outbox *model.JobResultOutbox, opts *datastore.ListOptions) bool {
	if opts == nil {
		return true
	}
	for _, filter := range opts.FilterOptions.In {
		if filter.Key != "state" {
			continue
		}
		matched := false
		for _, value := range filter.Values {
			if string(outbox.State) == value {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func MatchJobInfoFilters(jobInfo *model.JobInfo, opts *datastore.ListOptions) bool {
	if opts == nil {
		return true
	}
	for _, filter := range opts.FilterOptions.In {
		switch filter.Key {
		case "status":
			if !ContainsString(filter.Values, jobInfo.Status) {
				return false
			}
		case "delay_state":
			if !ContainsString(filter.Values, string(jobInfo.DelayState)) {
				return false
			}
		case "type":
			if !ContainsString(filter.Values, jobInfo.Type) {
				return false
			}
		case "service_name":
			if !ContainsString(filter.Values, jobInfo.ServiceName) {
				return false
			}
		case "execution_key":
			if jobInfo.ExecutionKey == nil || !ContainsString(filter.Values, *jobInfo.ExecutionKey) {
				return false
			}
		case "run_generation":
			if !ContainsString(filter.Values, fmt.Sprint(jobInfo.RunGeneration)) {
				return false
			}
		}
	}
	for _, filter := range opts.FilterOptions.LessThan {
		switch filter.Key {
		case "delay_execute_at":
			deadline, ok := filter.Value.(int64)
			if !ok || jobInfo.DelayExecuteAt >= deadline {
				return false
			}
		case "id":
			beforeID, ok := filter.Value.(int)
			if !ok || jobInfo.ID >= beforeID {
				return false
			}
		}
	}
	return true
}

func ContainsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func PaginateOutboxes(outboxes []*model.JobResultOutbox, opts *datastore.ListOptions) []datastore.Entity {
	start, end := PageBounds(len(outboxes), opts)
	list := make([]datastore.Entity, 0, end-start)
	for _, outbox := range outboxes[start:end] {
		list = append(list, outbox)
	}
	return list
}

func PaginateJobInfos(jobInfos []*model.JobInfo, opts *datastore.ListOptions) []datastore.Entity {
	start, end := PageBounds(len(jobInfos), opts)
	list := make([]datastore.Entity, 0, end-start)
	for _, jobInfo := range jobInfos[start:end] {
		list = append(list, jobInfo)
	}
	return list
}

func PageBounds(length int, opts *datastore.ListOptions) (int, int) {
	if opts == nil || opts.PageSize <= 0 || opts.Page <= 0 {
		return 0, length
	}
	start := (opts.Page - 1) * opts.PageSize
	if start >= length {
		return length, length
	}
	end := start + opts.PageSize
	if end > length {
		end = length
	}
	return start, end
}

func ApplyOutboxUpdates(outbox *model.JobResultOutbox, updates map[string]interface{}) {
	for key, value := range updates {
		switch key {
		case "state":
			outbox.State = value.(config.JobResultOutboxState)
		case "message_id":
			outbox.MessageID = value.(string)
		case "job_uid":
			outbox.JobUID = value.(string)
		case "lease_expires_at":
			outbox.LeaseExpiresAt, _ = value.(*time.Time)
		case "attempts":
			outbox.Attempts = value.(int)
		case "last_error":
			outbox.LastError = value.(string)
		}
	}
}
