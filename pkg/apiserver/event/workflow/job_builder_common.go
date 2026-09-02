package workflow

import (
	"sort"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func newJobBuckets() map[int][]*model.JobTask {
	return map[int][]*model.JobTask{
		config.JobPriorityMaxHigh: {},
		config.JobPriorityHigh:    {},
		config.JobPriorityNormal:  {},
		config.JobPriorityLow:     {},
	}
}

func mergeJobBuckets(dst, src map[int][]*model.JobTask) {
	for priority, jobs := range src {
		if len(jobs) == 0 {
			continue
		}
		dst[priority] = append(dst[priority], jobs...)
	}
}

func bucketsEmpty(buckets map[int][]*model.JobTask) bool {
	for _, jobs := range buckets {
		if len(jobs) > 0 {
			return false
		}
	}
	return true
}

func countJobs(buckets map[int][]*model.JobTask) int {
	count := 0
	for _, jobs := range buckets {
		count += len(jobs)
	}
	return count
}

func determineStepConcurrency(mode config.WorkflowMode, jobCount, sequentialLimit int) int {
	if jobCount <= 0 {
		return 0
	}
	if mode.IsParallel() {
		return jobCount
	}
	if sequentialLimit < 1 {
		sequentialLimit = 1
	}
	if jobCount < sequentialLimit {
		return jobCount
	}
	return sequentialLimit
}

func sortedPriorities(jobs map[int][]*model.JobTask) []int {
	var priorities []int
	for priority := range jobs {
		priorities = append(priorities, priority)
	}
	sort.Ints(priorities)
	return priorities
}

func nameOrFallback(name, fallback string) string {
	if name != "" {
		return name
	}
	return fallback
}

func cloneMapInterface(src map[string]interface{}) map[string]interface{} {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
