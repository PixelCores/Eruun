package repository

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type jobAdmissionRecoveryIdentity struct {
	UID           string
	Namespace     string
	Name          string
	TaskID        string
	ExecutionKey  string
	RunGeneration uint64
	Deadline      int64
	Attempt       uint
	Kind          string
}

// Only a previously admitted, immutable, already-created execution may retain
// capacity through ownership recovery. The runtime independently confirms the
// live UID before transferring this reservation and refuses replay on absence.
func recoverableJobAdmission(job *model.JobInfo, now time.Time) *jobAdmissionRecoveryIdentity {
	if job == nil || job.SchedulingQueuedAt == nil || job.SchedulingGeneration == 0 || jobSchedulingTerminal(job.Status) || jobAdmissionExpired(job, now) {
		return nil
	}
	if job.SchedulingState != workflowconfig.JobSchedulingAdmitted &&
		!(job.SchedulingState == workflowconfig.JobSchedulingReleased &&
			(job.SchedulingReason == "execution completed or scheduling ownership expired" || job.SchedulingReason == "job execution returned")) {
		return nil
	}
	return jobAdmissionCheckpointIdentity(job, now)
}

// Runner heartbeat/progress and retention metadata may change independently;
// compare only the immutable execution identity when transferring ownership.
func jobAdmissionCheckpointIdentity(job *model.JobInfo, now time.Time) *jobAdmissionRecoveryIdentity {
	if job == nil || (job.Type != string(config.JobCommand) && job.Type != string(config.JobEval)) || job.ExecutionKey == nil || *job.ExecutionKey == "" || job.RunGeneration == 0 {
		return nil
	}
	var checkpoint struct {
		Kind       string `json:"kind"`
		Version    int    `json:"version"`
		Attempt    uint   `json:"attempt"`
		CurrentUID string `json:"currentUID"`
		Deadline   int64  `json:"deadline"`
		Job        *struct {
			Metadata metav1.ObjectMeta `json:"metadata"`
		} `json:"job"`
	}
	if json.Unmarshal([]byte(job.InternalInfo), &checkpoint) != nil || checkpoint.Kind != "instant_job_retry" || checkpoint.Version != 1 ||
		checkpoint.CurrentUID == "" || checkpoint.Attempt != 1 || checkpoint.Attempt != job.Attempt ||
		!time.Unix(0, checkpoint.Deadline).After(now) || checkpoint.Job == nil {
		return nil
	}
	metadata := checkpoint.Job.Metadata
	if metadata.Name == "" || metadata.Namespace == "" || metadata.Annotations[config.AnnotationJobTaskID] != job.TaskID ||
		metadata.Annotations[config.AnnotationJobExecutionKey] != *job.ExecutionKey ||
		metadata.Annotations[config.AnnotationJobRunGeneration] != strconv.FormatUint(job.RunGeneration, 10) ||
		metadata.Annotations[workflowconfig.AnnotationJobAttempt] != strconv.FormatUint(uint64(checkpoint.Attempt), 10) {
		return nil
	}
	var policy workflowconfig.JobRetryPolicy
	if json.Unmarshal([]byte(metadata.Annotations[workflowconfig.AnnotationJobRetryPolicy]), &policy) != nil || policy.OnOOM != "stop" || policy.Validate() != nil {
		return nil
	}
	return &jobAdmissionRecoveryIdentity{UID: checkpoint.CurrentUID, Namespace: metadata.Namespace, Name: metadata.Name,
		TaskID: job.TaskID, ExecutionKey: *job.ExecutionKey, RunGeneration: job.RunGeneration, Deadline: checkpoint.Deadline, Attempt: checkpoint.Attempt, Kind: job.Type}
}
