package model

import "time"

// JobSandbox is an execution-bound creation intent and immutable resource identity.
// It contains no Runner capability, user environment, or task file contents.
type JobSandbox struct {
	ID               string     `json:"id" gorm:"primaryKey;type:varchar(64);column:id;index:idx_sandbox_reservation_scan,priority:2"`
	WorkspaceID      string     `json:"workspaceId" gorm:"type:varchar(64);column:workspace_id;index:idx_sandbox_execution,priority:1"`
	TaskID           string     `json:"taskId" gorm:"type:varchar(255);column:task_id;index"`
	JobID            int        `json:"-" gorm:"column:job_id"`
	ExecutionKey     string     `json:"executionKey" gorm:"type:varchar(255);column:execution_key;index:idx_sandbox_execution,priority:2"`
	TrialID          string     `json:"trialId" gorm:"type:varchar(128);column:trial_id"`
	RequestDigest    string     `json:"-" gorm:"type:varchar(64);column:request_digest"`
	Image            string     `json:"-" gorm:"type:varchar(1024);column:image"`
	StorageMiB       int64      `json:"-" gorm:"column:storage_mib"`
	Namespace        string     `json:"namespace" gorm:"type:varchar(63);column:namespace"`
	SandboxName      string     `json:"sandboxName" gorm:"type:varchar(63);column:sandbox_name"`
	SandboxUID       string     `json:"sandboxUID" gorm:"type:varchar(64);column:sandbox_uid"`
	AdmittedAt       *time.Time `json:"-" gorm:"column:admitted_at"`
	PodName          string     `json:"podName" gorm:"type:varchar(253);column:pod_name"`
	PodUID           string     `json:"podUID" gorm:"type:varchar(64);column:pod_uid"`
	RunnerPodName    string     `json:"-" gorm:"type:varchar(253);column:runner_pod_name"`
	RunnerUID        string     `json:"-" gorm:"type:varchar(64);column:runner_uid"`
	State            string     `json:"state" gorm:"type:varchar(16);column:state"`
	Reason           string     `json:"reason,omitempty" gorm:"type:varchar(40);column:reason"`
	Deadline         time.Time  `json:"-" gorm:"column:deadline;not null"`
	RetainUntil      *time.Time `json:"retainUntil,omitempty" gorm:"column:retain_until"`
	StartReserved    bool       `json:"-" gorm:"column:start_reserved;not null;default:false;index"`
	SlotReserved     bool       `json:"-" gorm:"column:slot_reserved;not null;default:false;index:idx_sandbox_execution,priority:3;index:idx_sandbox_reservation_scan,priority:1"`
	ReleaseRequested bool       `json:"-" gorm:"column:release_requested;not null;default:false"`
	CreateAttempts   int        `json:"-" gorm:"column:create_attempts;not null;default:0"`
	LeaseToken       string     `json:"-" gorm:"type:varchar(64);column:lease_token"`
	LeaseUntil       *time.Time `json:"-" gorm:"column:lease_until"`
	ReconcileAt      time.Time  `json:"-" gorm:"column:reconcile_at;not null;index"`
	BaseModel
}

func (s *JobSandbox) PrimaryKey() string     { return s.ID }
func (s *JobSandbox) TableName() string      { return tableNamePrefix + "job_sandbox" }
func (s *JobSandbox) ShortTableName() string { return "job_sandbox" }
func (s *JobSandbox) Index() map[string]interface{} {
	values := map[string]interface{}{}
	for key, value := range map[string]string{"id": s.ID, "workspace_id": s.WorkspaceID, "task_id": s.TaskID, "execution_key": s.ExecutionKey, "state": s.State} {
		if value != "" {
			values[key] = value
		}
	}
	if s.StartReserved {
		values["start_reserved"] = true
	}
	if s.SlotReserved {
		values["slot_reserved"] = true
	}
	return values
}
