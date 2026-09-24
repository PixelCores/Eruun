package model

import (
	"encoding/json"
	"time"
)

// JobCheckpoint binds one immutable Runner material archive to the complete
// filesystem snapshot set. A ready point is usable only before ExpiresAt.
type JobCheckpoint struct {
	ID                       string          `json:"id" gorm:"primaryKey;type:varchar(64);column:id"`
	WorkspaceID              string          `json:"workspaceId" gorm:"type:varchar(64);column:workspace_id;index:idx_checkpoint_execution,priority:1"`
	TaskID                   string          `json:"taskId" gorm:"type:varchar(255);column:task_id;index"`
	JobID                    int             `json:"-" gorm:"column:job_id"`
	ExecutionKey             string          `json:"executionKey" gorm:"type:varchar(255);column:execution_key;index:idx_checkpoint_execution,priority:2"`
	RunnerUID                string          `json:"-" gorm:"type:varchar(64);column:runner_uid"`
	RunnerPodName            string          `json:"-" gorm:"type:varchar(253);column:runner_pod_name"`
	Namespace                string          `json:"-" gorm:"type:varchar(63);column:namespace"`
	State                    string          `json:"state" gorm:"type:varchar(16);column:state;index"`
	Reason                   string          `json:"reason,omitempty" gorm:"type:varchar(64);column:reason"`
	MaterialID               string          `json:"-" gorm:"type:varchar(64);column:material_id"`
	Manifest                 json.RawMessage `json:"-" gorm:"type:longtext;column:manifest"`
	Members                  json.RawMessage `json:"-" gorm:"type:longtext;column:members"`
	SourceDeadline           time.Time       `json:"-" gorm:"column:source_deadline;not null"`
	ExpiresAt                time.Time       `json:"expiresAt" gorm:"column:expires_at;not null;index"`
	ReferencedByExecutionKey string          `json:"-" gorm:"type:varchar(255);column:referenced_by_execution_key"`
	LeaseToken               string          `json:"-" gorm:"type:varchar(64);column:lease_token"`
	LeaseUntil               *time.Time      `json:"-" gorm:"column:lease_until"`
	ReconcileAt              time.Time       `json:"-" gorm:"column:reconcile_at;not null;index"`
	Cleaned                  bool            `json:"-" gorm:"column:cleaned;not null;default:false"`
	BaseModel
}

func (c *JobCheckpoint) PrimaryKey() string     { return c.ID }
func (c *JobCheckpoint) TableName() string      { return tableNamePrefix + "job_checkpoint" }
func (c *JobCheckpoint) ShortTableName() string { return "job_checkpoint" }
func (c *JobCheckpoint) Index() map[string]interface{} {
	index := map[string]interface{}{}
	for key, value := range map[string]string{"id": c.ID, "workspace_id": c.WorkspaceID, "task_id": c.TaskID, "execution_key": c.ExecutionKey, "state": c.State, "referenced_by_execution_key": c.ReferencedByExecutionKey} {
		if value != "" {
			index[key] = value
		}
	}
	return index
}
