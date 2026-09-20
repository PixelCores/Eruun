package model

import (
	"encoding/json"
	"time"
)

// JobArtifact describes an immutable archive. Source expiry removes its chunks,
// while dataset and database destination copies have independent lifetimes.
type JobArtifact struct {
	ID           string          `json:"id" gorm:"primaryKey;type:varchar(64);column:id"`
	WorkspaceID  string          `json:"workspaceId" gorm:"type:varchar(64);column:workspace_id;index"`
	TaskID       string          `json:"taskId,omitempty" gorm:"type:varchar(255);column:task_id;index"`
	ExecutionKey string          `json:"executionKey,omitempty" gorm:"type:varchar(255);column:execution_key;index;not null;default:''"`
	Kind         string          `json:"kind" gorm:"type:varchar(32);column:kind;index"`
	Name         string          `json:"name" gorm:"type:varchar(255);column:name"`
	Digest       string          `json:"digest" gorm:"type:varchar(64);column:digest"`
	Size         int64           `json:"size" gorm:"column:size"`
	Chunks       int             `json:"-" gorm:"column:chunks"`
	Manifest     json.RawMessage `json:"manifest" gorm:"type:longtext;column:manifest"`
	Summary      json.RawMessage `json:"summary,omitempty" gorm:"type:longtext;column:summary"`
	Reference    string          `json:"reference,omitempty" gorm:"type:text;column:reference"`
	ExpiresAt    *time.Time      `json:"expiresAt,omitempty" gorm:"column:expires_at;index"`
	Expired      bool            `json:"expired" gorm:"column:expired;not null;default:false"`
	BaseModel
}

func (a *JobArtifact) PrimaryKey() string     { return a.ID }
func (a *JobArtifact) TableName() string      { return tableNamePrefix + "job_artifact" }
func (a *JobArtifact) ShortTableName() string { return "job_artifact" }
func (a *JobArtifact) Index() map[string]interface{} {
	m := map[string]interface{}{}
	if a.ID != "" {
		m["id"] = a.ID
	}
	if a.WorkspaceID != "" {
		m["workspace_id"] = a.WorkspaceID
	}
	if a.TaskID != "" {
		m["task_id"] = a.TaskID
	}
	if a.ExecutionKey != "" {
		m["execution_key"] = a.ExecutionKey
	}
	if a.Kind != "" {
		m["kind"] = a.Kind
	}
	return m
}

// ArtifactChunk keeps each database write below common MySQL packet limits.
type ArtifactChunk struct {
	ID          string `json:"-" gorm:"primaryKey;type:varchar(80);column:id"`
	ArtifactID  string `json:"-" gorm:"type:varchar(64);column:artifact_id;index:artifact_chunk,unique"`
	WorkspaceID string `json:"-" gorm:"type:varchar(64);column:workspace_id;index"`
	Ordinal     int    `json:"-" gorm:"column:ordinal;index:artifact_chunk,unique"`
	Data        []byte `json:"-" gorm:"type:longblob;column:data"`
	BaseModel
}

func (a *ArtifactChunk) PrimaryKey() string     { return a.ID }
func (a *ArtifactChunk) TableName() string      { return tableNamePrefix + "artifact_chunk" }
func (a *ArtifactChunk) ShortTableName() string { return "artifact_chunk" }
func (a *ArtifactChunk) Index() map[string]interface{} {
	m := map[string]interface{}{}
	if a.ID != "" {
		m["id"] = a.ID
	}
	if a.ArtifactID != "" {
		m["artifact_id"] = a.ArtifactID
	}
	if a.WorkspaceID != "" {
		m["workspace_id"] = a.WorkspaceID
	}
	return m
}

// JobDelivery tracks one destination independently of evaluation execution.
type JobDelivery struct {
	ID           string     `json:"id" gorm:"primaryKey;type:varchar(64);column:id"`
	WorkspaceID  string     `json:"workspaceId" gorm:"type:varchar(64);column:workspace_id;index"`
	TaskID       string     `json:"taskId" gorm:"type:varchar(255);column:task_id;index"`
	ExecutionKey string     `json:"executionKey,omitempty" gorm:"type:varchar(255);column:execution_key;index;not null;default:''"`
	SourceID     string     `json:"sourceId" gorm:"type:varchar(64);column:source_id"`
	Target       string     `json:"target" gorm:"type:varchar(32);column:target"`
	Mode         string     `json:"mode" gorm:"type:varchar(32);column:mode"`
	State        string     `json:"state" gorm:"type:varchar(32);column:state;index"`
	Attempts     int        `json:"attempts" gorm:"column:attempts"`
	LastError    string     `json:"error,omitempty" gorm:"type:text;column:last_error"`
	Reference    string     `json:"reference,omitempty" gorm:"type:text;column:reference"`
	LeaseToken   string     `json:"-" gorm:"type:varchar(64);column:lease_token"`
	LeaseUntil   *time.Time `json:"-" gorm:"column:lease_until"`
	BaseModel
}

func (d *JobDelivery) PrimaryKey() string     { return d.ID }
func (d *JobDelivery) TableName() string      { return tableNamePrefix + "job_delivery" }
func (d *JobDelivery) ShortTableName() string { return "job_delivery" }
func (d *JobDelivery) Index() map[string]interface{} {
	m := map[string]interface{}{}
	if d.ID != "" {
		m["id"] = d.ID
	}
	if d.WorkspaceID != "" {
		m["workspace_id"] = d.WorkspaceID
	}
	if d.TaskID != "" {
		m["task_id"] = d.TaskID
	}
	if d.ExecutionKey != "" {
		m["execution_key"] = d.ExecutionKey
	}
	if d.State != "" {
		m["state"] = d.State
	}
	if d.Target != "" {
		m["target"] = d.Target
	}
	return m
}
