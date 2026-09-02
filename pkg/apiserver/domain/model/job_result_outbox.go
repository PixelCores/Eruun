package model

import "github.com/PixelCores/Eruun/pkg/apiserver/config"

type JobResultOutbox struct {
	ID             string                      `json:"id" gorm:"primaryKey;type:varchar(64);column:id"`
	TaskID         string                      `json:"task_id" gorm:"type:varchar(255);column:task_id"`
	ExecutionKey   string                      `json:"execution_key" gorm:"type:varchar(255);column:execution_key;index"`
	RunGeneration  uint64                      `json:"run_generation" gorm:"column:run_generation;not null;default:0"`
	JobType        string                      `json:"job_type" gorm:"type:varchar(64);column:job_type"`
	Namespace      string                      `json:"namespace" gorm:"type:varchar(255);column:namespace"`
	Name           string                      `json:"name" gorm:"type:varchar(255);column:name"`
	ServiceName    string                      `json:"service_name" gorm:"type:varchar(255);column:service_name"`
	TimeoutSeconds int64                       `json:"timeout_seconds" gorm:"column:timeout_seconds"`
	RunToken       string                      `json:"-" gorm:"type:varchar(64);column:run_token"`
	WorkerID       string                      `json:"worker_id" gorm:"type:varchar(255);column:worker_id"`
	State          config.JobResultOutboxState `json:"state" gorm:"type:varchar(32);column:state"`
	MessageID      string                      `json:"message_id" gorm:"type:varchar(255);column:message_id"`
	Attempts       int                         `json:"attempts" gorm:"column:attempts"`
	LastError      string                      `json:"last_error" gorm:"type:text;column:last_error"`
	BaseModel
}

func (o *JobResultOutbox) PrimaryKey() string {
	return o.ID
}

func (o *JobResultOutbox) TableName() string {
	return tableNamePrefix + "job_result_outbox"
}

func (o *JobResultOutbox) ShortTableName() string {
	return "job_result_outbox"
}

func (o *JobResultOutbox) Index() map[string]interface{} {
	index := make(map[string]interface{})
	if o.ID != "" {
		index["id"] = o.ID
	}
	if o.TaskID != "" {
		index["task_id"] = o.TaskID
	}
	if o.ExecutionKey != "" {
		index["execution_key"] = o.ExecutionKey
	}
	if o.State != "" {
		index["state"] = string(o.State)
	}
	return index
}
