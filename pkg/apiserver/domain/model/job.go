package model

import (
	"strconv"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

type JobInfo struct {
	ID            int     `json:"id" gorm:"primaryKey;column:id"`
	Type          string  `json:"type" gorm:"type:varchar(64);column:type"`
	WorkflowID    string  `json:"workflow_id" gorm:"type:varchar(64);column:workflow_id"`
	ProductID     string  `json:"product_Id" gorm:"type:varchar(64);column:product_id"`
	AppID         string  `json:"app_id" gorm:"type:varchar(64);column:app_id"`
	TaskID        string  `json:"task_id" gorm:"type:varchar(255);column:task_id"`
	Status        string  `json:"status" bson:"status" gorm:"type:varchar(32);column:status"`
	StartTime     int64   `json:"start_time" bson:"start_time" gorm:"column:start_time"`
	EndTime       int64   `json:"end_time" bson:"end_time" gorm:"column:end_time"`
	Info          string  `json:"service_type" gorm:"type:longtext;column:info"`
	InternalInfo  string  `json:"-" gorm:"type:longtext;column:internal_info"`
	ServiceName   string  `json:"service_name" gorm:"type:varchar(255);column:service_name"`
	Error         string  `json:"error" gorm:"type:text;column:error"`
	Production    bool    `json:"production" gorm:"column:production"`                  // 是否生产
	TargetEnv     string  `json:"target_env" gorm:"type:varchar(64);column:target_env"` //目标环境
	ExecutionKey  *string `json:"execution_key,omitempty" gorm:"type:varchar(255);column:execution_key;uniqueIndex:idx_job_execution_key"`
	RunGeneration uint64  `json:"run_generation,omitempty" gorm:"column:run_generation;not null;default:0"`
	Attempt       uint    `json:"attempt,omitempty" gorm:"column:attempt;not null;default:0"`
	BaseModel
}

// JobTask 是最小的执行单位
type JobTask struct {
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	WorkflowID      string `json:"workflow_id"`
	ProjectID       string `json:"project_id"`
	AppID           string `json:"app_id"`
	ResourceAppName string `json:"resource_app_name,omitempty"`
	TaskID          string
	JobInfo         interface{}
	JobType         string
	FailurePolicy   config.WorkflowFailurePolicy
	Status          config.Status
	StartTime       int64
	EndTime         int64
	Info            string
	InternalInfo    string
	Error           string
	Timeout         int64
	RetryCount      int //重试次数
	ExecutionKey    string
	RunGeneration   uint64
	// OwnerRunGeneration identifies the current WorkflowQueue lease owner.
	// It remains separate from RunGeneration because recovery may reuse a
	// committed JobInfo identity from an older generation.
	OwnerRunGeneration uint64        `json:"-"`
	OwnerStatus        config.Status `json:"-"`
	RunToken           string        `json:"-"`
	WorkerID           string        `json:"-"`
	Attempt            uint
}

func (j *JobInfo) PrimaryKey() string {
	return strconv.FormatInt(int64(j.ID), 10)
}

func (j *JobInfo) TableName() string {
	return tableNamePrefix + "job"
}

func (j *JobInfo) ShortTableName() string {
	return "job_info"
}

func (j *JobTask) ResourceAppNameOrID() string {
	if j == nil {
		return ""
	}
	if name := strings.TrimSpace(j.ResourceAppName); name != "" {
		return name
	}
	return j.AppID
}

func (j *JobInfo) Index() map[string]interface{} {
	index := make(map[string]interface{})
	if j.TaskID != "" {
		index["task_id"] = j.TaskID
	}
	if j.WorkflowID != "" {
		index["workflow_id"] = j.WorkflowID
	}
	return index
}

type JobDeployInfo struct {
	Name          string
	Ready         bool
	Replicas      int32 //期望副本数量
	ReadyReplicas int32 //就绪副本数量
}
