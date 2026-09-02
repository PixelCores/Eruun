package model

import "github.com/PixelCores/Eruun/pkg/apiserver/config"

// Workflow application delivery database model
type Workflow struct {
	ID           string                  `json:"id" gorm:"primaryKey;type:varchar(64);column:id"`
	Name         string                  `json:"name" gorm:"type:varchar(255);column:name"`
	Namespace    string                  `json:"namespace" gorm:"type:varchar(64);column:namespace"`
	Alias        string                  `json:"alias" gorm:"type:varchar(128);column:alias"` //别名
	Disabled     bool                    `json:"disabled" gorm:"column:disabled"`             //是否关闭，创建时默认为false
	ProjectID    string                  `json:"project_id" gorm:"type:varchar(64);column:project_id"`
	AppID        string                  `json:"app_id" gorm:"type:varchar(64);column:app_id"`
	UserID       string                  `json:"user_id" gorm:"type:varchar(64);column:user_id"`
	Description  string                  `json:"description" gorm:"type:text;column:description"`
	WorkflowType config.WorkflowTaskType `json:"workflow_type" gorm:"type:varchar(32);column:workflow_type"` //工作流类型
	Status       config.Status           `json:"status" gorm:"type:varchar(32);column:status"`               //分为开启和关闭等状态
	Steps        *JSONStruct             `json:"steps,omitempty" gorm:"serializer:json"`
	Callback     *JSONStruct             `json:"callback,omitempty" gorm:"serializer:json"`
	BaseModel
}

type WorkflowSteps struct {
	FailurePolicy config.WorkflowFailurePolicy `json:"failurePolicy,omitempty"`
	Steps         []*WorkflowStep              `json:"steps"`
}

type WorkflowStep struct {
	Name         string                  `json:"name"`
	StepType     config.WorkflowStepType `json:"stepType,omitempty"`
	Level        int                     `json:"level,omitempty"`
	WorkflowType config.JobType          `json:"workflowType,omitempty"`
	Mode         config.WorkflowMode     `json:"mode,omitempty"`
	Approval     *WorkflowStepApproval   `json:"approval,omitempty"`
	Properties   []Policies              `json:"properties,omitempty"`
	SubSteps     []*WorkflowSubStep      `json:"subSteps,omitempty"`
}

type WorkflowStepApproval struct {
	NotifyURL      string            `json:"notifyUrl"`
	Message        string            `json:"message,omitempty"`
	Method         string            `json:"method,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	TimeoutSeconds int64             `json:"timeoutSeconds,omitempty"`
}

type WorkflowSubStep struct {
	Name         string         `json:"name"`
	WorkflowType config.JobType `json:"workflowType,omitempty"`
	Properties   []Policies     `json:"properties,omitempty"`
}

type WorkflowCallback struct {
	Success        string            `json:"success,omitempty"`
	Failure        string            `json:"failure,omitempty"`
	Timeout        string            `json:"timeout,omitempty"`
	Reject         string            `json:"reject,omitempty"`
	Cancelled      string            `json:"cancelled,omitempty"`
	Methods        map[string]string `json:"methods,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	TimeoutSeconds int64             `json:"timeoutSeconds,omitempty"`
}

// ComponentNames returns referenced component names for a workflow step.
func (w *WorkflowStep) ComponentNames() []string {
	if w == nil {
		return nil
	}
	if config.ParseWorkflowStepType(string(w.StepType)) == config.WorkflowStepTypeApproval {
		return nil
	}
	names := extractPolicyNames(w.Properties)
	if len(names) == 0 && w.Name != "" {
		names = append(names, w.Name)
	}
	return names
}

// ComponentNames returns referenced component names for a workflow sub-step.
func (w *WorkflowSubStep) ComponentNames() []string {
	if w == nil {
		return nil
	}
	names := extractPolicyNames(w.Properties)
	if len(names) == 0 && w.Name != "" {
		names = append(names, w.Name)
	}
	return names
}

func extractPolicyNames(policies []Policies) []string {
	if len(policies) == 0 {
		return nil
	}
	var names []string
	for _, policy := range policies {
		names = append(names, policy.Policies...)
	}
	return names
}

type Policies struct {
	Policies   []string `json:"policies"`
	Path       string   `json:"path,omitempty"`
	Container  string   `json:"container,omitempty"`
	InitSQLURL string   `json:"initSqlUrl,omitempty"`
}

func (w *Workflow) PrimaryKey() string {
	return w.ID
}

func (w *Workflow) TableName() string {
	return tableNamePrefix + "workflow"
}

func (w *Workflow) ShortTableName() string {
	return "workflow"
}

func (w *Workflow) Index() map[string]interface{} {
	index := make(map[string]interface{})
	if w.ID != "" {
		index["id"] = w.ID
	}
	if w.Name != "" {
		index["name"] = w.Name
	}
	if w.AppID != "" {
		index["app_id"] = w.AppID
	}
	return index
}
