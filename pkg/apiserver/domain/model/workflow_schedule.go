package model

// WorkflowSchedule defines a cron-based workflow trigger for an application.
type WorkflowSchedule struct {
	ID         string `json:"id" gorm:"primaryKey;type:varchar(64);column:id"`
	AppID      string `json:"app_id" gorm:"type:varchar(64);column:app_id"`
	WorkflowID string `json:"workflow_id" gorm:"type:varchar(64);column:workflow_id"`
	Cron       string `json:"cron" gorm:"type:varchar(64);column:cron"`
	Enabled    bool   `json:"enabled" gorm:"column:enabled;index:idx_workflow_schedule_enabled_next_run,priority:1"`
	NextRun    int64  `json:"next_run" gorm:"type:bigint;column:next_run;index:idx_workflow_schedule_enabled_next_run,priority:2"`
	LastRun    int64  `json:"last_run" gorm:"type:bigint;column:last_run"`
	BaseModel
}

func (ws *WorkflowSchedule) PrimaryKey() string {
	return ws.ID
}

func (ws *WorkflowSchedule) TableName() string {
	return tableNamePrefix + "workflow_schedule"
}

func (ws *WorkflowSchedule) ShortTableName() string {
	return "workflow_schedule"
}

func (ws *WorkflowSchedule) Index() map[string]interface{} {
	index := make(map[string]interface{})
	if ws.ID != "" {
		index["id"] = ws.ID
	}
	if ws.AppID != "" {
		index["app_id"] = ws.AppID
	}
	if ws.WorkflowID != "" {
		index["workflow_id"] = ws.WorkflowID
	}
	if ws.Enabled {
		index["enabled"] = ws.Enabled
	}
	return index
}
