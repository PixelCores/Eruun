package model

// JobDefinition stores a reusable Job specification. Runs copy Spec and Revision
// into WorkflowQueue so later updates or deletion cannot change queued work.
type JobDefinition struct {
	ID          string `json:"id" gorm:"primaryKey;type:varchar(24);column:id"`
	WorkspaceID string `json:"workspaceId" gorm:"type:varchar(36);column:workspace_id;not null;uniqueIndex:idx_job_definition_name,priority:1"`
	Name        string `json:"name" gorm:"type:varchar(128);column:name;not null;uniqueIndex:idx_job_definition_name,priority:2"`
	Revision    uint64 `json:"revision" gorm:"column:revision;not null"`
	Spec        string `json:"-" gorm:"type:longtext;column:spec;not null"`
	Creator     string `json:"creator" gorm:"type:varchar(128);column:creator"`
	BaseModel
}

func (d *JobDefinition) PrimaryKey() string { return d.ID }
func (d *JobDefinition) TableName() string  { return tableNamePrefix + "job_definition" }
func (d *JobDefinition) Index() map[string]interface{} {
	index := make(map[string]interface{})
	if d.ID != "" {
		index["id"] = d.ID
	}
	if d.WorkspaceID != "" {
		index["workspace_id"] = d.WorkspaceID
	}
	if d.Name != "" {
		index["name"] = d.Name
	}
	return index
}
