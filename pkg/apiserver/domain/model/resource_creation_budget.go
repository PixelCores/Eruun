package model

import "time"

// ResourceCreationBudget persists the shared creation-rate debt independently
// of administrator-edited policy JSON. A policy update or replica restart must
// not refill the bucket and allow another burst.
type ResourceCreationBudget struct {
	ID          string    `json:"-" gorm:"primaryKey;type:varchar(32);column:id"`
	AvailableAt time.Time `json:"-" gorm:"precision:6;column:available_at;not null"`
	BaseModel
}

func (b *ResourceCreationBudget) PrimaryKey() string { return b.ID }
func (b *ResourceCreationBudget) TableName() string {
	return tableNamePrefix + "resource_creation_budget"
}
func (b *ResourceCreationBudget) ShortTableName() string        { return "resource_creation_budget" }
func (b *ResourceCreationBudget) Index() map[string]interface{} { return nil }
