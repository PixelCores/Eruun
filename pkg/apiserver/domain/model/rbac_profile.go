package model

import "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"

// RBACProfile stores administrator-managed named configuration.
type RBACProfile struct {
	Name        string              `json:"name" gorm:"primaryKey;type:varchar(63);column:name"`
	Description string              `json:"description,omitempty" gorm:"type:text;column:description"`
	Rules       []spec.RBACRuleSpec `json:"rules" gorm:"serializer:json;column:rules"`
	BaseModel
}

func (p *RBACProfile) PrimaryKey() string { return p.Name }
func (p *RBACProfile) TableName() string  { return tableNamePrefix + "rbac_profiles" }
func (p *RBACProfile) Index() map[string]interface{} {
	index := make(map[string]interface{})
	if p.Name != "" {
		index["name"] = p.Name
	}
	return index
}
