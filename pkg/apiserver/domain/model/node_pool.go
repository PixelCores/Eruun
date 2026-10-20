package model

import corev1 "k8s.io/api/core/v1"

// NodePool stores administrator-managed named configuration.
type NodePool struct {
	Name         string              `json:"name" gorm:"primaryKey;type:varchar(63);column:name"`
	Description  string              `json:"description,omitempty" gorm:"type:text;column:description"`
	NodeSelector map[string]string   `json:"nodeSelector" gorm:"serializer:json;column:node_selector"`
	Tolerations  []corev1.Toleration `json:"tolerations,omitempty" gorm:"serializer:json;column:tolerations"`
	BaseModel
}

func (p *NodePool) PrimaryKey() string { return p.Name }

func (p *NodePool) TableName() string { return tableNamePrefix + "node_pools" }

func (p *NodePool) Index() map[string]interface{} {
	index := make(map[string]interface{})
	if p.Name != "" {
		index["name"] = p.Name
	}
	return index
}
