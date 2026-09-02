package model

import "encoding/json"

const (
	SystemSettingTypeNodeSelector      = "nodeSelector"
	SystemSettingTypeRBACPolicies      = "rbacPolicies"
	SystemSettingTypeAPIAuth           = "apiAuth"
	SystemSettingTypeOAuthAuth         = "oauthAuth"
	SystemSettingTypeAliyunCloud       = "aliyunCloud"
	SystemSettingTypeURLSecurityPolicy = "urlSecurityPolicy"
	SystemSettingTypePodRestartMonitor = "podRestartMonitor"
)

// SystemSetting stores system-level settings in a single table.
// Value stores JSON object/array for different setting types.
type SystemSetting struct {
	Type  string          `json:"type" gorm:"primaryKey;type:varchar(64);column:type"`
	Value json.RawMessage `json:"value" gorm:"serializer:json;column:value"`
	BaseModel
}

func (s *SystemSetting) PrimaryKey() string {
	return s.Type
}

func (s *SystemSetting) TableName() string {
	return tableNamePrefix + "system_setting"
}

func (s *SystemSetting) ShortTableName() string {
	return "system_setting"
}

func (s *SystemSetting) Index() map[string]interface{} {
	index := make(map[string]interface{})
	if s.Type != "" {
		index["type"] = s.Type
	}
	return index
}
