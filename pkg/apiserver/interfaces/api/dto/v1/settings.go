package v1

import (
	"encoding/json"
	"time"
)

// SystemSetting represents a system setting item.
type SystemSetting struct {
	Type       string          `json:"type"`
	Value      json.RawMessage `json:"value"`
	CreateTime time.Time       `json:"createTime"`
	UpdateTime time.Time       `json:"updateTime"`
}

// CreateSystemSettingRequest creates a setting.
type CreateSystemSettingRequest struct {
	Type  string          `json:"type" validate:"required"`
	Value json.RawMessage `json:"value" validate:"required"`
}

// UpdateSystemSettingRequest updates a setting value.
type UpdateSystemSettingRequest struct {
	Value json.RawMessage `json:"value" validate:"required"`
}

// ListSystemSettingResponse lists settings.
type ListSystemSettingResponse struct {
	Settings []*SystemSetting `json:"settings"`
}
