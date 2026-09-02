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

// APIAuthorizationRoute describes one route-level authorization rule.
type APIAuthorizationRoute struct {
	Method string   `json:"method"`
	Path   string   `json:"path"`
	Roles  []string `json:"roles"`
}

// APIAuthorizationPolicy describes route authorization configuration.
type APIAuthorizationPolicy struct {
	DefaultEffect string                  `json:"defaultEffect"`
	Routes        []APIAuthorizationRoute `json:"routes"`
}

// UpsertAPIAuthorizationRouteRequest upserts one route rule.
type UpsertAPIAuthorizationRouteRequest struct {
	Method string   `json:"method" validate:"required"`
	Path   string   `json:"path" validate:"required"`
	Roles  []string `json:"roles" validate:"required,min=1,dive,required"`
}

// UpdateAPIAuthorizationDefaultEffectRequest updates default effect.
type UpdateAPIAuthorizationDefaultEffectRequest struct {
	DefaultEffect string `json:"defaultEffect" validate:"required"`
}
