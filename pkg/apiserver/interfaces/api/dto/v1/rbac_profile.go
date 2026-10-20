package v1

import (
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

type RBACProfile struct {
	Name        string              `json:"name"`
	Description string              `json:"description,omitempty"`
	Rules       []spec.RBACRuleSpec `json:"rules"`
	CreateTime  time.Time           `json:"createTime"`
	UpdateTime  time.Time           `json:"updateTime"`
}

type CreateRBACProfileRequest struct {
	Name string `json:"name"`
	UpdateRBACProfileRequest
}

type UpdateRBACProfileRequest struct {
	Description string              `json:"description,omitempty"`
	Rules       []spec.RBACRuleSpec `json:"rules"`
}

type RBACProfileOption struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type ListRBACProfilesResponse struct {
	RBACProfiles []*RBACProfile `json:"rbacProfiles"`
}

type ListRBACProfileOptionsResponse struct {
	RBACProfiles []*RBACProfileOption `json:"rbacProfiles"`
}
