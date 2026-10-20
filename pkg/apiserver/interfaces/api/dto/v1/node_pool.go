package v1

import (
	"time"

	corev1 "k8s.io/api/core/v1"
)

type NodePool struct {
	Name         string              `json:"name"`
	Description  string              `json:"description,omitempty"`
	NodeSelector map[string]string   `json:"nodeSelector"`
	Tolerations  []corev1.Toleration `json:"tolerations,omitempty"`
	CreateTime   time.Time           `json:"createTime"`
	UpdateTime   time.Time           `json:"updateTime"`
}

type CreateNodePoolRequest struct {
	Name string `json:"name"`
	UpdateNodePoolRequest
}

type UpdateNodePoolRequest struct {
	Description  string              `json:"description,omitempty"`
	NodeSelector map[string]string   `json:"nodeSelector"`
	Tolerations  []corev1.Toleration `json:"tolerations,omitempty"`
}

type NodePoolOption struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type ListNodePoolsResponse struct {
	NodePools []*NodePool `json:"nodePools"`
}

type ListNodePoolOptionsResponse struct {
	NodePools []*NodePoolOption `json:"nodePools"`
}
