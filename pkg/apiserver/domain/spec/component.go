package spec

// JobType identifies a component or workflow job kind.
type JobType string

// Component is the canonical input specification shared by domain and API.
type Component struct {
	Name          string       `json:"name"`
	ComponentType JobType      `json:"type"`
	Image         string       `json:"image,omitempty"`
	Namespace     string       `json:"namespace"`
	Replicas      int32        `json:"replicas"`
	Properties    Properties   `json:"properties"`
	Traits        Traits       `json:"traits"`
	Template      *TemplateRef `json:"tmp,omitempty"`
}

type TemplateRef struct {
	ID                  string `json:"id"`
	Target              string `json:"target,omitempty"`              // 目标模板组件名，用于精确匹配
	DefaultStorageClass string `json:"defaultStorageClass,omitempty"` // 模板展开时写入空 persistent storageClass 的默认值
}

// ComponentUpdateSpec 组件更新规格
type ComponentUpdateSpec struct {
	// Action 操作类型：update（默认）、add、remove、restart
	Action string `json:"action,omitempty"`

	// Name 组件名称
	Name string `json:"name" validate:"required"`

	// 以下字段仅在 action 为 update 或 add 时有效

	// Image 新镜像地址（可选）
	Image string `json:"image,omitempty"`

	// Replicas 新副本数（可选，必须大于 0；/version 不支持 scale-to-zero）
	Replicas *int32 `json:"replicas,omitempty"`

	// Env 环境变量覆盖（可选，合并更新）
	Env map[string]string `json:"env,omitempty"`

	// 以下字段仅在 action 为 add 时需要

	// ComponentType 组件类型（新增时必填）
	ComponentType JobType `json:"type,omitempty"`

	// Properties 组件属性（新增时可选）
	Properties *Properties `json:"properties,omitempty"`

	// Traits 组件特性（新增时可选）
	Traits *Traits `json:"traits,omitempty"`
}
