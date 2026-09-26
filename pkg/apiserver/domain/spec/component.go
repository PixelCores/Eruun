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
