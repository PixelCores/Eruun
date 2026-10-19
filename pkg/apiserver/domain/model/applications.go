package model

import (
	"encoding/json"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

type Applications struct {
	WorkspaceID string      `json:"workspaceId" gorm:"type:varchar(36);not null;index"`
	ID          string      `json:"id" gorm:"primaryKey;type:varchar(64);column:id"`
	Name        string      `json:"name" gorm:"type:varchar(255);column:name"`          //应用名称
	Namespace   string      `json:"namespace" gorm:"type:varchar(64);column:namespace"` //命名空间，但是不对外暴露
	Version     string      `json:"version" gorm:"type:varchar(64);column:version"`     //版本，如果为空则默认为1.0.0
	Alias       string      `json:"alias" gorm:"type:varchar(128);column:alias"`        //别名
	Project     string      `json:"project" gorm:"type:varchar(128);column:project"`    //项目
	Description string      `json:"description" gorm:"type:text;column:description"`    //详情
	Icon        string      `json:"icon" gorm:"type:varchar(255);column:icon"`          //图标
	Callback    *JSONStruct `json:"callback,omitempty" gorm:"serializer:json"`
	BaseModel
}

func NewApplications(id, name, namespace, version, alias, project, description, icon string) *Applications {
	return &Applications{
		ID:          id,
		Name:        name,
		Namespace:   namespace,
		Version:     version,
		Alias:       alias,
		Project:     project,
		Description: description,
		Icon:        icon,
	}
}

func (a *Applications) PrimaryKey() string {
	return a.ID
}

func (a *Applications) TableName() string {
	return tableNamePrefix + "applications"
}

// Index return custom index
func (a *Applications) Index() map[string]interface{} {
	index := make(map[string]interface{})
	if a.WorkspaceID != "" {
		index["workspaceid"] = a.WorkspaceID
	}
	if a.ID != "" {
		index["id"] = a.ID
	}
	if a.Name != "" {
		index["name"] = a.Name
	}
	if a.Version != "" {
		index["version"] = a.Version
	}
	if a.Project != "" {
		index["project"] = a.Project
	}
	return index
}

// ApplicationComponent delivery database model 组件信息
type ApplicationComponent struct {
	ID            int            `json:"id" gorm:"primaryKey;column:id"`
	AppID         string         `json:"appId" gorm:"type:varchar(64);column:app_id"`
	Name          string         `json:"name" gorm:"type:varchar(255);column:name"`
	Namespace     string         `json:"namespace" gorm:"type:varchar(64);column:namespace"`
	Image         string         `json:"image" gorm:"type:varchar(255);column:image"`
	Replicas      int32          `json:"replicas" gorm:"column:replicas"`
	ComponentType config.JobType `json:"componentType" gorm:"type:varchar(32);column:component_type"`
	Properties    *JSONStruct    `json:"properties,omitempty" gorm:"serializer:json"`
	Traits        *JSONStruct    `json:"traits" gorm:"serializer:json"`
	// 运行时状态（由 Informer 同步）
	Status          string `json:"status" gorm:"type:varchar(32);column:status"`                 // Running/Pending/Failed/Unknown
	ReadyReplicas   int32  `json:"readyReplicas" gorm:"column:ready_replicas"`                   // 就绪副本数
	LastAbnormal    string `json:"lastAbnormal,omitempty" gorm:"type:text;column:last_abnormal"` // 最近一次异常信息
	ResourceAppName string `json:"-" gorm:"-"`                                                   // 运行期资源命名使用的应用名
	BaseModel
}

// ComponentStatusUpdate carries an observation of component runtime state.
// Nil fields mean no observation; non-nil zero values are explicit observations.
type ComponentStatusUpdate struct {
	AppID         string                  // 应用 ID
	ComponentID   int                     // 组件 ID
	ComponentName string                  // 组件名称
	Status        *config.ComponentStatus // 运行状态
	ReadyReplicas *int32                  // 就绪副本数
	Replicas      *int32                  // 观察到的副本数
	LastAbnormal  *string                 // 最近一次异常信息（指向空字符串表示清空）
}

func (w *ApplicationComponent) PrimaryKey() string {
	return w.Name
}

func (w *ApplicationComponent) TableName() string {
	return tableNamePrefix + "app_components"
}

func (w *ApplicationComponent) Index() map[string]interface{} {
	index := make(map[string]interface{})
	if w.Name != "" {
		index["name"] = w.Name
	}
	if w.AppID != "" {
		index["app_id"] = w.AppID
	}
	return index
}

func (w *ApplicationComponent) ResourceAppNameOrID() string {
	if w == nil {
		return ""
	}
	if name := strings.TrimSpace(w.ResourceAppName); name != "" {
		return name
	}
	return w.AppID
}

func (w *ApplicationComponent) ResourceNameKey() string {
	if w == nil {
		return ""
	}
	if w.ShareEnabled() {
		return w.Name
	}
	return w.ResourceAppNameOrID()
}

// ShareStrategy returns the normalized share strategy and whether a share trait
// is present. Unknown strategies follow the conservative default behavior.
func (w *ApplicationComponent) ShareStrategy() (spec.ShareStrategy, bool) {
	if w == nil || w.Traits == nil {
		return "", false
	}
	traitBytes, err := json.Marshal(w.Traits)
	if err != nil {
		return "", false
	}
	raw := string(traitBytes)
	if strings.TrimSpace(raw) == "" || raw == "null" || raw == "{}" {
		return "", false
	}
	var traits spec.Traits
	if err := json.Unmarshal(traitBytes, &traits); err != nil || traits.Share == nil {
		return "", false
	}
	strategy, _ := spec.NormalizeShareStrategy(traits.Share.Strategy)
	return strategy, true
}

func (w *ApplicationComponent) ShareEnabled() bool {
	_, enabled := w.ShareStrategy()
	return enabled
}

type Properties = spec.Properties

type Ports = spec.Ports

// Traits 附加特性
type Traits = spec.Traits

// EnvFromSourceSpec corresponds to a single corev1.EnvFromSource.
type EnvFromSourceSpec = spec.EnvFromSourceSpec

// SimplifiedEnvSpec is the user-friendly, simplified way to define environment variables.
type SimplifiedEnvSpec = spec.SimplifiedEnvSpec

// ValueSource defines the source for an environment variable's value.
// Only one of its fields may be set.
// Static 可能根本不需要实现，因为Env就直接实现这种方式
type ValueSource = spec.ValueSource

// SecretSelectorSpec selects a key from a Secret.
type SecretSelectorSpec = spec.SecretSelectorSpec

// ConfigMapSelectorSpec selects a key from a ConfigMap.
type ConfigMapSelectorSpec = spec.ConfigMapSelectorSpec

// InitTrait 初始化容器的特征
type InitTrait = spec.InitTraitSpec

// StorageTrait 描述存储特征
type StorageTrait = spec.StorageTraitSpec

type SidecarSpec = spec.SidecarTraitsSpec

// ResourceSpec defines CPU/Memory/GPU resources for containers
type ResourceSpec = spec.ResourceTraitsSpec
