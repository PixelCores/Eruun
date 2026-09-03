package model

import (
	"encoding/json"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

type Applications struct {
	WorkspaceID      string                `json:"workspaceId" gorm:"type:varchar(36);not null;index"`
	ID               string                `json:"id" gorm:"primaryKey;type:varchar(64);column:id"`
	Name             string                `json:"name" gorm:"type:varchar(255);column:name"`          //应用名称
	Namespace        string                `json:"namespace" gorm:"type:varchar(64);column:namespace"` //命名空间，但是不对外暴露
	Version          string                `json:"version" gorm:"type:varchar(64);column:version"`     //版本，如果为空则默认为1.0.0
	Alias            string                `json:"alias" gorm:"type:varchar(128);column:alias"`        //别名
	Project          string                `json:"project" gorm:"type:varchar(128);column:project"`    //项目
	Description      string                `json:"description" gorm:"type:text;column:description"`    //详情
	Icon             string                `json:"icon" gorm:"type:varchar(255);column:icon"`          //图标
	TemplateEnabled  bool                  `json:"templateEnabled" gorm:"column:tmp_enable"`           // 是否允许作为模板被引用
	ManagementMode   config.ManagementMode `json:"managementMode" gorm:"type:varchar(16);column:management_mode;index"`
	AdoptionSnapshot *JSONStruct           `json:"-" gorm:"column:adoption_snapshot;type:longtext;serializer:json"`
	Callback         *JSONStruct           `json:"callback,omitempty" gorm:"serializer:json"`
	BaseModel
}

func NewApplications(id, name, namespace, version, alias, project, description, icon string, templateEnabled bool) *Applications {
	return &Applications{
		ID:              id,
		Name:            name,
		Namespace:       namespace,
		Version:         version,
		Alias:           alias,
		Project:         project,
		Description:     description,
		Icon:            icon,
		TemplateEnabled: templateEnabled,
		ManagementMode:  config.ManagementModeNative,
	}
}

func (a *Applications) PrimaryKey() string {
	return a.ID
}

func (a *Applications) TableName() string {
	return tableNamePrefix + "applications"
}

func (a *Applications) ShortTableName() string {
	return "app"
}

// Index return custom index
func (a *Applications) Index() map[string]interface{} {
	index := make(map[string]interface{})
	if a.WorkspaceID != "" {
		index["workspace_id"] = a.WorkspaceID
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
	if a.TemplateEnabled {
		index["tmp_enable"] = a.TemplateEnabled
	}
	// A zero-value query must not implicitly filter out observe/adopted rows.
	if mode, ok := config.NormalizeManagementMode(string(a.ManagementMode)); ok {
		index["management_mode"] = mode
	}
	return index
}

// EffectiveManagementMode keeps legacy and rolling-upgrade rows fail-closed
// when an older writer omitted the nullable management_mode column.
func (a *Applications) EffectiveManagementMode() config.ManagementMode {
	if a == nil {
		return config.ManagementModeNative
	}
	rawMode := strings.TrimSpace(string(a.ManagementMode))
	if rawMode != "" {
		if mode, ok := config.NormalizeManagementMode(rawMode); ok {
			return mode
		}
		return config.ManagementModeObserve
	}
	if strings.EqualFold(strings.TrimSpace(a.Project), "imported") &&
		strings.EqualFold(strings.TrimSpace(a.Version), "imported") {
		return config.ManagementModeObserve
	}
	return config.ManagementModeNative
}

// ApplicationComponent delivery database model 组件信息
type ApplicationComponent struct {
	ID                       int            `json:"id" gorm:"primaryKey;column:id"`
	AppID                    string         `json:"appId" gorm:"type:varchar(64);column:app_id"`
	Name                     string         `json:"name" gorm:"type:varchar(255);column:name"`
	Namespace                string         `json:"namespace" gorm:"type:varchar(64);column:namespace"`
	Image                    string         `json:"image" gorm:"type:varchar(255);column:image"`
	Replicas                 int32          `json:"replicas" gorm:"column:replicas"`
	ComponentType            config.JobType `json:"componentType" gorm:"type:varchar(32);column:component_type"`
	Properties               *JSONStruct    `json:"properties,omitempty" gorm:"serializer:json"`
	Traits                   *JSONStruct    `json:"traits" gorm:"serializer:json"`
	SourceWorkloadAPIVersion string         `json:"-" gorm:"type:varchar(64);column:source_workload_api_version"`
	SourceWorkloadKind       string         `json:"-" gorm:"type:varchar(32);column:source_workload_kind"`
	SourceWorkloadName       string         `json:"-" gorm:"type:varchar(253);column:source_workload_name"`
	SourceWorkloadUID        *string        `json:"-" gorm:"type:varchar(128);column:source_workload_uid;uniqueIndex:uidx_component_source_workload_uid"`
	SourcePodSelector        *JSONStruct    `json:"-" gorm:"column:source_pod_selector;type:longtext;serializer:json"`
	ResumeReplicas           *int32         `json:"-" gorm:"column:resume_replicas"`
	AdoptedSecretData        *JSONStruct    `json:"-" gorm:"column:adopted_secret_data;type:longtext;serializer:json"`
	// 运行时状态（由 Informer 同步）
	Status          string `json:"status" gorm:"type:varchar(32);column:status"`                 // Running/Pending/Failed/Unknown
	ReadyReplicas   int32  `json:"readyReplicas" gorm:"column:ready_replicas"`                   // 就绪副本数
	LastAbnormal    string `json:"lastAbnormal,omitempty" gorm:"type:text;column:last_abnormal"` // 最近一次异常信息
	ResourceAppName string `json:"-" gorm:"-"`                                                   // 运行期资源命名使用的应用名
	BaseModel
}

func (w *ApplicationComponent) HasSourceWorkload() bool {
	return w != nil &&
		strings.TrimSpace(w.SourceWorkloadAPIVersion) != "" &&
		strings.TrimSpace(w.SourceWorkloadKind) != "" &&
		strings.TrimSpace(w.SourceWorkloadName) != "" &&
		w.SourceWorkloadUID != nil &&
		strings.TrimSpace(*w.SourceWorkloadUID) != ""
}

func (w *ApplicationComponent) PrimaryKey() string {
	return w.Name
}

func (w *ApplicationComponent) TableName() string {
	return tableNamePrefix + "app_components"
}

func (w *ApplicationComponent) ShortTableName() string {
	return "app_component"
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
