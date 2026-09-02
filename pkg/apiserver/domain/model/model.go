package model

import (
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

var tableNamePrefix = "eruun_"

// Interface model interface
type Interface interface {
	TableName() string
	ShortTableName() string
}

// JSONStruct json struct, same with runtime.RawExtension
type JSONStruct map[string]interface{}

// NewJSONStruct new json struct from runtime.RawExtension
func NewJSONStruct(raw *runtime.RawExtension) (*JSONStruct, error) {
	if raw == nil || raw.Raw == nil {
		return nil, nil
	}
	var data JSONStruct
	err := json.Unmarshal(raw.Raw, &data)
	if err != nil {
		return nil, fmt.Errorf("parse raw data failure %w", err)
	}
	return &data, nil
}

// NewJSONStructByString new json struct from string
func NewJSONStructByString(source string) (*JSONStruct, error) {
	if source == "" {
		return nil, nil
	}
	var data JSONStruct
	err := json.Unmarshal([]byte(source), &data)
	if err != nil {
		return nil, fmt.Errorf("parse raw data failure %w", err)
	}
	return &data, nil
}

// NewJSONStructByStruct new json struct from struct object
func NewJSONStructByStruct(object interface{}) (*JSONStruct, error) {
	if object == nil {
		return nil, nil
	}
	var data JSONStruct
	out, err := yaml.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("marshal object data failure %w", err)
	}
	if err := yaml.Unmarshal(out, &data); err != nil {
		return nil, fmt.Errorf("unmarshal object data failure %w", err)
	}
	return &data, nil
}

// Bytes encodes the JSONStruct and returns any serialization error to the caller.
func (j *JSONStruct) Bytes() ([]byte, error) {
	b, err := json.Marshal(j)
	if err != nil {
		return nil, fmt.Errorf("marshal JSON struct: %w", err)
	}
	return b, nil
}

// Properties return the map
func (j *JSONStruct) Properties() map[string]interface{} {
	return *j
}

// RawExtension encodes the JSONStruct as a RawExtension.
func (j *JSONStruct) RawExtension() (*runtime.RawExtension, error) {
	yamlByte, err := yaml.Marshal(j)
	if err != nil {
		return nil, fmt.Errorf("marshal JSON struct as YAML: %w", err)
	}
	b, err := yaml.YAMLToJSON(yamlByte)
	if err != nil {
		return nil, fmt.Errorf("convert JSON struct YAML to JSON: %w", err)
	}
	if len(b) == 0 || string(b) == "null" {
		return nil, nil
	}
	return &runtime.RawExtension{Raw: b}, nil
}

// BaseModel common model
type BaseModel struct {
	CreateTime time.Time `json:"createTime" gorm:"column:create_time"`
	UpdateTime time.Time `json:"updateTime" gorm:"column:update_time"`
}

// SetCreateTime set create time
func (m *BaseModel) SetCreateTime(time time.Time) {
	m.CreateTime = time
}

// SetUpdateTime set update time
func (m *BaseModel) SetUpdateTime(time time.Time) {
	m.UpdateTime = time
}
