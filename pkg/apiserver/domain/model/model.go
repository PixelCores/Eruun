package model

import (
	"encoding/json"
	"fmt"
	"time"
)

var tableNamePrefix = "eruun_"

// Interface model interface
type Interface interface {
	TableName() string
}

// JSONStruct stores a JSON object in model fields.
type JSONStruct map[string]interface{}

// NewJSONStructByStruct new json struct from struct object
func NewJSONStructByStruct(object interface{}) (*JSONStruct, error) {
	if object == nil {
		return nil, nil
	}
	var data JSONStruct
	out, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("marshal object data failure %w", err)
	}
	if err := json.Unmarshal(out, &data); err != nil {
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
