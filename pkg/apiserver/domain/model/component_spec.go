package model

import (
	"encoding/json"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

// ComponentSpec decodes the persisted component without computing display fields.
func (c *ApplicationComponent) ComponentSpec() (spec.Component, error) {
	if c == nil {
		return spec.Component{}, nil
	}
	result := spec.Component{
		Name:          c.Name,
		ComponentType: c.ComponentType,
		Image:         c.Image,
		Namespace:     c.Namespace,
		Replicas:      c.Replicas,
	}
	if c.Properties != nil {
		data, err := c.Properties.Bytes()
		if err != nil {
			return spec.Component{}, fmt.Errorf("convert component %s properties: %w", c.Name, err)
		}
		if err := json.Unmarshal(data, &result.Properties); err != nil {
			return spec.Component{}, fmt.Errorf("convert component %s properties: %w", c.Name, err)
		}
	}
	if c.Traits != nil {
		data, err := c.Traits.Bytes()
		if err != nil {
			return spec.Component{}, fmt.Errorf("convert component %s traits: %w", c.Name, err)
		}
		if err := json.Unmarshal(data, &result.Traits); err != nil {
			return spec.Component{}, fmt.Errorf("convert component %s traits: %w", c.Name, err)
		}
	}
	return result, nil
}
