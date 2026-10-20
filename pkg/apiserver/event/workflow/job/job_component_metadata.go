package job

import (
	"encoding/json"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func ParseProperties(properties *model.JSONStruct) (model.Properties, error) {
	encoded, err := json.Marshal(properties)
	if err != nil {
		return model.Properties{}, fmt.Errorf("marshal component properties: %w", err)
	}
	var parsed model.Properties
	if err := json.Unmarshal(encoded, &parsed); err != nil {
		return model.Properties{}, fmt.Errorf("decode component properties: %w", err)
	}
	return parsed, nil
}

func BuildLabels(c *model.ApplicationComponent, p *model.Properties) map[string]string {
	labels := make(map[string]string)
	if p != nil {
		for k, v := range naming.NormalizeLabelValues(p.Labels) {
			labels[k] = v
		}
	}
	return ApplyComponentManagedLabels(labels, c)
}

func ApplyComponentManagedLabels(labels map[string]string, c *model.ApplicationComponent) map[string]string {
	if labels == nil {
		labels = make(map[string]string)
	}
	if c == nil {
		return labels
	}
	labels[config.LabelManagedBy] = config.ManagedByEruun
	labels[config.LabelComponentID] = fmt.Sprintf("%d", c.ID)
	labels[config.LabelAppID] = c.AppID
	labels[config.LabelComponentName] = naming.BoundedLabelValue(c.Name)
	return labels
}

func BuildAnnotations(c *model.ApplicationComponent) map[string]string {
	if c == nil {
		return nil
	}
	name := strings.TrimSpace(c.Name)
	if name == "" {
		return nil
	}
	return map[string]string{
		config.AnnotationComponentName: name,
	}
}

func ApplyComponentAnnotationsToObject(obj metav1.Object, component *model.ApplicationComponent) {
	annotations := BuildAnnotations(component)
	if len(annotations) == 0 || obj == nil {
		return
	}
	current := obj.GetAnnotations()
	if current == nil {
		current = make(map[string]string, len(annotations))
	}
	for k, v := range annotations {
		current[k] = v
	}
	obj.SetAnnotations(current)
}
