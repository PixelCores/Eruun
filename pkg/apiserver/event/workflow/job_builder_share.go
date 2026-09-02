package workflow

import (
	"encoding/json"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

type shareConfig struct {
	Strategy config.ShareStrategy
	Name     string
}

func (s shareConfig) enabled() bool {
	return s.Strategy != ""
}

func (s shareConfig) ignore() bool {
	return s.Strategy == config.ShareStrategyIgnore
}

func shareConfigForComponent(component *model.ApplicationComponent) shareConfig {
	traits := decodeComponentTraits(component)
	if traits == nil || traits.Share == nil {
		return shareConfig{}
	}

	strategy, ok := config.NormalizeShareStrategy(traits.Share.Strategy)
	if !ok {
		klog.Warningf("unknown share strategy %q, falling back to default", traits.Share.Strategy)
	}
	shareName := shareNameForComponent(component)
	return shareConfig{
		Strategy: strategy,
		Name:     shareName,
	}
}

// rbacShareConfigForComponent gives RBAC resources a shared lifetime by default
// while preserving an explicitly configured component share strategy.
func rbacShareConfigForComponent(component *model.ApplicationComponent, share shareConfig) shareConfig {
	if share.enabled() {
		return share
	}
	return shareConfig{
		Strategy: config.ShareStrategyDefault,
		Name:     shareNameForComponent(component),
	}
}

func serviceTraitsForComponent(component *model.ApplicationComponent, properties *model.Properties) []spec.ServiceTraitSpec {
	traits := decodeComponentTraits(component)
	if traits == nil || len(traits.Service) == 0 {
		return defaultServiceTraitsFromProperties(component, properties)
	}
	return traits.Service
}

func defaultServiceTraitsFromProperties(component *model.ApplicationComponent, properties *model.Properties) []spec.ServiceTraitSpec {
	if component == nil || properties == nil || len(properties.Ports) == 0 {
		return nil
	}

	ports := make([]spec.ServicePortTraitSpec, 0, len(properties.Ports))
	for _, p := range properties.Ports {
		if p.Port <= 0 {
			continue
		}
		ports = append(ports, spec.ServicePortTraitSpec{
			Port:       p.Port,
			TargetPort: p.Port,
			Protocol:   "TCP",
		})
	}
	if len(ports) == 0 {
		return nil
	}

	return []spec.ServiceTraitSpec{
		{
			Name: "",
			Type: string(config.ServiceAccessInternal),
			Selector: map[string]string{
				config.LabelAppID:         component.AppID,
				config.LabelComponentName: naming.BoundedLabelValue(component.Name),
			},
			Ports: ports,
		},
	}
}

func decodeComponentTraits(component *model.ApplicationComponent) *spec.Traits {
	if component == nil || component.Traits == nil {
		return nil
	}

	traitBytes, err := json.Marshal(component.Traits)
	if err != nil {
		klog.Errorf("failed to marshal component traits: %v", err)
		return nil
	}

	if string(traitBytes) == "{}" || string(traitBytes) == "null" {
		return nil
	}

	var traits spec.Traits
	if err := json.Unmarshal(traitBytes, &traits); err != nil {
		klog.Errorf("failed to unmarshal component traits: %v", err)
		return nil
	}
	return &traits
}

func shareNameForComponent(component *model.ApplicationComponent) string {
	if component == nil {
		return ""
	}
	baseName := strings.TrimSpace(component.Namespace)
	if baseName != "" {
		baseName = naming.BoundedLabelValue(baseName)
		if baseName != "" {
			return baseName
		}
	}
	baseName = component.Name
	if baseName == "" {
		baseName = "shared"
	}
	kind := string(component.ComponentType)
	if kind != "" {
		baseName = fmt.Sprintf("%s-%s", baseName, kind)
	}
	baseName = naming.BoundedLabelValue(baseName)
	if baseName == "" {
		baseName = "shared"
	}
	return baseName
}

func applyShareLabels(labels map[string]string, share shareConfig) map[string]string {
	if !share.enabled() {
		return naming.NormalizeLabelValues(labels)
	}
	updated := make(map[string]string, len(labels)+2)
	for k, v := range naming.NormalizeLabelValues(labels) {
		updated[k] = v
	}
	updated[config.LabelShareName] = share.Name
	updated[config.LabelShareStrategy] = string(share.Strategy)
	return updated
}

func applyShareLabelsToObject(obj metav1.Object, share shareConfig) {
	if obj == nil {
		return
	}
	obj.SetLabels(applyShareLabels(obj.GetLabels(), share))
}

func applyShareLabelsToJobInfo(jobInfo interface{}, share shareConfig) {
	if !share.enabled() {
		return
	}
	switch info := jobInfo.(type) {
	case *appsv1.Deployment:
		applyShareLabelsToObject(info, share)
		info.Spec.Template.Labels = applyShareLabels(info.Spec.Template.Labels, share)
	case *appsv1.StatefulSet:
		applyShareLabelsToObject(info, share)
		info.Spec.Template.Labels = applyShareLabels(info.Spec.Template.Labels, share)
	case *batchv1.Job:
		applyShareLabelsToObject(info, share)
		info.Spec.Template.Labels = applyShareLabels(info.Spec.Template.Labels, share)
	case *batchv1.CronJob:
		applyShareLabelsToObject(info, share)
		info.Spec.JobTemplate.Labels = applyShareLabels(info.Spec.JobTemplate.Labels, share)
		info.Spec.JobTemplate.Spec.Template.Labels = applyShareLabels(info.Spec.JobTemplate.Spec.Template.Labels, share)
	case metav1.Object:
		applyShareLabelsToObject(info, share)
	case *applyv1.ServiceApplyConfiguration:
		info.Labels = applyShareLabels(info.Labels, share)
	case *model.ConfigMapInput:
		info.Labels = applyShareLabels(info.Labels, share)
	case *model.SecretInput:
		info.Labels = applyShareLabels(info.Labels, share)
	}
}
